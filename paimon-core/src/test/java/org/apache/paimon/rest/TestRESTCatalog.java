/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.rest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.PagedList;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.view.View;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.rest.RESTCatalogServer.DEFAULT_MAX_RESULTS;

/** A catalog for testing RESTCatalog. */
public class TestRESTCatalog extends FileSystemCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCatalog.class);

    public Map<String, TableSchema> tableFullName2Schema = new HashMap<String, TableSchema>();
    public Map<String, List<Partition>> tableFullName2Partitions =
            new HashMap<String, List<Partition>>();
    public final Map<String, View> viewFullName2View = new HashMap<String, View>();

    public TestRESTCatalog(FileIO fileIO, Path warehouse, Options options) {
        super(fileIO, warehouse, options);
    }

    public static TestRESTCatalog create(CatalogContext context) {
        String warehouse = CatalogFactory.warehouse(context).toUri().toString();

        Path warehousePath = new Path(warehouse);
        FileIO fileIO;

        try {
            fileIO = FileIO.get(warehousePath, context);
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new TestRESTCatalog(fileIO, warehousePath, context.options());
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        getTable(identifier);
        tableFullName2Partitions.put(
                identifier.getFullName(),
                partitions.stream()
                        .map(partition -> spec2Partition(partition))
                        .collect(Collectors.toList()));
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        getTable(identifier);
        List<Partition> existPartitions = tableFullName2Partitions.get(identifier.getFullName());
        partitions.forEach(
                partition -> {
                    for (Map.Entry<String, String> entry : partition.entrySet()) {
                        existPartitions.stream()
                                .filter(
                                        p ->
                                                p.spec().containsKey(entry.getKey())
                                                        && p.spec()
                                                                .get(entry.getKey())
                                                                .equals(entry.getValue()))
                                .findFirst()
                                .ifPresent(
                                        existPartition -> existPartitions.remove(existPartition));
                    }
                });
    }

    @Override
    public void alterPartitions(Identifier identifier, List<Partition> partitions)
            throws TableNotExistException {
        getTable(identifier);
        List<Partition> existPartitions = tableFullName2Partitions.get(identifier.getFullName());
        partitions.forEach(
                partition -> {
                    for (Map.Entry<String, String> entry : partition.spec().entrySet()) {
                        existPartitions.stream()
                                .filter(
                                        p ->
                                                p.spec().containsKey(entry.getKey())
                                                        && p.spec()
                                                                .get(entry.getKey())
                                                                .equals(entry.getValue()))
                                .findFirst()
                                .ifPresent(
                                        existPartition -> existPartitions.remove(existPartition));
                    }
                });
        existPartitions.addAll(partitions);
        tableFullName2Partitions.put(identifier.getFullName(), existPartitions);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        getTable(identifier);
        return tableFullName2Partitions.get(identifier.getFullName());
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier, Integer maxResults, String pageToken)
            throws TableNotExistException {
        getTable(identifier);
        List<Partition> partitions = tableFullName2Partitions.get(identifier.getFullName());
        if (Objects.nonNull(partitions)) {
            List<Partition> sortedPartitions =
                    partitions.stream()
                            .sorted(Comparator.comparing(this::getPartitionSortKey))
                            .collect(Collectors.toList());
            if (Objects.isNull(maxResults) || maxResults <= 0) {
                maxResults = DEFAULT_MAX_RESULTS;
            }

            if ((maxResults > sortedPartitions.size()) && pageToken == null) {
                return new PagedList<>(sortedPartitions, null);
            } else {
                List<Partition> pagedPartitions = new ArrayList<>();
                for (Partition partition : sortedPartitions) {
                    if (pagedPartitions.size() < maxResults) {
                        if (pageToken == null) {
                            pagedPartitions.add(partition);
                        } else if (getPartitionSortKey(partition).compareTo(pageToken) > 0) {
                            pagedPartitions.add(partition);
                        }
                    } else {
                        break;
                    }
                }
                String nextPageToken = getNextPageTokenForPartitions(pagedPartitions, maxResults);
                return new PagedList<>(pagedPartitions, nextPageToken);
            }
        } else {
            return new PagedList<>(Collections.emptyList(), null);
        }
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        if (viewFullName2View.containsKey(identifier.getFullName())) {
            return viewFullName2View.get(identifier.getFullName());
        }
        throw new ViewNotExistException(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        if (viewFullName2View.containsKey(identifier.getFullName())) {
            viewFullName2View.remove(identifier.getFullName());
        }
        if (!ignoreIfNotExists) {
            throw new ViewNotExistException(identifier);
        }
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        getDatabase(identifier.getDatabaseName());
        if (viewFullName2View.containsKey(identifier.getFullName()) && !ignoreIfExists) {
            throw new ViewAlreadyExistException(identifier);
        }
        viewFullName2View.put(identifier.getFullName(), view);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        getDatabase(databaseName);
        return viewFullName2View.keySet().stream()
                .map(v -> Identifier.fromString(v))
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .map(identifier -> identifier.getTableName())
                .collect(Collectors.toList());
    }

    @Override
    public PagedList<String> listViewsPaged(
            String databaseName, Integer maxResults, String pageToken)
            throws DatabaseNotExistException {
        List<String> views = listViews(databaseName);
        if (Objects.nonNull(views)) {
            List<String> sortedViews =
                    views.stream().sorted(String::compareTo).collect(Collectors.toList());
            if (Objects.isNull(maxResults) || maxResults <= 0) {
                maxResults = DEFAULT_MAX_RESULTS;
            }

            if ((maxResults > sortedViews.size()) && pageToken == null) {
                return new PagedList<>(sortedViews, null);
            } else {
                List<String> pagedViews = new ArrayList<>();
                for (String view : sortedViews) {
                    if (pagedViews.size() < maxResults) {
                        if (pageToken == null) {
                            pagedViews.add(view);
                        } else if (view.compareTo(pageToken) > 0) {
                            pagedViews.add(view);
                        }
                    } else {
                        break;
                    }
                }
                String nextPageToken = getNextPageTokenForNames(pagedViews, maxResults);
                return new PagedList<>(pagedViews, nextPageToken);
            }
        } else {
            return new PagedList<>(Collections.emptyList(), null);
        }
    }

    @Override
    public PagedList<View> listViewDetailsPaged(
            String databaseName, Integer maxResults, String pageToken)
            throws DatabaseNotExistException {
        List<View> viewDetails =
                listViews(databaseName).stream()
                        .map(
                                tableName -> {
                                    try {
                                        return this.getView(
                                                Identifier.create(databaseName, tableName));
                                    } catch (ViewNotExistException e) {
                                        LOG.warn(
                                                "view {}.{} does not exist",
                                                databaseName,
                                                tableName);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        List<View> sortedViewDetails =
                viewDetails.stream()
                        .sorted(Comparator.comparing(View::name))
                        .collect(Collectors.toList());

        if (Objects.isNull(maxResults) || maxResults <= 0) {
            maxResults = DEFAULT_MAX_RESULTS;
        }
        if ((maxResults > sortedViewDetails.size()) && pageToken == null) {
            return new PagedList<>(sortedViewDetails, null);
        } else {
            List<View> pagedViewDetails = new ArrayList<>();
            for (View view : sortedViewDetails) {
                if (pagedViewDetails.size() < maxResults) {
                    if (pageToken == null) {
                        pagedViewDetails.add(view);
                        pageToken = view.name();
                    } else if (view.name().compareTo(pageToken) > 0) {
                        pagedViewDetails.add(view);
                    }
                } else {
                    break;
                }
            }
            String nextPageToken = getNextPageTokenForViews(pagedViewDetails, maxResults);
            return new PagedList<>(pagedViewDetails, nextPageToken);
        }
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        if (!viewFullName2View.containsKey(fromView.getFullName()) && !ignoreIfNotExists) {
            throw new ViewNotExistException(fromView);
        }
        if (viewFullName2View.containsKey(toView.getFullName())) {
            throw new ViewAlreadyExistException(toView);
        }
        if (viewFullName2View.containsKey(fromView.getFullName())) {
            View view = viewFullName2View.get(fromView.getFullName());
            viewFullName2View.remove(fromView.getFullName());
            viewFullName2View.put(toView.getFullName(), view);
        }
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        List<String> tables = super.listTablesImpl(databaseName);
        for (Map.Entry<String, TableSchema> entry : tableFullName2Schema.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())) {
                tables.add(identifier.getTableName());
            }
        }
        return tables;
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName, Integer maxResults, String pageToken)
            throws DatabaseNotExistException {
        List<String> tables = listTables(databaseName);
        if (Objects.nonNull(tables)) {
            List<String> sortedTables =
                    tables.stream().sorted(String::compareTo).collect(Collectors.toList());
            if (Objects.isNull(maxResults) || maxResults <= 0) {
                maxResults = DEFAULT_MAX_RESULTS;
            }
            if ((maxResults > sortedTables.size()) && pageToken == null) {
                return new PagedList<>(sortedTables, null);
            } else {
                List<String> pagedTables = new ArrayList<>();
                for (String sortedTable : sortedTables) {
                    if (pagedTables.size() < maxResults) {
                        if (pageToken == null) {
                            pagedTables.add(sortedTable);
                        } else if (sortedTable.compareTo(pageToken) > 0) {
                            pagedTables.add(sortedTable);
                        }
                    } else {
                        break;
                    }
                }
                String nextPageToken = getNextPageTokenForNames(pagedTables, maxResults);
                return new PagedList<>(pagedTables, nextPageToken);
            }
        } else {
            return new PagedList<>(Collections.emptyList(), null);
        }
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String databaseName, Integer maxResults, String pageToken)
            throws DatabaseNotExistException {
        List<Table> tableDetails =
                listTables(databaseName).stream()
                        .map(
                                tableName -> {
                                    try {
                                        return this.getTable(
                                                Identifier.create(databaseName, tableName));
                                    } catch (TableNotExistException ignored) {
                                        LOG.warn(
                                                "table {}.{} does not exist",
                                                databaseName,
                                                tableName);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        List<Table> sortedTableDetails =
                tableDetails.stream()
                        .sorted(Comparator.comparing(Table::name))
                        .collect(Collectors.toList());
        if (Objects.isNull(maxResults) || maxResults <= 0) {
            maxResults = DEFAULT_MAX_RESULTS;
        }

        if ((maxResults > sortedTableDetails.size()) && pageToken == null) {
            return new PagedList<>(sortedTableDetails, null);
        } else {
            List<Table> pagedTableDetails = new ArrayList<>();
            for (Table table : sortedTableDetails) {
                if (pagedTableDetails.size() < maxResults) {
                    if (pageToken == null) {
                        pagedTableDetails.add(table);
                    } else if (table.name().compareTo(pageToken) > 0) {
                        pagedTableDetails.add(table);
                    }
                } else {
                    break;
                }
            }
            String nextPageToken = getNextPageTokenForTables(pagedTableDetails, maxResults);
            return new PagedList<>(pagedTableDetails, nextPageToken);
        }
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        if (tableFullName2Schema.containsKey(identifier.getFullName())) {
            tableFullName2Schema.remove(identifier.getFullName());
        } else {
            super.dropTableImpl(identifier);
        }
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        if (tableFullName2Schema.containsKey(fromTable.getFullName())) {
            TableSchema tableSchema = tableFullName2Schema.get(fromTable.getFullName());
            tableFullName2Schema.remove(fromTable.getFullName());
            tableFullName2Schema.put(toTable.getFullName(), tableSchema);
        } else {
            super.renameTableImpl(fromTable, toTable);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        if (tableFullName2Schema.containsKey(identifier.getFullName())) {
            TableSchema schema = tableFullName2Schema.get(identifier.getFullName());
            Options options = Options.fromMap(schema.options());
            if (options.get(CoreOptions.TYPE) == TableType.FORMAT_TABLE) {
                throw new UnsupportedOperationException("Only data table support alter table.");
            }
        } else {
            super.alterTableImpl(identifier, changes);
        }
    }

    @Override
    public void createFormatTable(Identifier identifier, Schema schema) {
        Map<String, String> options = new HashMap<>(schema.options());
        options.put("path", "/tmp/format_table");
        TableSchema tableSchema =
                new TableSchema(
                        1L,
                        schema.fields(),
                        1,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment());
        tableFullName2Schema.put(identifier.getFullName(), tableSchema);
    }

    @Override
    protected TableMetadata loadTableMetadata(Identifier identifier) throws TableNotExistException {
        if (tableFullName2Schema.containsKey(identifier.getFullName())) {
            TableSchema tableSchema = tableFullName2Schema.get(identifier.getFullName());
            return new TableMetadata(tableSchema, false, "uuid");
        }
        return super.loadTableMetadata(identifier);
    }

    private Partition spec2Partition(Map<String, String> spec) {
        return new Partition(spec, 123, 456, 789, 123);
    }

    private String getNextPageTokenForNames(List<String> names, Integer maxResults) {
        if (names == null
                || names.isEmpty()
                || Objects.isNull(maxResults)
                || names.size() < maxResults) {
            return null;
        }
        // return the last name
        return names.get(names.size() - 1);
    }

    private String getNextPageTokenForTables(List<Table> entities, Integer maxResults) {
        if (entities == null
                || entities.isEmpty()
                || Objects.isNull(maxResults)
                || entities.size() < maxResults) {
            return null;
        }
        // return the last entity name
        return entities.get(entities.size() - 1).name();
    }

    private String getNextPageTokenForViews(List<View> entities, Integer maxResults) {
        if (entities == null
                || entities.isEmpty()
                || Objects.isNull(maxResults)
                || entities.size() < maxResults) {
            return null;
        }
        // return the last entity name
        return entities.get(entities.size() - 1).name();
    }

    private String getNextPageTokenForPartitions(List<Partition> entities, Integer maxResults) {
        if (entities == null
                || entities.isEmpty()
                || Objects.isNull(maxResults)
                || entities.size() < maxResults) {
            return null;
        }
        // return the last entity name
        return getPartitionSortKey(entities.get(entities.size() - 1));
    }

    private String getPartitionSortKey(Partition partition) {
        return partition.spec().toString().replace("{", "").replace("}", "");
    }
}
