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

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.view.View;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.METASTORE_TAG_TO_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for REST Catalog. */
class RESTCatalogTest extends CatalogTestBase {

    private RESTCatalogServer restCatalogServer;
    private String initToken = "init_token";

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        restCatalogServer = new RESTCatalogServer(warehouse, initToken);
        restCatalogServer.start();
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        this.catalog = new RESTCatalog(CatalogContext.create(options));
    }

    @AfterEach
    public void tearDown() throws Exception {
        restCatalogServer.shutdown();
    }

    @Test
    void testAuthFail() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, "aaaaa");
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        assertThatThrownBy(() -> new RESTCatalog(CatalogContext.create(options)))
                .isInstanceOf(NotAuthorizedException.class);
    }

    @Test
    void testHeader() {
        RESTCatalog restCatalog = (RESTCatalog) catalog;
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("host", "/path", parameters, "method", "data");
        Map<String, String> headers = restCatalog.headers(restAuthParameter);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer init_token");
        assertEquals(headers.get("test-header"), "test-value");
    }

    @Test
    public void testListTables() throws Exception {
        super.testListTables();

        String databaseName = "tables_db";
        Options options = new Options(this.catalog.options());
        options.set(RESTCatalogOptions.REST_PAGE_MAX_RESULTS, 2);
        try (RESTCatalog restCatalog = new RESTCatalog(CatalogContext.create(options))) {

            restCatalog.createDatabase(databaseName, false);
            List<String> restTables = restCatalog.listTables(databaseName);
            assertThat(restTables).isEmpty();

            // List tables returns a list with the names of all tables in the database
            String[] tableNames = {"table4", "table5", "table1", "table2", "table3"};
            for (String tableName : tableNames) {
                restCatalog.createTable(
                        Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
            }

            restTables = restCatalog.listTables(databaseName);
            assertThat(restTables).containsExactlyInAnyOrder(tableNames);

            // List tables throws DatabaseNotExistException when the database does not exist
            assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                    .isThrownBy(() -> restCatalog.listTables("non_existing_db"));
        }

        options.set(RESTCatalogOptions.REST_PAGE_MAX_RESULTS.key(), "dummy");
        try (RESTCatalog dummyCatalog = new RESTCatalog(CatalogContext.create(options))) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> dummyCatalog.listTables(databaseName));
        }
    }

    @Test
    public void testListTablesPaged() throws Exception {
        // List tables paged returns an empty list when there are no tables in the database
        String databaseName = "tables_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<String> pagedTables = catalog.listTablesPaged(databaseName, null, null);
        assertThat(pagedTables.getPagedLists()).isEmpty();
        assertNull(pagedTables.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        // when maxResults is null or 0, the page length is set to a server configured value
        pagedTables = catalog.listTablesPaged(databaseName, null, null);
        List<String> tables = pagedTables.getPagedLists();
        assertThat(tables).containsExactlyInAnyOrder(tableNames);
        assertNull(pagedTables.getNextPageToken());

        // when maxResults is greater than 0, the page length is the minimum of this value and a
        // server configured value
        // when pageToken is null, will list tables from the beginning
        int maxResults = 2;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null);
        tables = pagedTables.getPagedLists();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("abd", "def");
        assertEquals("def", pagedTables.getNextPageToken());

        // when pageToken is not null, will list tables from the pageToken (exclusive)
        pagedTables =
                catalog.listTablesPaged(databaseName, maxResults, pagedTables.getNextPageToken());
        tables = pagedTables.getPagedLists();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("opr", "table1");
        assertEquals("table1", pagedTables.getNextPageToken());

        pagedTables =
                catalog.listTablesPaged(databaseName, maxResults, pagedTables.getNextPageToken());
        tables = pagedTables.getPagedLists();
        assertEquals(maxResults, tables.size());
        assertThat(tables).containsExactly("table2", "table3");
        assertEquals("table3", pagedTables.getNextPageToken());

        pagedTables =
                catalog.listTablesPaged(databaseName, maxResults, pagedTables.getNextPageToken());
        tables = pagedTables.getPagedLists();
        assertEquals(0, tables.size());
        assertNull(pagedTables.getNextPageToken());

        maxResults = 8;
        pagedTables = catalog.listTablesPaged(databaseName, maxResults, null);
        tables = pagedTables.getPagedLists();
        String[] expectedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        assertThat(tables).containsExactly(expectedTableNames);
        assertNull(pagedTables.getNextPageToken());

        pagedTables = catalog.listTablesPaged(databaseName, maxResults, "table1");
        tables = pagedTables.getPagedLists();
        assertEquals(2, tables.size());
        assertThat(tables).containsExactly("table2", "table3");
        assertNull(pagedTables.getNextPageToken());

        // List tables throws DatabaseNotExistException when the database does not exist
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(() -> catalog.listTables("non_existing_db"));
    }

    @Test
    public void testListTableDetailsPaged() throws Exception {
        // List table details returns an empty list when there are no tables in the database
        String databaseName = "table_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<Table> pagedTableDetails =
                catalog.listTableDetailsPaged(databaseName, null, null);
        assertThat(pagedTableDetails.getPagedLists()).isEmpty();
        assertNull(pagedTableDetails.getNextPageToken());

        String[] tableNames = {"table1", "table2", "table3", "abd", "def", "opr"};
        String[] expectedTableNames = Arrays.stream(tableNames).sorted().toArray(String[]::new);
        for (String tableName : tableNames) {
            catalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }

        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, null, null);
        assertPagedTableDetails(pagedTableDetails, tableNames.length, expectedTableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        int maxResults = 2;
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, null);
        assertPagedTableDetails(pagedTableDetails, maxResults, "abd", "def");
        assertEquals("def", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken());
        assertPagedTableDetails(pagedTableDetails, maxResults, "opr", "table1");
        assertEquals("table1", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken());
        assertPagedTableDetails(pagedTableDetails, maxResults, "table2", "table3");
        assertEquals("table3", pagedTableDetails.getNextPageToken());

        pagedTableDetails =
                catalog.listTableDetailsPaged(
                        databaseName, maxResults, pagedTableDetails.getNextPageToken());
        assertEquals(0, pagedTableDetails.getPagedLists().size());
        assertNull(pagedTableDetails.getNextPageToken());

        maxResults = 8;
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, null);
        assertPagedTableDetails(
                pagedTableDetails, Math.min(maxResults, tableNames.length), expectedTableNames);
        assertNull(pagedTableDetails.getNextPageToken());

        String pageToken = "table1";
        pagedTableDetails = catalog.listTableDetailsPaged(databaseName, maxResults, pageToken);
        assertPagedTableDetails(pagedTableDetails, 2, "table2", "table3");
        assertNull(pagedTableDetails.getNextPageToken());

        // List table details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listTableDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken));
    }

    @Test
    void testListViews() throws Exception {
        Options options = new Options(this.catalog.options());
        options.set(RESTCatalogOptions.REST_PAGE_MAX_RESULTS, 2);
        String databaseName;
        List<String> views;
        String[] viewNames;
        List<String> restViews;
        try (RESTCatalog restCatalog = new RESTCatalog(CatalogContext.create(options))) {

            // List tables returns an empty list when there are no tables in the database
            databaseName = "views_paged_db";
            restCatalog.createDatabase(databaseName, false);
            views = restCatalog.listViews(databaseName);
            assertThat(views).isEmpty();

            View view = buildView(databaseName);
            viewNames = new String[] {"view1", "view2", "view3", "abd", "def", "opr", "xyz"};

            for (String viewName : viewNames) {
                restCatalog.createView(Identifier.create(databaseName, viewName), view, false);
            }

            // when maxResults is null or 0, the page length is set to a server configured value
            views = restCatalog.listViews(databaseName);
            restViews = restCatalog.listViews(databaseName);
        }

        assertThat(views).containsExactlyInAnyOrder(viewNames);
        assertThat(restViews).containsExactlyInAnyOrder(viewNames);

        options.set(RESTCatalogOptions.REST_PAGE_MAX_RESULTS.key(), "dummy");
        try (RESTCatalog dummyCatalog = new RESTCatalog(CatalogContext.create(options))) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> dummyCatalog.listTables(databaseName));
        }
    }

    @Test
    public void testListViewsPaged() throws Exception {
        if (!supportsView()) {
            return;
        }

        // List views returns an empty list when there are no views in the database
        String databaseName = "views_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<String> pagedViews = catalog.listViewsPaged(databaseName, null, null);
        assertThat(pagedViews.getPagedLists()).isEmpty();
        assertNull(pagedViews.getNextPageToken());

        // List views paged returns a list with the names of all views in the database in all
        // catalogs except RestCatalog
        // even if the maxResults or pageToken is not null
        View view = buildView(databaseName);
        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        String[] sortedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViews = catalog.listViewsPaged(databaseName, null, null);
        assertThat(pagedViews.getPagedLists()).containsExactlyInAnyOrder(viewNames);
        assertNull(pagedViews.getNextPageToken());

        int maxResults = 2;
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null);
        assertPagedViews(pagedViews, "abd", "def");
        assertEquals("def", pagedViews.getNextPageToken());

        pagedViews =
                catalog.listViewsPaged(databaseName, maxResults, pagedViews.getNextPageToken());
        assertPagedViews(pagedViews, "opr", "view1");
        assertEquals("view1", pagedViews.getNextPageToken());

        pagedViews =
                catalog.listViewsPaged(databaseName, maxResults, pagedViews.getNextPageToken());
        assertPagedViews(pagedViews, "view2", "view3");
        assertEquals("view3", pagedViews.getNextPageToken());

        maxResults = 8;
        String[] expectedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, null);
        assertPagedViews(pagedViews, expectedViewNames);
        assertNull(pagedViews.getNextPageToken());

        String pageToken = "view1";
        pagedViews = catalog.listViewsPaged(databaseName, maxResults, pageToken);
        assertPagedViews(pagedViews, "view2", "view3");
        assertNull(pagedViews.getNextPageToken());

        // List views throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = 9;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewsPaged(
                                        "non_existing_db", finalMaxResults, pageToken));
    }

    @Test
    public void testListViewDetailsPaged() throws Exception {
        // List view details returns an empty list when there are no views in the database
        String databaseName = "view_details_paged_db";
        catalog.createDatabase(databaseName, false);
        PagedList<View> pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null);
        assertThat(pagedViewDetails.getPagedLists()).isEmpty();
        assertNull(pagedViewDetails.getNextPageToken());

        String[] viewNames = {"view1", "view2", "view3", "abd", "def", "opr"};
        View view = buildView(databaseName);
        for (String viewName : viewNames) {
            catalog.createView(Identifier.create(databaseName, viewName), view, false);
        }

        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, null, null);
        assertPagedViewDetails(pagedViewDetails, view, viewNames.length, viewNames);
        assertNull(pagedViewDetails.getNextPageToken());

        int maxResults = 2;
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, null);
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "abd", "def");
        assertEquals("def", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken());
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "opr", "view1");
        assertEquals("view1", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken());
        assertPagedViewDetails(pagedViewDetails, view, maxResults, "view2", "view3");
        assertEquals("view3", pagedViewDetails.getNextPageToken());

        pagedViewDetails =
                catalog.listViewDetailsPaged(
                        databaseName, maxResults, pagedViewDetails.getNextPageToken());
        assertEquals(0, pagedViewDetails.getPagedLists().size());
        assertNull(pagedViewDetails.getNextPageToken());

        maxResults = 8;
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, null);
        String[] expectedViewNames = Arrays.stream(viewNames).sorted().toArray(String[]::new);
        assertPagedViewDetails(
                pagedViewDetails,
                view,
                Math.min(maxResults, expectedViewNames.length),
                expectedViewNames);
        assertNull(pagedViewDetails.getNextPageToken());

        String pageToken = "view1";
        pagedViewDetails = catalog.listViewDetailsPaged(databaseName, maxResults, pageToken);
        assertPagedViewDetails(pagedViewDetails, view, 2, "view2", "view3");
        assertNull(pagedViewDetails.getNextPageToken());

        // List view details throws DatabaseNotExistException when the database does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.DatabaseNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listViewDetailsPaged(
                                        "non_existing_db", finalMaxResults, pageToken));
    }

    @Test
    void testListPartitionsWhenMetastorePartitionedIsTrue() throws Exception {
        Identifier identifier = Identifier.create("test_db", "test_table");
        createTable(
                identifier,
                ImmutableMap.of(METASTORE_PARTITIONED_TABLE.key(), "" + true),
                Lists.newArrayList("col1"));
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
    }

    @Test
    void testListPartitionsFromFile() throws Exception {
        Identifier identifier = Identifier.create("test_db", "test_table");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
    }

    @Test
    void testListPartitions() throws Exception {
        Options options = new Options(this.catalog.options());
        options.set(RESTCatalogOptions.REST_PAGE_MAX_RESULTS, 2);
        Identifier identifier;
        try (RESTCatalog restCatalog = new RESTCatalog(CatalogContext.create(options))) {

            String databaseName = "partitions_db";
            identifier = Identifier.create(databaseName, "table");
            Schema schema =
                    Schema.newBuilder()
                            .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                            .option(METASTORE_TAG_TO_PARTITION.key(), "dt")
                            .column("col", DataTypes.INT())
                            .column("dt", DataTypes.STRING())
                            .partitionKeys("dt")
                            .build();
            List<Map<String, String>> partitionSpecs =
                    Arrays.asList(
                            Collections.singletonMap("dt", "20250101"),
                            Collections.singletonMap("dt", "20250102"),
                            Collections.singletonMap("dt", "20240102"),
                            Collections.singletonMap("dt", "20260101"),
                            Collections.singletonMap("dt", "20250104"),
                            Collections.singletonMap("dt", "20250103"));

            restCatalog.createDatabase(databaseName, true);
            restCatalog.createTable(identifier, schema, true);
            restCatalog.createPartitions(identifier, partitionSpecs);

            List<Partition> restPartitions = restCatalog.listPartitions(identifier);
            assertThat(restPartitions.stream().map(Partition::spec))
                    .containsExactlyInAnyOrder(partitionSpecs.toArray(new Map[] {}));

            assertThatExceptionOfType(Catalog.TableNotExistException.class)
                    .isThrownBy(
                            () ->
                                    restCatalog.listPartitions(
                                            Identifier.create(databaseName, "non_existing_table")));
        }

        options.set(RESTCatalogOptions.REST_PAGE_MAX_RESULTS.key(), "dummy");
        try (RESTCatalog dummyCatalog = new RESTCatalog(CatalogContext.create(options))) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> dummyCatalog.listPartitions(identifier));
        }
    }

    @Test
    public void testListPartitionsPaged() throws Exception {
        String databaseName = "partitions_paged_db";
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        Collections.singletonMap("dt", "20250101"),
                        Collections.singletonMap("dt", "20250102"),
                        Collections.singletonMap("dt", "20240102"),
                        Collections.singletonMap("dt", "20260101"),
                        Collections.singletonMap("dt", "20250104"),
                        Collections.singletonMap("dt", "20250103"));
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, "table");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .option(METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(METASTORE_TAG_TO_PARTITION.key(), "dt")
                        .column("col", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build(),
                true);

        catalog.createPartitions(identifier, partitionSpecs);
        PagedList<Partition> pagedPartitions = catalog.listPartitionsPaged(identifier, null, null);
        assertPagedPartitions(
                pagedPartitions, partitionSpecs.size(), partitionSpecs.toArray(new Map[0]));

        int maxResults = 2;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null);
        assertPagedPartitions(
                pagedPartitions, maxResults, partitionSpecs.get(2), partitionSpecs.get(0));
        assertEquals("dt=20250101", pagedPartitions.getNextPageToken());

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken());
        assertPagedPartitions(
                pagedPartitions, maxResults, partitionSpecs.get(1), partitionSpecs.get(5));
        assertEquals("dt=20250103", pagedPartitions.getNextPageToken());

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken());
        assertPagedPartitions(
                pagedPartitions, maxResults, partitionSpecs.get(4), partitionSpecs.get(3));
        assertEquals("dt=20260101", pagedPartitions.getNextPageToken());

        pagedPartitions =
                catalog.listPartitionsPaged(
                        identifier, maxResults, pagedPartitions.getNextPageToken());
        assertThat(pagedPartitions.getPagedLists()).isEmpty();
        assertNull(pagedPartitions.getNextPageToken());

        maxResults = 8;
        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, null);
        Map[] sortedSpecs =
                partitionSpecs.stream()
                        .sorted(Comparator.comparing(i -> i.get("dt")))
                        .toArray(Map[]::new);
        assertPagedPartitions(
                pagedPartitions, Math.min(maxResults, partitionSpecs.size()), sortedSpecs);
        assertNull(pagedPartitions.getNextPageToken());

        pagedPartitions = catalog.listPartitionsPaged(identifier, maxResults, "dt=20250101");
        assertPagedPartitions(
                pagedPartitions,
                4,
                partitionSpecs.get(1),
                partitionSpecs.get(5),
                partitionSpecs.get(4),
                partitionSpecs.get(3));
        assertNull(pagedPartitions.getNextPageToken());

        // List partitions paged throws TableNotExistException when the table does not exist
        final int finalMaxResults = maxResults;
        assertThatExceptionOfType(Catalog.TableNotExistException.class)
                .isThrownBy(
                        () ->
                                catalog.listPartitionsPaged(
                                        Identifier.create(databaseName, "non_existing_table"),
                                        finalMaxResults,
                                        "dt=20250101"));
    }

    @Test
    void testRefreshFileIO() throws Exception {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.DATA_TOKEN_ENABLED, true);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        this.catalog = new RESTCatalog(CatalogContext.create(options));
        List<Identifier> identifiers =
                Lists.newArrayList(
                        Identifier.create("test_db_a", "test_table_a"),
                        Identifier.create("test_db_b", "test_table_b"),
                        Identifier.create("test_db_c", "test_table_c"));
        for (Identifier identifier : identifiers) {
            createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
            FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            assertEquals(true, fileStoreTable.fileIO().exists(fileStoreTable.location()));
        }
    }

    @Test
    void testSnapshotFromREST() throws Catalog.TableNotExistException {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        RESTCatalog catalog = new RESTCatalog(CatalogContext.create(options));

        Optional<Snapshot> snapshot =
                catalog.loadSnapshot(Identifier.create("test_db_a", "my_snapshot_table"));
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().id()).isEqualTo(10086);
        assertThat(snapshot.get().timeMillis()).isEqualTo(100);

        snapshot = catalog.loadSnapshot(Identifier.create("test_db_a", "unknown"));
        assertThat(snapshot).isEmpty();
    }

    @Override
    protected boolean supportsFormatTable() {
        return true;
    }

    @Override
    protected boolean supportPartitions() {
        return true;
    }

    @Override
    protected boolean supportsView() {
        return true;
    }

    @Override
    protected boolean supportPagedList() {
        return true;
    }

    private void createTable(
            Identifier identifier, Map<String, String> options, List<String> partitionKeys)
            throws Exception {
        catalog.createDatabase(identifier.getDatabaseName(), false);
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.INT())),
                        partitionKeys,
                        Collections.emptyList(),
                        options,
                        ""),
                true);
    }

    // TODO implement this
    @Override
    @Test
    public void testTableUUID() {}
}
