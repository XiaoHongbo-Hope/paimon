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

package org.apache.paimon.table;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A table wrapper to hold batch vector search information. This is used to pass batch vector search
 * pushdown information from logical plan optimization to physical plan execution. For now, it is
 * only used by internal for Spark engine.
 *
 * <p>Unlike {@link VectorSearchTable}, a batch search carries multiple query vectors; result {@code
 * i} corresponds to query vector {@code i}.
 */
public class BatchVectorSearchTable implements ReadonlyTable {

    /**
     * Leading synthetic output column identifying which query vector a result row belongs to (the
     * 0-based position in the input {@code query_vectors}).
     */
    public static final String QUERY_INDEX_COLUMN = "query_index";

    private final InnerTable origin;
    private final BatchVectorSearch batchVectorSearch;

    private BatchVectorSearchTable(InnerTable origin, BatchVectorSearch batchVectorSearch) {
        this.origin = origin;
        this.batchVectorSearch = batchVectorSearch;
    }

    public static BatchVectorSearchTable create(
            InnerTable origin, BatchVectorSearch batchVectorSearch) {
        return new BatchVectorSearchTable(origin, batchVectorSearch);
    }

    public BatchVectorSearch batchVectorSearch() {
        return batchVectorSearch;
    }

    public InnerTable origin() {
        return origin;
    }

    @Override
    public String name() {
        return origin.name();
    }

    @Override
    public RowType rowType() {
        // Prepend a synthetic query_index column. It is not read from data files; the Spark scan
        // injects its value (the query vector position) per result group.
        RowType originType = origin.rowType();
        int maxId = originType.getFields().stream().mapToInt(DataField::id).max().orElse(0);
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(maxId + 1, QUERY_INDEX_COLUMN, DataTypes.INT().notNull()));
        fields.addAll(originType.getFields());
        return new RowType(fields);
    }

    @Override
    public List<String> primaryKeys() {
        return origin.primaryKeys();
    }

    @Override
    public List<String> partitionKeys() {
        return origin.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return origin.options();
    }

    @Override
    public FileIO fileIO() {
        return origin.fileIO();
    }

    @Override
    public InnerTableRead newRead() {
        return origin.newRead();
    }

    @Override
    public InnerTableScan newScan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new BatchVectorSearchTable(
                (InnerTable) origin.copy(dynamicOptions), batchVectorSearch);
    }
}
