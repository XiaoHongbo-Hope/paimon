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

package org.apache.paimon.spark.read;

import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorReadImpl;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.spark.broadcast.Broadcast;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/**
 * Spark-aware {@link VectorReadImpl} that distributes grouped vector index evaluation across the
 * Spark cluster instead of evaluating them with the local thread pool.
 */
public class SparkVectorReadImpl extends VectorReadImpl {

    private static final long serialVersionUID = 1L;

    public SparkVectorReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, vector, options);
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        splitSearchSplits(plan.splits(), indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        GlobalIndexer globalIndexer =
                !indexSplits.isEmpty() && !rawSplits.isEmpty()
                        ? createGlobalIndexer(indexSplits)
                        : null;
        ScoredGlobalIndexResult result =
                indexSplits.isEmpty()
                        ? ScoredGlobalIndexResult.createEmpty()
                        : readIndexSplitsInSpark(indexSplits, globalIndexer);
        return result.or(readRawSplitsInSpark(rawSplits, globalIndexer, rawPreFilter(rawSplits)))
                .topK(limit);
    }

    protected ScoredGlobalIndexResult readIndexSplitsInSpark(
            List<IndexVectorSearchSplit> splits, @Nullable GlobalIndexer globalIndexer) {
        if (splits.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        int parallelism = sparkParallelism();
        if (splits.size() < parallelism * 2) {
            return readIndexed(
                    splits, globalIndexer == null ? createGlobalIndexer(splits) : globalIndexer);
        }

        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        String indexType = splits.get(0).vectorIndexFiles().get(0).indexType();
        List<SparkVectorReads.SerializedSplit> serializedSplits = new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            IndexVectorSearchSplit split = splits.get(i);
            RoaringNavigableMap64 preFilter = preFilters.isEmpty() ? null : preFilters.get(i);
            serializedSplits.add(
                    new SparkVectorReads.SerializedSplit(
                            SparkVectorReads.serialize(
                                    split, "Failed to serialize VectorSearchSplit"),
                            preFilter == null
                                    ? null
                                    : SparkVectorReads.serialize(
                                            preFilter, "Failed to serialize vector pre-filter")));
        }
        List<List<SparkVectorReads.SerializedSplit>> splitGroups =
                SparkVectorReads.evenGroups(serializedSplits, parallelism);
        SerializableFunction<List<SparkVectorReads.SerializedSplit>, byte[]> task =
                group -> {
                    GlobalIndexer taskGlobalIndexer =
                            GlobalIndexerFactoryUtils.load(indexType)
                                    .create(vectorColumn, table.coreOptions().toConfiguration());
                    IndexPathFactory indexPathFactory =
                            table.store().pathFactory().globalIndexFileFactory();

                    ExecutorService executor =
                            GlobalIndexReadThreadPool.getExecutorService(
                                    Math.min(parallelism, group.size()));
                    List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                            new ArrayList<>(group.size());
                    for (SparkVectorReads.SerializedSplit serializedSplit : group) {
                        IndexVectorSearchSplit split =
                                SparkVectorReads.deserializeSplit(serializedSplit.split);
                        futures.add(
                                eval(
                                        taskGlobalIndexer,
                                        indexPathFactory,
                                        split.rowRangeStart(),
                                        split.rowRangeEnd(),
                                        split.vectorIndexFiles(),
                                        vector,
                                        SparkVectorReads.deserializePreFilter(
                                                serializedSplit.preFilter),
                                        executor));
                    }
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                    ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
                    for (CompletableFuture<Optional<ScoredGlobalIndexResult>> f : futures) {
                        Optional<ScoredGlobalIndexResult> next = f.join();
                        if (next.isPresent()) {
                            result = result.or(next.get());
                        }
                    }
                    result = result.topK(limit);
                    if (result.results().isEmpty()) {
                        return null;
                    }
                    try {
                        return new GlobalIndexResultSerializer().serialize(result);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to serialize ScoredGlobalIndexResult", e);
                    }
                };

        List<byte[]> remoteResults = mapInSpark(splitGroups, task, splitGroups.size());

        return mergeRemoteResults(remoteResults);
    }

    protected ScoredGlobalIndexResult readRawSplitsInSpark(
            List<RawVectorSearchSplit> splits,
            @Nullable GlobalIndexer globalIndexer,
            @Nullable RoaringNavigableMap64 preFilter) {
        List<Range> rawRowRanges = rawRowRanges(splits);
        if (rawRowRanges.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        int parallelism = sparkParallelism();
        if (SparkVectorReads.rawRowCount(rawRowRanges) < parallelism * 2L) {
            return readRawSearch(
                    rawRowRanges, preFilter, rawSearchIndexer(splits, globalIndexer), vector);
        }

        String metric = rawSearchMetric(rawSearchIndexer(splits, globalIndexer));
        // The pre-filter is identical for every range group, so broadcast it once instead of
        // shipping a copy with each group.
        Broadcast<RoaringNavigableMap64> preFilterBroadcast =
                preFilter == null ? null : createEngineContext().broadcast(preFilter);
        List<byte[]> serializedRanges = new ArrayList<>();
        for (List<Range> rangeGroup : SparkVectorReads.rangeGroups(rawRowRanges, parallelism)) {
            serializedRanges.add(
                    SparkVectorReads.serialize(
                            rangeGroup, "Failed to serialize raw vector row ranges"));
        }
        List<List<byte[]>> splitGroups = SparkVectorReads.evenGroups(serializedRanges, parallelism);

        SerializableFunction<List<byte[]>, byte[]> task =
                group -> {
                    RoaringNavigableMap64 groupPreFilter =
                            preFilterBroadcast == null ? null : preFilterBroadcast.value();
                    ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
                    for (byte[] rangeBytes : group) {
                        List<Range> rowRanges =
                                SparkVectorReads.deserialize(
                                        rangeBytes, "Failed to deserialize raw vector row ranges");
                        result =
                                result.or(
                                        readRawSearch(rowRanges, groupPreFilter, metric, vector));
                    }
                    result = result.topK(limit);
                    if (result.results().isEmpty()) {
                        return null;
                    }
                    try {
                        return new GlobalIndexResultSerializer().serialize(result);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to serialize ScoredGlobalIndexResult", e);
                    }
                };

        List<byte[]> remoteResults = mapInSpark(splitGroups, task, splitGroups.size());
        if (preFilterBroadcast != null) {
            preFilterBroadcast.unpersist();
        }
        return mergeRemoteResults(remoteResults);
    }

    protected int sparkParallelism() {
        return Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
    }

    protected SparkEngineContext createEngineContext() {
        return new SparkEngineContext();
    }

    protected <I, O> List<O> mapInSpark(
            List<I> data, SerializableFunction<I, O> func, int parallelism) {
        return createEngineContext().map(data, func, parallelism);
    }

    private ScoredGlobalIndexResult mergeRemoteResults(List<byte[]> remoteResults) {
        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (byte[] bytes : remoteResults) {
            if (bytes != null) {
                try {
                    result = result.or(serializer.deserialize(bytes));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to deserialize ScoredGlobalIndexResult", e);
                }
            }
        }
        return result.topK(limit);
    }
}
