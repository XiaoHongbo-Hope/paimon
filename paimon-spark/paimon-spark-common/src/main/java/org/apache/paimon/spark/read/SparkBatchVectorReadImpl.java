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
import org.apache.paimon.table.source.BatchVectorReadImpl;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

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
 * Spark-aware {@link BatchVectorReadImpl} that distributes the per-split vector index evaluation
 * across the Spark cluster instead of evaluating it with the local thread pool. Mirrors {@link
 * SparkVectorReadImpl} for the batch (multi-vector) path: each task evaluates a group of index
 * splits against all query vectors and returns one partial result per query, then the driver merges
 * per query and keeps the top-K.
 *
 * <p>Raw (un-indexed) splits are still searched locally per query (TODO: distribute as well).
 */
public class SparkBatchVectorReadImpl extends BatchVectorReadImpl {

    private static final long serialVersionUID = 1L;

    public SparkBatchVectorReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, vectors, options);
    }

    @Override
    public List<GlobalIndexResult> readBatch(VectorScan.Plan plan) {
        int n = vectors.length;
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        splitSearchSplits(plan.splits(), indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            List<GlobalIndexResult> empty = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                empty.add(GlobalIndexResult.createEmpty());
            }
            return empty;
        }

        GlobalIndexer globalIndexer =
                indexSplits.isEmpty() ? null : createGlobalIndexer(indexSplits);
        ScoredGlobalIndexResult[] indexedResults =
                indexSplits.isEmpty()
                        ? emptyScoredResults(n)
                        : readIndexedBatchInSpark(indexSplits, globalIndexer);

        List<GlobalIndexResult> results = new ArrayList<>(n);
        RoaringNavigableMap64 rawPreFilter = rawPreFilter(rawSplits);
        for (int i = 0; i < n; i++) {
            results.add(
                    withRawSearch(
                            indexedResults[i], rawSplits, globalIndexer, rawPreFilter, vectors[i]));
        }
        return results;
    }

    private ScoredGlobalIndexResult[] readIndexedBatchInSpark(
            List<IndexVectorSearchSplit> splits, GlobalIndexer globalIndexer) {
        int n = vectors.length;
        int parallelism = sparkParallelism();
        if (splits.size() < parallelism * 2) {
            return readIndexedBatch(
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
        int queryCount = n;

        SerializableFunction<List<SparkVectorReads.SerializedSplit>, byte[][]> task =
                group -> {
                    GlobalIndexer taskGlobalIndexer =
                            GlobalIndexerFactoryUtils.load(indexType)
                                    .create(vectorColumn, table.coreOptions().toConfiguration());
                    IndexPathFactory indexPathFactory =
                            table.store().pathFactory().globalIndexFileFactory();
                    ExecutorService executor =
                            GlobalIndexReadThreadPool.getExecutorService(
                                    Math.min(parallelism, group.size()));

                    List<CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>> futures =
                            new ArrayList<>(group.size());
                    for (SparkVectorReads.SerializedSplit serializedSplit : group) {
                        IndexVectorSearchSplit split =
                                SparkVectorReads.deserializeSplit(serializedSplit.split);
                        futures.add(
                                evalBatch(
                                        taskGlobalIndexer,
                                        indexPathFactory,
                                        split.rowRangeStart(),
                                        split.rowRangeEnd(),
                                        split.vectorIndexFiles(),
                                        vectors,
                                        SparkVectorReads.deserializePreFilter(
                                                serializedSplit.preFilter),
                                        executor));
                    }
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                    ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[queryCount];
                    for (int i = 0; i < queryCount; i++) {
                        merged[i] = ScoredGlobalIndexResult.createEmpty();
                    }
                    for (CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> f : futures) {
                        List<Optional<ScoredGlobalIndexResult>> splitResults = f.join();
                        for (int i = 0; i < queryCount; i++) {
                            if (splitResults.get(i).isPresent()) {
                                merged[i] = merged[i].or(splitResults.get(i).get());
                            }
                        }
                    }

                    GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
                    byte[][] out = new byte[queryCount][];
                    for (int i = 0; i < queryCount; i++) {
                        ScoredGlobalIndexResult r = merged[i].topK(limit);
                        if (r.results().isEmpty()) {
                            out[i] = null;
                        } else {
                            try {
                                out[i] = serializer.serialize(r);
                            } catch (IOException e) {
                                throw new RuntimeException(
                                        "Failed to serialize ScoredGlobalIndexResult", e);
                            }
                        }
                    }
                    return out;
                };

        List<byte[][]> remoteResults = mapInSpark(splitGroups, task, splitGroups.size());

        ScoredGlobalIndexResult[] result = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            result[i] = ScoredGlobalIndexResult.createEmpty();
        }
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (byte[][] groupOut : remoteResults) {
            if (groupOut == null) {
                continue;
            }
            for (int i = 0; i < n; i++) {
                if (groupOut[i] != null) {
                    try {
                        result[i] = result[i].or(serializer.deserialize(groupOut[i]));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to deserialize ScoredGlobalIndexResult", e);
                    }
                }
            }
        }
        for (int i = 0; i < n; i++) {
            result[i] = result[i].topK(limit);
        }
        return result;
    }

    protected int sparkParallelism() {
        return Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
    }

    protected <I, O> List<O> mapInSpark(
            List<I> data, SerializableFunction<I, O> func, int parallelism) {
        return new SparkEngineContext().map(data, func, parallelism);
    }
}
