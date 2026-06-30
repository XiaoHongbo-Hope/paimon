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

import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared helpers for the Spark-dispatched vector reads ({@link SparkVectorReadImpl} and {@link
 * SparkBatchVectorReadImpl}): split serialization and even grouping for {@code SparkEngineContext}
 * dispatch.
 */
final class SparkVectorReads {

    private SparkVectorReads() {}

    /** Splits {@code items} into at most {@code parallelism} contiguous, evenly sized groups. */
    static <T> List<List<T>> evenGroups(List<T> items, int parallelism) {
        List<List<T>> groups = new ArrayList<>(parallelism);
        int groupSize = (items.size() + parallelism - 1) / parallelism;
        for (int start = 0; start < items.size(); start += groupSize) {
            groups.add(new ArrayList<>(items.subList(start, Math.min(start + groupSize, items.size()))));
        }
        return groups;
    }

    /** Splits raw row ranges into at most {@code parallelism} groups of (roughly) equal row count. */
    static List<List<Range>> rangeGroups(List<Range> ranges, int parallelism) {
        long rowCount = rawRowCount(ranges);
        int groupCount = (int) Math.min(parallelism, rowCount);
        long targetRowsPerGroup = (rowCount - 1) / groupCount + 1;

        List<List<Range>> groups = new ArrayList<>(groupCount);
        List<Range> currentGroup = new ArrayList<>();
        long currentRows = 0;
        for (Range range : ranges) {
            long from = range.from;
            while (from <= range.to) {
                if (currentRows == targetRowsPerGroup) {
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentRows = 0;
                }
                long remainingGroupRows = targetRowsPerGroup - currentRows;
                long to = Math.min(range.to, from + remainingGroupRows - 1);
                Range next = new Range(from, to);
                currentGroup.add(next);
                currentRows += next.count();
                from = to + 1;
            }
        }
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    static long rawRowCount(List<Range> ranges) {
        long rowCount = 0;
        for (Range range : ranges) {
            long count = range.count();
            if (Long.MAX_VALUE - rowCount < count) {
                return Long.MAX_VALUE;
            }
            rowCount += count;
        }
        return rowCount;
    }

    static IndexVectorSearchSplit deserializeSplit(byte[] bytes) {
        return deserialize(bytes, "Failed to deserialize VectorSearchSplit");
    }

    @Nullable
    static RoaringNavigableMap64 deserializePreFilter(@Nullable byte[] bytes) {
        return bytes == null ? null : deserialize(bytes, "Failed to deserialize vector pre-filter");
    }

    static <T> T deserialize(byte[] bytes, String message) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(message, e);
        }
    }

    static byte[] serialize(Object value, String message) {
        try {
            return InstantiationUtil.serializeObject((Serializable) value);
        } catch (IOException e) {
            throw new RuntimeException(message, e);
        }
    }

    /** A search split (and its optional row-id pre-filter) serialized for Spark dispatch. */
    static final class SerializedSplit implements Serializable {

        private static final long serialVersionUID = 1L;

        final byte[] split;
        @Nullable final byte[] preFilter;

        SerializedSplit(byte[] split, @Nullable byte[] preFilter) {
            this.split = split;
            this.preFilter = preFilter;
        }
    }
}
