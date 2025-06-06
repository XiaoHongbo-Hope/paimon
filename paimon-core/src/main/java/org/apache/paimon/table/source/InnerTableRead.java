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

package org.apache.paimon.table.source;

import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Inner {@link TableRead} contains filter and projection push down. */
public interface InnerTableRead extends TableRead {

    default InnerTableRead withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    InnerTableRead withFilter(Predicate predicate);

    /** Use {@link #withReadType(RowType)} instead. */
    @Deprecated
    default InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        throw new UnsupportedOperationException();
    }

    default InnerTableRead withReadType(RowType readType) {
        throw new UnsupportedOperationException();
    }

    default InnerTableRead forceKeepDelete() {
        return this;
    }

    @Override
    default TableRead executeFilter() {
        return this;
    }

    @Override
    default InnerTableRead withMetricRegistry(MetricRegistry registry) {
        return this;
    }
}
