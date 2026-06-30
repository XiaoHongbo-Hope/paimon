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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.globalindex.GlobalIndexResult
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{BatchVectorSearch, PredicateBuilder}
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.read.{BaseScan, BatchReadTagCleanupListener, PaimonSupportsRuntimeFiltering, SparkBatchVectorSearchBuilderImpl, SparkHybridSearchBuilderImpl, SparkVectorSearchBuilderImpl}
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.{BatchVectorSearchTable, DataTable, FileStoreTable, InnerTable}
import org.apache.paimon.table.source.{DataTableBatchScan, InnerTableScan, Split}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.{IntegerType, StructField}

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(table: InnerTable)
  extends BaseScan
  with PaimonSupportsRuntimeFiltering
  with SQLConfHelper {

  private lazy val paimonMetricsRegistry: SparkMetricRegistry = SparkMetricRegistry()

  // Set by PaimonScanBuilder.build() for the batch_vector_search TVF. Threaded as a
  // post-construction field (rather than a constructor param) so the per-version PaimonScan
  // case-class constructors do not need to change.
  private[spark] var pushedBatchVectorSearch: Option[BatchVectorSearch] = None

  protected def getInputSplits: Array[Split] = {
    if (pushedBatchVectorSearch.isDefined) {
      // Flattened union of all query results' splits (used for stats/metrics); the per-query
      // tagged partitions used for the actual read are produced in [[inputPartitions]].
      return batchVectorResults.flatMap(splitsOf).toArray
    }
    val scan = readBuilder
      .newScan()
      .withGlobalIndexResult(evalGlobalIndexSearch())
      .asInstanceOf[InnerTableScan]
      .withMetricRegistry(paimonMetricsRegistry)

    val plan = scan.plan()

    Option(scan.readProtectionTagName).foreach {
      name =>
        BatchReadTagCleanupListener
          .getOrCreate(SparkSession.active)
          .registerCleanup(name, table)
    }

    plan.splits().asScala.toArray
  }

  // ---- batch_vector_search ----

  /** Native batch search executed once; result i corresponds to query vector i. */
  private lazy val batchVectorResults: Seq[GlobalIndexResult] =
    evalBatchVectorSearch(pushedBatchVectorSearch.get)

  override protected def leadingSyntheticColumns: Seq[StructField] = {
    if (pushedBatchVectorSearch.isDefined) {
      Seq(StructField(BatchVectorSearchTable.QUERY_INDEX_COLUMN, IntegerType, nullable = false))
    } else {
      Seq.empty
    }
  }

  override protected def toBatchInputPartitions: Seq[PaimonInputPartition] = {
    if (pushedBatchVectorSearch.isEmpty) {
      return super.toBatchInputPartitions
    }
    batchVectorResults.zipWithIndex.flatMap {
      case (result, queryIndex) =>
        getInputPartitions(splitsOf(result).toArray)
          .map(p => PaimonQueryIndexedInputPartition(p.splits, queryIndex))
    }
  }

  private def splitsOf(result: GlobalIndexResult): Seq[Split] = {
    readBuilder
      .newScan()
      .withGlobalIndexResult(result)
      .asInstanceOf[InnerTableScan]
      .withMetricRegistry(paimonMetricsRegistry)
      .plan()
      .splits()
      .asScala
      .toSeq
  }

  private def evalBatchVectorSearch(bvs: BatchVectorSearch): Seq[GlobalIndexResult] = {
    val builder =
      if (CoreOptions.fromMap(table.options).vectorSearchDistributeEnabled()) {
        new SparkBatchVectorSearchBuilderImpl(table)
      } else {
        table.newBatchVectorSearchBuilder()
      }
    builder
      .withVectors(bvs.vectors())
      .withVectorColumn(bvs.fieldName())
      .withLimit(bvs.limit())
      .withOptions(bvs.options())
    if (pushedPartitionFilters.nonEmpty) {
      builder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      builder.withFilter(PredicateBuilder.and(pushedDataFilters.asJava))
    }
    builder.executeBatchLocal().asScala.toSeq
  }

  private def evalGlobalIndexSearch(): GlobalIndexResult = {
    val globalSearchCount =
      Seq(pushedVectorSearch, pushedHybridSearch, pushedFullTextSearch).count(_.isDefined)
    if (globalSearchCount > 1) {
      throw new UnsupportedOperationException(
        "Cannot push down vector search, hybrid search and full-text search simultaneously.")
    }
    if (pushedVectorSearch.isDefined) {
      return evalVectorSearch()
    }
    if (pushedHybridSearch.isDefined) {
      return evalHybridSearch()
    }
    if (pushedFullTextSearch.isDefined) {
      return evalFullTextSearch()
    }
    null
  }

  private def evalVectorSearch(): GlobalIndexResult = {
    val vectorSearch = pushedVectorSearch.get
    val vectorSearchBuilder =
      if (CoreOptions.fromMap(table.options).vectorSearchDistributeEnabled()) {
        new SparkVectorSearchBuilderImpl(table)
      } else {
        table.newVectorSearchBuilder()
      }
    val vectorBuilder = vectorSearchBuilder
      .withVector(vectorSearch.vector())
      .withVectorColumn(vectorSearch.fieldName())
      .withLimit(vectorSearch.limit())
      .withOptions(vectorSearch.options())
    if (pushedPartitionFilters.nonEmpty) {
      vectorBuilder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      vectorBuilder.withFilter(PredicateBuilder.and(pushedDataFilters.asJava))
    }
    vectorBuilder.newVectorRead().read(vectorBuilder.newVectorScan().scan())
  }

  private def evalHybridSearch(): GlobalIndexResult = {
    val hybridSearch = pushedHybridSearch.get
    val hybridSearchBuilder =
      if (CoreOptions.fromMap(table.options).vectorSearchDistributeEnabled()) {
        new SparkHybridSearchBuilderImpl(table)
      } else {
        table.newHybridSearchBuilder()
      }
    val builder = hybridSearchBuilder
      .withLimit(hybridSearch.limit())
      .withRanker(hybridSearch.ranker())
    hybridSearch.routes().asScala.foreach(route => builder.addRoute(route))
    if (pushedPartitionFilters.nonEmpty) {
      builder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      builder.withFilter(PredicateBuilder.and(pushedDataFilters.asJava))
    }
    builder.executeLocal()
  }

  private def evalFullTextSearch(): GlobalIndexResult = {
    val fullTextSearch = pushedFullTextSearch.get
    val ftBuilder = table
      .newFullTextSearchBuilder()
      .withQuery(fullTextSearch.query())
      .withLimit(fullTextSearch.limit())
    if (pushedPartitionFilters.nonEmpty) {
      ftBuilder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      throw new UnsupportedOperationException(
        "Full-text search does not support non-partition filters because full-text indexes " +
          "cannot apply row-id pre-filters before top-k ranking.")
    }
    ftBuilder.newFullTextRead().read(ftBuilder.newFullTextScan().scan())
  }

  override def toBatch: Batch = {
    ensureNoFullScan()
    super.toBatch
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    super.supportedCustomMetrics ++
      Array(
        PaimonPlanningDurationMetric(),
        PaimonScannedSnapshotIdMetric(),
        PaimonScannedManifestsMetric(),
        PaimonSkippedTableFilesMetric()
      )
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    paimonMetricsRegistry.buildSparkScanMetrics()
  }

  private def ensureNoFullScan(): Unit = {
    if (OptionUtils.readAllowFullScan()) {
      return
    }

    table match {
      case t: FileStoreTable if !t.partitionKeys().isEmpty =>
        val skippedFiles = paimonMetricsRegistry.buildSparkScanMetrics().collectFirst {
          case m: PaimonSkippedTableFilesTaskMetric => m.value
        }
        if (skippedFiles.contains(0)) {
          throw new RuntimeException("Full scan is not supported.")
        }
      case _ =>
    }
  }
}
