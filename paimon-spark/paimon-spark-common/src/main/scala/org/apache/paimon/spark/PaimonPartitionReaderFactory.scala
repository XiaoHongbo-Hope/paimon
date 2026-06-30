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

import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.source.ReadBuilder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow}
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.util.Objects

case class PaimonPartitionReaderFactory(
    readBuilder: ReadBuilder,
    metadataColumns: Seq[PaimonMetadataColumn] = Seq.empty,
    blobAsDescriptor: Boolean)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case qip: PaimonQueryIndexedInputPartition =>
        val base = PaimonPartitionReader(readBuilder, qip, metadataColumns, blobAsDescriptor)
        new QueryIndexPrependPartitionReader(base, qip.queryIndex)
      case paimonInputPartition: PaimonInputPartition =>
        PaimonPartitionReader(readBuilder, paimonInputPartition, metadataColumns, blobAsDescriptor)
      case _ =>
        throw new RuntimeException(s"It's not a Paimon input partition, $partition")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: PaimonPartitionReaderFactory =>
        this.readBuilder.equals(other.readBuilder) && this.metadataColumns == other.metadataColumns

      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(readBuilder)
  }
}

/**
 * Wraps a base reader to prepend a constant `query_index` value as the leading column of every row,
 * for the batch_vector_search TVF.
 */
private class QueryIndexPrependPartitionReader(
    delegate: PartitionReader[InternalRow],
    queryIndex: Int)
  extends PartitionReader[InternalRow] {

  private val prefix = new GenericInternalRow(Array[Any](queryIndex))
  private val joined = new JoinedRow()

  override def next(): Boolean = delegate.next()

  override def get(): InternalRow = joined(prefix, delegate.get())

  override def currentMetricsValues(): Array[CustomTaskMetric] = delegate.currentMetricsValues()

  override def close(): Unit = delegate.close()
}
