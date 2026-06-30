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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import scala.collection.JavaConverters._

/** Tests for Lumina vector index read/write operations. */
class LuminaVectorIndexTest extends PaimonSparkTestBase {

  private val indexType = "lumina"
  private val legacyIndexType = "lumina-vector-ann"
  private val defaultOptions = "lumina.index.dimension=3"

  // ========== Index Creation Tests ==========

  test("create lumina vector index - basic") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == indexType)

      assert(indexEntries.nonEmpty)
      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 100L)
    }
  }

  test("batch_vector_search - multiple query vectors with query_index") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val created = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(created.getBoolean(0))

      // Two query vectors, top-5 each => 10 rows tagged with query_index 0 and 1.
      val batch = spark
        .sql(
          """
            |SELECT * FROM batch_vector_search(
            |  'T', 'v', array(array(10.0f, 11.0f, 12.0f), array(80.0f, 81.0f, 82.0f)), 5)
            |""".stripMargin)
        .collect()
      assert(batch.length == 10)
      assert(batch.count(_.getInt(0) == 0) == 5)
      assert(batch.count(_.getInt(0) == 1) == 5)

      // Each query_index group must equal the corresponding single vector_search result.
      val single0 = spark
        .sql("SELECT id FROM vector_search('T', 'v', array(10.0f, 11.0f, 12.0f), 5)")
        .collect()
        .map(_.getInt(0))
        .toSet
      val single1 = spark
        .sql("SELECT id FROM vector_search('T', 'v', array(80.0f, 81.0f, 82.0f), 5)")
        .collect()
        .map(_.getInt(0))
        .toSet
      val batch0 = batch.filter(_.getInt(0) == 0).map(_.getInt(1)).toSet
      val batch1 = batch.filter(_.getInt(0) == 1).map(_.getInt(1)).toSet
      assert(batch0 == single0)
      assert(batch1 == single1)
    }
  }

  test("batch_vector_search - distributed across index shards") {
    withTable("T") {
      // Small shard size + small thread-num so the index splits exceed thread-num * 2 and the
      // distributed read (SparkBatchVectorReadImpl) actually fans out instead of running locally.
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10',
                  |  'global-index.thread-num' = '2',
                  |  'vector-search.distribute.enabled' = 'true',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val created = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(created.getBoolean(0))

      // Distribution only kicks in when splits >= thread-num * 2 (= 4); assert we have enough shards.
      val shardCount = loadTable("T")
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .count(_.indexFile().indexType() == indexType)
      assert(shardCount >= 4, s"need >= 4 shards to exercise the distributed path, got $shardCount")

      val batch = spark
        .sql(
          """
            |SELECT * FROM batch_vector_search(
            |  'T', 'v', array(array(10.0f, 11.0f, 12.0f), array(80.0f, 81.0f, 82.0f)), 5)
            |""".stripMargin)
        .collect()
      assert(batch.length == 10)
      assert(batch.count(_.getInt(0) == 0) == 5)
      assert(batch.count(_.getInt(0) == 1) == 5)

      val single0 = spark
        .sql("SELECT id FROM vector_search('T', 'v', array(10.0f, 11.0f, 12.0f), 5)")
        .collect()
        .map(_.getInt(0))
        .toSet
      val single1 = spark
        .sql("SELECT id FROM vector_search('T', 'v', array(80.0f, 81.0f, 82.0f), 5)")
        .collect()
        .map(_.getInt(0))
        .toSet
      val batch0 = batch.filter(_.getInt(0) == 0).map(_.getInt(1)).toSet
      val batch1 = batch.filter(_.getInt(0) == 1).map(_.getInt(1)).toSet
      assert(batch0 == single0)
      assert(batch1 == single1)
    }
  }

  test("create lumina vector index - legacy index type") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 10)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$legacyIndexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == legacyIndexType)

      assert(indexEntries.nonEmpty)
      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 10L)

      // End-to-end read: vector_search must resolve the legacy identifier through
      // LegacyLuminaVectorGlobalIndexerFactory and return results.
      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
               |""".stripMargin)
        .collect()
      assert(result.length == 5)
    }
  }

  test("table_indexes system table - global index metadata") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()

      // Query table_indexes system table
      val indexRows = spark
        .sql("""
               |SELECT index_type, row_count, row_range_start, row_range_end,
               |       index_field_id, index_field_name
               |FROM `T$table_indexes`
               |WHERE index_type = 'lumina'
               |""".stripMargin)
        .collect()

      assert(indexRows.nonEmpty)
      val row = indexRows.head
      assert(row.getAs[String]("index_type") == "lumina")
      assert(row.getAs[Long]("row_count") == 100L)
      assert(row.getAs[Long]("row_range_start") == 0L)
      assert(row.getAs[Long]("row_range_end") == 99L)
      assert(row.getAs[String]("index_field_name") == "v")

      // Verify max row id matches snapshot next_row_id - 1
      val nextRowId = spark
        .sql("SELECT next_row_id FROM `T$snapshots` ORDER BY snapshot_id DESC LIMIT 1")
        .collect()
        .head
        .getAs[Long]("next_row_id")
      assert(row.getAs[Long]("row_range_end") == nextRowId - 1)
    }
  }

  test("create lumina vector index - with partitioned table") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>, pt STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |  PARTITIONED BY (pt)
                  |""".stripMargin)

      var values = (0 until 500)
        .map(
          i =>
            s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)), 'p0')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 300)
        .map(
          i =>
            s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)), 'p1')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == indexType)

      assert(indexEntries.nonEmpty)
      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 800L)
    }
  }

  // ========== Index Write Tests ==========

  test("write vectors - large dataset") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val df = spark
        .range(0, 10000)
        .selectExpr(
          "cast(id as int) as id",
          "array(cast(id as float), cast(id + 1 as float), cast(id + 2 as float)) as v")
      df.write.insertInto("T")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == indexType)

      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 10000L)
    }
  }

  // ========== Index Read/Search Tests ==========

  test("read vectors - basic search") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()

      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
               |""".stripMargin)
        .collect()
      assert(result.length == 5)
    }
  }

  test("read vectors - top-k search with different k values") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 200)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()

      // Test with k=1
      var result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(100.0f, 101.0f, 102.0f), 1)
               |""".stripMargin)
        .collect()
      assert(result.length == 1)

      // Test with k=10
      result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(100.0f, 101.0f, 102.0f), 10)
               |""".stripMargin)
        .collect()
      assert(result.length == 10)
    }
  }

  test("read vectors - normalized vectors search") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (1 to 100)
        .map {
          i =>
            val v = math.sqrt(3.0 * i * i)
            val normalized = i.toFloat / v.toFloat
            s"($i, array($normalized, $normalized, $normalized))"
        }
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark.sql(
        s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")

      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(0.577f, 0.577f, 0.577f), 10)
               |""".stripMargin)
        .collect()

      assert(result.length == 10)
    }
  }

  // ========== Integration Tests ==========

  test("end-to-end: write, index, read cycle") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING, embedding ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 1000)
        .map(
          i =>
            s"($i, 'item_$i', array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val indexResult = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'embedding', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(indexResult.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == indexType)
      assert(indexEntries.nonEmpty)

      val searchResult = spark
        .sql(
          """
            |SELECT id, name FROM vector_search('T', 'embedding', array(500.0f, 501.0f, 502.0f), 10)
            |""".stripMargin)
        .collect()

      assert(searchResult.length == 10)
    }
  }
}
