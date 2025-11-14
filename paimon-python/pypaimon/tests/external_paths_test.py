"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.core_options import CoreOptions
from pypaimon.common.external_path_provider import (
    ExternalPathProvider,
    ExternalPathStrategy,
    create_external_path_provider,
    create_external_paths,
)
from urlpath import URL


class ExternalPathProviderTest(unittest.TestCase):

    def test_round_robin_path_selection(self):
        """Test round-robin path selection."""
        external_paths = [
            URL("oss://bucket1/external"),
            URL("oss://bucket2/external"),
            URL("oss://bucket3/external"),
        ]
        relative_path = URL("partition=value/bucket-0")

        provider = ExternalPathProvider(external_paths, relative_path)

        # Get multiple paths and verify round-robin behavior
        paths = []
        for _ in range(6):  # 2 full cycles
            path = provider.get_next_external_data_path("file.parquet")
            paths.append(str(path))

        # Verify all three buckets are used (order may vary due to random start)
        bucket_counts = {
            "bucket1": sum(1 for p in paths if "bucket1" in p),
            "bucket2": sum(1 for p in paths if "bucket2" in p),
            "bucket3": sum(1 for p in paths if "bucket3" in p),
        }
        # Each bucket should appear exactly 2 times in 2 full cycles
        self.assertEqual(bucket_counts["bucket1"], 2, f"bucket1 should appear 2 times, got {bucket_counts['bucket1']}. Paths: {paths}")
        self.assertEqual(bucket_counts["bucket2"], 2, f"bucket2 should appear 2 times, got {bucket_counts['bucket2']}. Paths: {paths}")
        self.assertEqual(bucket_counts["bucket3"], 2, f"bucket3 should appear 2 times, got {bucket_counts['bucket3']}. Paths: {paths}")

        # Verify round-robin: consecutive paths should use different buckets
        # (except when wrapping around, which happens once in 6 calls)
        consecutive_same = 0
        for i in range(len(paths) - 1):
            # Extract bucket name from path
            bucket_i = None
            bucket_next = None
            for bucket in ["bucket1", "bucket2", "bucket3"]:
                if bucket in paths[i]:
                    bucket_i = bucket
                if bucket in paths[i + 1]:
                    bucket_next = bucket
            if bucket_i == bucket_next:
                consecutive_same += 1
        # In 6 calls (2 cycles), there should be at most 1 wrap-around (consecutive same)
        self.assertLessEqual(consecutive_same, 1, f"Too many consecutive same buckets: {consecutive_same}. Paths: {paths}")

        # Verify path structure
        self.assertIn("partition=value", paths[0])
        self.assertIn("bucket-0", paths[0])
        self.assertIn("file.parquet", paths[0])

    def test_single_external_path(self):
        """Test with single external path."""
        external_paths = [URL("oss://bucket/external")]
        relative_path = URL("bucket-0")

        provider = ExternalPathProvider(external_paths, relative_path)
        path = provider.get_next_external_data_path("data.parquet")

        self.assertIn("bucket/external", str(path))
        self.assertIn("bucket-0", str(path))
        self.assertIn("data.parquet", str(path))

    def test_empty_relative_path(self):
        """Test with empty relative path."""
        external_paths = [URL("oss://bucket/external")]
        relative_path = URL("")

        provider = ExternalPathProvider(external_paths, relative_path)
        path = provider.get_next_external_data_path("file.parquet")

        self.assertIn("bucket/external", str(path))
        self.assertIn("file.parquet", str(path))
        # Should not have bucket-0 in path
        self.assertNotIn("bucket-0", str(path))


class ExternalPathsConfigTest(unittest.TestCase):
    """Test external paths configuration parsing."""

    def test_create_external_paths_round_robin(self):
        """Test creating external paths with round-robin strategy."""
        external_paths_str = "oss://bucket1/path1,oss://bucket2/path2,oss://bucket3/path3"
        strategy = ExternalPathStrategy.ROUND_ROBIN

        paths = create_external_paths(external_paths_str, strategy, None)

        self.assertEqual(len(paths), 3)
        self.assertEqual(str(paths[0]), "oss://bucket1/path1")
        self.assertEqual(str(paths[1]), "oss://bucket2/path2")
        self.assertEqual(str(paths[2]), "oss://bucket3/path3")

    def test_create_external_paths_specific_fs(self):
        """Test creating external paths with specific-fs strategy."""
        external_paths_str = "oss://bucket1/path1,s3://bucket2/path2,oss://bucket3/path3"
        strategy = ExternalPathStrategy.SPECIFIC_FS
        specific_fs = "oss"

        paths = create_external_paths(external_paths_str, strategy, specific_fs)

        # Should only include OSS paths
        self.assertEqual(len(paths), 2)
        self.assertIn("oss://bucket1/path1", [str(p) for p in paths])
        self.assertIn("oss://bucket3/path3", [str(p) for p in paths])
        self.assertNotIn("s3://bucket2/path2", [str(p) for p in paths])

    def test_create_external_paths_none_strategy(self):
        """Test creating external paths with none strategy."""
        external_paths_str = "oss://bucket1/path1"
        strategy = ExternalPathStrategy.NONE

        paths = create_external_paths(external_paths_str, strategy, None)

        self.assertIsNone(paths)

    def test_create_external_paths_empty_string(self):
        """Test creating external paths with empty string."""
        paths = create_external_paths("", ExternalPathStrategy.ROUND_ROBIN, None)
        self.assertIsNone(paths)

        paths = create_external_paths(None, ExternalPathStrategy.ROUND_ROBIN, None)
        self.assertIsNone(paths)

    def test_create_external_paths_invalid_scheme(self):
        """Test creating external paths with invalid scheme."""
        external_paths_str = "/invalid/path"  # No scheme

        with self.assertRaises(ValueError) as context:
            create_external_paths(external_paths_str, ExternalPathStrategy.ROUND_ROBIN, None)

        self.assertIn("scheme", str(context.exception))

    def test_create_external_path_provider(self):
        """Test creating ExternalPathProvider from external paths list."""
        external_paths_str = "oss://bucket1/path1,oss://bucket2/path2"
        strategy = ExternalPathStrategy.ROUND_ROBIN
        partition = ("value1",)
        bucket = 0
        partition_keys = ["dt"]

        # Create external paths list first
        external_paths = create_external_paths(external_paths_str, strategy, None)
        self.assertIsNotNone(external_paths)

        # Create provider from external paths
        provider = create_external_path_provider(
            external_paths, partition, bucket, partition_keys
        )

        self.assertIsNotNone(provider)
        path = provider.get_next_external_data_path("file.parquet")
        # Verify path contains one of the buckets (order may vary due to random start)
        self.assertTrue(
            "bucket1" in str(path) or "bucket2" in str(path),
            f"Path should contain bucket1 or bucket2, got: {path}"
        )
        self.assertIn("dt=value1", str(path))
        self.assertIn("bucket-0", str(path))

    def test_create_external_path_provider_none_strategy(self):
        """Test creating provider with none strategy (empty external paths)."""
        external_paths = []  # Empty list when strategy is NONE

        provider = create_external_path_provider(
            external_paths, (), 0, []
        )

        self.assertIsNone(provider)

    def test_create_external_path_provider_no_config(self):
        """Test creating provider without configuration (empty external paths)."""
        external_paths = []  # Empty list when not configured

        provider = create_external_path_provider(
            external_paths, (), 0, []
        )

        self.assertIsNone(provider)


class ExternalPathsIntegrationTest(unittest.TestCase):
    """Integration tests for external paths feature."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.external_dir = os.path.join(cls.temp_dir, "external_data")

        # Create external directory
        os.makedirs(cls.external_dir, exist_ok=True)

        cls.catalog = CatalogFactory.create({
            "warehouse": cls.warehouse
        })
        cls.catalog.create_database("test_db", False)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def test_write_with_external_paths(self):
        """Test writing data with external paths configured."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])

        # Create table with external paths
        external_path = f"file://{self.external_dir}"
        table_options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS: external_path,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY: ExternalPathStrategy.ROUND_ROBIN,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, options=table_options)

        self.catalog.create_table("test_db.external_test", schema, False)
        table = self.catalog.get_table("test_db.external_test")

        # Write data (use explicit schema to match table schema)
        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.7],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()

        # Verify external_path is set in file metadata
        self.assertGreater(len(commit_messages), 0)
        for commit_msg in commit_messages:
            self.assertGreater(len(commit_msg.new_files), 0)
            for file_meta in commit_msg.new_files:
                # External path should be set
                self.assertIsNotNone(file_meta.external_path)
                self.assertTrue(file_meta.external_path.startswith("file://"))
                self.assertIn(self.external_dir, file_meta.external_path)

        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

    def test_read_with_external_paths(self):
        """Test reading data with external paths."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
        ])

        # Create table with external paths
        external_path = f"file://{self.external_dir}"
        table_options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS: external_path,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY: ExternalPathStrategy.ROUND_ROBIN,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, options=table_options)

        self.catalog.create_table("test_db.external_read_test", schema, False)
        table = self.catalog.get_table("test_db.external_read_test")

        # Write data (use explicit schema to match table schema)
        write_data = pa.Table.from_pydict({
            "id": [1, 2, 3, 4, 5],
            "name": ["A", "B", "C", "D", "E"],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(write_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify data
        self.assertEqual(result.num_rows, 5)
        self.assertEqual(result.num_columns, 2)
        self.assertListEqual(result.column("id").to_pylist(), [1, 2, 3, 4, 5])
        self.assertListEqual(result.column("name").to_pylist(), ["A", "B", "C", "D", "E"])

    def test_write_without_external_paths(self):
        """Test that writing without external paths still works."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table("test_db.normal_test", schema, False)
        table = self.catalog.get_table("test_db.normal_test")

        # Write data (use explicit schema to match table schema)
        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["X", "Y", "Z"],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()

        # Verify external_path is None (not configured)
        for commit_msg in commit_messages:
            for file_meta in commit_msg.new_files:
                self.assertIsNone(file_meta.external_path)

        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

    def test_external_paths_with_partition(self):
        """Test external paths with partitioned table."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("dt", pa.string()),
        ])

        external_path = f"file://{self.external_dir}"
        table_options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS: external_path,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY: ExternalPathStrategy.ROUND_ROBIN,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"], options=table_options)

        self.catalog.create_table("test_db.partitioned_external", schema, False)
        table = self.catalog.get_table("test_db.partitioned_external")

        # Write data with partition (use explicit schema to match table schema)
        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-02"],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()

        # Verify external paths include partition info
        for commit_msg in commit_messages:
            for file_meta in commit_msg.new_files:
                self.assertIsNotNone(file_meta.external_path)
                # Should contain partition path
                self.assertIn("dt=", file_meta.external_path)

        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()


if __name__ == "__main__":
    unittest.main()

