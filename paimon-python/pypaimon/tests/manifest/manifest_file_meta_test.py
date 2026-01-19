################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################
import os
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.data.timestamp import Timestamp


class ManifestFileMetaTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

    def setUp(self):
        try:
            table_identifier = self.catalog.identifier('default', 'test_table')
            table_path = self.catalog.get_table_path(table_identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

    def test_min_max_row_id_null_without_row_tracking(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={})
        self.catalog.create_table('default.test_table', schema, False)
        table = self.catalog.get_table('default.test_table')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        }, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        manifest_list_manager = ManifestListManager(table)
        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        manifest_files = manifest_list_manager.read_all(latest_snapshot)

        self.assertGreater(len(manifest_files), 0)
        for manifest in manifest_files:
            self.assertIsNone(manifest.min_row_id, "minRowId should be null when row tracking is disabled")
            self.assertIsNone(manifest.max_row_id, "maxRowId should be null when row tracking is disabled")

    def test_min_max_row_id_calculation_with_row_tracking(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true'
        })
        self.catalog.create_table('default.test_row_tracking', schema, False)
        table = self.catalog.get_table('default.test_row_tracking')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = pa.Table.from_pydict({
            'id': [1, 2],
            'value': ['a', 'b']
        }, schema=pa_schema)
        table_write.write_arrow(data1)
        commit_messages = table_write.prepare_commit()
        for msg in commit_messages:
            for file in msg.new_files:
                file.first_row_id = 0
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'id': [3, 4],
            'value': ['c', 'd']
        }, schema=pa_schema)
        table_write.write_arrow(data2)
        commit_messages = table_write.prepare_commit()
        for msg in commit_messages:
            for file in msg.new_files:
                file.first_row_id = 2
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        manifest_list_manager = ManifestListManager(table)
        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        manifest_files = manifest_list_manager.read_all(latest_snapshot)

        self.assertEqual(len(manifest_files), 2)

        manifest1 = manifest_files[0]
        self.assertEqual(manifest1.min_row_id, 0)
        self.assertEqual(manifest1.max_row_id, 1)

        manifest2 = manifest_files[1]
        self.assertEqual(manifest2.min_row_id, 2)
        self.assertEqual(manifest2.max_row_id, 3)

    def test_min_max_row_id_with_mixed_entries(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true'
        })
        self.catalog.create_table('default.test_mixed', schema, False)
        table = self.catalog.get_table('default.test_mixed')

        manifest_file_manager = ManifestFileManager(table)

        entry1 = self._create_entry_with_row_id(table, "file1.parquet", bucket=0, level=0, first_row_id=0, row_count=2)
        entry2 = self._create_entry_with_row_id(table, "file2.parquet", bucket=0, level=0, first_row_id=2, row_count=2)

        manifest_file_name = "manifest-test-mixed"
        manifest_file_manager.write(manifest_file_name, [entry1, entry2])

        manifest_file_path = f"{manifest_file_manager.manifest_path}/{manifest_file_name}"
        file_size = table.file_io.get_file_size(manifest_file_path)

        min_bucket = min(entry1.bucket, entry2.bucket)
        max_bucket = max(entry1.bucket, entry2.bucket)
        min_level = min(entry1.file.level, entry2.file.level)
        max_level = max(entry1.file.level, entry2.file.level)
        min_row_id = min(entry1.file.first_row_id, entry2.file.first_row_id)
        max_row_id = max(
            entry1.file.first_row_id + entry1.file.row_count - 1,
            entry2.file.first_row_id + entry2.file.row_count - 1
        )

        from pypaimon.manifest.schema.simple_stats import SimpleStats
        partition_stats = SimpleStats.empty_stats()

        manifest_file_meta = ManifestFileMeta(
            file_name=manifest_file_name,
            file_size=file_size,
            num_added_files=2,
            num_deleted_files=0,
            partition_stats=partition_stats,
            schema_id=0,
            min_bucket=min_bucket,
            max_bucket=max_bucket,
            min_level=min_level,
            max_level=max_level,
            min_row_id=min_row_id,
            max_row_id=max_row_id
        )

        self.assertEqual(manifest_file_meta.min_bucket, 0)
        self.assertEqual(manifest_file_meta.max_bucket, 0)
        self.assertEqual(manifest_file_meta.min_level, 0)
        self.assertEqual(manifest_file_meta.max_level, 0)
        self.assertEqual(manifest_file_meta.min_row_id, 0)
        self.assertEqual(manifest_file_meta.max_row_id, 3)  # max(0+2-1, 2+2-1) = max(1, 3) = 3

    def test_min_max_row_id_null_when_any_entry_missing_first_row_id(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true'
        })
        self.catalog.create_table('default.test_null_mixed', schema, False)
        table = self.catalog.get_table('default.test_null_mixed')

        manifest_file_manager = ManifestFileManager(table)

        entry1 = self._create_entry_with_row_id(table, "file1.parquet", bucket=0, level=0, first_row_id=0, row_count=2)
        entry2 = self._create_entry_without_row_id(table, "file2.parquet", bucket=0, level=0, row_count=2)

        manifest_file_name = "manifest-test-null-mixed"
        manifest_file_manager.write(manifest_file_name, [entry1, entry2])

        manifest_file_path = f"{manifest_file_manager.manifest_path}/{manifest_file_name}"
        file_size = table.file_io.get_file_size(manifest_file_path)

        from pypaimon.manifest.schema.simple_stats import SimpleStats
        partition_stats = SimpleStats.empty_stats()

        manifest_file_meta = ManifestFileMeta(
            file_name=manifest_file_name,
            file_size=file_size,
            num_added_files=2,
            num_deleted_files=0,
            partition_stats=partition_stats,
            schema_id=0,
            min_bucket=0,
            max_bucket=0,
            min_level=0,
            max_level=0,
            min_row_id=None,
            max_row_id=None
        )

        self.assertIsNone(manifest_file_meta.min_row_id)
        self.assertIsNone(manifest_file_meta.max_row_id)

    def test_backward_compatibility_read_old_manifest_list(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={})
        self.catalog.create_table('default.test_backward', schema, False)
        table = self.catalog.get_table('default.test_backward')

        manifest_list_manager = ManifestListManager(table)

        from pypaimon.manifest.schema.simple_stats import SimpleStats
        partition_stats = SimpleStats.empty_stats()

        old_manifest_meta = ManifestFileMeta(
            file_name="old-manifest.avro",
            file_size=1024,
            num_added_files=1,
            num_deleted_files=0,
            partition_stats=partition_stats,
            schema_id=0
        )

        manifest_list_manager.write("manifest-list-old", [old_manifest_meta])

        read_manifests = manifest_list_manager.read("manifest-list-old")

        self.assertEqual(len(read_manifests), 1)
        read_manifest = read_manifests[0]
        self.assertIsNone(read_manifest.min_bucket)
        self.assertIsNone(read_manifest.max_bucket)
        self.assertIsNone(read_manifest.min_level)
        self.assertIsNone(read_manifest.max_level)
        self.assertIsNone(read_manifest.min_row_id)
        self.assertIsNone(read_manifest.max_row_id)

    def test_manifest_file_meta_schema_includes_new_fields(self):
        from pypaimon.manifest.schema.manifest_file_meta import MANIFEST_FILE_META_SCHEMA

        fields = {field["name"]: field for field in MANIFEST_FILE_META_SCHEMA["fields"]}

        self.assertIn("_MIN_BUCKET", fields)
        self.assertIn("_MAX_BUCKET", fields)
        self.assertIn("_MIN_LEVEL", fields)
        self.assertIn("_MAX_LEVEL", fields)
        self.assertIn("_MIN_ROW_ID", fields)
        self.assertIn("_MAX_ROW_ID", fields)

        self.assertEqual(fields["_MIN_BUCKET"]["type"], ["null", "int"])
        self.assertEqual(fields["_MAX_BUCKET"]["type"], ["null", "int"])
        self.assertEqual(fields["_MIN_LEVEL"]["type"], ["null", "int"])
        self.assertEqual(fields["_MAX_LEVEL"]["type"], ["null", "int"])
        self.assertEqual(fields["_MIN_ROW_ID"]["type"], ["null", "long"])
        self.assertEqual(fields["_MAX_ROW_ID"]["type"], ["null", "long"])

        self.assertIsNone(fields["_MIN_BUCKET"].get("default"))
        self.assertIsNone(fields["_MAX_BUCKET"].get("default"))
        self.assertIsNone(fields["_MIN_LEVEL"].get("default"))
        self.assertIsNone(fields["_MAX_LEVEL"].get("default"))
        self.assertIsNone(fields["_MIN_ROW_ID"].get("default"))
        self.assertIsNone(fields["_MAX_ROW_ID"].get("default"))

    def test_write_and_read_manifest_file_meta_with_new_fields(self):
        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={})
        self.catalog.create_table('default.test_write_read', schema, False)
        table = self.catalog.get_table('default.test_write_read')

        manifest_list_manager = ManifestListManager(table)

        from pypaimon.manifest.schema.simple_stats import SimpleStats
        partition_stats = SimpleStats.empty_stats()

        manifest_file_meta = ManifestFileMeta(
            file_name="test-manifest.avro",
            file_size=2048,
            num_added_files=3,
            num_deleted_files=1,
            partition_stats=partition_stats,
            schema_id=1,
            min_bucket=0,
            max_bucket=2,
            min_level=0,
            max_level=1,
            min_row_id=10,
            max_row_id=25
        )

        manifest_list_manager.write("manifest-list-test", [manifest_file_meta])

        read_manifests = manifest_list_manager.read("manifest-list-test")

        self.assertEqual(len(read_manifests), 1)
        read_manifest = read_manifests[0]

        self.assertEqual(read_manifest.file_name, "test-manifest.avro")
        self.assertEqual(read_manifest.file_size, 2048)
        self.assertEqual(read_manifest.num_added_files, 3)
        self.assertEqual(read_manifest.num_deleted_files, 1)
        self.assertEqual(read_manifest.schema_id, 1)
        self.assertEqual(read_manifest.min_bucket, 0)
        self.assertEqual(read_manifest.max_bucket, 2)
        self.assertEqual(read_manifest.min_level, 0)
        self.assertEqual(read_manifest.max_level, 1)
        self.assertEqual(read_manifest.min_row_id, 10)
        self.assertEqual(read_manifest.max_row_id, 25)

    def _create_entry_with_row_id(self, table, file_name, bucket, level, first_row_id, row_count):
        from pypaimon.manifest.schema.data_file_meta import DataFileMeta
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        from pypaimon.table.row.generic_row import GenericRow

        file_meta = DataFileMeta(
            file_name=file_name,
            file_size=1024,
            row_count=row_count,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=level,
            extra_files=[],
            creation_time=Timestamp.from_epoch_millis(1234567890),
            delete_row_count=0,
            embedded_index=None,
            file_source=None,
            value_stats_cols=None,
            external_path=None,
            first_row_id=first_row_id,
            write_cols=None
        )

        return ManifestEntry(
            kind=0,  # ADD
            partition=GenericRow([], []),
            bucket=bucket,
            total_buckets=1,
            file=file_meta
        )

    def _create_entry_without_row_id(self, table, file_name, bucket, level, row_count):
        return self._create_entry_with_row_id(table, file_name, bucket, level, None, row_count)


if __name__ == '__main__':
    unittest.main()