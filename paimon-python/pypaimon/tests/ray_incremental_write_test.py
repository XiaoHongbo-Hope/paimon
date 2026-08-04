# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import shutil
import tempfile
import unittest
import uuid
from unittest import mock

import pyarrow as pa
import pytest

ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import (
    PaimonOffsetSource,
    delete_write_paimon_checkpoint,
    write_paimon,
)


class RayIncrementalWriteTest(unittest.TestCase):

    target_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        ("payload", pa.string()),
        ("feature", pa.int32()),
    ])
    source_schema = pa.schema([
        ("id", pa.int64()),
        ("feature", pa.int32()),
    ])

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {
            "warehouse": os.path.join(cls.tempdir, "warehouse")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        if ray.is_initialized():
            ray.shutdown()
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_tables(self):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_target_{}".format(suffix)
        source = "default.pk_source_{}".format(suffix)
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                self.target_schema,
                primary_keys=["id"],
                options={
                    "bucket": "2",
                    "merge-engine": "partial-update",
                },
            ),
            False,
        )
        self.catalog.create_table(
            source,
            Schema.from_pyarrow_schema(self.source_schema),
            False,
        )
        self._write(target, pa.table({
            "id": [1, 2, 3],
            "payload": ["a", "b", "c"],
            "feature": [10, 20, 30],
        }, schema=self.target_schema))
        self._write(source, pa.table({
            "id": [1, 3],
            "feature": [101, 303],
        }, schema=self.source_schema))
        return target, source

    def _write(self, identifier, data):
        table = self.catalog.get_table(identifier)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def _read(self, identifier):
        table = self.catalog.get_table(identifier)
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(
            builder.new_scan().plan().splits()).sort_by("id")

    def test_resumes_after_a_committed_window(self):
        target, source_table = self._create_tables()
        source = PaimonOffsetSource(
            source_table,
            rows_per_unit=1,
            units_per_checkpoint=1,
        )
        operation_id = "pk-resume-{}".format(uuid.uuid4().hex)

        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._prepare_primary_key_groups
        calls = 0

        def fail_second_window(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected driver failure")
            return real_prepare(*args, **kwargs)

        with mock.patch.object(
                ray_datasink,
                "_prepare_primary_key_groups",
                side_effect=fail_second_window):
            with self.assertRaisesRegex(
                    RuntimeError, "injected driver failure"):
                write_paimon(
                    source,
                    target,
                    self.catalog_options,
                    commit_mode="incremental",
                    update_cols=["feature"],
                    operation_id=operation_id,
                )

        partial = self._read(target).to_pydict()
        self.assertEqual(["a", "b", "c"], partial["payload"])
        self.assertIn(
            partial["feature"],
            ([101, 20, 30], [10, 20, 303]),
        )

        result = write_paimon(
            source,
            target,
            self.catalog_options,
            commit_mode="incremental",
            update_cols=["feature"],
            operation_id=operation_id,
        )

        self.assertEqual({"num_written": 2}, result)
        self.assertEqual({
            "id": [1, 2, 3],
            "payload": ["a", "b", "c"],
            "feature": [101, 20, 303],
        }, self._read(target).to_pydict())

    def test_rejects_non_partial_update_target(self):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_dedupe_{}".format(suffix)
        source = "default.pk_dedupe_source_{}".format(suffix)
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                self.target_schema,
                primary_keys=["id"],
                options={"bucket": "1"},
            ),
            False,
        )
        self.catalog.create_table(
            source,
            Schema.from_pyarrow_schema(self.source_schema),
            False,
        )
        with self.assertRaisesRegex(ValueError, "partial-update"):
            write_paimon(
                PaimonOffsetSource(source),
                target,
                self.catalog_options,
                commit_mode="incremental",
                update_cols=["feature"],
                operation_id="reject-dedupe-{}".format(suffix),
            )

    def test_rejects_unprovided_non_nullable_column(self):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_not_null_{}".format(suffix)
        source = "default.pk_not_null_source_{}".format(suffix)
        target_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("payload", pa.string(), nullable=False),
            ("feature", pa.int32()),
        ])
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                target_schema,
                primary_keys=["id"],
                options={
                    "bucket": "1",
                    "merge-engine": "partial-update",
                },
            ),
            False,
        )
        self.catalog.create_table(
            source,
            Schema.from_pyarrow_schema(self.source_schema),
            False,
        )

        with self.assertRaisesRegex(ValueError, "payload.*nullable"):
            write_paimon(
                PaimonOffsetSource(source),
                target,
                self.catalog_options,
                commit_mode="incremental",
                update_cols=["feature"],
                operation_id="reject-not-null-{}".format(suffix),
            )

    def test_inserts_into_empty_target(self):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_empty_{}".format(suffix)
        source_table = "default.pk_empty_source_{}".format(suffix)
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                self.target_schema,
                primary_keys=["id"],
                options={
                    "bucket": "2",
                    "merge-engine": "partial-update",
                },
            ),
            False,
        )
        self.catalog.create_table(
            source_table,
            Schema.from_pyarrow_schema(self.source_schema),
            False,
        )
        self._write(source_table, pa.table({
            "id": [4, 5],
            "feature": [404, 505],
        }, schema=self.source_schema))

        operation_id = "insert-empty-{}".format(suffix)
        result = write_paimon(
            PaimonOffsetSource(
                source_table,
                rows_per_unit=1,
                units_per_checkpoint=1,
            ),
            target,
            self.catalog_options,
            commit_mode="incremental",
            update_cols=["feature"],
            operation_id=operation_id,
        )

        self.assertEqual({"num_written": 2}, result)
        self.assertEqual({
            "id": [4, 5],
            "payload": [None, None],
            "feature": [404, 505],
        }, self._read(target).to_pydict())
        self.assertTrue(delete_write_paimon_checkpoint(
            target, self.catalog_options, operation_id))
        self.assertFalse(delete_write_paimon_checkpoint(
            target, self.catalog_options, operation_id))

    def test_rejects_concurrent_target_write(self):
        target, source_table = self._create_tables()
        source = PaimonOffsetSource(
            source_table,
            rows_per_unit=2,
            units_per_checkpoint=1,
        )

        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._prepare_primary_key_groups

        def write_concurrently(*args, **kwargs):
            self._write(target, pa.table({
                "id": [4],
                "payload": ["external"],
                "feature": [40],
            }, schema=self.target_schema))
            return real_prepare(*args, **kwargs)

        with mock.patch.object(
                ray_datasink,
                "_prepare_primary_key_groups",
                side_effect=write_concurrently):
            with self.assertRaisesRegex(
                    RuntimeError, "Concurrent target commit"):
                write_paimon(
                    source,
                    target,
                    self.catalog_options,
                    commit_mode="incremental",
                    update_cols=["feature"],
                    operation_id="concurrent-{}".format(uuid.uuid4().hex),
                )

        self.assertEqual({
            "id": [1, 2, 3, 4],
            "payload": ["a", "b", "c", "external"],
            "feature": [10, 20, 30, 40],
        }, self._read(target).to_pydict())
