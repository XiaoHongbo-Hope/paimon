#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os
import shutil
import tempfile
import threading
import types
import unittest
import uuid
from unittest import mock

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import update_by_row_id


class RayUpdateByRowIdTest(unittest.TestCase):
    """Distributed row-id update: rewrite only the files owning the given row ids,
    without reading or joining the whole target (unlike merge_into(on=_ROW_ID))."""

    pa_schema = pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
        ("age", pa.int32()),
    ])
    de_options = {"row-tracking.enabled": "true", "data-evolution.enabled": "true"}

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {"warehouse": os.path.join(cls.tempdir, "wh")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, options=None):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        opts = self.de_options if options is None else options
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(self.pa_schema, options=opts), False)
        return name

    def _write(self, target, data):
        t = self.catalog.get_table(target)
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()

    def _read(self, target, projection=None):
        t = self.catalog.get_table(target)
        rb = t.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        return rb.new_read().to_arrow(rb.new_scan().plan().splits())

    def _rowid_by_id(self, target):
        tab = self._read(target, ["_ROW_ID", "id"])
        return dict(zip(tab.column("id").to_pylist(), tab.column("_ROW_ID").to_pylist()))

    def test_update_by_row_id_basic(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": list(range(1, 7)), "name": [f"n{i}" for i in range(1, 7)],
             "age": [i * 10 for i in range(1, 7)]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)

        # update age for ids 2 and 5 only, addressed by their _ROW_ID
        src = pa.table({"_ROW_ID": [rid[2], rid[5]], "age": [999, 888]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        # Proof of no full-target read: read_paimon is never called (source is a
        # Dataset, and the update routes by manifest metadata, not a scan).
        import pypaimon.ray.ray_paimon as rp
        with mock.patch.object(rp, "read_paimon",
                               side_effect=AssertionError("target was read!")):
            stats = update_by_row_id(target, ray.data.from_arrow(src),
                                     self.catalog_options, update_cols=["age"])
        self.assertEqual(stats, {"num_updated": 2})

        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(back["age"], [10, 999, 30, 40, 888, 60])
        self.assertEqual(back["name"], [f"n{i}" for i in range(1, 7)])  # untouched

    def test_updates_correct_row_across_files(self):
        # A _ROW_ID owned by a middle data file must update only that row.
        target = self._create()
        for chunk in ([10, 11, 12], [20, 21], [30, 31, 32, 33]):
            self._write(target, pa.Table.from_pydict(
                {"id": chunk, "name": ["x"] * len(chunk), "age": [0] * len(chunk)},
                schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[21]], "age": [999]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        stats = update_by_row_id(target, ray.data.from_arrow(src),
                                 self.catalog_options, update_cols=["age"])
        self.assertEqual(stats, {"num_updated": 1})
        back = self._read(target).sort_by("id").to_pydict()
        got = dict(zip(back["id"], back["age"]))
        self.assertEqual(got[21], 999)
        self.assertTrue(all(v == 0 for k, v in got.items() if k != 21))

    def test_incrementally_commits_file_group_windows(self):
        from pypaimon.write.commit.commit_scanner import CommitScanner
        from pypaimon.write.commit.conflict_detection import ConflictDetection

        target = self._create()
        chunks = [
            [group_start, group_start + 1]
            for group_start in range(10, 90, 10)
        ]
        for chunk in chunks:
            self._write(target, pa.Table.from_pydict(
                {"id": chunk, "name": ["x"] * len(chunk), "age": [0] * len(chunk)},
                schema=self.pa_schema,
            ))

        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        rid = self._rowid_by_id(target)
        updated_ids = [chunk[0] for chunk in chunks]
        src = pa.table(
            {
                "_ROW_ID": [rid[row_id] for row_id in updated_ids],
                "age": updated_ids,
            },
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )

        original_iter_batches = ray.data.Dataset.iter_batches
        iter_batch_options = []
        first_window_committed = threading.Event()
        resume_iteration = threading.Event()
        observed = {}
        scan_counts = {"fallback": 0, "incremental": 0}
        query_sizes = []
        original_fallback_scan = CommitScanner.read_conflict_entries
        original_incremental_scan = (
            CommitScanner.read_incremental_raw_entries_for_scope
        )
        original_base_read = ConflictDetection.read_row_id_base_entries

        def counting_fallback_scan(scanner, *args, **kwargs):
            scan_counts["fallback"] += 1
            return original_fallback_scan(scanner, *args, **kwargs)

        def counting_incremental_scan(scanner, *args, **kwargs):
            scan_counts["incremental"] += 1
            return original_incremental_scan(scanner, *args, **kwargs)

        def counting_base_read(detection, *args, **kwargs):
            result = original_base_read(detection, *args, **kwargs)
            query_sizes.append(len(result))
            return result

        def pausing_iter_batches(dataset, *args, **kwargs):
            iter_batch_options.append((
                kwargs.get("batch_size"),
                kwargs.get("prefetch_batches"),
            ))
            for batch_number, batch in enumerate(
                    original_iter_batches(dataset, *args, **kwargs), 1):
                yield batch
                if batch_number == 3:
                    first_window_committed.set()
                    if not resume_iteration.wait(30):
                        raise TimeoutError("timed out waiting to resume iteration")

        def observe_first_window():
            if not first_window_committed.wait(30):
                observed["error"] = "first commit was not observed"
            else:
                observed["snapshot_id"] = (
                    table.snapshot_manager().get_latest_snapshot().id
                )
            resume_iteration.set()

        observer = threading.Thread(target=observe_first_window)
        observer.start()

        try:
            with mock.patch.object(
                    ray.data.Dataset, "iter_batches", pausing_iter_batches):
                with mock.patch.object(
                    CommitScanner,
                    "read_conflict_entries",
                    counting_fallback_scan,
                ), mock.patch.object(
                    CommitScanner,
                    "read_incremental_raw_entries_for_scope",
                    counting_incremental_scan,
                ), mock.patch.object(
                    ConflictDetection,
                    "read_row_id_base_entries",
                    counting_base_read,
                ):
                    stats = update_by_row_id(
                        target,
                        ray.data.from_arrow(src).repartition(8),
                        self.catalog_options,
                        update_cols=["age"],
                        num_partitions=4,
                        max_groups_per_commit=3,
                    )
        finally:
            resume_iteration.set()
            observer.join(30)

        self.assertEqual({"num_updated": 8}, stats)
        self.assertEqual([(1, 0)], iter_batch_options)
        self.assertNotIn("error", observed)
        self.assertEqual(base_snapshot_id + 1, observed["snapshot_id"])
        self.assertEqual({"fallback": 0, "incremental": 0}, scan_counts)
        self.assertEqual([2, 3, 3], sorted(query_sizes))
        latest = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual(base_snapshot_id + 3, latest.id)
        incremental_snapshots = [
            table.snapshot_manager().get_snapshot_by_id(base_snapshot_id + offset)
            for offset in range(1, 4)
        ]
        self.assertEqual(1, len({
            snapshot.commit_user for snapshot in incremental_snapshots
        }))
        self.assertEqual(
            [1, 2, 3],
            [snapshot.commit_identifier for snapshot in incremental_snapshots],
        )

        back = self._read(target).sort_by("id").to_pydict()
        got = dict(zip(back["id"], back["age"]))
        self.assertEqual(
            {row_id: row_id for row_id in updated_ids},
            {row_id: got[row_id] for row_id in updated_ids},
        )
        for chunk in chunks:
            self.assertEqual(0, got[chunk[1]])

    def test_incremental_commit_failure_drains_later_groups(self):
        import importlib
        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError
        from pypaimon.write.table_commit import StreamTableCommit

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        target = self._create()
        chunks = [[start, start + 1] for start in range(10, 50, 10)]
        for chunk in chunks:
            self._write(target, pa.Table.from_pydict(
                {"id": chunk, "name": ["x", "x"], "age": [0, 0]},
                schema=self.pa_schema,
            ))

        rid = self._rowid_by_id(target)
        src = pa.table(
            {
                "_ROW_ID": [rid[chunk[0]] for chunk in chunks],
                "age": [chunk[0] for chunk in chunks],
            },
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )
        seen_groups = []
        aborted = []
        commit_calls = []
        retry_error = ValueError("commit retry failed")
        commit_error = CommitOutcomeUnknownError("commit failed")
        original_add_group = m._IncrementalUpdateCommitter.add_group
        original_abort = StreamTableCommit.abort

        def record_group(committer, messages, num_updated, row_ids):
            seen_groups.append(list(messages))
            return original_add_group(
                committer, messages, num_updated, row_ids)

        def fail_commit(_commit, _messages, commit_identifier):
            commit_calls.append(commit_identifier)
            raise commit_error from retry_error

        def record_abort(commit, messages):
            aborted.append(list(messages))
            return original_abort(commit, messages)

        with mock.patch.object(
                m._IncrementalUpdateCommitter, "add_group", record_group), \
                mock.patch.object(StreamTableCommit, "commit", fail_commit), \
                mock.patch.object(StreamTableCommit, "abort", record_abort):
            with self.assertRaises(CommitOutcomeUnknownError) as raised:
                update_by_row_id(
                    target,
                    ray.data.from_arrow(src).repartition(4),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=4,
                    max_groups_per_commit=2,
                )

        self.assertIs(commit_error, raised.exception)
        self.assertIs(retry_error, raised.exception.__cause__)
        self.assertEqual(4, len(seen_groups))
        self.assertEqual([1], commit_calls)
        self.assertEqual(
            [list(group) for group in seen_groups[2:]],
            aborted,
        )

    def test_incremental_commit_error_precedes_later_worker_failure(self):
        import importlib

        from ray.exceptions import RayTaskError

        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError
        from pypaimon.write.table_commit import StreamTableCommit

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["x"], "age": [0]},
            schema=self.pa_schema,
        ))
        src = pa.table(
            {"_ROW_ID": [0], "age": [1]},
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )
        commit_error = CommitOutcomeUnknownError("commit outcome is uncertain")
        worker_error = ValueError("worker failed")
        aborted = []
        close_calls = []

        def fail_commit(_commit, _messages, _commit_identifier):
            raise commit_error

        def record_abort(_commit, messages):
            aborted.append(list(messages))

        def record_close(_commit):
            close_calls.append(True)

        def fail_after_commit(_update_ds, _table, _update_cols, **kwargs):
            on_group_result = kwargs["on_group_result"]
            on_group_result(["uncertain-1"], 1, [])
            on_group_result(["uncertain-2"], 1, [])
            on_group_result(["pending"], 1, [])
            raise RayTaskError("worker", "trace", worker_error)

        with mock.patch.object(
                m, "distributed_update_apply", fail_after_commit), \
                mock.patch.object(StreamTableCommit, "commit", fail_commit), \
                mock.patch.object(StreamTableCommit, "abort", record_abort), \
                mock.patch.object(StreamTableCommit, "close", record_close):
            with self.assertRaises(CommitOutcomeUnknownError) as raised:
                update_by_row_id(
                    target,
                    src,
                    self.catalog_options,
                    update_cols=["age"],
                    max_groups_per_commit=2,
                )

        self.assertIs(commit_error, raised.exception)
        self.assertIs(worker_error, raised.exception.__cause__)
        self.assertEqual([["pending"]], aborted)
        self.assertEqual([True], close_calls)

    def test_incremental_commit_preserves_cause_before_worker_failure(self):
        import importlib

        from ray.exceptions import RayTaskError

        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError
        from pypaimon.write.table_commit import StreamTableCommit

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["x"], "age": [0]},
            schema=self.pa_schema,
        ))
        src = pa.table(
            {"_ROW_ID": [0], "age": [1]},
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )
        commit_cause = ValueError("commit retry failed")
        commit_error = CommitOutcomeUnknownError("commit outcome is uncertain")
        worker_error = ValueError("worker failed")
        aborted = []
        close_calls = []

        def fail_commit(_commit, _messages, _commit_identifier):
            raise commit_error from commit_cause

        def record_abort(_commit, messages):
            aborted.append(list(messages))

        def record_close(_commit):
            close_calls.append(True)

        def fail_after_commit(_update_ds, _table, _update_cols, **kwargs):
            on_group_result = kwargs["on_group_result"]
            on_group_result(["uncertain-1"], 1, [])
            on_group_result(["uncertain-2"], 1, [])
            on_group_result(["pending"], 1, [])
            raise RayTaskError("worker", "trace", worker_error)

        with mock.patch.object(
                m, "distributed_update_apply", fail_after_commit), \
                mock.patch.object(StreamTableCommit, "commit", fail_commit), \
                mock.patch.object(StreamTableCommit, "abort", record_abort), \
                mock.patch.object(StreamTableCommit, "close", record_close), \
                self.assertLogs(
                    "pypaimon.ray.update_by_row_id", level="WARNING") as logs:
            with self.assertRaises(CommitOutcomeUnknownError) as raised:
                update_by_row_id(
                    target,
                    src,
                    self.catalog_options,
                    update_cols=["age"],
                    max_groups_per_commit=2,
                )

        self.assertIs(commit_error, raised.exception)
        self.assertIs(commit_cause, raised.exception.__cause__)
        self.assertIs(worker_error, raised.exception.__context__)
        self.assertIn(
            "preserving the original commit error: worker failed",
            "\n".join(logs.output),
        )
        self.assertEqual([["pending"]], aborted)
        self.assertEqual([True], close_calls)

    def test_incremental_conflict_aborts_output_files(self):
        from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
        from pypaimon.write.table_commit import StreamTableCommit
        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [0]},
            schema=self.pa_schema,
        ))
        table = self.catalog.get_table(target)
        row_id = self._rowid_by_id(target)[1]
        update_schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int32()),
        ])
        source = pa.table(
            {"_ROW_ID": [row_id], "age": [300]},
            schema=update_schema,
        )
        output_paths = []
        original_commit = StreamTableCommit.commit

        def commit_after_concurrent_update(
                stream_commit, messages, commit_identifier):
            output_paths.extend(
                file.file_path
                for message in messages
                for file in message.new_files
            )
            updater = TableUpdateByRowId(
                table, "concurrent", BATCH_COMMIT_IDENTIFIER)
            concurrent_messages = updater.update_columns(
                pa.table(
                    {"_ROW_ID": [row_id], "age": [999]},
                    schema=update_schema,
                ),
                ["age"],
            )
            concurrent_commit = table.new_batch_write_builder().new_commit()
            try:
                concurrent_commit.commit(concurrent_messages)
            finally:
                concurrent_commit.close()
            return original_commit(
                stream_commit, messages, commit_identifier)

        with mock.patch.object(
                StreamTableCommit, "commit", commit_after_concurrent_update):
            with self.assertRaisesRegex(
                    RuntimeError, "Concurrent commit detected"):
                update_by_row_id(
                    target,
                    ray.data.from_arrow(source),
                    self.catalog_options,
                    update_cols=["age"],
                    max_groups_per_commit=1,
                )

        self.assertTrue(output_paths)
        self.assertTrue(all(
            not table.file_io.exists(path) for path in output_paths
        ))
        self.assertEqual([999], self._read(target)["age"].to_pylist())

    def test_rollback_reused_snapshot_id_detects_conflict(self):
        from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
        from pypaimon.write.table_commit import StreamTableCommit
        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

        target = self._create()
        for row_id in range(1, 4):
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema,
            ))
        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        row_ids = self._rowid_by_id(target)
        update_schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int32()),
        ])
        source = pa.table(
            {
                "_ROW_ID": [row_ids[row_id] for row_id in range(1, 4)],
                "age": [100, 200, 300],
            },
            schema=update_schema,
        )
        original_commit = StreamTableCommit.commit
        commit_calls = []

        def commit_then_rebuild_history(
                stream_commit, messages, commit_identifier):
            commit_calls.append(commit_identifier)
            result = original_commit(
                stream_commit, messages, commit_identifier)
            if commit_identifier == 2:
                table.rollback_to(base_snapshot_id)
                updater = TableUpdateByRowId(
                    table, "concurrent", BATCH_COMMIT_IDENTIFIER)
                concurrent_messages = updater.update_columns(
                    pa.table(
                        {
                            "_ROW_ID": [
                                row_ids[row_id] for row_id in range(1, 4)
                            ],
                            "age": [999, 999, 999],
                        },
                        schema=update_schema,
                    ),
                    ["age"],
                )
                concurrent_commit = (
                    table.new_batch_write_builder().new_commit()
                )
                try:
                    concurrent_commit.commit(concurrent_messages)
                finally:
                    concurrent_commit.close()
            return result

        with mock.patch.object(
                StreamTableCommit, "commit", commit_then_rebuild_history):
            with self.assertRaisesRegex(
                    RuntimeError, "no longer in the current lineage"):
                update_by_row_id(
                    target,
                    ray.data.from_arrow(source).repartition(3),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=3,
                    max_groups_per_commit=1,
                )

        self.assertEqual([1, 2], commit_calls)
        self.assertEqual(
            [999, 999, 999],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_incremental_commit_survives_a_failing_group(self):
        # QuakeWang review: a later group's failure must not discard the groups
        # that already succeeded in the *same* Ray task. Force both groups into
        # one task (num_partitions=1) and make the second group fail (duplicate
        # _ROW_ID). The first group must still be committed, and the failure
        # must still surface. Bounding partitions to num_partitions (rather than
        # to the file count) can co-locate groups in a task, so this is exactly
        # the scenario that per-group exception encoding must handle.
        target = self._create()
        for row_id in range(1, 3):  # two data files => two groups
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema))
        row_ids = self._rowid_by_id(target)
        update_schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int32()),
        ])
        # Group for id=1 is valid; group for id=2 has a duplicate _ROW_ID and
        # fails inside the worker.
        source = pa.table(
            {"_ROW_ID": [row_ids[1], row_ids[2], row_ids[2]],
             "age": [111, 222, 333]},
            schema=update_schema)

        # The worker exception is reconstructed on the driver as a
        # GroupApplyError (a RuntimeError) carrying the original message.
        with self.assertRaisesRegex(RuntimeError, "Deduplicate"):
            update_by_row_id(
                target,
                ray.data.from_arrow(source),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                max_groups_per_commit=1,
            )

        # The healthy group committed even though its sibling failed in the
        # same task; the failed group left its row untouched.
        ages = dict(zip(
            self._read(target)["id"].to_pylist(),
            self._read(target)["age"].to_pylist(),
        ))
        self.assertEqual(111, ages[1])
        self.assertEqual(0, ages[2])

    def test_incremental_commit_flushes_successes_when_group_fails(self):
        # max_groups_per_commit > 1: a successful group that has not yet filled
        # a window must still be committed when a later group fails, instead of
        # being aborted with the failed one.
        target = self._create()
        for row_id in range(1, 3):  # two data files => two groups
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema))
        row_ids = self._rowid_by_id(target)
        update_schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int32()),
        ])
        source = pa.table(
            {"_ROW_ID": [row_ids[1], row_ids[2], row_ids[2]],
             "age": [111, 222, 333]},
            schema=update_schema)

        with self.assertRaisesRegex(RuntimeError, "Deduplicate"):
            update_by_row_id(
                target,
                ray.data.from_arrow(source),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                max_groups_per_commit=5,  # window never fills before the failure
            )

        ages = dict(zip(
            self._read(target)["id"].to_pylist(),
            self._read(target)["age"].to_pylist(),
        ))
        self.assertEqual(111, ages[1])
        self.assertEqual(0, ages[2])

    def test_atomic_group_failure_commits_nothing(self):
        # Atomic mode (no max_groups_per_commit) is all-or-nothing: a group
        # failure must abort the successful group's files, not commit them.
        target = self._create()
        for row_id in range(1, 3):
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema))
        row_ids = self._rowid_by_id(target)
        base_snapshot_id = (
            self.catalog.get_table(target)
            .snapshot_manager().get_latest_snapshot().id)
        update_schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int32()),
        ])
        source = pa.table(
            {"_ROW_ID": [row_ids[1], row_ids[2], row_ids[2]],
             "age": [111, 222, 333]},
            schema=update_schema)

        with self.assertRaisesRegex(RuntimeError, "Deduplicate"):
            update_by_row_id(
                target,
                ray.data.from_arrow(source),
                self.catalog_options,
                update_cols=["age"],
            )

        # Nothing committed: no new snapshot, both rows unchanged.
        self.assertEqual(
            base_snapshot_id,
            self.catalog.get_table(target)
            .snapshot_manager().get_latest_snapshot().id)
        self.assertEqual(
            [0, 0],
            self._read(target).sort_by("id")["age"].to_pylist())

    def test_group_error_channel_is_string_only_and_round_trips(self):
        # The worker->driver error channel must not carry the exception
        # instance: a custom exception whose __init__ takes extra required args
        # can pickle-dump on the worker yet fail to load on the driver, which
        # would drop the groups that succeeded. Encoding must be strings only,
        # and must round-trip through pickle for any exception.
        import importlib
        import pickle
        m = importlib.import_module(
            "pypaimon.ray.data_evolution_merge_join")

        class _Undeserializable(Exception):
            def __init__(self, a, b):  # two required args; bad pickle round-trip
                super().__init__(f"unpicklable boom {a}/{b}")
                self.a, self.b = a, b

        try:
            raise _Undeserializable("x", "y")
        except _Undeserializable as exc:
            payload = m._encode_group_error(exc)

        # Directly loading the instance would fail; the payload must not.
        with self.assertRaises(Exception):
            pickle.loads(pickle.dumps(exc))
        self.assertEqual(payload, pickle.loads(pickle.dumps(payload)))
        self.assertTrue(all(isinstance(v, str) for v in payload.values()))
        self.assertEqual("_Undeserializable", payload["type"])

        with self.assertRaisesRegex(m.GroupApplyError, "unpicklable boom"):
            m.raise_group_apply_error(payload)

    def test_sparse_source_keeps_partitions_bounded(self):
        # QuakeWang review: partitions must follow num_partitions, not the whole
        # table's file count -- a one-row update over many files must not plan a
        # partition per untouched file.
        target = self._create()
        for row_id in range(1, 6):  # five data files
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema))
        row_ids = self._rowid_by_id(target)
        source = pa.table(
            {"_ROW_ID": [row_ids[1]], "age": [111]},
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        captured = []
        original_groupby = ray.data.Dataset.groupby

        def recording_groupby(dataset, *args, **kwargs):
            captured.append(kwargs.get("num_partitions"))
            return original_groupby(dataset, *args, **kwargs)

        with mock.patch.object(
                ray.data.Dataset, "groupby", recording_groupby):
            update_by_row_id(
                target,
                ray.data.from_arrow(source),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=2,
                max_groups_per_commit=1,
            )
        # Capped at num_partitions=2, not inflated to the five-file count.
        self.assertEqual([2], captured)
        self.assertEqual(
            111,
            dict(zip(self._read(target)["id"].to_pylist(),
                     self._read(target)["age"].to_pylist()))[1],
        )

    def test_incremental_commit_fails_after_external_rollback(self):
        target = self._create()
        for row_id in range(1, 3):
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema,
            ))
        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        row_ids = self._rowid_by_id(target)
        source = pa.table(
            {
                "_ROW_ID": [row_ids[1], row_ids[2]],
                "age": [100, 200],
            },
            schema=pa.schema([
                ("_ROW_ID", pa.int64()),
                ("age", pa.int32()),
            ]),
        )

        original_iter_batches = ray.data.Dataset.iter_batches
        first_window_committed = threading.Event()
        resume_iteration = threading.Event()
        rollback_error = []

        def pausing_iter_batches(dataset, *args, **kwargs):
            for batch_number, batch in enumerate(
                    original_iter_batches(dataset, *args, **kwargs), 1):
                yield batch
                if batch_number == 1:
                    first_window_committed.set()
                    if not resume_iteration.wait(30):
                        raise TimeoutError("timed out waiting for rollback")

        def rollback_first_window():
            try:
                if not first_window_committed.wait(30):
                    raise TimeoutError("first commit was not observed")
                table.rollback_to(base_snapshot_id)
            except Exception as error:
                rollback_error.append(error)
            finally:
                resume_iteration.set()

        rollback_thread = threading.Thread(target=rollback_first_window)
        rollback_thread.start()
        try:
            with mock.patch.object(
                    ray.data.Dataset, "iter_batches", pausing_iter_batches):
                with self.assertRaisesRegex(
                        RuntimeError, "no longer in the current lineage"):
                    update_by_row_id(
                        target,
                        ray.data.from_arrow(source).repartition(2),
                        self.catalog_options,
                        update_cols=["age"],
                        num_partitions=2,
                        max_groups_per_commit=1,
                    )
        finally:
            resume_iteration.set()
            rollback_thread.join(30)

        self.assertEqual([], rollback_error)
        self.assertFalse(rollback_thread.is_alive())
        self.assertEqual(
            base_snapshot_id,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        self.assertEqual(
            [0, 0],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_incremental_commit_rejects_overwrite_of_committed_window(self):
        # QuakeWang review #1: a concurrent overwrite of an already-committed
        # window must be caught -- the protected cumulative scope covers earlier
        # windows, not just the window currently being committed. Commit the
        # first window, overwrite the table (replacing that window's file
        # group), then let the second window proceed; it must fail closed.
        target = self._create()
        for row_id in range(1, 3):  # two files => two windows
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema))
        table = self.catalog.get_table(target)
        row_ids = self._rowid_by_id(target)
        update_schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int32()),
        ])
        source = pa.table(
            {"_ROW_ID": [row_ids[1], row_ids[2]], "age": [100, 200]},
            schema=update_schema)

        original_iter_batches = ray.data.Dataset.iter_batches
        first_window_committed = threading.Event()
        resume_iteration = threading.Event()
        overwrite_error = []

        def pausing_iter_batches(dataset, *args, **kwargs):
            for batch_number, batch in enumerate(
                    original_iter_batches(dataset, *args, **kwargs), 1):
                yield batch
                if batch_number == 1:
                    first_window_committed.set()
                    if not resume_iteration.wait(30):
                        raise TimeoutError("timed out waiting for overwrite")

        def overwrite_first_window():
            try:
                if not first_window_committed.wait(30):
                    raise TimeoutError("first commit was not observed")
                wb = table.new_batch_write_builder().overwrite()
                w = wb.new_write()
                w.write_arrow(pa.Table.from_pydict(
                    {"id": [9], "name": ["z"], "age": [9]},
                    schema=self.pa_schema))
                wb.new_commit().commit(w.prepare_commit())
                w.close()
            except Exception as error:
                overwrite_error.append(error)
            finally:
                resume_iteration.set()

        overwrite_thread = threading.Thread(target=overwrite_first_window)
        overwrite_thread.start()
        try:
            with mock.patch.object(
                    ray.data.Dataset, "iter_batches", pausing_iter_batches):
                with self.assertRaisesRegex(
                        RuntimeError, "already-committed update_by_row_id"):
                    update_by_row_id(
                        target,
                        ray.data.from_arrow(source),
                        self.catalog_options,
                        update_cols=["age"],
                        num_partitions=1,
                        max_groups_per_commit=1,
                    )
        finally:
            resume_iteration.set()
            overwrite_thread.join(30)

        self.assertEqual([], overwrite_error)
        self.assertFalse(overwrite_thread.is_alive())

    def test_incremental_committer_batches_complete_groups(self):
        import importlib
        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {
            "commits": [],
            "ignored": [],
            "aborts": [],
            "close_calls": 0,
            "bounded": 0,
        }

        class FakeCommit:
            def enable_bounded_row_id_conflict_state(self):
                recorder["bounded"] += 1

            def commit(self, msgs, commit_identifier):
                recorder["commits"].append((list(msgs), commit_identifier))

            def abort(self, msgs):
                recorder["aborts"].append(list(msgs))

            def ignore_row_id_conflict_for_commit(self, commit_identifier):
                recorder["ignored"].append(commit_identifier)

            def close(self):
                recorder["close_calls"] += 1

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        committer.add_group(["group-1"], 1, [])
        self.assertEqual([], recorder["commits"])
        committer.add_group(["group-2"], 1, [])
        committer.add_group(["group-3"], 1, [])
        committer.finish()
        committer.close()

        self.assertEqual([
            (["group-1", "group-2"], 1),
            (["group-3"], 2),
        ], recorder["commits"])
        self.assertEqual([1, 2], recorder["ignored"])
        self.assertEqual([], recorder["aborts"])
        self.assertEqual(1, recorder["close_calls"])
        self.assertEqual(1, recorder["bounded"])

    def test_incremental_committer_aborts_only_uncommitted_groups(self):
        import importlib
        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {"commits": [], "ignored": [], "aborts": []}

        class FakeCommit:
            def enable_bounded_row_id_conflict_state(self):
                pass

            def commit(self, msgs, commit_identifier):
                recorder["commits"].append((list(msgs), commit_identifier))

            def abort(self, msgs):
                recorder["aborts"].append(list(msgs))

            def ignore_row_id_conflict_for_commit(self, commit_identifier):
                recorder["ignored"].append(commit_identifier)

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        committer.add_group(["committed-1"], 1, [])
        committer.add_group(["committed-2"], 1, [])
        committer.add_group(["pending"], 1, [])
        committer.abort_pending()
        committer.close()

        self.assertEqual([
            (["committed-1", "committed-2"], 1),
        ], recorder["commits"])
        self.assertEqual([1], recorder["ignored"])
        self.assertEqual([["pending"]], recorder["aborts"])

    def test_incremental_committer_drains_after_uncertain_commit(self):
        import importlib
        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {"commit_calls": 0, "ignored": [], "aborts": []}

        class FakeCommit:
            def enable_bounded_row_id_conflict_state(self):
                pass

            def commit(self, msgs, commit_identifier):
                recorder["commit_calls"] += 1
                raise CommitOutcomeUnknownError(
                    "commit outcome is uncertain")

            def abort(self, msgs):
                recorder["aborts"].append(list(msgs))

            def ignore_row_id_conflict_for_commit(self, commit_identifier):
                recorder["ignored"].append(commit_identifier)

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        committer.add_group(["uncertain-1"], 1, [])
        committer.add_group(["uncertain-2"], 1, [])
        self.assertEqual([], committer._pending_messages)
        self.assertEqual(
            ["uncertain-1", "uncertain-2"],
            committer._uncertain_messages,
        )
        for group in ("pending-1", "pending-2"):
            committer.add_group([group], 1, [])
            self.assertEqual([], committer._pending_messages)
            self.assertEqual(2, len(committer._uncertain_messages))
        with self.assertRaisesRegex(RuntimeError, "outcome is uncertain"):
            committer.finish()
        committer.abort_pending()
        committer.close()

        self.assertEqual(1, recorder["commit_calls"])
        self.assertEqual([], recorder["ignored"])
        self.assertEqual(
            [["pending-1"], ["pending-2"]],
            recorder["aborts"],
        )

    def test_incremental_committer_aborts_deterministic_failure(self):
        import importlib

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {"commit_calls": 0, "aborts": []}

        class FakeCommit:
            def enable_bounded_row_id_conflict_state(self):
                pass

            def commit(self, msgs, commit_identifier):
                recorder["commit_calls"] += 1
                raise RuntimeError("conflict")

            def abort(self, msgs):
                recorder["aborts"].append(list(msgs))

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        for group in ("failed-1", "failed-2"):
            committer.add_group([group], 1, [])
        self.assertEqual(
            ["failed-1", "failed-2"], committer._pending_messages)
        for group in ("pending-1", "pending-2"):
            committer.add_group([group], 1, [])
            self.assertEqual(
                ["failed-1", "failed-2"], committer._pending_messages)
        with self.assertRaisesRegex(RuntimeError, "conflict"):
            committer.finish()
        committer.abort_pending()
        committer.close()

        self.assertEqual(1, recorder["commit_calls"])
        self.assertEqual([
            ["pending-1"],
            ["pending-2"],
            ["failed-1", "failed-2"],
        ], recorder["aborts"])

    def test_incremental_committer_releases_groups_while_draining(self):
        import gc
        import importlib
        import weakref

        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {"aborted": []}

        class Message:
            def __init__(self, value):
                self.value = value

        class FakeCommit:
            def enable_bounded_row_id_conflict_state(self):
                pass

            def commit(self, _msgs, _commit_identifier):
                raise CommitOutcomeUnknownError("outcome is uncertain")

            def abort(self, msgs):
                recorder["aborted"].extend(msg.value for msg in msgs)

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        uncertain = [Message("uncertain-1"), Message("uncertain-2")]
        committer.add_group([uncertain[0]], 1, [])
        committer.add_group([uncertain[1]], 1, [])

        drained_refs = []
        for value in range(100):
            message = Message(value)
            drained_refs.append(weakref.ref(message))
            committer.add_group([message], 1, [])
            del message

        gc.collect()
        self.assertTrue(all(ref() is None for ref in drained_refs))
        self.assertEqual(list(range(100)), recorder["aborted"])
        self.assertEqual([], committer._pending_messages)
        self.assertEqual(uncertain, committer._uncertain_messages)

        with self.assertRaisesRegex(RuntimeError, "outcome is uncertain"):
            committer.finish()
        committer.abort_pending()
        committer.close()

    def test_incremental_committer_protects_committed_window(self):
        import importlib

        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {"commits": [], "aborts": []}

        class FakeCommit:
            def enable_bounded_row_id_conflict_state(self):
                pass

            def commit(self, msgs, commit_identifier):
                recorder["commits"].append(list(msgs))

            def abort(self, msgs):
                recorder["aborts"].append(list(msgs))

            def ignore_row_id_conflict_for_commit(self, commit_identifier):
                raise RuntimeError("ignore failed")

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        for group in ("committed-1", "committed-2", "pending-1", "pending-2"):
            committer.add_group([group], 1, [])
        with self.assertRaisesRegex(RuntimeError, "ignore failed"):
            committer.finish()
        committer.abort_pending()
        committer.close()

        self.assertEqual([[
            "committed-1", "committed-2"
        ]], recorder["commits"])
        self.assertEqual(
            [["pending-1"], ["pending-2"]],
            recorder["aborts"],
        )

    def test_incremental_committer_aborts_all_after_new_commit_failure(self):
        import importlib
        m = importlib.import_module("pypaimon.ray.update_by_row_id")
        recorder = {"aborts": [], "close_calls": 0}

        class AbortCommit:
            def abort(self, msgs):
                recorder["aborts"].append(list(msgs))

            def close(self):
                recorder["close_calls"] += 1

        class FailingBuilder:
            def new_commit(self):
                raise RuntimeError("new_commit failed")

        class AbortBuilder:
            def new_commit(self):
                return AbortCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FailingBuilder()

            def new_batch_write_builder(self):
                return AbortBuilder()

        committer = m._IncrementalUpdateCommitter(FakeTable(), 2)
        for group in ("group-1", "group-2", "group-3"):
            committer.add_group([group], 1, [])
        with self.assertRaisesRegex(RuntimeError, "new_commit failed"):
            committer.finish()
        committer.abort_pending()

        self.assertEqual(
            [["group-3"], ["group-1", "group-2"]],
            recorder["aborts"],
        )
        self.assertEqual(2, recorder["close_calls"])

    def test_rejects_invalid_max_groups_per_commit(self):
        target = self._create()
        src = pa.table({"_ROW_ID": pa.array([], pa.int64()),
                        "age": pa.array([], pa.int32())})
        for value in (0, -1, True, 1.5):
            with self.assertRaisesRegex(
                    ValueError, "max_groups_per_commit must be a positive integer"):
                update_by_row_id(
                    target,
                    src,
                    self.catalog_options,
                    update_cols=["age"],
                    max_groups_per_commit=value,
                )

    def test_pins_base_snapshot_for_conflict_detection(self):
        # The update pins its base snapshot and threads it to distributed_update_apply,
        # which uses it for commit-time conflict detection against concurrent writers.
        import importlib
        m = importlib.import_module("pypaimon.ray.update_by_row_id")  # module, not the fn
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        expected_sid = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[1]], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        captured = {}

        def fake_apply(update_ds, table, cols, *, num_partitions,
                       ray_remote_args=None, base_snapshot_id=None):
            captured["base_snapshot_id"] = base_snapshot_id
            return [], 0, [], None

        with mock.patch.object(m, "distributed_update_apply", fake_apply):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])
        self.assertEqual(captured["base_snapshot_id"], expected_sid)

    def test_new_commit_failure_aborts_pending_messages(self):
        err = RuntimeError("new_commit failed")
        recorder = {}

        with self.assertRaisesRegex(RuntimeError, "new_commit failed"):
            self._run_with_fake_commit(
                recorder=recorder,
                new_commit_errors=[err],
            )

        self.assertEqual(recorder["commit_calls"], 0)
        self.assertEqual(recorder["abort_calls"], 1)
        self.assertEqual(recorder["abort_msgs"], recorder["msgs"])

    def test_deterministic_commit_failure_aborts_pending_messages(self):
        err = RuntimeError("commit failed")
        recorder = {}

        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            self._run_with_fake_commit(
                recorder=recorder,
                commit_error=err,
            )

        self.assertEqual(recorder["commit_calls"], 1)
        self.assertEqual(recorder["abort_calls"], 1)
        self.assertEqual(recorder["abort_msgs"], recorder["msgs"])
        self.assertEqual(recorder["close_calls"], 2)

    def test_unknown_commit_failure_does_not_abort_pending_messages(self):
        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError

        recorder = {}
        with self.assertRaises(CommitOutcomeUnknownError):
            self._run_with_fake_commit(
                recorder=recorder,
                commit_error=CommitOutcomeUnknownError("commit failed"),
            )

        self.assertEqual(recorder["commit_calls"], 1)
        self.assertEqual(recorder["abort_calls"], 0)
        self.assertEqual(recorder["close_calls"], 1)

    def test_driver_commit_failure_is_not_unwrapped(self):
        from pypaimon.write.file_store_commit import CommitOutcomeUnknownError

        retry_error = ValueError("retry failed")
        for error_type in (RuntimeError, CommitOutcomeUnknownError):
            try:
                raise error_type("commit failed") from retry_error
            except error_type as commit_error:
                chained_error = commit_error

            with self.assertRaises(error_type) as raised:
                self._run_with_fake_commit(commit_error=chained_error)

            self.assertIs(chained_error, raised.exception)
            self.assertIs(retry_error, raised.exception.__cause__)

    def test_ray_task_error_is_unwrapped(self):
        from ray.exceptions import RayTaskError

        from pypaimon.ray.data_evolution_merge_into import _reraise_inner

        worker_error = ValueError("worker failed")
        ray_error = RayTaskError("worker", "trace", worker_error)

        with self.assertRaises(ValueError) as raised:
            _reraise_inner(ray_error)

        self.assertIs(worker_error, raised.exception)
        self.assertIsNone(raised.exception.__cause__)

    def test_close_failure_after_success_warns_and_returns_stats(self):
        close_error = RuntimeError("close failed")

        with self.assertLogs("pypaimon.ray.update_by_row_id", level="WARNING") as logs:
            recorder = self._run_with_fake_commit(close_error=close_error)

        self.assertEqual(recorder["result"], {"num_updated": 3})
        self.assertEqual(recorder["commit_calls"], 1)
        self.assertEqual(recorder["abort_calls"], 0)
        self.assertIn(
            "Failed to close update_by_row_id commit",
            "\n".join(logs.output),
        )

    def test_accepts_pyarrow_and_pandas_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        # pyarrow.Table source
        update_by_row_id(
            target,
            pa.table({"_ROW_ID": [rid[1]], "age": [77]},
                     schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())])),
            self.catalog_options, update_cols=["age"])
        self.assertEqual(self._read(target).sort_by("id").to_pydict()["age"], [77, 2])

        # pandas.DataFrame source, updating multiple columns at once
        import pandas as pd
        update_by_row_id(
            target,
            pd.DataFrame({"_ROW_ID": pd.array([rid[2]], dtype="int64"),
                          "name": ["z"], "age": pd.array([88], dtype="int32")}),
            self.catalog_options, update_cols=["name", "age"])
        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(back["age"], [77, 88])
        self.assertEqual(back["name"], ["a", "z"])

    def test_rejects_table_name_source(self):
        # A source table's system _ROW_ID is its own, not the target's row ids, so a
        # table-name source is rejected rather than silently updating wrong rows.
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        with self.assertRaises(ValueError):
            update_by_row_id(target, "default.some_source", self.catalog_options,
                             update_cols=["age"])

    def test_rejects_non_data_evolution_table(self):
        target = self._create(options={})  # plain append table
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_missing_row_id_column(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"age": [9]}, schema=pa.schema([("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_partition_column_update(self):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        s = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=["name"],
                                       options=self.de_options)
        self.catalog.create_table(name, s, False)
        self._write(name, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "name": ["b"]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("name", pa.string())]))
        with self.assertRaises(ValueError):
            update_by_row_id(name, src, self.catalog_options, update_cols=["name"])

    def test_rejects_deletion_vectors_table(self):
        # A DV-deleted row still lives in its file, so update_by_row_id can't tell it is
        # gone without reading the target; DV tables are refused for now.
        opts = dict(self.de_options, **{"deletion-vectors.enabled": "true"})
        target = self._create(options=opts)
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_blob_column_update(self):
        blob_schema = pa.schema([("id", pa.int32()), ("payload", pa.large_binary())])
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(blob_schema, options=self.de_options), False)
        self._write(name, pa.Table.from_pydict(
            {"id": [1], "payload": pa.array([b"x"], pa.large_binary())}, schema=blob_schema))
        src = pa.table({"_ROW_ID": [0], "payload": pa.array([b"y"], pa.large_binary())},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("payload", pa.large_binary())]))
        with self.assertRaises(ValueError):
            update_by_row_id(name, src, self.catalog_options, update_cols=["payload"])

    def test_empty_target_foreign_row_id_raises(self):
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        empty_src = pa.table({"_ROW_ID": pa.array([], pa.int64()),
                              "age": pa.array([], pa.int32())})

        # (a) never written -> no snapshot
        target = self._create()
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])
        # empty source against an empty target is a no-op, not an error
        self.assertEqual(
            update_by_row_id(target, empty_src, self.catalog_options, update_cols=["age"]),
            {"num_updated": 0})

        # (b) written then emptied by overwrite -> snapshot exists but 0 live rows
        target2 = self._create()
        self._write(target2, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        wb = self.catalog.get_table(target2).new_batch_write_builder().overwrite()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": pa.array([], pa.int32()), "name": pa.array([], pa.string()),
             "age": pa.array([], pa.int32())}, schema=self.pa_schema))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        with self.assertRaises(ValueError):
            update_by_row_id(target2, src, self.catalog_options, update_cols=["age"])

    def test_rejects_unknown_and_empty_update_cols(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["nope"])
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=[])

    def _run_with_fake_commit(self, *, recorder=None, new_commit_errors=None,
                              commit_error=None, close_error=None):
        import importlib

        m = importlib.import_module("pypaimon.ray.update_by_row_id")

        if recorder is None:
            recorder = {}
        recorder.update({
            "msgs": [object()],
            "new_commit_errors": list(new_commit_errors or []),
            "commit_error": commit_error,
            "close_error": close_error,
            "new_commit_calls": 0,
            "commit_calls": 0,
            "abort_calls": 0,
            "close_calls": 0,
        })

        class FakeOptions:
            def data_evolution_enabled(self):
                return True

            def row_tracking_enabled(self):
                return True

            def deletion_vectors_enabled(self):
                return False

        class FakeCommit:
            def commit(self, msgs):
                recorder["commit_calls"] += 1
                recorder["commit_msgs"] = list(msgs)
                if recorder["commit_error"] is not None:
                    raise recorder["commit_error"]

            def abort(self, msgs):
                recorder["abort_calls"] += 1
                recorder["abort_msgs"] = list(msgs)

            def close(self):
                recorder["close_calls"] += 1
                if recorder["close_error"] is not None:
                    raise recorder["close_error"]

        class FakeWriteBuilder:
            def new_commit(self):
                recorder["new_commit_calls"] += 1
                if recorder["new_commit_errors"]:
                    raise recorder["new_commit_errors"].pop(0)
                return FakeCommit()

        class FakeTable:
            field_names = ["age"]
            partition_keys = []
            options = FakeOptions()
            table_schema = types.SimpleNamespace(fields=[
                types.SimpleNamespace(
                    name="age",
                    type=types.SimpleNamespace(type="INT"),
                )
            ])

            def snapshot_manager(self):
                return types.SimpleNamespace(get_latest_snapshot=lambda: types.SimpleNamespace(
                    id=1,
                    total_record_count=1,
                ))

            def new_batch_write_builder(self):
                return FakeWriteBuilder()

        class FakeCatalog:
            def get_table(self, target):
                return FakeTable()

        class FakeSource:
            def schema(self):
                return types.SimpleNamespace(names=["_ROW_ID", "age"])

            def map_batches(self, fn, batch_format=None):
                return self

        parser_path = (
            "pypaimon.schema.data_types.PyarrowFieldParser.from_paimon_schema"
        )
        with mock.patch(
                "pypaimon.catalog.catalog_factory.CatalogFactory.create",
                return_value=FakeCatalog()), \
                mock.patch.object(m, "_normalize_source",
                                  side_effect=lambda source, catalog_options: source), \
                mock.patch.object(m, "build_update_schema",
                                  return_value=pa.schema([
                                      ("_ROW_ID", pa.int64()),
                                      ("age", pa.int32()),
                                  ])), \
                mock.patch(parser_path,
                           return_value=pa.schema([
                               ("age", pa.int32()),
                           ])), \
                mock.patch.object(m, "distributed_update_apply",
                                  return_value=(recorder["msgs"], 3, [], None)):
            recorder["result"] = m.update_by_row_id(
                "default.fake",
                FakeSource(),
                self.catalog_options,
                update_cols=["age"],
            )
        return recorder


if __name__ == "__main__":
    unittest.main()
