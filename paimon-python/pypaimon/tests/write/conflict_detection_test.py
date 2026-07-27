# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from dataclasses import dataclass
from typing import List

from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range
from pypaimon.write.commit.commit_scanner import (
    CommitScanner,
    _ConflictEntryScope,
)
from pypaimon.write.commit.conflict_detection import (
    ConflictDetection,
    RowIdColumnConflictChecker,
)


def _make_file(file_name, row_count=100, first_row_id=None,
               schema_id=0, write_cols=None):
    return DataFileMeta(
        file_name=file_name,
        file_size=1024,
        row_count=row_count,
        min_key=None,
        max_key=None,
        key_stats=None,
        value_stats=None,
        min_sequence_number=0,
        max_sequence_number=0,
        schema_id=schema_id,
        level=0,
        extra_files=[],
        first_row_id=first_row_id,
        write_cols=write_cols,
    )


_EMPTY_PARTITION = GenericRow([], [])


def _make_entry(file_name, kind=0, bucket=0, first_row_id=None,
                row_count=100, write_cols=None, schema_id=0,
                partition=None):
    return ManifestEntry(
        kind=kind,
        partition=(
            _EMPTY_PARTITION
            if partition is None else GenericRow(list(partition), [])),
        bucket=bucket,
        total_buckets=1,
        file=_make_file(file_name, row_count=row_count,
                        first_row_id=first_row_id, schema_id=schema_id,
                        write_cols=write_cols),
    )


@dataclass
class _FakeSchema:
    id: int
    fields: List[DataField]


class _FakeSchemaManager:

    def __init__(self, schemas=None):
        self._schemas = {}
        if schemas:
            for s in schemas:
                self._schemas[s.id] = s

    def get_schema(self, schema_id):
        return self._schemas.get(schema_id)


_DEFAULT_SCHEMA = _FakeSchema(
    id=0,
    fields=[
        DataField(1, "col_a", AtomicType("INT")),
        DataField(2, "col_b", AtomicType("STRING")),
        DataField(3, "col_c", AtomicType("BIGINT")),
    ],
)


class TestCheckRowIdExistence(unittest.TestCase):

    def _make_detection(self):
        return ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=None,
            manifest_list_manager=None,
            table=None,
            commit_scanner=None,
        )

    def test_no_conflict_when_base_file_exists(self):
        detection = self._make_detection()
        base = [_make_entry("f1", kind=0, first_row_id=0, row_count=100)]
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_conflict_when_base_file_removed(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        result = detection.check_row_id_existence(base, delta, next_row_id=200)
        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))

    def test_conflict_when_base_file_rewritten(self):
        detection = self._make_detection()
        base = [_make_entry("f2", kind=0, first_row_id=0, row_count=200)]
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        result = detection.check_row_id_existence(base, delta, next_row_id=200)
        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))

    def test_dedicated_file_is_not_a_normal_file_anchor(self):
        detection = self._make_detection()
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]

        for file_name in ("p0.blob", "p0.vector.parquet"):
            with self.subTest(file_name=file_name):
                base = [_make_entry(
                    file_name, kind=0, first_row_id=0, row_count=100)]
                result = detection.check_row_id_existence(
                    base, delta, next_row_id=200)
                self.assertIsNotNone(result)
                self.assertIn("Row ID existence conflict", str(result))

    def test_no_conflict_when_blob_file_range_is_covered(self):
        detection = self._make_detection()
        base = [_make_entry("f1", kind=0, first_row_id=0, row_count=100)]
        delta = [_make_entry("p1.blob", kind=0, first_row_id=20, row_count=10)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_no_conflict_when_vector_file_range_is_covered(self):
        detection = self._make_detection()
        base = [_make_entry("f1", kind=0, first_row_id=0, row_count=100)]
        delta = [_make_entry("p1.vector.0", kind=0, first_row_id=20, row_count=10)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_conflict_when_blob_file_range_is_not_covered(self):
        detection = self._make_detection()
        base = [_make_entry("f1", kind=0, first_row_id=0, row_count=100)]
        delta = [_make_entry("p1.blob", kind=0, first_row_id=95, row_count=10)]
        result = detection.check_row_id_existence(base, delta, next_row_id=200)
        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))

    def test_no_conflict_when_blob_file_range_is_covered_by_multiple_files(self):
        detection = self._make_detection()
        base = [
            _make_entry("f1", kind=0, first_row_id=0, row_count=50),
            _make_entry("f2", kind=0, first_row_id=50, row_count=50),
        ]
        delta = [_make_entry("p1.blob", kind=0, first_row_id=25, row_count=50)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_conflict_when_blob_file_range_is_only_covered_by_base_blob_file(self):
        detection = self._make_detection()
        base = [
            _make_entry("f1", kind=0, first_row_id=0, row_count=50),
            _make_entry("p0.blob", kind=0, first_row_id=50, row_count=50),
        ]
        delta = [_make_entry("p1.blob", kind=0, first_row_id=60, row_count=10)]
        result = detection.check_row_id_existence(base, delta, next_row_id=200)
        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))

    def test_skip_newly_appended_files(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=200, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_when_no_pre_assigned_row_id(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("f1", kind=0)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_delete_entries(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("f1", kind=1, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_when_data_evolution_disabled(self):
        detection = ConflictDetection(
            data_evolution_enabled=False,
            snapshot_manager=None,
            manifest_list_manager=None,
            table=None,
            commit_scanner=None,
        )
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_when_next_row_id_is_none(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=None))


class TestCheckRowIdRangeConflicts(unittest.TestCase):

    def _make_detection(self):
        return ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=None,
            manifest_list_manager=None,
            table=None,
            commit_scanner=None,
        )

    def test_reports_dedicated_file_spanning_data_files(self):
        detection = self._make_detection()
        entries = [
            _make_entry("f1", kind=0, first_row_id=0, row_count=2),
            _make_entry("f2", kind=0, first_row_id=2, row_count=2),
            _make_entry("p1.blob", kind=0, first_row_id=0, row_count=4),
        ]

        result = detection.check_row_id_range_conflicts("COMPACT", entries)

        self.assertIsNotNone(result)
        self.assertIn("dedicated file", str(result))
        self.assertIn("p1.blob", str(result))
        self.assertIn("spans multiple data file ranges", str(result))
        self.assertIn("f1", str(result))
        self.assertIn("f2", str(result))

    def test_allows_adjacent_data_files(self):
        detection = self._make_detection()
        entries = [
            _make_entry("f1", kind=0, first_row_id=0, row_count=2),
            _make_entry("f2", kind=0, first_row_id=2, row_count=2),
        ]

        result = detection.check_row_id_range_conflicts("COMPACT", entries)

        self.assertIsNone(result)

    def test_allows_dedicated_file_covered_by_one_data_file(self):
        detection = self._make_detection()
        entries = [
            _make_entry("f1", kind=0, first_row_id=0, row_count=4),
            _make_entry("p1.blob", kind=0, first_row_id=1, row_count=2),
        ]

        result = detection.check_row_id_range_conflicts("COMPACT", entries)

        self.assertIsNone(result)


class TestOverwriteConflictDetection(unittest.TestCase):

    def _make_detection(self):
        return ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=None,
            manifest_list_manager=None,
            table=None,
            commit_scanner=None,
        )

    def test_deleted_files_trigger_overwrite_commit(self):
        detection = self._make_detection()
        entries = [
            _make_entry("f1", kind=0),
            _make_entry("f2", kind=1),
        ]
        self.assertTrue(detection.should_be_overwrite_commit(entries, []))

    def test_deletion_vector_index_files_trigger_overwrite_commit(self):
        detection = self._make_detection()
        index_entry = IndexManifestEntry(
            kind=0,
            partition=_EMPTY_PARTITION,
            bucket=0,
            index_file=IndexFileMeta(
                index_type=IndexManifestFile.DELETION_VECTORS_INDEX,
                file_name="dv",
                file_size=1,
                row_count=1,
            ),
        )
        self.assertTrue(detection.should_be_overwrite_commit([], [index_entry]))

    def test_delete_entry_missing_from_base_conflicts(self):
        detection = self._make_detection()
        result = detection.check_conflicts(
            latest_snapshot=None,
            base_entries=[],
            delta_entries=[_make_entry("missing", kind=1)],
            commit_kind="OVERWRITE",
        )
        self.assertIsNotNone(result)
        self.assertIn("File deletion conflicts", str(result))


class _FakeSnapshot:

    def __init__(self, snapshot_id, commit_kind, next_row_id=None,
                 commit_user=None, commit_identifier=None,
                 delta_manifest_list=None, index_manifest=None):
        self.id = snapshot_id
        self.commit_kind = commit_kind
        self.next_row_id = next_row_id
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier
        self.delta_manifest_list = delta_manifest_list
        self.index_manifest = index_manifest


class _FakeSnapshotManager:

    def __init__(self, snapshots):
        self._by_id = {s.id: s for s in snapshots}
        self.requests = []

    def get_snapshot_by_id(self, snapshot_id):
        self.requests.append(snapshot_id)
        return self._by_id.get(snapshot_id)


class _FakeCommitScanner:

    def __init__(self, entries_by_snapshot_id, raw_entries_by_snapshot_id=None):
        self._by_id = entries_by_snapshot_id
        self._raw_by_id = raw_entries_by_snapshot_id or {}
        self.entry_calls = []
        self.raw_entry_calls = []

    def read_incremental_entries_from_changed_partitions(self, snapshot, _):
        self.entry_calls.append(snapshot.id)
        return self._by_id.get(
            snapshot.id, self._raw_by_id.get(snapshot.id, []))

    def read_incremental_raw_entries_from_changed_partitions(self, snapshot, _):
        self.raw_entry_calls.append(snapshot.id)
        return self._raw_by_id.get(snapshot.id, self._by_id.get(snapshot.id, []))

    def read_incremental_raw_entries_for_scope(
            self, snapshot, _entries, _index_entries=None):
        self.entry_calls.append(snapshot.id)
        self.raw_entry_calls.append(snapshot.id)
        return self._raw_by_id.get(snapshot.id, self._by_id.get(snapshot.id, []))


class _FakeBaseEntryScanner:

    def __init__(self, _full_entries, incremental_entries, fallback_entries=None):
        self._incremental_entries = incremental_entries
        self._fallback_entries = fallback_entries or []
        self.fallback_calls = 0
        self.scoped_raw_calls = []

    def read_conflict_entries(self, snapshot, entries, index_entries=None):
        self.fallback_calls += 1
        scope = CommitScanner.conflict_entry_scope(entries, index_entries)
        if scope.is_empty():
            return list(self._fallback_entries)
        return [
            entry for entry in self._fallback_entries
            if scope.matches_entry(entry)
        ]

    def read_incremental_entries_from_changed_partitions(
            self, snapshot, _entries):
        return self._incremental_entries.get(snapshot.id, [])

    def read_incremental_raw_entries_from_changed_partitions(
            self, snapshot, _entries):
        return self._incremental_entries.get(snapshot.id, [])

    def read_incremental_raw_entries_for_scope(
            self, snapshot, entries, index_entries=None):
        self.scoped_raw_calls.append(snapshot.id)
        scope = CommitScanner.conflict_entry_scope(entries, index_entries)
        return [
            entry for entry in self._incremental_entries.get(snapshot.id, [])
            if scope.matches_entry(entry)
        ]


class _FakeTable:

    def __init__(self, schema_manager):
        self.schema_manager = schema_manager


class TestRowIdWindowBaseEntries(unittest.TestCase):

    def _make_detection(self, snapshots, scanner):
        detection = ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=_FakeSnapshotManager(snapshots),
            manifest_list_manager=None,
            table=_FakeTable(_FakeSchemaManager([_DEFAULT_SCHEMA])),
            commit_scanner=scanner,
        )
        detection.set_row_id_check_from_snapshot(1)
        return detection

    def test_applies_external_snapshot_deltas(self):
        base = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        latest = _FakeSnapshot(
            2, "APPEND", commit_user="external", commit_identifier=1,
            delta_manifest_list="latest")
        base_entry = _make_entry(
            "base.parquet", first_row_id=0, row_count=100)
        added_entry = _make_entry(
            "added.parquet", first_row_id=0, row_count=100)
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]
        scanner = _FakeBaseEntryScanner(
            {"base": [base_entry]}, {2: [added_entry]})
        detection = self._make_detection([base, latest], scanner)

        self.assertEqual(
            [base_entry, added_entry],
            detection.read_row_id_base_entries(
                latest,
                window,
                planned_base_entries=[base_entry],
                planned_base_snapshot_identity=(
                    detection._snapshot_identity(base)),
            ),
        )
        self.assertEqual([2], scanner.scoped_raw_calls)
        self.assertEqual(0, scanner.fallback_calls)

    def test_fallback_rejects_index_changing_overwrite(self):
        base = _FakeSnapshot(
            1, "APPEND", next_row_id=100,
            delta_manifest_list="base", index_manifest="old-index")
        overwrite = _FakeSnapshot(
            2, "OVERWRITE", next_row_id=100,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="overwrite", index_manifest="new-index")
        base_entry = _make_entry(
            "base.parquet", first_row_id=0, row_count=100)
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]
        scanner = _FakeBaseEntryScanner(
            {"base": [base_entry]}, {2: []},
            fallback_entries=[base_entry])
        detection = self._make_detection([base, overwrite], scanner)

        with self.assertRaisesRegex(RuntimeError, "index-changing overwrite"):
            detection.read_row_id_base_entries(overwrite, window)

        self.assertEqual(0, scanner.fallback_calls)

    def test_disjoint_partition_overwrite_uses_current_state(self):
        base = _FakeSnapshot(
            1, "APPEND", next_row_id=200, delta_manifest_list="base")
        overwrite = _FakeSnapshot(
            2, "OVERWRITE", next_row_id=200,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="overwrite")
        old_entry = _make_entry(
            "old.parquet", first_row_id=0, row_count=100,
            partition=("a",))
        overwritten = _make_entry(
            "other.parquet", first_row_id=0, row_count=100,
            partition=("b",))
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100,
            partition=("a",))]

        for bounded in (False, True):
            with self.subTest(bounded=bounded):
                scanner = _FakeBaseEntryScanner(
                    {"base": [old_entry]},
                    {2: [overwritten]},
                    fallback_entries=[old_entry, overwritten],
                )
                detection = self._make_detection(
                    [base, overwrite], scanner)
                if bounded:
                    detection.enable_bounded_row_id_conflict_state()

                current = detection.read_row_id_base_entries(
                    overwrite,
                    window,
                    planned_base_entries=[old_entry],
                    planned_base_snapshot_identity=(
                        detection._snapshot_identity(base)),
                )
                result = detection.check_conflicts(
                    overwrite, current, window, "APPEND")

                self.assertIsNone(result)
                self.assertEqual([old_entry], current)
                self.assertEqual(1, scanner.fallback_calls)
                self.assertEqual([] if bounded else [2],
                                 scanner.scoped_raw_calls)

    def test_overlapping_overwrite_still_conflicts(self):
        base = _FakeSnapshot(
            1, "APPEND", next_row_id=200, delta_manifest_list="base")
        overwrite = _FakeSnapshot(
            2, "OVERWRITE", next_row_id=200,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="overwrite")
        old_entry = _make_entry(
            "old.parquet", first_row_id=0, row_count=100,
            partition=("a",), write_cols=["col_a"])
        replacement = _make_entry(
            "replacement.parquet", first_row_id=0, row_count=100,
            partition=("a",), write_cols=["col_a"])
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100,
            partition=("a",), write_cols=["col_b"])]
        scanner = _FakeBaseEntryScanner(
            {"base": [old_entry]},
            {2: []},
            fallback_entries=[replacement],
        )
        detection = self._make_detection([base, overwrite], scanner)
        detection.enable_bounded_row_id_conflict_state()

        with self.assertRaisesRegex(
                RuntimeError, "Concurrent overwrite changed row ID files"):
            detection.read_row_id_base_entries(
                overwrite,
                window,
                planned_base_entries=[old_entry],
                planned_base_snapshot_identity=(
                    detection._snapshot_identity(base)),
            )

    def test_index_changing_overwrite_checks_deletion_vectors(self):
        base = _FakeSnapshot(
            1, "APPEND", next_row_id=100,
            delta_manifest_list="base", index_manifest="old-index")
        overwrite = _FakeSnapshot(
            2, "OVERWRITE", next_row_id=100,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="overwrite", index_manifest="new-index")
        base_entry = _make_entry(
            "base.parquet", first_row_id=0, row_count=100)
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]

        for bounded in (False, True):
            for changed in (False, True):
                with self.subTest(bounded=bounded, changed=changed):
                    scanner = _FakeBaseEntryScanner(
                        {"base": [base_entry]}, {2: []},
                        fallback_entries=[base_entry])
                    detection = self._make_detection(
                        [base, overwrite], scanner)
                    detection._row_id_deletion_vectors_changed = (
                        lambda *_args, changed=changed: changed)
                    if bounded:
                        detection.enable_bounded_row_id_conflict_state()

                    if not changed:
                        self.assertEqual(
                            [base_entry],
                            detection.read_row_id_base_entries(
                                overwrite,
                                window,
                                planned_base_entries=[base_entry],
                                planned_base_snapshot_identity=(
                                    detection._snapshot_identity(base)),
                            ),
                        )
                        continue

                    with self.assertRaisesRegex(
                            RuntimeError, "deletion vectors"):
                        detection.read_row_id_base_entries(
                            overwrite,
                            window,
                            planned_base_entries=[base_entry],
                            planned_base_snapshot_identity=(
                                detection._snapshot_identity(base)),
                        )

                    self.assertEqual(0, scanner.fallback_calls)

    def test_missing_normal_dv_target_fails_closed(self):
        base = _FakeSnapshot(1, "APPEND")
        latest = _FakeSnapshot(2, "OVERWRITE")
        window = [_make_entry(
            "window.blob", first_row_id=0, row_count=1)]
        detection = self._make_detection(
            [base, latest], _FakeBaseEntryScanner({}, {}))

        self.assertTrue(detection._row_id_deletion_vectors_changed(
            base, latest, [], window))

    def test_dv_targets_use_exact_update_ranges(self):
        detection = self._make_detection(
            [_FakeSnapshot(1, "APPEND")],
            _FakeBaseEntryScanner({}, {}),
        )
        base = [_make_entry(
            "base.parquet", first_row_id=100, row_count=10)]
        window = [_make_entry(
            "update.blob", first_row_id=100, row_count=10)]

        targets = detection._row_id_deletion_vector_targets(
            base,
            window,
            {((), 0): [Range(102, 102), Range(109, 109)]},
        )

        self.assertEqual(
            {((), 0, "base.parquet"): [Range(2, 2), Range(9, 9)]},
            targets,
        )

    def test_bounded_overwrite_is_checked_for_later_windows(self):
        base = _FakeSnapshot(
            1, "APPEND", next_row_id=200, delta_manifest_list="base")
        overwrite = _FakeSnapshot(
            2, "OVERWRITE", next_row_id=200,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="overwrite")
        old_a = _make_entry(
            "old-a.parquet", first_row_id=0, row_count=100,
            partition=("a",))
        old_b = _make_entry(
            "old-b.parquet", first_row_id=100, row_count=100,
            partition=("b",))
        replacement_b = _make_entry(
            "replacement-b.parquet", first_row_id=100, row_count=100,
            partition=("b",))
        scanner = _FakeBaseEntryScanner(
            {"base": [old_a, old_b]},
            {2: []},
            fallback_entries=[old_a, replacement_b],
        )
        detection = self._make_detection([base, overwrite], scanner)
        detection.enable_bounded_row_id_conflict_state()
        window_a = [_make_entry(
            "window-a.parquet", first_row_id=0, row_count=100,
            partition=("a",))]
        window_b = [_make_entry(
            "window-b.parquet", first_row_id=100, row_count=100,
            partition=("b",))]

        current = detection.read_row_id_base_entries(
            overwrite,
            window_a,
            planned_base_entries=[old_a],
            planned_base_snapshot_identity=detection._snapshot_identity(base),
        )
        self.assertIsNone(detection.check_conflicts(
            overwrite, current, window_a, "APPEND"))

        with self.assertRaisesRegex(
                RuntimeError, "Concurrent overwrite changed row ID files"):
            detection.read_row_id_base_entries(
                overwrite,
                window_b,
                planned_base_entries=[old_b],
                planned_base_snapshot_identity=(
                    detection._snapshot_identity(base)),
            )

        self.assertEqual(2, scanner.fallback_calls)
        self.assertEqual([], detection._row_id_external_snapshots)

    def test_bounded_state_rejects_external_snapshot(self):
        base = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        latest = _FakeSnapshot(
            2, "APPEND", commit_user="external", commit_identifier=1,
            delta_manifest_list="latest")
        base_entry = _make_entry(
            "base.parquet", first_row_id=0, row_count=100)
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]
        scanner = _FakeBaseEntryScanner({"base": [base_entry]}, {})
        detection = self._make_detection([base, latest], scanner)
        detection.enable_bounded_row_id_conflict_state()

        with self.assertRaisesRegex(RuntimeError, "Concurrent commit detected"):
            detection.read_row_id_base_entries(
                latest,
                window,
                planned_base_entries=[base_entry],
                planned_base_snapshot_identity=(
                    detection._snapshot_identity(base)),
            )

        self.assertEqual([], detection._row_id_external_snapshots)
        self.assertEqual([], scanner.scoped_raw_calls)

    def test_planned_windows_do_not_scan_base_manifests(self):
        snapshot = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        planned = [
            _make_entry(
                "p{}.parquet".format(i), first_row_id=i * 100,
                row_count=100, partition=("p{}".format(i),))
            for i in range(16)
        ]
        scanner = _FakeBaseEntryScanner({"base": planned}, {})
        detection = self._make_detection([snapshot], scanner)

        for entry in planned:
            window = [_make_entry(
                "update-{}".format(entry.file.file_name),
                first_row_id=entry.file.first_row_id,
                row_count=entry.file.row_count,
                partition=tuple(entry.partition.values))]
            self.assertEqual(
                [entry],
                detection.read_row_id_base_entries(
                    snapshot,
                    window,
                    planned_base_entries=[entry],
                    planned_base_snapshot_identity=(
                        detection._snapshot_identity(snapshot)),
                ),
            )

        self.assertEqual(0, scanner.fallback_calls)
        self.assertEqual([], scanner.scoped_raw_calls)

    def test_rollback_before_base_fails_closed(self):
        base = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        latest = _FakeSnapshot(2, "APPEND", delta_manifest_list="latest")
        scanner = _FakeBaseEntryScanner({}, {})
        detection = self._make_detection([base, latest], scanner)
        detection.set_row_id_check_from_snapshot(2)
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]

        with self.assertRaisesRegex(RuntimeError, "no longer available"):
            detection.read_row_id_base_entries(
                base,
                window,
                planned_base_entries=window,
                planned_base_snapshot_identity=(
                    detection._snapshot_identity(latest)),
            )

    def test_missing_snapshot_fails_closed(self):
        base = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        latest = _FakeSnapshot(3, "APPEND", delta_manifest_list="latest")
        scanner = _FakeBaseEntryScanner({}, {})
        detection = self._make_detection([base, latest], scanner)
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]

        with self.assertRaisesRegex(RuntimeError, "snapshot 2"):
            detection.read_row_id_base_entries(
                latest,
                window,
                planned_base_entries=window,
                planned_base_snapshot_identity=(
                    detection._snapshot_identity(base)),
            )

    def test_replaced_base_snapshot_fails_closed(self):
        base = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        replacement = _FakeSnapshot(
            1, "APPEND", commit_user="external", commit_identifier=1,
            delta_manifest_list="replacement")
        scanner = _FakeBaseEntryScanner({}, {})
        detection = self._make_detection([base], scanner)
        fingerprint = detection._snapshot_identity(base)
        detection.snapshot_manager._by_id[1] = replacement
        window = [_make_entry(
            "window.parquet", first_row_id=0, row_count=100)]

        with self.assertRaisesRegex(RuntimeError, "base snapshot 1 changed"):
            detection.read_row_id_base_entries(
                replacement,
                window,
                planned_base_entries=window,
                planned_base_snapshot_identity=fingerprint,
            )

    def test_missing_planned_base_uses_scoped_scan(self):
        snapshot = _FakeSnapshot(1, "APPEND", delta_manifest_list="base")
        anchor = _make_entry(
            "base.parquet", first_row_id=0, row_count=100)
        scanner = _FakeBaseEntryScanner(
            {"base": [anchor]}, {}, fallback_entries=[anchor])
        detection = self._make_detection([snapshot], scanner)
        window = [_make_entry(
            "delta.parquet", first_row_id=20, row_count=10)]

        self.assertEqual(
            [anchor], detection.read_row_id_base_entries(snapshot, window))
        self.assertEqual(1, scanner.fallback_calls)

    def test_compaction_overlap_cannot_be_hidden_by_blob_anchor(self):
        base = _FakeSnapshot(
            1, "APPEND", next_row_id=200, delta_manifest_list="base")
        compact = _FakeSnapshot(
            2, "COMPACT", next_row_id=200, delta_manifest_list="compact")
        normal_0 = _make_entry(
            "normal-0.parquet", first_row_id=0, row_count=100,
            write_cols=["col_a"])
        normal_100 = _make_entry(
            "normal-100.parquet", first_row_id=100, row_count=100,
            write_cols=["col_a"])
        blob = _make_entry(
            "payload.blob", first_row_id=0, row_count=100,
            write_cols=["col_c"])
        compact_entries = [
            _make_entry(
                "normal-0.parquet", kind=1, first_row_id=0, row_count=100,
                write_cols=["col_a"]),
            _make_entry(
                "normal-100.parquet", kind=1, first_row_id=100,
                row_count=100, write_cols=["col_a"]),
            _make_entry(
                "merged.parquet", first_row_id=0, row_count=200,
                write_cols=["col_a"]),
        ]
        staged = [_make_entry(
            "update.parquet", first_row_id=0, row_count=100,
            write_cols=["col_b"])]
        scanner = _FakeBaseEntryScanner(
            {"base": [normal_0, normal_100, blob]},
            {2: compact_entries},
        )
        detection = self._make_detection([base, compact], scanner)

        current = detection.read_row_id_base_entries(
            compact,
            staged,
            planned_base_entries=[normal_0, normal_100, blob],
            planned_base_snapshot_identity=detection._snapshot_identity(base),
        )
        result = detection.check_conflicts(
            compact, current, staged, "APPEND")

        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))
        self.assertEqual([2], scanner.scoped_raw_calls)
        self.assertEqual(0, scanner.fallback_calls)


class TestCheckRowIdFromSnapshot(unittest.TestCase):

    def _make_detection(self, snapshots, raw_entries_by_snapshot_id):
        detection = ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=_FakeSnapshotManager(snapshots),
            manifest_list_manager=None,
            table=_FakeTable(_FakeSchemaManager([_DEFAULT_SCHEMA])),
            commit_scanner=_FakeCommitScanner({}, raw_entries_by_snapshot_id),
        )
        detection._row_id_check_from_snapshot = 1
        return detection

    def _blob_delta(self):
        return [_make_entry("d.blob", first_row_id=0, row_count=51,
                            write_cols=["col_a"])]

    def _vector_delta(self):
        return [_make_entry(
            "d.vector.parquet", first_row_id=0, row_count=51,
            write_cols=["col_a"])]

    def test_compact_blob_delete_raises_at_first_match(self):
        check_snap = _FakeSnapshot(1, "APPEND", next_row_id=200)
        compact1 = _FakeSnapshot(2, "COMPACT", next_row_id=200)
        compact2 = _FakeSnapshot(3, "COMPACT", next_row_id=200)
        entries = {
            2: [_make_entry("first.blob", kind=1, first_row_id=0, row_count=200)],
            3: [_make_entry("second.blob", kind=1, first_row_id=0, row_count=200)],
        }
        detection = self._make_detection(
            [check_snap, compact1, compact2], entries)
        result = detection.check_row_id_from_snapshot(compact2, self._blob_delta())
        self.assertIsNotNone(result)
        self.assertIn("snapshot 2", str(result))
        self.assertIn("COMPACT", str(result))

    def test_compact_vector_delete_raises(self):
        check_snap = _FakeSnapshot(1, "APPEND", next_row_id=200)
        compact_snap = _FakeSnapshot(2, "COMPACT", next_row_id=200)
        entries = {2: [_make_entry(
            "old.vector.parquet", kind=1, first_row_id=0, row_count=100)]}
        detection = self._make_detection(
            [check_snap, compact_snap], entries)

        result = detection.check_row_id_from_snapshot(
            compact_snap, self._vector_delta())

        self.assertIsNotNone(result)
        self.assertIn("COMPACT", str(result))

    def test_compact_other_file_type_does_not_raise(self):
        check_snap = _FakeSnapshot(1, "APPEND", next_row_id=200)
        compact_snap = _FakeSnapshot(2, "COMPACT", next_row_id=200)
        cases = [
            (self._blob_delta(), "old.parquet"),
            (self._vector_delta(), "old.parquet"),
            (self._vector_delta(), "old.blob"),
        ]
        for delta, deleted_file in cases:
            with self.subTest(
                    delta=delta[0].file.file_name,
                    deleted_file=deleted_file):
                compact_entries = [
                    _make_entry(
                        deleted_file, kind=1, first_row_id=0, row_count=100),
                    _make_entry(
                        "merged.parquet", kind=0, first_row_id=0,
                        row_count=200),
                ]
                detection = self._make_detection(
                    [check_snap, compact_snap], {2: compact_entries})
                self.assertIsNone(
                    detection.check_row_id_from_snapshot(compact_snap, delta))

    def test_compact_no_conflict_when_no_matching_delete(self):
        check_snap = _FakeSnapshot(1, "APPEND", next_row_id=400)
        compact_snap = _FakeSnapshot(2, "COMPACT", next_row_id=400)
        col_a_delta = self._blob_delta()
        col_b_delta = [_make_entry("d.parquet", first_row_id=0, row_count=51,
                                   write_cols=["col_b"])]
        cases = [
            ("disjoint_range", col_a_delta, [
                _make_entry("old.blob", kind=1, first_row_id=200, row_count=200),
            ]),
            ("add_only", col_a_delta, [
                _make_entry("merged.blob", kind=0, first_row_id=0, row_count=200),
            ]),
            ("other_column_shard", col_b_delta, [
                _make_entry("old.parquet", kind=1, first_row_id=0, row_count=100,
                            write_cols=["col_a"]),
            ]),
        ]
        for name, delta, compact_entries in cases:
            with self.subTest(case=name):
                detection = self._make_detection(
                    [check_snap, compact_snap], {2: compact_entries})
                self.assertIsNone(
                    detection.check_row_id_from_snapshot(compact_snap, delta))

    def test_own_history_is_not_scanned(self):
        base = _FakeSnapshot(1, "APPEND", next_row_id=1000)
        own = [
            _FakeSnapshot(
                snapshot_id,
                "APPEND",
                next_row_id=1000,
                commit_user="incremental",
                commit_identifier=snapshot_id - 1,
            )
            for snapshot_id in range(2, 9)
        ]
        manager = _FakeSnapshotManager([base] + own)
        scanner = _FakeCommitScanner({})
        detection = ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=manager,
            manifest_list_manager=None,
            table=_FakeTable(_FakeSchemaManager([_DEFAULT_SCHEMA])),
            commit_scanner=scanner,
        )
        detection.set_row_id_check_from_snapshot(1)
        detection.enable_bounded_row_id_conflict_state()
        for snapshot in own:
            detection.ignore_row_id_commit(
                snapshot.commit_user, snapshot.commit_identifier)

        delta = self._blob_delta()
        for latest in own:
            self.assertIsNone(
                detection.check_row_id_from_snapshot(latest, delta))

        self.assertEqual([], scanner.entry_calls)
        self.assertEqual(
            {"incremental": 7},
            detection._row_id_ignored_commit_high_watermarks,
        )
        for snapshot_id in range(2, 8):
            self.assertEqual(2, manager.requests.count(snapshot_id))
        self.assertEqual(1, manager.requests.count(8))

    def test_cached_external_history_checks_later_windows(self):
        base = _FakeSnapshot(1, "APPEND", next_row_id=1000)
        external = _FakeSnapshot(
            2, "APPEND", next_row_id=1000,
            commit_user="external", commit_identifier=1)
        own = _FakeSnapshot(
            3, "APPEND", next_row_id=1000,
            commit_user="incremental", commit_identifier=1)
        external_entry = _make_entry(
            "external.parquet",
            first_row_id=100,
            row_count=50,
            write_cols=["col_a"],
        )
        manager = _FakeSnapshotManager([base, external, own])
        scanner = _FakeCommitScanner({2: [external_entry]})
        detection = ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=manager,
            manifest_list_manager=None,
            table=_FakeTable(_FakeSchemaManager([_DEFAULT_SCHEMA])),
            commit_scanner=scanner,
        )
        detection.set_row_id_check_from_snapshot(1)
        detection.ignore_row_id_commit("incremental", 1)

        self.assertIsNone(
            detection.check_row_id_from_snapshot(own, self._blob_delta()))
        later_delta = [_make_entry(
            "later.blob",
            first_row_id=100,
            row_count=50,
            write_cols=["col_a"],
        )]
        result = detection.check_row_id_from_snapshot(own, later_delta)

        self.assertIsNotNone(result)
        self.assertIn("updating the same file", str(result))
        self.assertEqual([2, 2], scanner.entry_calls)

    def test_reused_snapshot_id_invalidates_cached_history(self):
        base = _FakeSnapshot(1, "APPEND", next_row_id=1000)
        own = _FakeSnapshot(
            2, "APPEND", next_row_id=1000,
            commit_user="incremental", commit_identifier=1,
            delta_manifest_list="own-manifest")
        manager = _FakeSnapshotManager([base, own])
        scanner = _FakeCommitScanner({})
        detection = ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=manager,
            manifest_list_manager=None,
            table=_FakeTable(_FakeSchemaManager([_DEFAULT_SCHEMA])),
            commit_scanner=scanner,
        )
        detection.set_row_id_check_from_snapshot(1)
        detection.ignore_row_id_commit("incremental", 1)

        delta = self._blob_delta()
        self.assertIsNone(detection.check_row_id_from_snapshot(own, delta))

        replacement = _FakeSnapshot(
            2, "APPEND", next_row_id=1000,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="external-manifest")
        manager._by_id[2] = replacement
        scanner._by_id[2] = [_make_entry(
            "external.parquet",
            first_row_id=0,
            row_count=51,
            write_cols=["col_a"],
        )]

        result = detection.check_row_id_from_snapshot(replacement, delta)

        self.assertIsNotNone(result)
        self.assertIn("updating the same file", str(result))
        self.assertEqual([2], scanner.entry_calls)

    def test_rebuilt_history_past_cursor_invalidates_cache(self):
        base = _FakeSnapshot(1, "APPEND", next_row_id=1000)
        own = _FakeSnapshot(
            2, "APPEND", next_row_id=1000,
            commit_user="incremental", commit_identifier=1,
            delta_manifest_list="own-manifest")
        manager = _FakeSnapshotManager([base, own])
        scanner = _FakeCommitScanner({})
        detection = ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=manager,
            manifest_list_manager=None,
            table=_FakeTable(_FakeSchemaManager([_DEFAULT_SCHEMA])),
            commit_scanner=scanner,
        )
        detection.set_row_id_check_from_snapshot(1)
        detection.ignore_row_id_commit("incremental", 1)

        delta = self._blob_delta()
        self.assertIsNone(detection.check_row_id_from_snapshot(own, delta))

        replacement = _FakeSnapshot(
            2, "APPEND", next_row_id=1000,
            commit_user="external", commit_identifier=1,
            delta_manifest_list="external-manifest")
        latest = _FakeSnapshot(
            3, "APPEND", next_row_id=1000,
            commit_user="external", commit_identifier=2,
            delta_manifest_list="latest-manifest")
        manager._by_id.update({2: replacement, 3: latest})
        scanner._by_id[2] = [_make_entry(
            "external.parquet",
            first_row_id=0,
            row_count=51,
            write_cols=["col_a"],
        )]

        result = detection.check_row_id_from_snapshot(latest, delta)

        self.assertIsNotNone(result)
        self.assertIn("updating the same file", str(result))
        self.assertEqual([2], scanner.entry_calls)


class TestRowIdColumnConflictChecker(unittest.TestCase):

    def _make_checker(self, delta_files, schema=None):
        schema_mgr = _FakeSchemaManager([schema or _DEFAULT_SCHEMA])
        return RowIdColumnConflictChecker.from_data_files(schema_mgr, delta_files)

    def test_no_conflict_disjoint_rows(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=200,
                               write_cols=["col_a"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_no_conflict_same_rows_different_columns(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["col_b"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_conflict_same_rows_same_columns(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["col_a"])
        self.assertTrue(checker.conflicts_with(committed))

    def test_conflict_overlapping_rows_overlapping_columns(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0,
                       write_cols=["col_a", "col_b"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=50,
                               write_cols=["col_b", "col_c"])
        self.assertTrue(checker.conflicts_with(committed))

    def test_conflict_null_write_cols_committed(self):
        """null write_cols means full-schema write — always conflicts on column dimension."""
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=None)
        self.assertTrue(checker.conflicts_with(committed))

    def test_conflict_null_write_cols_delta(self):
        """null write_cols in delta means all columns are in the write range."""
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=None),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["col_b"])
        self.assertTrue(checker.conflicts_with(committed))

    def test_no_conflict_committed_file_no_row_id(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=None,
                               write_cols=["col_a"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_none_when_no_delta_files_with_row_id(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=None),
        ]
        schema_mgr = _FakeSchemaManager([_DEFAULT_SCHEMA])
        checker = RowIdColumnConflictChecker.from_data_files(schema_mgr, delta_files)
        self.assertIsNone(checker)

    def test_system_fields_skipped(self):
        """System fields like _ROW_ID should not count as column conflicts."""
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0,
                       write_cols=["_ROW_ID", "col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["_ROW_ID", "col_b"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_cross_schema_field_id_resolution(self):
        """Fields with same ID but different names across schema versions should still match."""
        schema_v0 = _FakeSchema(
            id=0,
            fields=[
                DataField(1, "col_a", AtomicType("INT")),
                DataField(2, "col_b", AtomicType("STRING")),
            ],
        )
        schema_v1 = _FakeSchema(
            id=1,
            fields=[
                DataField(1, "col_a_renamed", AtomicType("INT")),
                DataField(2, "col_b", AtomicType("STRING")),
                DataField(3, "col_c", AtomicType("BIGINT")),
            ],
        )
        schema_mgr = _FakeSchemaManager([schema_v0, schema_v1])
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0,
                       schema_id=0, write_cols=["col_a"]),
        ]
        checker = RowIdColumnConflictChecker.from_data_files(schema_mgr, delta_files)
        committed_same_field = _make_file(
            "c1", row_count=100, first_row_id=0,
            schema_id=1, write_cols=["col_a_renamed"])
        self.assertTrue(checker.conflicts_with(committed_same_field))
        committed_diff_field = _make_file(
            "c2", row_count=100, first_row_id=0,
            schema_id=1, write_cols=["col_c"])
        self.assertFalse(checker.conflicts_with(committed_diff_field))


class TestConflictEntryScopeFromRanges(unittest.TestCase):
    """The compact row-id scope used to protect committed incremental windows
    must isolate by (partition, bucket) even though the early bucket filter can
    over-read; matches_entry is the exact filter."""

    def test_scope_isolates_by_partition_and_bucket(self):
        # Overlapping range but a different partition or bucket must not match.
        scope = _ConflictEntryScope.from_ranges({(("p1",), 0): [Range(0, 99)]})
        target = _make_entry(
            "f1", bucket=0, first_row_id=0, row_count=100, partition=["p1"])
        self.assertTrue(scope.matches_entry(target))
        for label, entry in [
            ("different partition", _make_entry(
                "f2", bucket=0, first_row_id=0, row_count=100, partition=["p2"])),
            ("different bucket", _make_entry(
                "f3", bucket=1, first_row_id=0, row_count=100, partition=["p1"])),
        ]:
            with self.subTest(label):
                self.assertFalse(scope.matches_entry(entry))

    def test_overlapping_and_adjacent_ranges_merge(self):
        scope = _ConflictEntryScope.from_ranges({
            (("p1",), 0): [Range(0, 49), Range(50, 99), Range(90, 120)],
        })
        # Overlapping + adjacent ranges collapse to a single [0, 120] span,
        # giving stable, comparable signatures for checkpoint vs latest scans.
        merged = scope._ranges[(("p1",), 0)]
        self.assertEqual(1, len(merged))
        self.assertEqual((0, 120), (merged[0].from_, merged[0].to))
        inside = _make_entry(
            "f1", bucket=0, first_row_id=100, row_count=10, partition=["p1"])
        outside = _make_entry(
            "f2", bucket=0, first_row_id=200, row_count=10, partition=["p1"])
        self.assertTrue(scope.matches_entry(inside))
        self.assertFalse(scope.matches_entry(outside))

    def test_empty_scope_reads_nothing_without_partition_fallback(self):
        # An empty protected scope must return [] -- never fall back to scanning
        # changed partitions the way read_conflict_entries does for an empty
        # commit window.
        self.assertTrue(_ConflictEntryScope.from_ranges({}).is_empty())
        scanner = CommitScanner(None, None)
        self.assertEqual([], scanner.read_entries_for_row_id_scope(None, {}))
        self.assertEqual(
            [], scanner.read_entries_for_row_id_scope(object(), {}))


if __name__ == '__main__':
    unittest.main()
