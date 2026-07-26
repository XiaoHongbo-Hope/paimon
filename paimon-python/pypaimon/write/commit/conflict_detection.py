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

"""
Conflict detection for commit operations.
"""

import bisect

from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.file_entry import FileEntry
from pypaimon.table.special_fields import SpecialFields
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper
from pypaimon.utils.roaring_bitmap import RoaringBitmap
from pypaimon.write.commit.commit_scanner import CommitScanner


class RowIdColumnConflictChecker:
    """Checks for row ID × column conflicts between delta files and committed files.

    Built from the current commit's delta files. For each committed file,
    checks whether it overlaps with the delta files on BOTH dimensions:
    row-id range AND write columns.
    """

    def __init__(self, write_ranges, schema_manager):
        self._write_ranges = write_ranges
        self._schema_manager = schema_manager
        self._field_id_cache = {}

    @classmethod
    def from_data_files(cls, schema_manager, delta_files):
        files_with_row_id = [f for f in delta_files if f.first_row_id is not None]
        if not files_with_row_id:
            return None

        range_helper = RangeHelper(lambda f: f.row_id_range())
        groups = range_helper.merge_overlapping_ranges(files_with_row_id)

        write_ranges = []
        for group in groups:
            merged_from = min(f.first_row_id for f in group)
            merged_to = max(f.first_row_id + f.row_count - 1 for f in group)
            merged_range = Range(merged_from, merged_to)

            field_ids = set()
            for f in group:
                cls._add_write_field_ids(field_ids, f, schema_manager)

            write_ranges.append(_WriteRange(merged_range, field_ids))

        write_ranges.sort(key=lambda wr: (wr.range.from_, wr.range.to))
        return cls(write_ranges, schema_manager)

    def is_empty(self):
        return len(self._write_ranges) == 0

    def conflicts_with(self, file):
        if file.first_row_id is None:
            return False

        file_range = Range(file.first_row_id, file.first_row_id + file.row_count - 1)
        index = self._first_possible_range(file_range)

        while index < len(self._write_ranges):
            wr = self._write_ranges[index]
            if wr.range.from_ > file_range.to:
                return False
            if wr.range.overlaps(file_range) and self._contains_any_write_field(wr.field_ids, file):
                return True
            index += 1

        return False

    def _first_possible_range(self, target):
        keys = [wr.range.to for wr in self._write_ranges]
        return bisect.bisect_left(keys, target.from_)

    def _contains_any_write_field(self, field_ids, file):
        if file.write_cols is None:
            return True
        for col_name in file.write_cols:
            fid = self._field_id(file, col_name)
            if fid is not None and fid in field_ids:
                return True
        return False

    def _field_id(self, file, col_name):
        if SpecialFields.is_system_field(col_name):
            return None
        name_to_id = self._field_id_by_name(file.schema_id)
        fid = name_to_id.get(col_name)
        if fid is None:
            raise RuntimeError(
                f"Column '{col_name}' not found in schema {file.schema_id}")
        return fid

    def _field_id_by_name(self, schema_id):
        if schema_id not in self._field_id_cache:
            schema = self._schema_manager.get_schema(schema_id)
            if schema is None:
                raise RuntimeError(f"Schema {schema_id} not found")
            self._field_id_cache[schema_id] = {
                field.name: field.id for field in schema.fields
            }
        return self._field_id_cache[schema_id]

    @classmethod
    def _add_write_field_ids(cls, field_ids, file, schema_manager):
        if file.write_cols is None:
            schema = schema_manager.get_schema(file.schema_id)
            if schema is not None:
                for field in schema.fields:
                    if not SpecialFields.is_system_field(field.name):
                        field_ids.add(field.id)
        else:
            name_to_id = {}
            schema = schema_manager.get_schema(file.schema_id)
            if schema is not None:
                name_to_id = {field.name: field.id for field in schema.fields}
            for col_name in file.write_cols:
                if SpecialFields.is_system_field(col_name):
                    continue
                fid = name_to_id.get(col_name)
                if fid is not None:
                    field_ids.add(fid)


class _WriteRange:

    def __init__(self, range_, field_ids):
        self.range = range_
        self.field_ids = field_ids


class CommitConflictError(RuntimeError):
    """A deterministic pre-snapshot conflict which is safe to abort."""


class RowIdPlanningConflictError(RuntimeError):
    """A row-id conflict detected before snapshot creation."""


class ConflictDetection:
    """Detects conflicts between base and delta files during commit."""

    def __init__(self, data_evolution_enabled, snapshot_manager,
                 manifest_list_manager: ManifestListManager, table, commit_scanner: CommitScanner):
        self.data_evolution_enabled = data_evolution_enabled
        self.snapshot_manager = snapshot_manager
        self.manifest_list_manager = manifest_list_manager
        self.table = table
        self._row_id_check_from_snapshot = None
        self._row_id_ignored_commit_high_watermarks = {}
        self._row_id_history_base_snapshot = None
        self._row_id_history_cursor = None
        self._row_id_history_cursor_identity = None
        self._row_id_external_snapshots = []
        self._row_id_window_changes = None
        self._row_id_overwrite_seen = False
        self._row_id_index_overwrite_seen = False
        self._row_id_index_manifest_cache = {}
        self._bounded_row_id_conflict_state = False
        self.commit_scanner = commit_scanner

    def should_be_overwrite_commit(self, append_file_entries=None, append_index_files=None):
        for entry in append_file_entries or []:
            if entry.kind == 1:
                return True
        for entry in append_index_files or []:
            if entry.index_file.index_type == IndexManifestFile.DELETION_VECTORS_INDEX:
                return True
        return False

    def has_row_id_check_from_snapshot(self):
        return self._row_id_check_from_snapshot is not None

    def set_row_id_check_from_snapshot(self, snapshot_id):
        if self._row_id_check_from_snapshot == snapshot_id:
            return
        self._row_id_check_from_snapshot = snapshot_id
        self.reset_row_id_history()

    def reset_row_id_history(self):
        """Clear cached row-id history."""
        self._row_id_history_base_snapshot = None
        self._row_id_history_cursor = None
        self._row_id_history_cursor_identity = None
        self._row_id_external_snapshots = []
        self._row_id_window_changes = None
        self._row_id_overwrite_seen = False
        self._row_id_index_overwrite_seen = False
        self._row_id_index_manifest_cache = {}

    def clear_row_id_window_changes(self):
        self._row_id_window_changes = None

    def enable_bounded_row_id_conflict_state(self):
        self._bounded_row_id_conflict_state = True

    def ignore_row_id_commit(self, commit_user, commit_identifier):
        """Skip a disjoint commit in later checks."""
        current = self._row_id_ignored_commit_high_watermarks.get(commit_user)
        if current is None or commit_identifier > current:
            self._row_id_ignored_commit_high_watermarks[commit_user] = commit_identifier

    def read_row_id_base_entries(self, latest_snapshot, commit_entries,
                                 index_entries=None, planned_base_entries=None,
                                 planned_base_snapshot_identity=None,
                                 planned_row_id_update_ranges=None):
        """Read the current entries relevant to one row-id commit window."""
        base_snapshot_id = self._row_id_check_from_snapshot
        if base_snapshot_id is None:
            return self.commit_scanner.read_conflict_entries(
                latest_snapshot, commit_entries, index_entries)
        base_snapshot = self.snapshot_manager.get_snapshot_by_id(
            base_snapshot_id)
        if base_snapshot is None or latest_snapshot.id < base_snapshot_id:
            raise RowIdPlanningConflictError(
                "Row ID conflict check base snapshot {} is no longer "
                "available.".format(base_snapshot_id))
        complete_evidence = (
            planned_base_entries is not None
            and planned_base_snapshot_identity is not None
        )
        if (complete_evidence
                and self._snapshot_identity(base_snapshot)
                != planned_base_snapshot_identity):
            raise RowIdPlanningConflictError(
                "Row ID conflict check base snapshot {} changed.".format(
                    base_snapshot_id))

        self._row_id_window_changes = None
        if not complete_evidence:
            self._row_id_history_snapshots(latest_snapshot)
            has_row_id_additions = any(
                entry.kind == 0 and entry.file.row_id_range() is not None
                for entry in commit_entries
            )
            if (self._row_id_index_overwrite_seen
                    and has_row_id_additions):
                raise RowIdPlanningConflictError(
                    "Cannot validate an index-changing overwrite without "
                    "planned row ID base files.")
            if (self._bounded_row_id_conflict_state
                    and self._row_id_overwrite_seen):
                raise RowIdPlanningConflictError(
                    "Cannot validate a concurrent overwrite without "
                    "planned row ID base files.")
            return self.commit_scanner.read_conflict_entries(
                latest_snapshot, commit_entries, index_entries)

        if self._bounded_row_id_conflict_state:
            self._row_id_history_snapshots(latest_snapshot)
            self._validate_row_id_deletion_vectors(
                base_snapshot, latest_snapshot, planned_base_entries,
                commit_entries, planned_row_id_update_ranges)
            if self._row_id_overwrite_seen:
                return self._read_current_row_id_entries(
                    latest_snapshot,
                    commit_entries,
                    index_entries,
                    planned_base_entries,
                )

        entries = list(planned_base_entries)
        changes = self._row_id_changes(
            latest_snapshot, commit_entries, index_entries, cache_result=True)
        self._validate_row_id_deletion_vectors(
            base_snapshot, latest_snapshot, planned_base_entries,
            commit_entries, planned_row_id_update_ranges)
        for snapshot, raw_entries in changes:
            if snapshot.commit_kind == "OVERWRITE":
                return self.commit_scanner.read_conflict_entries(
                    latest_snapshot, commit_entries, index_entries)
            entries.extend(raw_entries)
        try:
            return FileEntry.merge_entries(entries)
        except RuntimeError as error:
            raise RowIdPlanningConflictError(str(error)) from error

    def _read_current_row_id_entries(
            self, latest_snapshot, commit_entries, index_entries,
            planned_base_entries):
        current = self.commit_scanner.read_conflict_entries(
            latest_snapshot, commit_entries, index_entries)
        scope = CommitScanner.conflict_entry_scope(
            commit_entries, index_entries)
        planned = (
            list(planned_base_entries)
            if scope.is_empty()
            else [
                entry for entry in planned_base_entries
                if scope.matches_entry(entry)
            ]
        )
        if (self._row_id_entry_signatures(planned)
                != self._row_id_entry_signatures(current)):
            raise RowIdPlanningConflictError(
                "Concurrent overwrite changed row ID files for the current "
                "update window.")

        self._row_id_external_snapshots = []
        self._row_id_window_changes = (
            self._row_id_change_key(latest_snapshot, commit_entries), [])
        return current

    @staticmethod
    def _row_id_entry_signatures(entries):
        return {
            (
                entry.identifier(),
                entry.file.first_row_id,
                entry.file.row_count,
                entry.file.schema_id,
                tuple(entry.file.write_cols)
                if entry.file.write_cols is not None else None,
            )
            for entry in entries
            if entry.kind == 0
        }

    def _row_id_changes(self, latest_snapshot, commit_entries,
                        index_entries=None, cache_result=False):
        key = self._row_id_change_key(latest_snapshot, commit_entries)
        cached = self._row_id_window_changes
        if cached is not None and cached[0] == key:
            return cached[1]

        def changes():
            for snapshot in self._row_id_history_snapshots(latest_snapshot):
                yield (
                    snapshot,
                    self.commit_scanner.read_incremental_raw_entries_for_scope(
                        snapshot, commit_entries, index_entries),
                )

        if not cache_result:
            return changes()
        result = list(changes())
        self._row_id_window_changes = (key, result)
        return result

    def _row_id_change_key(self, latest_snapshot, commit_entries):
        return (
            self._snapshot_identity(latest_snapshot),
            tuple(
                (
                    tuple(entry.partition.values),
                    entry.bucket,
                    entry.kind,
                    entry.file.file_name,
                    entry.file.first_row_id,
                    entry.file.row_count,
                )
                for entry in commit_entries
            ),
        )

    def _row_id_history_snapshots(self, latest_snapshot):
        """Cache history except disjoint commits."""
        base_snapshot = self._row_id_check_from_snapshot
        reset_history = (
            self._row_id_history_base_snapshot != base_snapshot
            or self._row_id_history_cursor is None
            or latest_snapshot.id < self._row_id_history_cursor
        )
        if not reset_history:
            cursor_snapshot = (
                latest_snapshot
                if latest_snapshot.id == self._row_id_history_cursor
                else self.snapshot_manager.get_snapshot_by_id(
                    self._row_id_history_cursor)
            )
            reset_history = (
                self._snapshot_identity(cursor_snapshot)
                != self._row_id_history_cursor_identity
            )

        if reset_history:
            self._row_id_history_base_snapshot = base_snapshot
            self._row_id_history_cursor = base_snapshot
            self._row_id_history_cursor_identity = None
            self._row_id_external_snapshots = []
            self._row_id_overwrite_seen = False
            self._row_id_index_overwrite_seen = False
            self._row_id_index_manifest_cache = {}

        # Snapshot files below the cursor are immutable. A rollback replaces the
        # whole tail, including the cursor, whose identity check above resets cache.
        for snapshot_id in range(
                self._row_id_history_cursor + 1,
                latest_snapshot.id + 1):
            snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                raise RowIdPlanningConflictError(
                    "Row ID conflict check snapshot {} is no longer "
                    "available.".format(snapshot_id))
            commit_user = getattr(snapshot, "commit_user", None)
            commit_identifier = getattr(snapshot, "commit_identifier", None)
            ignored_through = self._row_id_ignored_commit_high_watermarks.get(
                commit_user)
            if (ignored_through is None
                    or commit_identifier is None
                    or commit_identifier > ignored_through):
                if (snapshot.commit_kind == "OVERWRITE"
                        and self._index_manifest_changed(snapshot)):
                    self._row_id_index_overwrite_seen = True
                if self._bounded_row_id_conflict_state:
                    if snapshot.commit_kind == "OVERWRITE":
                        self._row_id_overwrite_seen = True
                    else:
                        raise RowIdPlanningConflictError(
                            "Concurrent commit detected during incremental "
                            "update_by_row_id.")
                else:
                    self._row_id_external_snapshots.append(snapshot)

        self._row_id_history_cursor = latest_snapshot.id
        self._row_id_history_cursor_identity = self._snapshot_identity(
            latest_snapshot)
        return self._row_id_external_snapshots

    def _index_manifest_changed(self, snapshot):
        previous = self.snapshot_manager.get_snapshot_by_id(snapshot.id - 1)
        if previous is None:
            return True
        return (getattr(previous, "index_manifest", None)
                != getattr(snapshot, "index_manifest", None))

    def _validate_row_id_deletion_vectors(
            self, base_snapshot, latest_snapshot, base_entries,
            commit_entries, row_id_update_ranges=None):
        if (self._row_id_index_overwrite_seen
                and self._row_id_deletion_vectors_changed(
                    base_snapshot, latest_snapshot, base_entries,
                    commit_entries, row_id_update_ranges)):
            raise RowIdPlanningConflictError(
                "Concurrent overwrite changed deletion vectors for the "
                "current update window.")

    def _row_id_deletion_vectors_changed(
            self, base_snapshot, latest_snapshot, base_entries,
            commit_entries, row_id_update_ranges=None):
        targets = self._row_id_deletion_vector_targets(
            base_entries, commit_entries, row_id_update_ranges)
        if not targets:
            return any(
                entry.kind == 0 and entry.file.row_id_range() is not None
                for entry in commit_entries
            )

        base_files = self._deletion_vector_files(base_snapshot)
        latest_files = self._deletion_vector_files(latest_snapshot)
        manifest_names = {
            getattr(snapshot, "index_manifest", None)
            for snapshot in (base_snapshot, latest_snapshot)
        }
        self._row_id_index_manifest_cache = {
            name: self._row_id_index_manifest_cache[name]
            for name in manifest_names
            if name in self._row_id_index_manifest_cache
        }

        for target, ranges in targets.items():
            base_file = base_files.get(target)
            latest_file = latest_files.get(target)
            if base_file == latest_file:
                continue
            scope = RoaringBitmap()
            for range_ in ranges:
                scope.add_range(range_.from_, range_.to)
            base_bitmap = (
                RoaringBitmap()
                if base_file is None
                else DeletionVector.read(
                    self.table.file_io, base_file).bit_map()
            )
            latest_bitmap = (
                RoaringBitmap()
                if latest_file is None
                else DeletionVector.read(
                    self.table.file_io, latest_file).bit_map()
            )
            if (RoaringBitmap.and_(base_bitmap, scope)
                    != RoaringBitmap.and_(latest_bitmap, scope)):
                return True
        return False

    def _row_id_deletion_vector_targets(
            self, base_entries, commit_entries,
            row_id_update_ranges=None):
        update_ranges = row_id_update_ranges
        if update_ranges is None:
            update_ranges = {}
            for entry in commit_entries:
                row_range = entry.file.row_id_range()
                if entry.kind != 0 or row_range is None:
                    continue
                key = (tuple(entry.partition.values), entry.bucket)
                update_ranges.setdefault(key, []).append(row_range)
            update_ranges = {
                key: Range.sort_and_merge_overlap(ranges, True, True)
                for key, ranges in update_ranges.items()
            }

        targets = {}
        for entry in base_entries:
            if (entry.kind != 0
                    or self._is_dedicated_file(entry.file.file_name)):
                continue
            base_range = entry.file.row_id_range()
            if base_range is None:
                continue
            key = (tuple(entry.partition.values), entry.bucket)
            for update_range in update_ranges.get(key, ()):
                if not base_range.overlaps(update_range):
                    continue
                target = key + (entry.file.file_name,)
                targets.setdefault(target, []).append(Range(
                    max(base_range.from_, update_range.from_)
                    - base_range.from_,
                    min(base_range.to, update_range.to)
                    - base_range.from_,
                ))
        return {
            key: Range.sort_and_merge_overlap(ranges, True, True)
            for key, ranges in targets.items()
        }

    def _deletion_vector_files(self, snapshot):
        manifest_name = getattr(snapshot, "index_manifest", None)
        if manifest_name is None:
            return {}
        if manifest_name in self._row_id_index_manifest_cache:
            return self._row_id_index_manifest_cache[manifest_name]

        index_path = self.table.path_factory().index_path()
        result = {}
        for entry in IndexManifestFile(self.table).read(manifest_name):
            index_file = entry.index_file
            if (entry.kind != 0
                    or index_file.index_type
                    != IndexManifestFile.DELETION_VECTORS_INDEX):
                continue
            path = index_file.external_path or (
                f"{index_path}/{index_file.file_name}")
            for data_file_name, meta in (index_file.dv_ranges or {}).items():
                result[(
                    tuple(entry.partition.values),
                    entry.bucket,
                    data_file_name,
                )] = DeletionFile(
                    dv_index_path=path,
                    offset=meta.offset,
                    length=meta.length,
                    cardinality=meta.cardinality,
                )
        self._row_id_index_manifest_cache[manifest_name] = result
        return result

    @staticmethod
    def _snapshot_identity(snapshot):
        if snapshot is None:
            return None
        return (
            snapshot.id,
            getattr(snapshot, "commit_user", None),
            getattr(snapshot, "commit_identifier", None),
            getattr(snapshot, "commit_kind", None),
            getattr(snapshot, "time_millis", None),
            getattr(snapshot, "base_manifest_list", None),
            getattr(snapshot, "delta_manifest_list", None),
            getattr(snapshot, "changelog_manifest_list", None),
            getattr(snapshot, "index_manifest", None),
        )

    @staticmethod
    def has_global_index_additions(index_entries=None):
        return bool(ConflictDetection.global_index_file_additions(index_entries))

    @staticmethod
    def has_hash_index_changes(index_entries=None):
        return any(
            entry.index_file.index_type == IndexManifestFile.HASH_INDEX
            for entry in (index_entries or [])
        )

    def check_conflicts(
            self,
            latest_snapshot,
            base_entries,
            delta_entries,
            commit_kind,
            delta_index_entries=None):
        try:
            FileEntry.merge_entries(delta_entries)
        except Exception as e:
            return RuntimeError(
                "File deletion conflicts detected! Give up committing. " + str(e))

        all_entries = list(base_entries) + list(delta_entries)
        try:
            merged_entries = FileEntry.merge_entries(all_entries)
        except Exception as e:
            return RuntimeError(
                "File deletion conflicts detected! Give up committing. " + str(e))

        for entry in merged_entries:
            if entry.kind == 1:
                return RuntimeError(
                    "File deletion conflicts detected! Give up committing. "
                    "Trying to delete file {} which is not previously added.".format(
                        entry.file.file_name))

        conflict = self.check_overwrite_from_snapshot(
            latest_snapshot, delta_entries, commit_kind)
        if conflict is not None:
            return conflict

        conflict = self.check_deletion_vector_index_conflicts(
            latest_snapshot, delta_index_entries, base_entries, delta_entries)
        if conflict is not None:
            return conflict

        conflict = self.check_hash_index_conflicts(
            latest_snapshot, delta_index_entries)
        if conflict is not None:
            return conflict

        if commit_kind != "COMPACT":
            next_row_id = latest_snapshot.next_row_id if latest_snapshot else None
            conflict = self.check_row_id_existence(
                base_entries, delta_entries, next_row_id)
            if conflict is not None:
                return conflict

        conflict = self.check_row_id_range_conflicts(commit_kind, merged_entries)
        if conflict is not None:
            return conflict

        conflict = self.check_global_index_row_id_existence(
            base_entries, delta_index_entries)
        if conflict is not None:
            return conflict

        return self.check_row_id_from_snapshot(latest_snapshot, delta_entries)

    def check_hash_index_conflicts(
            self, latest_snapshot, delta_index_entries=None):
        """Detect stale full-file replacements of dynamic-bucket HASH indexes."""
        hash_entries = [
            entry for entry in (delta_index_entries or [])
            if entry.index_file.index_type == IndexManifestFile.HASH_INDEX
        ]
        if not hash_entries:
            return None

        delete_entries = [entry for entry in hash_entries if entry.kind == 1]
        add_entries = [entry for entry in hash_entries if entry.kind == 0]
        delete_names = {
            entry.index_file.file_name for entry in delete_entries
        }

        current_entries = []
        if latest_snapshot is not None and latest_snapshot.index_manifest is not None:
            current_entries = [
                entry for entry in IndexManifestFile(self.table).read(
                    latest_snapshot.index_manifest)
                if entry.kind == 0
                and entry.index_file.index_type == IndexManifestFile.HASH_INDEX
            ]

        current_names = {
            entry.index_file.file_name for entry in current_entries
        }
        for delete in delete_entries:
            if delete.index_file.file_name not in current_names:
                return RuntimeError(
                    "HASH index conflict detected: index file {} is not "
                    "present in the latest snapshot.".format(
                        delete.index_file.file_name
                    )
                )

        additions_by_bucket = {}
        for add in add_entries:
            key = (tuple(add.partition.values), add.bucket)
            previous_add = additions_by_bucket.get(key)
            if previous_add is not None:
                return RuntimeError(
                    "HASH index conflict detected: multiple index files {} "
                    "and {} were added for partition {}, bucket {} in one "
                    "commit.".format(
                        previous_add.index_file.file_name,
                        add.index_file.file_name,
                        key[0],
                        key[1],
                    )
                )
            additions_by_bucket[key] = add

            retained = [
                entry for entry in current_entries
                if entry.index_file.file_name not in delete_names
                and tuple(entry.partition.values) == key[0]
                and entry.bucket == key[1]
            ]
            if retained:
                return RuntimeError(
                    "HASH index conflict detected: partition {}, bucket {} "
                    "already has newer index file {}.".format(
                        key[0],
                        key[1],
                        retained[0].index_file.file_name,
                    )
                )

        return None

    def check_deletion_vector_index_conflicts(self,
                                              latest_snapshot,
                                              delta_index_entries=None,
                                              base_entries=None,
                                              delta_entries=None):
        dv_entries = [
            entry for entry in (delta_index_entries or [])
            if entry.index_file.index_type == IndexManifestFile.DELETION_VECTORS_INDEX
        ]
        if not dv_entries:
            return None

        delete_entries = [entry for entry in dv_entries if entry.kind == 1]
        add_entries = [entry for entry in dv_entries if entry.kind == 0]
        delete_names = {entry.index_file.file_name for entry in delete_entries}

        current_entries = []
        if latest_snapshot is not None and latest_snapshot.index_manifest is not None:
            current_entries = [
                entry for entry in IndexManifestFile(self.table).read(
                    latest_snapshot.index_manifest)
                if entry.kind == 0
                and entry.index_file.index_type == IndexManifestFile.DELETION_VECTORS_INDEX
            ]

        current_names = {entry.index_file.file_name for entry in current_entries}
        for delete in delete_entries:
            if delete.index_file.file_name not in current_names:
                return RuntimeError(
                    "Deletion vector index conflict detected: index file {} "
                    "is not present in the latest snapshot.".format(
                        delete.index_file.file_name))

        existing_data_files = {
            (tuple(entry.partition.values), entry.bucket, entry.file.file_name)
            for entry in list(base_entries or []) + list(delta_entries or [])
            if entry.kind == 0
        }
        affected_files = []
        for add in add_entries:
            for data_file_name in self._deletion_vector_data_file_names(add.index_file):
                affected_files.append((add.partition, add.bucket, data_file_name))

        missing_files = {
            (tuple(partition.values), bucket, data_file_name)
            for partition, bucket, data_file_name in affected_files
        }.difference(existing_data_files)
        if missing_files and latest_snapshot is not None:
            current = self.commit_scanner.read_conflict_entries(
                latest_snapshot, [], add_entries)
            existing_data_files.update(
                (tuple(entry.partition.values), entry.bucket,
                 entry.file.file_name)
                for entry in current
                if entry.kind == 0
            )

        for partition, bucket, data_file_name in affected_files:
            data_file_key = (tuple(partition.values), bucket, data_file_name)
            if data_file_key not in existing_data_files:
                return RuntimeError(
                    "Deletion vector index conflict detected: data file {} "
                    "is not present in the latest snapshot.".format(
                        data_file_name))

            for current in current_entries:
                if current.index_file.file_name in delete_names:
                    continue
                if current.partition != partition or current.bucket != bucket:
                    continue
                if data_file_name in self._deletion_vector_data_file_names(current.index_file):
                    return RuntimeError(
                        "Deletion vector index conflict detected: data file {} "
                        "already has a newer deletion vector index file {}.".format(
                            data_file_name, current.index_file.file_name))

        return None

    @staticmethod
    def _deletion_vector_data_file_names(index_file):
        return [
            meta.data_file_name
            for meta in (index_file.dv_ranges or {}).values()
        ]

    def check_global_index_row_id_existence(self, base_entries, delta_index_entries=None):
        if not self.data_evolution_enabled:
            return None

        indexes_to_check = self.global_index_file_additions(delta_index_entries)
        if not indexes_to_check:
            return None

        data_ranges = {}
        for entry in base_entries or []:
            row_range = entry.file.row_id_range()
            if entry.kind == 0 and row_range is not None:
                key = (tuple(entry.partition.values), entry.bucket)
                data_ranges.setdefault(key, []).append(row_range)

        data_ranges = {
            key: Range.sort_and_merge_overlap(ranges, True, True)
            for key, ranges in data_ranges.items()
        }

        for index_entry in indexes_to_check:
            global_index = index_entry.index_file.global_index_meta
            index_range = Range(
                global_index.row_range_start,
                global_index.row_range_end,
            )
            key = (tuple(index_entry.partition.values), index_entry.bucket)
            if index_range.exclude(data_ranges.get(key, [])):
                return RuntimeError(
                    "Global index row ID existence conflict: index file '{}' "
                    "references row range {}, but this range is not fully "
                    "covered by current data files. The referenced row IDs "
                    "may have been reassigned or removed by a concurrent "
                    "commit.".format(index_entry.index_file.file_name, index_range))

        return None

    @staticmethod
    def global_index_file_additions(index_entries=None):
        return [
            entry for entry in (index_entries or [])
            if entry.kind == 0 and entry.index_file.global_index_meta is not None
        ]

    def check_overwrite_from_snapshot(self, latest_snapshot, delta_entries, commit_kind):
        if commit_kind != "OVERWRITE":
            return None
        if self._row_id_check_from_snapshot is None:
            return None
        if latest_snapshot is None or latest_snapshot.id <= self._row_id_check_from_snapshot:
            return None
        if not any(entry.kind == 1 for entry in delta_entries):
            return None

        check_snapshot = self.snapshot_manager.get_snapshot_by_id(
            self._row_id_check_from_snapshot)
        if check_snapshot is None:
            return RuntimeError(
                "Overwrite conflict detected: base snapshot {} cannot be found.".format(
                    self._row_id_check_from_snapshot))

        for snapshot_id in range(
                self._row_id_check_from_snapshot + 1,
                latest_snapshot.id + 1):
            snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                return RuntimeError(
                    "Overwrite conflict detected: snapshot {} cannot be found.".format(
                        snapshot_id))
            incremental_entries = (
                self.commit_scanner.read_incremental_raw_entries_from_changed_partitions(
                    snapshot, delta_entries))
            if incremental_entries:
                return RuntimeError(
                    "Overwrite conflict detected: target partitions were modified "
                    "after snapshot {}.".format(self._row_id_check_from_snapshot))

        return None

    def check_row_id_existence(self, base_entries, delta_entries, next_row_id=None):
        if not self.data_evolution_enabled:
            return None

        if next_row_id is None:
            return None

        files_to_check = [
            entry for entry in delta_entries
            if entry.kind == 0
            and entry.file.first_row_id is not None
            and entry.file.first_row_id < next_row_id
        ]

        if not files_to_check:
            return None

        existing_index = set()
        existing_ranges = {}
        for base in base_entries:
            if (base.kind != 0
                    or base.file.first_row_id is None
                    or self._is_dedicated_file(base.file.file_name)):
                continue
            existing_index.add((
                base.partition, base.bucket,
                base.file.first_row_id, base.file.row_count))
            existing_ranges.setdefault((base.partition, base.bucket), []).append(
                base.file.row_id_range())

        existing_ranges = {
            key: Range.sort_and_merge_overlap(ranges, True, True)
            for key, ranges in existing_ranges.items()
        }

        for entry in files_to_check:
            if self._is_dedicated_file(entry.file.file_name):
                base_ranges = existing_ranges.get((entry.partition, entry.bucket), [])
                if not entry.file.row_id_range().exclude(base_ranges):
                    continue

            key = (entry.partition, entry.bucket,
                   entry.file.first_row_id, entry.file.row_count)
            if key not in existing_index:
                return RuntimeError(
                    "Row ID existence conflict: file '{}' references "
                    "firstRowId={}, rowCount={} in bucket {}, "
                    "but no matching file exists in the current snapshot. "
                    "The referenced file may have been rewritten by a "
                    "concurrent compaction or removed by an overwrite.".format(
                        entry.file.file_name,
                        entry.file.first_row_id,
                        entry.file.row_count,
                        entry.bucket))

        return None

    def check_row_id_range_conflicts(self, commit_kind, commit_entries):
        if not self.data_evolution_enabled:
            return None
        if self._row_id_check_from_snapshot is None and commit_kind != "COMPACT":
            return None

        entries_with_row_id = [
            entry for entry in commit_entries
            if entry.file.first_row_id is not None
        ]

        if not entries_with_row_id:
            return None

        range_helper = RangeHelper(lambda entry: entry.file.row_id_range())
        data_files = [
            entry for entry in entries_with_row_id
            if not self._is_dedicated_file(entry.file.file_name)
        ]

        conflict = self._check_data_file_row_id_range_conflicts(
            range_helper, data_files)
        if conflict is not None:
            return conflict

        dedicated_files = [
            entry for entry in entries_with_row_id
            if self._is_dedicated_file(entry.file.file_name)
        ]
        conflict = self._check_dedicated_file_row_id_range_conflicts(
            data_files, dedicated_files)
        if conflict is not None:
            return conflict

        return None

    def _check_data_file_row_id_range_conflicts(self, range_helper, data_files):
        for data_file_group in range_helper.merge_overlapping_ranges(data_files):
            if not range_helper.are_all_ranges_same(data_file_group):
                file_descriptions = [
                    self._file_description(entry) for entry in data_file_group
                ]
                return RuntimeError(
                    "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                    "operations have encountered conflicts, data files: "
                    + str(file_descriptions))
        return None

    def _check_dedicated_file_row_id_range_conflicts(
            self, data_files, dedicated_files):
        if not dedicated_files:
            return None

        data_ranges = self._data_file_row_ranges(data_files)

        for dedicated_file in dedicated_files:
            dedicated_range = dedicated_file.file.row_id_range()
            if any(self._contains(row_range, dedicated_range) for row_range in data_ranges):
                continue

            intersecting_ranges = [
                row_range for row_range in data_ranges
                if row_range.overlaps(dedicated_range)
            ]
            intersecting_files = [
                self._file_description(entry)
                for entry in data_files
                if entry.file.row_id_range().overlaps(dedicated_range)
            ]
            conflict_reason = (
                "spans multiple data file ranges"
                if len(intersecting_ranges) > 1
                else "is not covered by one data file range"
            )
            return RuntimeError(
                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                "operations have encountered conflicts, dedicated file "
                "{file} {row_range} {reason}: {groups}".format(
                    file=self._file_description(dedicated_file),
                    row_range=dedicated_range,
                    reason=conflict_reason,
                    groups=intersecting_files))

        return None

    @staticmethod
    def _data_file_row_ranges(data_files):
        return Range.sort_and_merge_overlap(
            [entry.file.row_id_range() for entry in data_files],
            True,
            False,
        )

    @staticmethod
    def _contains(container, row_range):
        return container.from_ <= row_range.from_ and container.to >= row_range.to

    @staticmethod
    def _is_dedicated_file(file_name):
        return (DataFileMeta.is_blob_file(file_name)
                or DataFileMeta.is_vector_file(file_name))

    @staticmethod
    def _file_description(entry):
        return "{name}(rowId={row_id}, count={count})".format(
            name=entry.file.file_name,
            row_id=entry.file.first_row_id,
            count=entry.file.row_count,
        )

    def check_row_id_from_snapshot(self, latest_snapshot, commit_entries):
        if not self.data_evolution_enabled:
            return None
        if self._row_id_check_from_snapshot is None:
            return None

        delta_files = [entry.file for entry in commit_entries]
        column_checker = RowIdColumnConflictChecker.from_data_files(
            self.table.schema_manager, delta_files)
        if column_checker is None or column_checker.is_empty():
            return None

        check_snapshot = self.snapshot_manager.get_snapshot_by_id(
            self._row_id_check_from_snapshot)
        if check_snapshot is None or check_snapshot.next_row_id is None:
            raise RuntimeError(
                "Next row id cannot be null for snapshot "
                "{snapshot}.".format(snapshot=self._row_id_check_from_snapshot))
        check_next_row_id = check_snapshot.next_row_id

        # Pair each delta with its anchor file type so a parquet-only
        # compact does not flag a blob delta whose .blob anchor is intact.
        delta_signatures = []
        for f in delta_files:
            r = f.row_id_range()
            if r is not None:
                delta_signatures.append(
                    (self._file_kind(f.file_name), r.from_, r.to))

        try:
            for snapshot, raw_entries in self._row_id_changes(
                    latest_snapshot, commit_entries):
                if snapshot.commit_kind == "COMPACT":
                    err = self._compact_conflicts_with_delta(
                        snapshot, delta_signatures, column_checker, raw_entries)
                    if err is not None:
                        return err
                    continue

                for entry in FileEntry.merge_entries(raw_entries):
                    if entry.kind != 0:
                        continue
                    file_range = entry.file.row_id_range()
                    if file_range is None:
                        continue
                    if (file_range.from_ < check_next_row_id
                            and column_checker.conflicts_with(entry.file)):
                        return RuntimeError(
                            "For Data Evolution table, multiple 'MERGE INTO' "
                            "operations have encountered conflicts, updating "
                            "the same file, which can render some updates "
                            "ineffective.")

            return None
        finally:
            self._row_id_window_changes = None

    def _compact_conflicts_with_delta(self, snapshot, delta_signatures,
                                      column_checker, raw_entries):
        """Return RuntimeError if a COMPACT snapshot deleted a same-kind
        anchor file whose row-id range AND write columns overlap any
        staged delta; otherwise None.

        File-type match guards against `write_cols=None` ambiguity (an
        initial full-row parquet does not actually contain blob columns);
        column_checker guards against unrelated column-write shards
        (compacting an f1-only parquet must not block an f2 update on
        the same row range).
        """
        if not delta_signatures:
            return None
        for entry in raw_entries:
            if entry.kind != 1:
                continue
            file_range = entry.file.row_id_range()
            if file_range is None:
                continue
            deleted_kind = self._file_kind(entry.file.file_name)
            for delta_kind, from_, to in delta_signatures:
                if delta_kind != deleted_kind:
                    continue
                if file_range.from_ > to or from_ > file_range.to:
                    continue
                if not column_checker.conflicts_with(entry.file):
                    continue
                return RuntimeError(
                    "Blob/row-id update conflicts with concurrent COMPACT "
                    "(snapshot {sid}): anchor file {name} [{ff}, {ft}] "
                    "was compacted away, overlaps staged delta "
                    "[{df}, {dt}].".format(
                        sid=snapshot.id,
                        name=entry.file.file_name,
                        ff=file_range.from_,
                        ft=file_range.to,
                        df=from_,
                        dt=to))
        return None

    @staticmethod
    def _file_kind(file_name):
        if DataFileMeta.is_blob_file(file_name):
            return "blob"
        if DataFileMeta.is_vector_file(file_name):
            return "vector"
        return "normal"
