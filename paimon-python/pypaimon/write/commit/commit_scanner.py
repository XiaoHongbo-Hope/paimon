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
Manifest entries scanner for commit operations.
"""
import bisect
import os
from typing import Optional, List

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.file_scanner import (
    FileScanner,
    _filter_manifest_files_by_row_ranges,
)
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.utils.range import Range


class _ConflictEntryScope:
    """Manifest entries which can interact with one commit window."""

    def __init__(self, entries, index_entries=None):
        ranges = {}
        file_names = {}

        for entry in entries or []:
            key = self._entry_key(entry)
            row_range = entry.file.row_id_range()
            if row_range is None:
                file_names.setdefault(key, set()).add(entry.file.file_name)
            else:
                ranges.setdefault(key, []).append(row_range)

        for entry in index_entries or []:
            if entry.kind != 0:
                continue
            key = self._entry_key(entry)
            index_file = entry.index_file
            if index_file.index_type == IndexManifestFile.DELETION_VECTORS_INDEX:
                file_names.setdefault(key, set()).update(
                    (index_file.dv_ranges or {}).keys())
            global_index = index_file.global_index_meta
            if global_index is not None:
                ranges.setdefault(key, []).append(Range(
                    global_index.row_range_start,
                    global_index.row_range_end,
                ))

        self._init_from_ranges(ranges, file_names)

    @classmethod
    def from_ranges(cls, ranges_by_key):
        """Build a scope from a compact ``{(partition, bucket): [Range]}`` map.

        Used to protect an already-committed cumulative row-id scope without
        keeping the committed windows' ManifestEntry/DataFileMeta resident --
        only the merged non-contiguous ranges are retained. There are no
        file-name-only entries (row-id file groups always carry a range).
        """
        scope = cls.__new__(cls)
        scope._init_from_ranges(
            {key: list(values) for key, values in ranges_by_key.items()}, {})
        return scope

    def _init_from_ranges(self, ranges, file_names):
        self._ranges = {
            key: Range.sort_and_merge_overlap(values, True, True)
            for key, values in ranges.items()
        }
        self._range_ends = {
            key: [row_range.to for row_range in values]
            for key, values in self._ranges.items()
        }
        self._file_names = file_names
        self._buckets = {key[1] for key in set(self._ranges) | set(file_names)}

        ranges_by_bucket = {}
        for (_, bucket), values in self._ranges.items():
            ranges_by_bucket.setdefault(bucket, []).extend(values)
        self._ranges_by_bucket = {
            bucket: Range.sort_and_merge_overlap(values, True, True)
            for bucket, values in ranges_by_bucket.items()
        }
        self._range_ends_by_bucket = {
            bucket: [row_range.to for row_range in values]
            for bucket, values in self._ranges_by_bucket.items()
        }

        names_by_bucket = {}
        for (_, bucket), values in self._file_names.items():
            names_by_bucket.setdefault(bucket, set()).update(values)
        self._names_by_bucket = names_by_bucket

        self.row_ranges = Range.sort_and_merge_overlap(
            [row_range for values in self._ranges.values() for row_range in values],
            True,
            True,
        )

    def is_empty(self):
        return not self._ranges and not self._file_names

    def can_prune_manifest_files(self):
        return bool(self.row_ranges) and not self._file_names

    def matches_bucket(self, bucket, _total_buckets=None):
        return bucket in self._buckets

    def matches_record(self, record):
        bucket = record.get('_BUCKET')
        if bucket not in self._buckets:
            return False
        file_dict = record.get('_FILE')
        if file_dict is None:
            return False
        if file_dict.get('_FILE_NAME') in self._names_by_bucket.get(bucket, ()):
            return True
        first_row_id = file_dict.get('_FIRST_ROW_ID')
        row_count = file_dict.get('_ROW_COUNT')
        if first_row_id is None or row_count is None:
            return False
        return self._matches_range(
            self._ranges_by_bucket.get(bucket, ()),
            self._range_ends_by_bucket.get(bucket, ()),
            int(first_row_id),
            int(first_row_id) + int(row_count) - 1,
        )

    def matches_entry(self, entry):
        key = self._entry_key(entry)
        if entry.file.file_name in self._file_names.get(key, ()):
            return True
        row_range = entry.file.row_id_range()
        if row_range is None:
            return False
        return self._matches_range(
            self._ranges.get(key, ()),
            self._range_ends.get(key, ()),
            row_range.from_,
            row_range.to,
        )

    @staticmethod
    def _matches_range(ranges, range_ends, from_, to):
        index = bisect.bisect_left(range_ends, from_)
        return index < len(ranges) and ranges[index].from_ <= to

    @staticmethod
    def _entry_key(entry):
        return tuple(entry.partition.values), entry.bucket


class CommitScanner:
    """Manifest entries scanner for commit operations.

    This class provides methods to scan manifest entries for commit operations
    """

    def __init__(self, table, manifest_list_manager: ManifestListManager):
        """Initialize CommitScanner.

        Args:
            table: The FileStoreTable instance.
            manifest_list_manager: Manager for reading manifest lists.
        """
        self.table = table
        self.manifest_list_manager = manifest_list_manager

    @staticmethod
    def conflict_entry_scope(commit_entries, index_entries=None):
        return _ConflictEntryScope(commit_entries, index_entries)

    def read_all_entries_from_changed_partitions(self,
                                                 latest_snapshot: Optional[Snapshot],
                                                 commit_entries: List[ManifestEntry],
                                                 index_entries=None):
        """Read all entries from the latest snapshot for partitions that are changed.

        Builds a partition predicate from delta entries and passes it to FileScanner,
        so that manifest files and entries are filtered during reading rather than
        after a full scan.

        Args:
            latest_snapshot: The latest snapshot to read entries from.
            commit_entries: The delta entries being committed, used to determine
                which partitions have changed.

        Returns:
            List of ManifestEntry from the latest snapshot for changed partitions.
        """
        if latest_snapshot is None:
            return []

        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)

        all_manifests = self.manifest_list_manager.read_all(latest_snapshot)
        return FileScanner(
            self.table, lambda: ([], None), partition_predicate=partition_filter
        ).read_manifest_entries(all_manifests)

    def read_conflict_entries(self,
                              latest_snapshot: Optional[Snapshot],
                              commit_entries: List[ManifestEntry],
                              index_entries=None):
        """Read only live entries which can interact with this commit window."""
        if latest_snapshot is None:
            return []

        scope = _ConflictEntryScope(commit_entries, index_entries)
        if scope.is_empty():
            return self.read_all_entries_from_changed_partitions(
                latest_snapshot, commit_entries, index_entries)

        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)
        return self._read_entries_for_scope(
            latest_snapshot, scope, partition_filter)

    def read_entries_for_row_id_scope(self, latest_snapshot: Optional[Snapshot],
                                      ranges_by_key):
        """Read live entries within a compact row-id scope.

        ``ranges_by_key`` maps ``(partition_values, bucket)`` to a list of
        merged row-id ``Range``s. Unlike :meth:`read_conflict_entries`, an empty
        scope returns ``[]`` -- it never falls back to scanning changed
        partitions, because an empty protected scope means "nothing to protect",
        not "scan everything". Callers must fail closed when a window yields no
        range rather than registering an empty scope.
        """
        if latest_snapshot is None:
            return []
        scope = _ConflictEntryScope.from_ranges(ranges_by_key)
        if scope.is_empty():
            return []
        # Build the partition filter straight from the scope keys -- no synthetic
        # ManifestEntry/DataFileMeta. Unpartitioned tables (partition == ()) get
        # a None filter and rely on the bucket/range filters below.
        partition_filter = self._build_partition_filter_from_values(
            {key[0] for key in ranges_by_key})
        return self._read_entries_for_scope(
            latest_snapshot, scope, partition_filter)

    def _read_entries_for_scope(self, latest_snapshot, scope, partition_filter):
        """Shared read path for a non-empty ``_ConflictEntryScope``.

        Partitions may be over-read by the bucket-level early filters, but
        ``scope.matches_entry`` is the final, exact ``(partition, bucket)`` +
        range filter, so distinct partitions/buckets stay isolated.
        """
        manifest_files = self.manifest_list_manager.read_all(latest_snapshot)
        if scope.can_prune_manifest_files():
            manifest_files = _filter_manifest_files_by_row_ranges(
                manifest_files, scope.row_ranges)

        max_workers = self.table.options.scan_manifest_parallelism(
            os.cpu_count() or 8)
        return ManifestFileManager(self.table).read_entries_parallel(
            manifest_files,
            manifest_entry_filter=scope.matches_entry,
            max_workers=max_workers,
            early_entry_filter=scope.matches_bucket,
            early_record_filter=scope.matches_record,
            partition_filter=partition_filter,
        )

    def read_incremental_entries_from_changed_partitions(self,
                                                         snapshot: Snapshot,
                                                         commit_entries: List[ManifestEntry],
                                                         index_entries=None):
        """Read incremental manifest entries from a snapshot's delta manifest list.

        Builds a partition predicate from delta entries and passes it to FileScanner,
        so that manifest files and entries are filtered during reading rather than
        after a full scan.

        Args:
            snapshot: The snapshot to read incremental entries from.
            commit_entries: The delta entries being committed, used to determine
                which partitions have changed.

        Returns:
            List of ManifestEntry matching the partition filter.
        """
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return []

        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)

        return FileScanner(
            self.table, lambda: ([], None), partition_predicate=partition_filter
        ).read_manifest_entries(delta_manifests)

    def read_incremental_raw_entries_from_changed_partitions(self, snapshot: Snapshot,
                                                             commit_entries: List[ManifestEntry],
                                                             partition_filter=None,
                                                             index_entries=None):
        """Like ``read_incremental_entries_from_changed_partitions`` but preserves
        DELETE entries (kind=1). ``partition_filter`` may be passed to avoid
        rebuilding it per call.
        """
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return []

        if partition_filter is None:
            partition_filter = self._build_partition_filter_from_changes(
                commit_entries, index_entries)
        mfm = ManifestFileManager(self.table)
        entries = []
        for mf in delta_manifests:
            for entry in mfm.read(
                    mf.file_name, partition_filter=partition_filter):
                if (partition_filter is not None
                        and not partition_filter.test(entry.partition)):
                    continue
                entries.append(entry)
        return entries

    def read_incremental_raw_entries_for_scope(
            self, snapshot, commit_entries, index_entries=None):
        delta_manifests = self.manifest_list_manager.read_delta(snapshot)
        if not delta_manifests:
            return []

        scope = _ConflictEntryScope(commit_entries, index_entries)
        if scope.is_empty():
            return self.read_incremental_raw_entries_from_changed_partitions(
                snapshot, commit_entries, index_entries=index_entries)
        if scope.can_prune_manifest_files():
            delta_manifests = _filter_manifest_files_by_row_ranges(
                delta_manifests, scope.row_ranges)

        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)
        mfm = ManifestFileManager(self.table)
        entries = []
        for manifest in delta_manifests:
            entries.extend(mfm.read(
                manifest.file_name,
                manifest_entry_filter=scope.matches_entry,
                early_entry_filter=scope.matches_bucket,
                early_record_filter=scope.matches_record,
                partition_filter=partition_filter,
            ))
        return entries

    def read_incremental_changes(self,
                                 from_snapshot: Snapshot,
                                 to_snapshot: Snapshot,
                                 commit_entries: List[ManifestEntry],
                                 index_entries=None) -> Optional[List[ManifestEntry]]:
        """Delta entries (incl. DELETEs) in ``(from_snapshot, to_snapshot]``,
        changed-partition filtered, so a retry can reuse the prior base and read
        only the changes since. Returns None on a missing or OVERWRITE snapshot
        (caller then full-scans). An OVERWRITE may replace the base manifest
        without fully describing the replacement in its delta manifest.
        """
        snapshot_manager = self.table.snapshot_manager()
        partition_filter = self._build_partition_filter_from_changes(
            commit_entries, index_entries)
        entries = []
        for snapshot_id in range(from_snapshot.id + 1, to_snapshot.id + 1):
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None or snapshot.commit_kind == "OVERWRITE":
                return None
            entries.extend(
                self.read_incremental_raw_entries_from_changed_partitions(
                    snapshot, commit_entries, partition_filter))
        return entries

    def changed_partition_signature(self, entries, index_entries=None):
        """Return the changed partitions used by conflict scans."""
        if not self.table.partition_keys:
            return None

        changed_partitions = set()
        for entry in entries or []:
            changed_partitions.add(tuple(entry.partition.values))
        for entry in index_entries or []:
            if self._index_entry_changes_partition(entry):
                changed_partitions.add(tuple(entry.partition.values))
        return frozenset(changed_partitions) or None

    def _build_partition_filter_from_entries(self, entries: List[ManifestEntry]):
        return self._build_partition_filter_from_changes(entries)

    def _build_partition_filter_from_changes(self, entries, index_entries=None):
        """Build a partition predicate that matches all partitions present in the given entries.

        Args:
            entries: List of ManifestEntry whose partitions should be matched.
            index_entries: Optional index manifest entries whose partitions
                should be matched.

        Returns:
            A Predicate matching any of the changed partitions, or None if
            partition keys are empty.
        """
        partition_keys = self.table.partition_keys
        if not partition_keys:
            return None

        changed_partitions = self.changed_partition_signature(
            entries, index_entries)

        if not changed_partitions:
            return None

        return self._build_partition_filter_from_values(changed_partitions)

    def _build_partition_filter_from_values(self, changed_partitions):
        partition_keys = self.table.partition_keys
        if not partition_keys:
            return None

        predicate_builder = PredicateBuilder(self.table.partition_keys_fields)
        partition_predicates = []
        for partition_values in changed_partitions:
            sub_predicates = []
            for i, key in enumerate(partition_keys):
                if partition_values[i] is None:
                    sub_predicates.append(predicate_builder.is_null(key))
                else:
                    sub_predicates.append(predicate_builder.equal(key, partition_values[i]))
            partition_predicates.append(predicate_builder.and_predicates(sub_predicates))

        return predicate_builder.or_predicates(partition_predicates)

    @staticmethod
    def _index_entry_changes_partition(entry):
        return (entry.index_file.index_type == IndexManifestFile.DELETION_VECTORS_INDEX
                or entry.index_file.global_index_meta is not None)
