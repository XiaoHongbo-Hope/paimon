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

"""The ``$files`` system table.

It exposes per-data-file detail from the latest snapshot.
"""

import json
from typing import Any, Dict, List, Optional, Sequence

import pyarrow

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        PyarrowFieldParser, RowType)
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "partition", AtomicType("STRING", nullable=True)),
    DataField(1, "bucket", AtomicType("INT", nullable=False)),
    DataField(2, "file_path", AtomicType("STRING", nullable=False)),
    DataField(3, "file_format", AtomicType("STRING", nullable=False)),
    DataField(4, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(5, "level", AtomicType("INT", nullable=False)),
    DataField(6, "record_count", AtomicType("BIGINT", nullable=False)),
    DataField(7, "file_size_in_bytes", AtomicType("BIGINT", nullable=False)),
    DataField(8, "min_key", AtomicType("STRING", nullable=True)),
    DataField(9, "max_key", AtomicType("STRING", nullable=True)),
    DataField(10, "null_value_counts", AtomicType("STRING", nullable=False)),
    DataField(11, "min_value_stats", AtomicType("STRING", nullable=False)),
    DataField(12, "max_value_stats", AtomicType("STRING", nullable=False)),
    DataField(13, "min_sequence_number", AtomicType("BIGINT", nullable=True)),
    DataField(14, "max_sequence_number", AtomicType("BIGINT", nullable=True)),
    DataField(15, "creation_time", AtomicType("TIMESTAMP(3)", nullable=True)),
    # ``deleteRowCount`` is intentionally camelCase to keep the on-wire
    # column name stable.
    DataField(16, "deleteRowCount", AtomicType("BIGINT", nullable=True)),
    DataField(17, "file_source", AtomicType("STRING", nullable=True)),
    DataField(18, "first_row_id", AtomicType("BIGINT", nullable=True)),
    DataField(
        19,
        "write_cols",
        ArrayType(nullable=True,
                  element_type=AtomicType("STRING", nullable=True))),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")
_WRITE_COLS_TYPE = pyarrow.list_(pyarrow.string())
_VALUE_STATS_FIELDS = frozenset({
    "null_value_counts",
    "min_value_stats",
    "max_value_stats",
})

_ARROW_TYPES = {
    "partition": pyarrow.string(),
    "bucket": pyarrow.int32(),
    "file_path": pyarrow.string(),
    "file_format": pyarrow.string(),
    "schema_id": pyarrow.int64(),
    "level": pyarrow.int32(),
    "record_count": pyarrow.int64(),
    "file_size_in_bytes": pyarrow.int64(),
    "min_key": pyarrow.string(),
    "max_key": pyarrow.string(),
    "null_value_counts": pyarrow.string(),
    "min_value_stats": pyarrow.string(),
    "max_value_stats": pyarrow.string(),
    "min_sequence_number": pyarrow.int64(),
    "max_sequence_number": pyarrow.int64(),
    "creation_time": _TIMESTAMP_TYPE,
    "deleteRowCount": pyarrow.int64(),
    "file_source": pyarrow.string(),
    "first_row_id": pyarrow.int64(),
    "write_cols": _WRITE_COLS_TYPE,
}


def _to_json(obj: Any) -> str:
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False,
                      default=str)


def _stringify_path(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return str(value)


def _stats_columns(file_meta, table_field_names: List[str]) -> List[str]:
    cols = getattr(file_meta, "value_stats_cols", None)
    if cols:
        return list(cols)
    return list(table_field_names)


def _to_python(value: Any) -> Any:
    """Render an internal-row cell value into a JSON-safe primitive."""
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, (int, float, bool, str, list, dict)):
        return value
    return str(value)


def _render_key(row) -> Optional[str]:
    if row is None:
        return None
    values = getattr(row, "values", None)
    if not values:
        return None
    return _to_json([_to_python(v) for v in values])


def _render_partition(partition_row) -> Optional[str]:
    if partition_row is None:
        return None
    fields = getattr(partition_row, "fields", None) or []
    values = getattr(partition_row, "values", None) or []
    if not fields:
        return None
    return "/".join("{}={}".format(field.name, value)
                    for field, value in zip(fields, values))


def _render_stats_map(values: List[Any], columns: List[str]) -> str:
    pairs = {}
    n = min(len(columns), len(values) if values is not None else 0)
    for i in range(n):
        pairs[columns[i]] = _to_python(values[i])
    return _to_json(pairs)


def _render_null_counts(null_counts: Optional[List[int]],
                        columns: List[str]) -> str:
    pairs = {}
    if null_counts:
        n = min(len(columns), len(null_counts))
        for i in range(n):
            pairs[columns[i]] = (None if null_counts[i] is None
                                 else int(null_counts[i]))
    return _to_json(pairs)


class FilesTable(SystemTable):
    """The ``$files`` system table."""

    def system_table_name(self) -> str:
        return "files"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["file_path"]

    def new_read_builder(self):
        # ``$files`` can contain many thousands of rows. Unlike the small
        # metadata system tables, keep manifest decoding and Arrow conversion
        # out of scan planning and stream projected batches from the read.
        from pypaimon.table.system.files_table_read import FilesReadBuilder
        return FilesReadBuilder(self)

    def manifest_files(self):
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return None, []
        manager = ManifestListManager(self.base_table)
        return snapshot, manager.read_all(snapshot)

    def read_entries(self, manifest_files, projected_names: Sequence[str]):
        include_value_stats = bool(
            _VALUE_STATS_FIELDS.intersection(projected_names))
        manager = ManifestFileManager(self.base_table)
        return manager.read_entries_parallel(
            manifest_files, drop_stats=not include_value_stats)

    def entries_to_record_batch(
        self,
        entries,
        projected_names: Sequence[str],
    ) -> pyarrow.RecordBatch:
        file_format = self.base_table.options.file_format()
        table_field_names = list(self.base_table.field_names)
        columns: Dict[str, pyarrow.Array] = {}
        for name in projected_names:
            values = [
                self._entry_value(
                    entry, name, file_format, table_field_names)
                for entry in entries
            ]
            columns[name] = pyarrow.array(values, type=_ARROW_TYPES[name])
        field_map = {field.name: field for field in TABLE_TYPE.fields}
        schema = PyarrowFieldParser.from_paimon_schema(
            [field_map[name] for name in projected_names])
        return pyarrow.RecordBatch.from_arrays(
            list(columns.values()), schema=schema)

    def _build_arrow_table(self) -> pyarrow.Table:
        _, manifest_files = self.manifest_files()
        if not manifest_files:
            return self._empty_table()

        projected_names = [field.name for field in TABLE_TYPE.fields]
        entries = self.read_entries(manifest_files, projected_names)
        if not entries:
            return self._empty_table()
        batch = self.entries_to_record_batch(entries, projected_names)
        return pyarrow.Table.from_batches([batch])

    @staticmethod
    def _entry_value(entry, name, file_format, table_field_names):
        meta = entry.file
        if name == "partition":
            return _render_partition(entry.partition)
        if name == "bucket":
            return int(entry.bucket)
        if name == "file_path":
            return _stringify_path(meta.file_path or meta.file_name)
        if name == "file_format":
            return file_format
        if name == "schema_id":
            return int(meta.schema_id)
        if name == "level":
            return int(meta.level)
        if name == "record_count":
            return int(meta.row_count)
        if name == "file_size_in_bytes":
            return int(meta.file_size)
        if name == "min_key":
            return _render_key(meta.min_key)
        if name == "max_key":
            return _render_key(meta.max_key)
        if name in _VALUE_STATS_FIELDS:
            stats_cols = _stats_columns(meta, table_field_names)
            value_stats = meta.value_stats
            if name == "null_value_counts":
                return _render_null_counts(
                    value_stats.null_counts, stats_cols)
            values = (
                value_stats.min_values
                if name == "min_value_stats"
                else value_stats.max_values
            )
            return _render_stats_map(
                getattr(values, "values", []) or [], stats_cols)
        if name == "min_sequence_number":
            return int(meta.min_sequence_number)
        if name == "max_sequence_number":
            return int(meta.max_sequence_number)
        if name == "creation_time":
            return meta.creation_time_epoch_millis()
        if name == "deleteRowCount":
            return (
                None
                if meta.delete_row_count is None
                else int(meta.delete_row_count)
            )
        if name == "file_source":
            return None if meta.file_source is None else str(meta.file_source)
        if name == "first_row_id":
            return (
                None if meta.first_row_id is None else int(meta.first_row_id)
            )
        if name == "write_cols":
            return list(meta.write_cols) if meta.write_cols else None
        raise ValueError("Unknown $files column: {}".format(name))

    @staticmethod
    def _empty_table() -> pyarrow.Table:
        return pyarrow.table({
            "partition": pyarrow.array([], type=pyarrow.string()),
            "bucket": pyarrow.array([], type=pyarrow.int32()),
            "file_path": pyarrow.array([], type=pyarrow.string()),
            "file_format": pyarrow.array([], type=pyarrow.string()),
            "schema_id": pyarrow.array([], type=pyarrow.int64()),
            "level": pyarrow.array([], type=pyarrow.int32()),
            "record_count": pyarrow.array([], type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array([], type=pyarrow.int64()),
            "min_key": pyarrow.array([], type=pyarrow.string()),
            "max_key": pyarrow.array([], type=pyarrow.string()),
            "null_value_counts": pyarrow.array([], type=pyarrow.string()),
            "min_value_stats": pyarrow.array([], type=pyarrow.string()),
            "max_value_stats": pyarrow.array([], type=pyarrow.string()),
            "min_sequence_number": pyarrow.array([], type=pyarrow.int64()),
            "max_sequence_number": pyarrow.array([], type=pyarrow.int64()),
            "creation_time": pyarrow.array([], type=_TIMESTAMP_TYPE),
            "deleteRowCount": pyarrow.array([], type=pyarrow.int64()),
            "file_source": pyarrow.array([], type=pyarrow.string()),
            "first_row_id": pyarrow.array([], type=pyarrow.int64()),
            "write_cols": pyarrow.array([], type=_WRITE_COLS_TYPE),
        })
