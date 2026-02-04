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
from typing import Callable, List

from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow


def _is_blob_file(file_name: str) -> bool:
    return file_name.endswith('.blob')


def _non_null_first_row_id(file: DataFileMeta) -> int:
    if file.first_row_id is None:
        raise ValueError("Data evolution requires first_row_id on file")
    return file.first_row_id


def merge_overlapping_ranges(
    entries: List[ManifestEntry],
) -> List[List[ManifestEntry]]:
    if not entries:
        return []

    def start_func(e: ManifestEntry) -> int:
        return _non_null_first_row_id(e.file)

    def end_func(e: ManifestEntry) -> int:
        return _non_null_first_row_id(e.file) + e.file.row_count - 1

    indexed = [(i, e) for i, e in enumerate(entries)]
    indexed.sort(key=lambda x: (start_func(x[1]), end_func(x[1])))

    groups: List[List[ManifestEntry]] = []
    current_group = [indexed[0][1]]
    current_end = end_func(indexed[0][1])

    for i in range(1, len(indexed)):
        entry = indexed[i][1]
        start = start_func(entry)
        end = end_func(entry)

        if start <= current_end:
            current_group.append(entry)
            if end > current_end:
                current_end = end
        else:
            groups.append(current_group)
            current_group = [entry]
            current_end = end

    groups.append(current_group)
    return groups


def _evolution_stats(
    table_fields: List[DataField],
    schema_fields_func: Callable[[int], List[DataField]],
    table_schema_id: int,
    entries: List[ManifestEntry],
) -> tuple:
    metas = [e for e in entries if not _is_blob_file(e.file.file_name)]
    if not metas:
        return SimpleStats.empty_stats(), entries[0].file.row_count

    metas.sort(key=lambda e: e.file.max_sequence_number, reverse=True)

    fields_count = len(table_fields)
    row_offsets = [-1] * fields_count

    for i, meta in enumerate(metas):
        file_meta = meta.file
        data_fields = schema_fields_func(file_meta.schema_id)
        data_field_names = {f.name for f in data_fields}

        write_cols = file_meta.write_cols or []
        stats_cols = (
            file_meta.value_stats_cols
            if file_meta.value_stats_cols
            else write_cols
        )

        for j in range(fields_count):
            if row_offsets[j] != -1:
                continue
            target_name = table_fields[j].name
            if target_name not in data_field_names:
                continue

            if target_name not in write_cols:
                row_offsets[j] = -2
                continue

            if stats_cols and target_name in stats_cols:
                row_offsets[j] = i
            else:
                row_offsets[j] = -2

    from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions

    evolutions = SimpleStatsEvolutions(schema_fields_func, table_schema_id)
    evolution_cache: dict = {}
    for meta in metas:
        sid = meta.file.schema_id
        if sid not in evolution_cache:
            evolution_cache[sid] = evolutions.get_or_create(sid)

    min_values = []
    max_values = []
    null_counts = []

    for j in range(fields_count):
        ri = row_offsets[j]
        if ri >= 0:
            meta = metas[ri]
            evolution = evolution_cache[meta.file.schema_id]
            stats_fields = meta.file.value_stats_cols or meta.file.write_cols
            evolved = evolution.evolution(
                meta.file.value_stats,
                meta.file.row_count,
                stats_fields,
            )
            min_values.append(evolved.min_values.get_field(j))
            max_values.append(evolved.max_values.get_field(j))
            nc = evolved.null_counts
            null_counts.append(nc[j] if nc and j < len(nc) else None)
        else:
            min_values.append(None)
            max_values.append(None)
            null_counts.append(None)

    contributing_row_counts = [
        metas[row_offsets[j]].file.row_count
        for j in range(fields_count)
        if row_offsets[j] >= 0
    ]
    row_count = (
        max(contributing_row_counts)
        if contributing_row_counts
        else metas[0].file.row_count
    )
    merged_stats = SimpleStats(
        GenericRow(min_values, table_fields),
        GenericRow(max_values, table_fields),
        null_counts,
    )
    return merged_stats, row_count


def post_filter_manifest_entries(
    entries: List[ManifestEntry],
    predicate: Predicate,
    table,
) -> List[ManifestEntry]:
    if not predicate or not entries:
        return entries

    with_row_id = [e for e in entries if e.file.first_row_id is not None]
    without_row_id = [e for e in entries if e.file.first_row_id is None]

    def schema_fields_func(schema_id: int):
        schema = table.schema_manager.get_schema(schema_id)
        return schema.fields if schema else []

    table_fields = table.table_schema.fields
    table_schema_id = table.table_schema.id

    groups = merge_overlapping_ranges(with_row_id)
    result = []

    for group in groups:
        try:
            merged_stats, row_count = _evolution_stats(
                table_fields,
                schema_fields_func,
                table_schema_id,
                group,
            )
            if predicate.test_by_simple_stats(merged_stats, row_count):
                result.extend(group)
        except (ValueError, KeyError, IndexError):
            result.extend(group)

    return result + without_row_id
