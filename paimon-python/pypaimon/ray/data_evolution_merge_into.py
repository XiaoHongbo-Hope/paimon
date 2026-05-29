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
# limitations under the License.
################################################################################

"""MERGE INTO ... USING ... for Paimon data-evolution tables via Ray Datasets."""

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pyarrow as pa

SetSpec = Union[str, Dict[str, Any]]
Condition = Callable[[Mapping[str, Any]], bool]
OnSpec = Union[Sequence[str], Mapping[str, str]]


@dataclass
class WhenMatched:
    update: SetSpec
    condition: Optional[Condition] = None


@dataclass
class WhenNotMatched:
    insert: SetSpec
    condition: Optional[Condition] = None


@dataclass
class _NormalizedClause:
    spec: Dict[str, Any]
    condition: Optional[Condition]


def merge_into(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    on: OnSpec,
    merge_condition: Optional[Condition] = None,
    when_matched: Sequence[WhenMatched] = (),
    when_not_matched: Sequence[WhenNotMatched] = (),
    num_partitions: int = 16,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    when_matched = list(when_matched)
    when_not_matched = list(when_not_matched)
    if not when_matched and not when_not_matched:
        raise ValueError(
            "At least one of when_matched or when_not_matched must be non-empty."
        )

    target_on_cols, source_on_cols = _normalize_on(on)

    from pypaimon.catalog.catalog_factory import CatalogFactory

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"merge_into requires 'data-evolution.enabled' = 'true' on '{target}'."
        )
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"merge_into requires 'row-tracking.enabled' = 'true' on '{target}'."
        )

    target_field_names = list(table.field_names)
    matched_specs = [
        _NormalizedClause(
            spec=_normalize_set_spec(c.update, target_field_names),
            condition=c.condition,
        )
        for c in when_matched
    ]
    not_matched_specs = [
        _NormalizedClause(
            spec=_normalize_set_spec(c.insert, target_field_names),
            condition=c.condition,
        )
        for c in when_not_matched
    ]

    source_ds = _normalize_source(source, catalog_options)
    _validate_source_on_cols(source_ds, source_on_cols)

    base_snapshot = table.snapshot_manager().get_latest_snapshot()
    if base_snapshot is not None:
        # Pin the snapshot so the final commit aborts if another writer
        # commits between our read and our commit.
        table = table.copy(
            {"commit.strict-mode.last-safe-snapshot": str(base_snapshot.id)}
        )
    is_self_merge = isinstance(source, str) and source == target

    # Row-precise routing needs a stable per-source-row id when merge_condition
    # may differ between source rows sharing the same ON key.
    if when_not_matched and merge_condition is not None and not is_self_merge:
        source_ds = _add_paimon_src_idx(source_ds)

    from pypaimon.schema.data_types import PyarrowFieldParser

    target_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )

    if not_matched_specs and base_snapshot is not None:
        _check_global_index_for_insert(table, base_snapshot)

    update_ds = None
    update_cols_union: List[str] = []
    # Empty target → no rows can match; matched UPDATE is a no-op.
    if matched_specs and base_snapshot is not None:
        update_cols_union = _union_update_cols(matched_specs)
        _check_global_index_collision(table, base_snapshot, update_cols_union)
        update_ds = _build_matched_update_ds(
            target_identifier=target,
            source_ds=source_ds,
            target_on=target_on_cols,
            source_on=source_on_cols,
            merge_condition=merge_condition,
            clauses=matched_specs,
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            update_cols=update_cols_union,
            catalog_options=catalog_options,
            is_self_merge=is_self_merge,
            num_partitions=num_partitions,
        )

    insert_ds = None
    if not_matched_specs and not is_self_merge:
        matched_keys_ds = None
        if merge_condition is not None:
            matched_keys_ds = _compute_matched_source_idx_ds(
                target_identifier=target,
                source_ds=source_ds,
                target_on=target_on_cols,
                source_on=source_on_cols,
                merge_condition=merge_condition,
                catalog_options=catalog_options,
                num_partitions=num_partitions,
            )
        insert_ds = _build_not_matched_insert_ds(
            target_identifier=target,
            source_ds=source_ds,
            target_on=target_on_cols,
            source_on=source_on_cols,
            clauses=not_matched_specs,
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            catalog_options=catalog_options,
            num_partitions=num_partitions,
            matched_idx_ds=matched_keys_ds,
        )

    all_msgs: list = []
    if update_ds is not None:
        all_msgs.extend(
            _distributed_update_apply(
                update_ds,
                table,
                update_cols_union,
                ray_remote_args=ray_remote_args,
            )
        )
    if insert_ds is not None:
        all_msgs.extend(
            _distributed_write_collect_msgs(
                insert_ds, table, ray_remote_args=ray_remote_args, concurrency=concurrency
            )
        )
    if all_msgs:
        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.commit(all_msgs)
        tc.close()


def _normalize_on(on: OnSpec) -> Tuple[List[str], List[str]]:
    if isinstance(on, Mapping):
        target_cols = list(on.keys())
        source_cols = list(on.values())
    else:
        target_cols = list(on)
        source_cols = list(on)
    if not target_cols:
        raise ValueError("'on' must be non-empty.")
    return target_cols, source_cols


def _build_matched_update_ds(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    merge_condition: Optional[Condition],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    catalog_options: Dict[str, str],
    is_self_merge: bool,
    num_partitions: int,
):
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    needs_full = merge_condition is not None or any(
        c.condition is not None for c in clauses
    )
    if needs_full:
        needed_cols = list(target_field_names)
    else:
        needed_cols = _needed_target_cols(
            clauses, target_on, update_cols, target_field_names
        )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]

    target_ds = read_paimon(target_identifier, catalog_options, projection=projection)
    update_schema = _build_update_schema(target_pa_schema, update_cols, row_id_name)

    if is_self_merge:
        return _build_self_merge_update_ds(
            target_ds=target_ds,
            merge_condition=merge_condition,
            clauses=clauses,
            target_field_names=target_field_names,
            update_cols=update_cols,
            row_id_name=row_id_name,
            update_schema=update_schema,
        )

    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in target_ds.schema().names}
    )
    source_schema = source_ds.schema()
    source_cols = list(source_schema.names) if source_schema is not None else list(source_on)
    source_renamed = source_ds.rename_columns({c: f"s.{c}" for c in source_cols})

    joined = target_renamed.join(
        source_renamed,
        join_type="inner",
        num_partitions=num_partitions,
        on=tuple(f"t.{c}" for c in target_on),
        right_on=tuple(f"s.{c}" for c in source_on),
    )

    captured_clauses = clauses
    captured_merge_cond = merge_condition
    captured_update_cols = list(update_cols)
    captured_field_names = list(target_field_names)
    captured_row_id_name = row_id_name
    captured_on_pairs = list(zip(source_on, target_on))
    captured_schema = update_schema

    def _transform(batch: pa.Table) -> pa.Table:
        rows = batch.to_pylist()
        out_row_ids: list = []
        out_cols: Dict[str, list] = {c: [] for c in captured_update_cols}
        for row in rows:
            s_row = {k[2:]: v for k, v in row.items() if k.startswith("s.")}
            t_row = {k[2:]: v for k, v in row.items() if k.startswith("t.")}
            for s_key, t_key in captured_on_pairs:
                if s_key not in s_row and t_key in t_row:
                    s_row[s_key] = t_row[t_key]
            combined = _prefixed(s_row, t_row)
            if captured_merge_cond is not None and not captured_merge_cond(combined):
                continue
            for clause in captured_clauses:
                if clause.condition is not None and not clause.condition(combined):
                    continue
                new_values = _apply_set(
                    clause.spec, s_row, t_row, captured_field_names
                )
                out_row_ids.append(t_row[captured_row_id_name])
                for col in captured_update_cols:
                    out_cols[col].append(new_values.get(col, t_row.get(col)))
                break
        return pa.Table.from_pydict(
            {captured_row_id_name: out_row_ids, **out_cols},
            schema=captured_schema,
        )

    return joined.map_batches(_transform, batch_format="pyarrow")


def _build_self_merge_update_ds(
    *,
    target_ds,
    merge_condition: Optional[Condition],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    update_cols: Sequence[str],
    row_id_name: str,
    update_schema: pa.Schema,
):
    captured_clauses = clauses
    captured_merge_cond = merge_condition
    captured_update_cols = list(update_cols)
    captured_field_names = list(target_field_names)
    captured_row_id_name = row_id_name
    captured_schema = update_schema

    def _transform(batch: pa.Table) -> pa.Table:
        rows = batch.to_pylist()
        out_row_ids: list = []
        out_cols: Dict[str, list] = {c: [] for c in captured_update_cols}
        for row in rows:
            s_row = dict(row)
            t_row = dict(row)
            combined = _prefixed(s_row, t_row)
            if captured_merge_cond is not None and not captured_merge_cond(combined):
                continue
            for clause in captured_clauses:
                if clause.condition is not None and not clause.condition(combined):
                    continue
                new_values = _apply_set(
                    clause.spec, s_row, t_row, captured_field_names
                )
                out_row_ids.append(t_row[captured_row_id_name])
                for col in captured_update_cols:
                    out_cols[col].append(new_values.get(col, t_row.get(col)))
                break
        return pa.Table.from_pydict(
            {captured_row_id_name: out_row_ids, **out_cols},
            schema=captured_schema,
        )

    return target_ds.map_batches(_transform, batch_format="pyarrow")


def _build_update_schema(
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> pa.Schema:
    return pa.schema(
        [pa.field(row_id_name, pa.int64(), nullable=False)]
        + [target_pa_schema.field(col) for col in update_cols]
    )


def _distributed_update_apply(
    update_ds,
    table,
    write_update_cols: Sequence[str],
    *,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> list:
    import bisect
    import pickle
    import uuid

    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.table.special_fields import SpecialFields
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    row_id_name = SpecialFields.ROW_ID.name
    cols = list(write_update_cols)

    for col in cols:
        if col not in table.field_names:
            raise ValueError(f"Column '{col}' is not in target table schema.")

    planner = TableUpdateByRowId(
        table,
        "_merge_into_planner_" + uuid.uuid4().hex[:8],
        BATCH_COMMIT_IDENTIFIER,
    )
    sorted_first_row_ids = list(planner.first_row_ids)
    if not sorted_first_row_ids:
        return []

    # Broadcast the file-info snapshot to every worker so they skip the
    # per-task manifest scan and observe a single consistent target view.
    precomputed_info = (
        planner.snapshot_id,
        planner.first_row_ids,
        planner._first_row_id_index,
        planner.total_row_count,
    )

    frid_col = "_FIRST_ROW_ID"
    captured_sorted = sorted_first_row_ids
    captured_precomputed = precomputed_info
    total_row_count = planner.total_row_count

    def _assign_frid(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch.append_column(frid_col, pa.array([], type=pa.int64()))
        row_ids = batch.column(row_id_name).to_pylist()
        bisect_right = bisect.bisect_right
        values: list = []
        first = captured_sorted[0]
        for rid in row_ids:
            # Out-of-range _ROW_IDs would silently map via bisect wrap-around.
            if rid is None or rid < first or rid >= total_row_count:
                raise ValueError(
                    f"_ROW_ID {rid} is out of valid range "
                    f"[{first}, {total_row_count}); planner snapshot is stale "
                    f"or matched rows come from a different table."
                )
            values.append(captured_sorted[bisect_right(captured_sorted, rid) - 1])
        return batch.append_column(frid_col, pa.array(values, type=pa.int64()))

    with_frid = update_ds.map_batches(_assign_frid, batch_format="pyarrow")

    captured_table = table
    captured_cols = cols

    def _apply_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return pa.Table.from_pydict({"msgs_blob": pa.array([], type=pa.binary())})

        group_row_ids = group.column(row_id_name).to_pylist()
        if len(set(group_row_ids)) != len(group_row_ids):
            seen: set = set()
            dupes: set = set()
            for rid in group_row_ids:
                if rid in seen:
                    dupes.add(rid)
                seen.add(rid)
            raise ValueError(
                f"MERGE INTO matched the same target _ROW_IDs {sorted(dupes)[:5]} "
                f"via multiple source rows; source must be unique on the join keys."
            )

        for_update = group.drop_columns([frid_col])
        worker = TableUpdateByRowId(
            captured_table,
            "_merge_into_shard_" + uuid.uuid4().hex[:8],
            BATCH_COMMIT_IDENTIFIER,
            precomputed_files_info=captured_precomputed,
        )
        msgs = worker.update_columns(for_update, list(captured_cols))
        return pa.Table.from_pydict({"msgs_blob": [pickle.dumps(msgs)]})

    msgs_ds = with_frid.groupby(frid_col).map_groups(
        _apply_group, batch_format="pyarrow"
    )

    all_msgs: list = []
    for batch in msgs_ds.iter_batches(batch_format="pyarrow"):
        for blob in batch.column("msgs_blob").to_pylist():
            all_msgs.extend(pickle.loads(blob))
    return all_msgs


PAIMON_SRC_IDX_COL = "_paimon_src_idx"
MATCHED_SRC_IDX_MARKER = "_paimon_matched_src_idx"


def _add_paimon_src_idx(source_ds):
    """Append a unique per-row index so INSERTs are routed by row identity,
    not by content. Duplicate identical rows must remain distinguishable."""
    import ray

    n = source_ds.count()
    idx_ds = ray.data.range(n).rename_columns({"id": PAIMON_SRC_IDX_COL})
    return source_ds.zip(idx_ds)


def _compute_matched_source_idx_ds(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    merge_condition: Condition,
    catalog_options: Dict[str, str],
    num_partitions: int,
):
    from pypaimon.ray.ray_paimon import read_paimon

    target_ds = read_paimon(target_identifier, catalog_options)
    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in target_ds.schema().names}
    )
    source_schema = source_ds.schema()
    source_cols = list(source_schema.names) if source_schema is not None else list(source_on)
    source_renamed = source_ds.rename_columns({c: f"s.{c}" for c in source_cols})

    joined = target_renamed.join(
        source_renamed,
        join_type="inner",
        num_partitions=num_partitions,
        on=tuple(f"t.{c}" for c in target_on),
        right_on=tuple(f"s.{c}" for c in source_on),
    )

    captured_merge_cond = merge_condition
    captured_on_pairs = list(zip(source_on, target_on))
    out_schema = pa.schema([pa.field(MATCHED_SRC_IDX_MARKER, pa.int64())])

    def _emit_matched_idx(batch: pa.Table) -> pa.Table:
        out_idx: list = []
        for row in batch.to_pylist():
            s_row = {k[2:]: v for k, v in row.items() if k.startswith("s.")}
            t_row = {k[2:]: v for k, v in row.items() if k.startswith("t.")}
            for sk, tk in captured_on_pairs:
                if sk not in s_row and tk in t_row:
                    s_row[sk] = t_row[tk]
            combined = _prefixed(s_row, t_row)
            if captured_merge_cond(combined):
                out_idx.append(s_row.get(PAIMON_SRC_IDX_COL))
        return pa.Table.from_pydict(
            {MATCHED_SRC_IDX_MARKER: out_idx}, schema=out_schema
        )

    return joined.map_batches(_emit_matched_idx, batch_format="pyarrow")


def _build_not_matched_insert_ds(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    catalog_options: Dict[str, str],
    num_partitions: int,
    matched_idx_ds=None,
):
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.ray.shuffle import _coerce_large_string_types

    captured_clauses = clauses
    captured_field_names = list(target_field_names)
    out_schema = target_pa_schema

    source_schema = source_ds.schema()
    source_cols = list(source_schema.names) if source_schema is not None else list(source_on)
    source_renamed = source_ds.rename_columns({c: f"s.{c}" for c in source_cols})

    if matched_idx_ds is not None:
        # Ray join hits a pyarrow projection bug when the right side is
        # empty; collect matched-idx to a driver set instead. The set is
        # bounded by # of matched source rows × ~32B per row-hash.
        matched_idx_set: set = set()
        for batch in matched_idx_ds.iter_batches(batch_format="pyarrow"):
            if batch.num_rows == 0:
                continue
            matched_idx_set.update(
                batch.column(MATCHED_SRC_IDX_MARKER).to_pylist()
            )
        captured_idx_set = matched_idx_set

        def _filter_unmatched(batch: pa.Table) -> pa.Table:
            idx_arr = batch.column(f"s.{PAIMON_SRC_IDX_COL}").to_pylist()
            mask = [v not in captured_idx_set for v in idx_arr]
            return batch.filter(pa.array(mask))

        unmatched = source_renamed.map_batches(
            _filter_unmatched, batch_format="pyarrow"
        )
    else:
        target_ds = read_paimon(
            target_identifier, catalog_options, projection=list(target_on)
        )
        target_renamed = target_ds.rename_columns(
            {c: f"t.{c}" for c in target_on}
        )
        unmatched = source_renamed.join(
            target_renamed,
            join_type="left_anti",
            num_partitions=num_partitions,
            on=tuple(f"s.{c}" for c in source_on),
            right_on=tuple(f"t.{c}" for c in target_on),
        )

    def _transform(batch: pa.Table) -> pa.Table:
        rows = batch.to_pylist()
        out = []
        for row in rows:
            s_row = {k[2:]: v for k, v in row.items() if k.startswith("s.")}
            s_row.pop(PAIMON_SRC_IDX_COL, None)
            combined = _prefixed(s_row, None)
            for clause in captured_clauses:
                if clause.condition is not None and not clause.condition(combined):
                    continue
                out.append(
                    _apply_set(
                        clause.spec,
                        s_row,
                        None,
                        captured_field_names,
                        null_unspecified=True,
                    )
                )
                break
        aligned = [{name: r.get(name) for name in captured_field_names} for r in out]
        return _coerce_large_string_types(pa.Table.from_pylist(aligned, schema=out_schema))

    return unmatched.map_batches(_transform, batch_format="pyarrow")


def _distributed_write_collect_msgs(
    insert_ds,
    table,
    *,
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> list:
    from pypaimon.write.ray_datasink import PaimonDatasink

    class _CollectingDatasink(PaimonDatasink):
        def __init__(self, t):
            super().__init__(t, overwrite=False)
            self.collected: list = []

        def on_write_complete(self, write_result):
            if hasattr(write_result, "write_returns"):
                write_returns = write_result.write_returns
            elif isinstance(write_result, list):
                write_returns = write_result
            else:
                raise TypeError(
                    f"Unexpected write_result type {type(write_result).__name__}"
                )
            self.collected = [
                m
                for batch in write_returns
                for m in batch
                if not m.is_empty()
            ]

    sink = _CollectingDatasink(table)
    write_kwargs: Dict[str, Any] = {}
    if ray_remote_args is not None:
        write_kwargs["ray_remote_args"] = ray_remote_args
    if concurrency is not None:
        write_kwargs["concurrency"] = concurrency
    insert_ds.write_datasink(sink, **write_kwargs)
    return sink.collected


def _check_global_index_collision(
    table, snapshot, update_cols: Sequence[str]
) -> None:
    entries = _scan_global_index_entries(table, snapshot)
    if not entries:
        return
    field_by_id = {f.id: f.name for f in table.fields}
    update_set = set(update_cols)
    conflicted = sorted(
        {
            field_by_id.get(e.index_file.global_index_meta.index_field_id)
            for e in entries
        }
        & update_set
    )
    if conflicted:
        raise NotImplementedError(
            f"MERGE INTO would update columns {conflicted} that have a global "
            f"index; not supported (refusing to leave the index stale)."
        )


def _check_global_index_for_insert(table, snapshot) -> None:
    entries = _scan_global_index_entries(table, snapshot)
    if not entries:
        return
    field_by_id = {f.id: f.name for f in table.fields}
    indexed = sorted(
        {
            field_by_id.get(e.index_file.global_index_meta.index_field_id)
            for e in entries
        }
    )
    raise NotImplementedError(
        f"MERGE INTO INSERT is not supported on tables with global-index "
        f"columns {indexed} (btree/lumina/tantivy). Inserted rows would not "
        f"appear in the index. Drop the global index or omit when_not_matched."
    )


def _scan_global_index_entries(table, snapshot):
    from pypaimon.index.index_file_handler import IndexFileHandler

    handler = IndexFileHandler(table=table)
    return handler.scan(
        snapshot, lambda e: e.index_file.global_index_meta is not None
    )


def _union_update_cols(clauses: List[_NormalizedClause]) -> List[str]:
    seen: List[str] = []
    seen_set: set = set()
    for clause in clauses:
        for col in clause.spec.keys():
            if col not in seen_set:
                seen.append(col)
                seen_set.add(col)
    return seen


def _needed_target_cols(
    clauses: List[_NormalizedClause],
    on: Sequence[str],
    update_cols: Sequence[str],
    all_target_cols: Sequence[str],
) -> list:
    needed = set(on) | set(update_cols)
    for clause in clauses:
        for value in clause.spec.values():
            if callable(value):
                return list(all_target_cols)
            if isinstance(value, str) and value.startswith("t."):
                needed.add(value[2:])
    return [c for c in all_target_cols if c in needed]


def _normalize_set_spec(
    spec: SetSpec,
    target_field_names: Sequence[str],
) -> Dict[str, Any]:
    if isinstance(spec, str):
        if spec != "*":
            raise ValueError(
                f"SET spec strings other than '*' are not supported; got {spec!r}."
            )
        return {col: f"s.{col}" for col in target_field_names}
    if not isinstance(spec, dict):
        raise ValueError(
            f"SET spec must be '*' or a dict, got {type(spec).__name__}."
        )
    target_set = set(target_field_names)
    for col in spec:
        if col not in target_set:
            raise ValueError(
                f"SET key '{col}' is not a column of the target table "
                f"(columns: {list(target_field_names)})."
            )
    return dict(spec)


def _normalize_source(source: Any, catalog_options: Dict[str, str]):
    import ray.data

    if isinstance(source, ray.data.Dataset):
        return source
    if isinstance(source, str):
        from pypaimon.ray.ray_paimon import read_paimon
        return read_paimon(source, catalog_options)
    if isinstance(source, pa.Table):
        return ray.data.from_arrow(source)
    try:
        import pandas as pd
    except ImportError:
        pd = None
    if pd is not None and isinstance(source, pd.DataFrame):
        return ray.data.from_pandas(source)
    raise TypeError(
        "source must be a ray.data.Dataset, a Paimon table identifier string, "
        f"a pyarrow.Table, or a pandas.DataFrame; got {type(source).__name__}."
    )


def _validate_source_on_cols(source_ds, on: Sequence[str]) -> None:
    schema = source_ds.schema()
    if schema is None:
        return
    names = set(schema.names)
    missing = [c for c in on if c not in names]
    if missing:
        raise ValueError(
            f"'on' columns {missing} missing from source schema {list(names)}."
        )


def _apply_set(
    spec: Dict[str, Any],
    s_row: Optional[Dict[str, Any]],
    t_row: Optional[Dict[str, Any]],
    target_field_names: Sequence[str],
    null_unspecified: bool = False,
) -> Dict[str, Any]:
    combined = _prefixed(s_row, t_row)
    if t_row is not None:
        base = t_row
    elif s_row is not None and not null_unspecified:
        base = s_row
    else:
        base = {}
    out: Dict[str, Any] = {}
    for col in target_field_names:
        if col in spec:
            out[col] = _eval_set_value(spec[col], combined, s_row, t_row)
        elif col in base:
            out[col] = base[col]
        else:
            out[col] = None
    return out


def _prefixed(
    s_row: Optional[Dict[str, Any]], t_row: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if s_row is not None:
        for k, v in s_row.items():
            out[f"s.{k}"] = v
    if t_row is not None:
        for k, v in t_row.items():
            out[f"t.{k}"] = v
    return out


def _eval_set_value(
    value: Any,
    combined: Mapping[str, Any],
    s_row: Optional[Dict[str, Any]],
    t_row: Optional[Dict[str, Any]],
) -> Any:
    if callable(value):
        return value(combined)
    if isinstance(value, str):
        if value.startswith("s.") and s_row is not None:
            return s_row.get(value[2:])
        if value.startswith("t.") and t_row is not None:
            return t_row.get(value[2:])
    return value
