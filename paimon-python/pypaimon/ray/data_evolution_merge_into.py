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
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pyarrow as pa

from pypaimon.ray.condition_expr import ConditionExpr
from pypaimon.ray.condition_expr import parse as parse_condition

SetSpec = Union[str, Dict[str, Any]]
Condition = str
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
    condition: Optional[ConditionExpr]


def merge_into(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    on: OnSpec,
    when_matched: Sequence[WhenMatched] = (),
    when_not_matched: Sequence[WhenNotMatched] = (),
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    allow_multiple_matches: bool = False,
) -> Dict[str, int]:
    _require_ray_join()
    num_partitions = _resolve_num_partitions(num_partitions)
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
    on_map = dict(zip(target_on_cols, source_on_cols))
    matched_specs = [
        _NormalizedClause(
            spec=_normalize_set_spec(c.update, target_field_names, on_map),
            condition=parse_condition(c.condition) if c.condition else None,
        )
        for c in when_matched
    ]
    not_matched_specs = [
        _NormalizedClause(
            spec=_normalize_set_spec(c.insert, target_field_names, on_map),
            condition=parse_condition(c.condition) if c.condition else None,
        )
        for c in when_not_matched
    ]

    update_cols: set = set()
    for clause in matched_specs:
        update_cols.update(clause.spec.keys())
    _reject_blob_updates(table, update_cols)

    source_ds = _normalize_source(source, catalog_options)
    _validate_source_on_cols(source_ds, source_on_cols)

    base_snapshot = table.snapshot_manager().get_latest_snapshot()

    global_index_action = (
        table.options.global_index_column_update_action()
        or GLOBAL_INDEX_ACTION_THROW_ERROR
    )

    from pypaimon.schema.data_types import PyarrowFieldParser

    target_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )

    update_ds = None
    insert_ds = None
    update_cols_union: List[str] = []

    # With both clauses on a non-empty target, matched and not-matched routing
    # share the same source/target equi-join. Build them from one materialized
    # LEFT_OUTER join instead of reading and shuffling the target table twice.
    if matched_specs and not_matched_specs and base_snapshot is not None:
        update_cols_union = _union_update_cols(matched_specs)
        update_ds, insert_ds = _build_unified_both(
            target_identifier=target,
            source_ds=source_ds,
            target_on=target_on_cols,
            source_on=source_on_cols,
            matched_clauses=matched_specs,
            not_matched_clauses=not_matched_specs,
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            update_cols=update_cols_union,
            catalog_options=catalog_options,
            num_partitions=num_partitions,
        )
    else:
        # Empty target → no rows can match; matched UPDATE is a no-op.
        if matched_specs and base_snapshot is not None:
            update_cols_union = _union_update_cols(matched_specs)
            update_ds = _build_matched_update_ds(
                target_identifier=target,
                source_ds=source_ds,
                target_on=target_on_cols,
                source_on=source_on_cols,
                clauses=matched_specs,
                target_field_names=target_field_names,
                target_pa_schema=target_pa_schema,
                update_cols=update_cols_union,
                catalog_options=catalog_options,
                num_partitions=num_partitions,
            )

        if not_matched_specs:
            # Empty target: nothing can match, so every source row inserts.
            # Skip all joins (ray's hash join crashes on empty partitions).
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
                target_empty=base_snapshot is None,
            )

    update_msgs: list = []
    num_updated = 0
    if update_ds is not None:
        update_msgs, num_updated = _distributed_update_apply(
            update_ds,
            table,
            update_cols_union,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            allow_multiple_matches=allow_multiple_matches,
        )

    all_msgs: list = list(update_msgs)
    num_inserted = 0
    if insert_ds is not None:
        insert_msgs = _distributed_write_collect_msgs(
            insert_ds, table, ray_remote_args=ray_remote_args, concurrency=concurrency
        )
        num_inserted = sum(f.row_count for m in insert_msgs for f in m.new_files)
        all_msgs.extend(insert_msgs)
    # Mirror Spark's checkUpdateResult: scope the global-index action to the
    # partitions the update actually wrote and the updated indexed columns.
    all_msgs.extend(
        _apply_global_index_update_action(
            table, base_snapshot, update_cols_union, update_msgs, global_index_action
        )
    )
    if all_msgs:
        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.commit(all_msgs)
        tc.close()

    return {"num_updated": num_updated, "num_inserted": num_inserted}


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
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    catalog_options: Dict[str, str],
    num_partitions: int,
):
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    needed_cols = _resolve_target_projection(
        clauses, target_on, update_cols, target_field_names,
    )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]

    target_ds = read_paimon(target_identifier, catalog_options, projection=projection)
    update_schema = _build_update_schema(target_pa_schema, update_cols, row_id_name)

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
    captured_update_cols = list(update_cols)
    captured_field_names = list(target_field_names)
    captured_row_id_name = row_id_name
    captured_on_pairs = list(zip(source_on, target_on))
    captured_schema = update_schema

    if _clauses_use_vector_fast_path(clauses):
        first_spec = clauses[0].spec

        def _fast(batch: pa.Table) -> pa.Table:
            return _vectorized_matched_transform(
                batch,
                first_spec,
                captured_on_pairs,
                captured_update_cols,
                captured_row_id_name,
                captured_schema,
            )

        return joined.map_batches(_fast, batch_format="pyarrow")

    def _transform(batch: pa.Table) -> pa.Table:
        return _apply_matched_transform(
            batch,
            captured_clauses,
            captured_on_pairs,
            captured_update_cols,
            captured_field_names,
            captured_row_id_name,
            captured_schema,
        )

    return joined.map_batches(_transform, batch_format="pyarrow")


def _apply_matched_transform(
    batch: pa.Table,
    clauses: List[_NormalizedClause],
    on_pairs: Sequence[Tuple[str, str]],
    update_cols: Sequence[str],
    field_names: Sequence[str],
    row_id_name: str,
    update_schema: pa.Schema,
) -> pa.Table:
    rows = batch.to_pylist()
    out_row_ids: list = []
    out_cols: Dict[str, list] = {c: [] for c in update_cols}
    for row in rows:
        s_row = {k[2:]: v for k, v in row.items() if k.startswith("s.")}
        t_row = {k[2:]: v for k, v in row.items() if k.startswith("t.")}
        for s_key, t_key in on_pairs:
            if s_key not in s_row and t_key in t_row:
                s_row[s_key] = t_row[t_key]
        combined = _prefixed(s_row, t_row)
        for clause in clauses:
            if clause.condition is not None and not clause.condition.eval(combined):
                continue
            new_values = _apply_set(clause.spec, s_row, t_row, field_names)
            out_row_ids.append(t_row[row_id_name])
            for col in update_cols:
                out_cols[col].append(new_values.get(col, t_row.get(col)))
            break
    return pa.Table.from_pydict(
        {row_id_name: out_row_ids, **out_cols},
        schema=update_schema,
    )


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
    num_partitions: int,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    allow_multiple_matches: bool = False,
) -> Tuple[list, int]:
    import numpy as np
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
        return [], 0

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
    captured_sorted_arr = np.asarray(captured_sorted, dtype=np.int64)
    first = captured_sorted_arr[0]
    captured_precomputed = precomputed_info
    total_row_count = planner.total_row_count

    def _assign_frid(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch.append_column(frid_col, pa.array([], type=pa.int64()))
        rid_col = batch.column(row_id_name)
        if rid_col.null_count:
            raise ValueError(
                "_ROW_ID is null; planner snapshot is stale "
                "or matched rows come from a different table."
            )
        rids = rid_col.to_numpy(zero_copy_only=False)
        # Out-of-range _ROW_IDs would silently map via searchsorted wrap-around.
        out_of_range = (rids < first) | (rids >= total_row_count)
        if out_of_range.any():
            bad = rids[out_of_range][0]
            raise ValueError(
                f"_ROW_ID {bad} is out of valid range "
                f"[{first}, {total_row_count}); planner snapshot is stale "
                f"or matched rows come from a different table."
            )
        idx = np.searchsorted(captured_sorted_arr, rids, side="right") - 1
        frids = captured_sorted_arr[idx]
        return batch.append_column(frid_col, pa.array(frids, type=pa.int64()))

    with_frid = update_ds.map_batches(_assign_frid, batch_format="pyarrow")

    captured_table = table
    captured_cols = cols

    def _apply_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return pa.Table.from_pydict({
                "msgs_blob": pa.array([], type=pa.binary()),
                "n_updated": pa.array([], type=pa.int64()),
            })

        # One target _ROW_ID matched by several source rows. Default: refuse
        # (the winning value is otherwise undefined, as in Spark DE's
        # checkCardinality=false). Opt-in keeps the first match deterministically.
        group_row_ids = group.column(row_id_name).to_pylist()
        if len(set(group_row_ids)) != len(group_row_ids):
            if not allow_multiple_matches:
                raise ValueError(
                    "MERGE matched multiple source rows to the same target "
                    "_ROW_ID. Deduplicate the source, or pass "
                    "allow_multiple_matches=True to keep the first match."
                )
            seen: set = set()
            keep_indices: list = []
            for i, rid in enumerate(group_row_ids):
                if rid not in seen:
                    seen.add(rid)
                    keep_indices.append(i)
            group = group.take(pa.array(keep_indices, type=pa.int64()))

        for_update = group.drop_columns([frid_col])
        worker = TableUpdateByRowId(
            captured_table,
            "_merge_into_shard_" + uuid.uuid4().hex[:8],
            BATCH_COMMIT_IDENTIFIER,
            precomputed_files_info=captured_precomputed,
        )
        msgs = worker.update_columns(for_update, list(captured_cols))
        return pa.Table.from_pydict({
            "msgs_blob": [pickle.dumps(msgs)],
            "n_updated": pa.array([for_update.num_rows], type=pa.int64()),
        })

    # One group per target data file (distinct _FIRST_ROW_ID). Drive the write
    # shuffle with the same num_partitions knob as the join (Spark's single
    # shuffle.partitions), bounded by the file count so small merges don't spawn
    # empty reduce tasks and large ones scale past a fixed cap.
    group_partitions = max(1, min(len(captured_sorted), num_partitions))
    msgs_ds = with_frid.groupby(frid_col, num_partitions=group_partitions).map_groups(
        _apply_group, batch_format="pyarrow"
    )

    all_msgs: list = []
    num_updated = 0
    for batch in msgs_ds.iter_batches(batch_format="pyarrow"):
        for blob in batch.column("msgs_blob").to_pylist():
            all_msgs.extend(pickle.loads(blob))
        for n in batch.column("n_updated").to_pylist():
            num_updated += n
    return all_msgs, num_updated


GLOBAL_INDEX_ACTION_THROW_ERROR = "THROW_ERROR"
GLOBAL_INDEX_ACTION_DROP_PARTITION_INDEX = "DROP_PARTITION_INDEX"


def _resolve_num_partitions(num_partitions: Optional[int]) -> int:
    if num_partitions is not None:
        return num_partitions
    try:
        import ray

        cpus = ray.cluster_resources().get("CPU", 16)
        return max(16, int(cpus) * 2)
    except Exception:
        return 16


def _clauses_use_vector_fast_path(
    clauses: List[_NormalizedClause],
) -> bool:
    if not clauses:
        return False
    for c in clauses:
        if c.condition is not None:
            return False
        for v in c.spec.values():
            if callable(v):
                return False
    return True


def _vectorized_matched_transform(
    batch: pa.Table,
    spec: Dict[str, Any],
    on_pairs: Sequence[Tuple[str, str]],
    update_cols: Sequence[str],
    row_id_name: str,
    update_schema: pa.Schema,
) -> pa.Table:
    available = set(batch.schema.names)
    arrays: list = [batch.column(f"t.{row_id_name}")]
    for col in update_cols:
        out_type = update_schema.field(col).type
        if col in spec:
            arrays.append(_resolve_spec_array(spec[col], batch, available, on_pairs, out_type))
        else:
            arrays.append(batch.column(f"t.{col}"))
    return pa.Table.from_arrays(arrays, schema=update_schema)


def _vectorized_insert_transform(
    batch: pa.Table,
    spec: Dict[str, Any],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
) -> pa.Table:
    available = set(batch.schema.names)
    arrays: list = []
    for col in target_field_names:
        out_type = target_pa_schema.field(col).type
        if col in spec:
            arrays.append(_resolve_spec_array(spec[col], batch, available, (), out_type))
        else:
            arrays.append(pa.nulls(batch.num_rows, type=out_type))
    return pa.Table.from_arrays(arrays, schema=target_pa_schema)


def _resolve_spec_array(
    val: Any,
    batch: pa.Table,
    available: set,
    on_pairs: Sequence[Tuple[str, str]],
    out_type: pa.DataType,
):
    if isinstance(val, str) and val.startswith("s."):
        ref = val[2:]
        if f"s.{ref}" in available:
            return batch.column(f"s.{ref}")
        # Equi-join drops the right-side join key; fall back to target's value.
        for sk, tk in on_pairs:
            if sk == ref and f"t.{tk}" in available:
                return batch.column(f"t.{tk}")
        return pa.nulls(batch.num_rows, type=out_type)
    if isinstance(val, str) and val.startswith("t."):
        ref = val[2:]
        col_name = f"t.{ref}"
        return batch.column(col_name) if col_name in available else pa.nulls(
            batch.num_rows, type=out_type
        )
    return pa.array([val] * batch.num_rows, type=out_type)


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
    target_empty: bool = False,
):
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.ray.shuffle import _coerce_large_string_types

    captured_clauses = clauses
    captured_field_names = list(target_field_names)
    out_schema = target_pa_schema

    source_schema = source_ds.schema()
    source_cols = list(source_schema.names) if source_schema is not None else list(source_on)
    source_renamed = source_ds.rename_columns({c: f"s.{c}" for c in source_cols})

    if target_empty:
        unmatched = source_renamed
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

    if _clauses_use_vector_fast_path(clauses):
        first_spec = clauses[0].spec

        def _fast(batch: pa.Table) -> pa.Table:
            return _coerce_large_string_types(
                _vectorized_insert_transform(
                    batch, first_spec, captured_field_names, out_schema
                )
            )

        return unmatched.map_batches(_fast, batch_format="pyarrow")

    def _transform(batch: pa.Table) -> pa.Table:
        return _apply_insert_transform(
            batch, captured_clauses, captured_field_names, out_schema
        )

    return unmatched.map_batches(_transform, batch_format="pyarrow")


def _apply_insert_transform(
    batch: pa.Table,
    clauses: List[_NormalizedClause],
    field_names: Sequence[str],
    out_schema: pa.Schema,
) -> pa.Table:
    from pypaimon.ray.shuffle import _coerce_large_string_types

    rows = batch.to_pylist()
    out = []
    for row in rows:
        s_row = {k[2:]: v for k, v in row.items() if k.startswith("s.")}
        combined = _prefixed(s_row, None)
        for clause in clauses:
            if clause.condition is not None and not clause.condition.eval(combined):
                continue
            out.append(
                _apply_set(
                    clause.spec, s_row, None, field_names, null_unspecified=True
                )
            )
            break
    aligned = [{name: r.get(name) for name in field_names} for r in out]
    return _coerce_large_string_types(pa.Table.from_pylist(aligned, schema=out_schema))


def _build_unified_both(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    matched_clauses: List[_NormalizedClause],
    not_matched_clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    catalog_options: Dict[str, str],
    num_partitions: int,
):
    import pyarrow.compute as pc

    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.ray.shuffle import _coerce_large_string_types
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name

    needed_cols = _resolve_target_projection(
        matched_clauses, target_on, update_cols, target_field_names,
    )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]
    target_ds = read_paimon(target_identifier, catalog_options, projection=projection)
    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in target_ds.schema().names}
    )
    source_schema = source_ds.schema()
    source_cols = list(source_schema.names) if source_schema is not None else list(source_on)
    source_renamed = source_ds.rename_columns({c: f"s.{c}" for c in source_cols})

    # One LEFT_OUTER join feeds both branches: rows with a non-null target side
    # are matched (UPDATE), null target side means no key match (INSERT). The
    # join shuffle is the dominant cost, so materialize once and route both ways
    # instead of reading and shuffling the target table twice.
    joined = source_renamed.join(
        target_renamed,
        join_type="left_outer",
        num_partitions=num_partitions,
        on=tuple(f"s.{c}" for c in source_on),
        right_on=tuple(f"t.{c}" for c in target_on),
    ).materialize()

    t_row_id_col = f"t.{row_id_name}"
    on_pairs = list(zip(source_on, target_on))
    update_schema = _build_update_schema(target_pa_schema, update_cols, row_id_name)

    use_fast_matched = _clauses_use_vector_fast_path(matched_clauses)
    first_matched_spec = matched_clauses[0].spec if use_fast_matched else None
    m_update_cols = list(update_cols)
    m_field_names = list(target_field_names)

    def _matched_batch(batch: pa.Table) -> pa.Table:
        sub = batch.filter(pc.is_valid(batch.column(t_row_id_col)))
        if use_fast_matched:
            return _vectorized_matched_transform(
                sub, first_matched_spec, on_pairs, m_update_cols,
                row_id_name, update_schema,
            )
        return _apply_matched_transform(
            sub, matched_clauses, on_pairs, m_update_cols,
            m_field_names, row_id_name, update_schema,
        )

    update_ds = joined.map_batches(_matched_batch, batch_format="pyarrow")

    i_field_names = list(target_field_names)
    use_fast_insert = _clauses_use_vector_fast_path(not_matched_clauses)
    first_insert_spec = not_matched_clauses[0].spec if use_fast_insert else None

    def _insert_batch(batch: pa.Table) -> pa.Table:
        sub = batch.filter(pc.is_null(batch.column(t_row_id_col)))
        if use_fast_insert:
            return _coerce_large_string_types(
                _vectorized_insert_transform(
                    sub, first_insert_spec, i_field_names, target_pa_schema
                )
            )
        return _apply_insert_transform(
            sub, not_matched_clauses, i_field_names, target_pa_schema
        )

    insert_ds = joined.map_batches(_insert_batch, batch_format="pyarrow")

    return update_ds, insert_ds


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


def _apply_global_index_update_action(
    table, snapshot, update_cols: Sequence[str], update_msgs, action: str
) -> list:
    """Handle updates touching globally-indexed columns, mirroring Spark's
    ``checkUpdateResult``.

    Scoped exactly like Spark: only index entries whose partition was written
    by the update *and* whose indexed column is among the updated columns are
    affected. THROW_ERROR (default) raises; DROP_PARTITION_INDEX drops those
    entries (returned as index-delete commit messages, rebuild afterwards).
    Like Spark, the INSERT path is left untouched.
    """
    if snapshot is None or not update_cols or not update_msgs:
        return []
    entries = _scan_global_index_entries(table, snapshot)
    if not entries:
        return []
    field_by_id = {f.id: f.name for f in table.fields}
    update_set = set(update_cols)
    affected_partitions = {tuple(m.partition) for m in update_msgs}
    affected = [
        e for e in entries
        if field_by_id.get(e.index_file.global_index_meta.index_field_id) in update_set
        and tuple(e.partition.values) in affected_partitions
    ]
    if not affected:
        return []
    if action == GLOBAL_INDEX_ACTION_DROP_PARTITION_INDEX:
        return _build_index_delete_msgs(affected)
    conflicted = sorted(
        {field_by_id.get(e.index_file.global_index_meta.index_field_id) for e in affected}
    )
    raise NotImplementedError(
        f"MERGE INTO would update columns {conflicted} that have a global "
        f"index; not supported (refusing to leave the index stale). Set "
        f"'global-index.column-update-action' = 'DROP_PARTITION_INDEX' to drop "
        f"the affected index instead."
    )


def _build_index_delete_msgs(entries) -> list:
    """Group scanned index entries by partition into index-delete messages."""
    from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
    from pypaimon.write.commit_message import CommitMessage

    by_partition: Dict[tuple, list] = {}
    for e in entries:
        key = tuple(e.partition.values)
        by_partition.setdefault(key, []).append(
            IndexManifestEntry(
                kind=1, partition=e.partition, bucket=e.bucket, index_file=e.index_file
            )
        )
    return [
        CommitMessage(partition=key, bucket=0, new_files=[], index_files=dels)
        for key, dels in by_partition.items()
    ]


def _scan_global_index_entries(table, snapshot):
    from pypaimon.index.index_file_handler import IndexFileHandler

    handler = IndexFileHandler(table=table)
    return handler.scan(
        snapshot, lambda e: e.index_file.global_index_meta is not None
    )


def _require_ray_join() -> None:
    """merge_into relies on ``Dataset.join`` (ray>=2.50). Read/sink users on
    older ray are unaffected unless they call this, so check only here."""
    import ray
    from ray.data import Dataset

    if not hasattr(Dataset, "join"):
        raise RuntimeError(
            f"merge_into requires ray>=2.50 (Dataset.join); "
            f"installed ray is {ray.__version__}."
        )


def _reject_blob_updates(table, update_cols: set) -> None:
    blob_cols = [
        f.name
        for f in table.table_schema.fields
        if f.name in update_cols and getattr(f.type, "type", None) == "BLOB"
    ]
    if blob_cols:
        raise NotImplementedError(
            f"merge_into cannot update blob columns {blob_cols}; "
            f"the row-id rewrite path skips .blob files."
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
    # Target needs only: join keys, t.col refs, and cols that may fall back
    # (not set by every clause). Cols all clauses set from source aren't read.
    needed = set(on)
    set_by_all = set(update_cols)
    for clause in clauses:
        for value in clause.spec.values():
            if callable(value):
                return list(all_target_cols)
            if isinstance(value, str) and value.startswith("t."):
                needed.add(value[2:])
        set_by_all &= set(clause.spec.keys())
    needed |= set(update_cols) - set_by_all
    return [c for c in all_target_cols if c in needed]


def _resolve_target_projection(
    clauses: List[_NormalizedClause],
    target_on: Sequence[str],
    update_cols: Sequence[str],
    target_field_names: Sequence[str],
) -> list:
    # Precise: SET-side needs plus the target columns each parsed condition
    # references. Anything not referenced (e.g. blob) is never read.
    needed = set(_needed_target_cols(clauses, target_on, update_cols, target_field_names))
    target_set = set(target_field_names)
    for clause in clauses:
        if clause.condition is not None:
            needed |= clause.condition.target_columns() & target_set
    return [c for c in target_field_names if c in needed]


def _normalize_set_spec(
    spec: SetSpec,
    target_field_names: Sequence[str],
    on_map: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    on_map = on_map or {}
    if isinstance(spec, str):
        if spec != "*":
            raise ValueError(
                f"SET spec strings other than '*' are not supported; got {spec!r}."
            )
        # A renamed ON key resolves via the source's ON column, not its own name.
        return {col: f"s.{on_map.get(col, col)}" for col in target_field_names}
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
