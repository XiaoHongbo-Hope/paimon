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

    from pypaimon.schema.data_types import PyarrowFieldParser

    target_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )
    base_snapshot = table.snapshot_manager().get_latest_snapshot()
    is_self_merge = isinstance(source, str) and source == target

    update_arrow: Optional[pa.Table] = None
    update_cols_union: List[str] = []
    write_update_cols: List[str] = []
    if matched_specs:
        update_cols_union = _union_update_cols(matched_specs)
        if base_snapshot is not None:
            _check_global_index_collision(table, base_snapshot, update_cols_union)
        write_update_cols = [c for c in update_cols_union if c not in target_on_cols]
        if write_update_cols:
            update_arrow = _compute_matched_update(
                target_identifier=target,
                source_ds=source_ds,
                target_on=target_on_cols,
                source_on=source_on_cols,
                merge_condition=merge_condition,
                clauses=matched_specs,
                target_field_names=target_field_names,
                target_pa_schema=target_pa_schema,
                update_cols=write_update_cols,
                catalog_options=catalog_options,
                is_self_merge=is_self_merge,
                num_partitions=num_partitions,
            )

    insert_ds = None
    if not_matched_specs and not is_self_merge:
        matched_keys_override: Optional[set] = None
        if merge_condition is not None:
            matched_keys_override = _compute_matched_source_keys(
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
            matched_keys_override=matched_keys_override,
        )

    wb = table.new_batch_write_builder()
    all_msgs: list = []
    if update_arrow is not None and update_arrow.num_rows > 0:
        tu = wb.new_update().with_update_type(write_update_cols)
        all_msgs.extend(tu.update_by_arrow_with_row_id(update_arrow))
    if insert_ds is not None:
        all_msgs.extend(
            _distributed_write_collect_msgs(
                insert_ds, table, ray_remote_args=ray_remote_args, concurrency=concurrency
            )
        )
    if all_msgs:
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


def _compute_matched_update(
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
) -> Optional[pa.Table]:
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

    if is_self_merge:
        return _materialize_self_merge_update(
            target_ds=target_ds,
            merge_condition=merge_condition,
            clauses=clauses,
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            update_cols=update_cols,
            row_id_name=row_id_name,
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
            {captured_row_id_name: out_row_ids, **out_cols}
        )

    transformed = joined.map_batches(_transform, batch_format="pyarrow")
    batches = [b for b in transformed.iter_batches(batch_format="pyarrow") if b.num_rows > 0]
    if not batches:
        return None
    combined_table = pa.concat_tables(batches)

    _check_cardinality(combined_table, row_id_name)
    return _cast_update_arrow(
        combined_table, target_pa_schema, update_cols, row_id_name
    )


def _materialize_self_merge_update(
    *,
    target_ds,
    merge_condition: Optional[Condition],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> Optional[pa.Table]:
    captured_clauses = clauses
    captured_merge_cond = merge_condition
    captured_update_cols = list(update_cols)
    captured_field_names = list(target_field_names)

    out_row_ids: list = []
    out_cols: Dict[str, list] = {c: [] for c in captured_update_cols}
    for batch in target_ds.iter_batches(batch_format="pyarrow"):
        for row in batch.to_pylist():
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
                out_row_ids.append(t_row[row_id_name])
                for col in captured_update_cols:
                    out_cols[col].append(new_values.get(col, t_row.get(col)))
                break

    if not out_row_ids:
        return None
    combined_table = pa.Table.from_pydict({row_id_name: out_row_ids, **out_cols})
    _check_cardinality(combined_table, row_id_name)
    return _cast_update_arrow(
        combined_table, target_pa_schema, update_cols, row_id_name
    )


def _compute_matched_source_keys(
    *,
    target_identifier: str,
    source_ds,
    target_on: Sequence[str],
    source_on: Sequence[str],
    merge_condition: Condition,
    catalog_options: Dict[str, str],
    num_partitions: int,
) -> set:
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

    on_pairs = list(zip(source_on, target_on))
    captured_merge_cond = merge_condition
    captured_source_on = list(source_on)

    def _emit_matched_keys(batch: pa.Table) -> pa.Table:
        out_cols: Dict[str, list] = {c: [] for c in captured_source_on}
        for row in batch.to_pylist():
            s_row = {k[2:]: v for k, v in row.items() if k.startswith("s.")}
            t_row = {k[2:]: v for k, v in row.items() if k.startswith("t.")}
            for sk, tk in on_pairs:
                if sk not in s_row and tk in t_row:
                    s_row[sk] = t_row[tk]
            combined = _prefixed(s_row, t_row)
            if captured_merge_cond(combined):
                for c in captured_source_on:
                    out_cols[c].append(s_row.get(c))
        return pa.Table.from_pydict(out_cols)

    matched_ds = joined.map_batches(_emit_matched_keys, batch_format="pyarrow")
    matched_keys: set = set()
    for batch in matched_ds.iter_batches(batch_format="pyarrow"):
        if batch.num_rows == 0:
            continue
        cols = [batch.column(c).to_pylist() for c in source_on]
        for tup in zip(*cols):
            matched_keys.add(tup)
    return matched_keys


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
    matched_keys_override: Optional[set] = None,
):
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.ray.shuffle import _coerce_large_string_types

    captured_clauses = clauses
    captured_field_names = list(target_field_names)
    out_schema = target_pa_schema

    if matched_keys_override is not None:
        captured_keys = matched_keys_override
        captured_source_on = list(source_on)

        def _filter_and_apply(batch: pa.Table) -> pa.Table:
            rows = batch.to_pylist()
            out = []
            for s_row in rows:
                key = tuple(s_row.get(c) for c in captured_source_on)
                if key in captured_keys:
                    continue
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

        return source_ds.map_batches(_filter_and_apply, batch_format="pyarrow")

    target_ds = read_paimon(
        target_identifier, catalog_options, projection=list(target_on)
    )
    target_renamed = target_ds.rename_columns(
        {c: f"t.{c}" for c in target_on}
    )
    source_schema = source_ds.schema()
    source_cols = list(source_schema.names) if source_schema is not None else list(source_on)
    source_renamed = source_ds.rename_columns({c: f"s.{c}" for c in source_cols})

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


def _check_cardinality(update_table: pa.Table, row_id_name: str) -> None:
    row_ids = update_table.column(row_id_name).to_pylist()
    if len(set(row_ids)) == len(row_ids):
        return
    seen: set = set()
    dupes: set = set()
    for rid in row_ids:
        if rid in seen:
            dupes.add(rid)
        seen.add(rid)
    raise ValueError(
        f"MERGE INTO matched the same target _ROW_IDs {sorted(dupes)[:5]} "
        f"via multiple source rows; source must be unique on the join keys."
    )


def _cast_update_arrow(
    update_table: pa.Table,
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> pa.Table:
    schema_fields = [pa.field(row_id_name, pa.int64(), nullable=False)]
    for col in update_cols:
        schema_fields.append(target_pa_schema.field(col))
    return update_table.cast(pa.schema(schema_fields))


def _check_global_index_collision(
    table, snapshot, update_cols: Sequence[str]
) -> None:
    from pypaimon.index.index_file_handler import IndexFileHandler

    handler = IndexFileHandler(table=table)
    entries = handler.scan(
        snapshot, lambda e: e.index_file.global_index_meta is not None
    )
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
