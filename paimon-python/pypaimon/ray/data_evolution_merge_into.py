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

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import pyarrow as pa

SetSpec = Union[str, Dict[str, Any]]
Condition = Callable[[Mapping[str, Any]], bool]


@dataclass
class WhenMatched:
    update: SetSpec
    condition: Optional[Condition] = None


@dataclass
class WhenNotMatched:
    insert: SetSpec
    condition: Optional[Condition] = None


def merge_into(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    on: Sequence[str],
    merge_condition: Optional[Condition] = None,
    when_matched: Sequence[WhenMatched] = (),
    when_not_matched: Sequence[WhenNotMatched] = (),
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    when_matched = list(when_matched)
    when_not_matched = list(when_not_matched)
    if not when_matched and not when_not_matched:
        raise ValueError(
            "At least one of when_matched or when_not_matched must be non-empty."
        )

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
    _validate_source_on_cols(source_ds, on)

    from pypaimon.schema.data_types import PyarrowFieldParser

    target_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )

    if matched_specs:
        _do_matched_update(
            target_table=table,
            target_identifier=target,
            source_ds=source_ds,
            on=list(on),
            merge_condition=merge_condition,
            clauses=matched_specs,
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            catalog_options=catalog_options,
        )

    if not_matched_specs:
        _do_not_matched_insert(
            target_identifier=target,
            source_ds=source_ds,
            on=list(on),
            merge_condition=merge_condition,
            clauses=not_matched_specs,
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            catalog_options=catalog_options,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )


@dataclass
class _NormalizedClause:
    spec: Dict[str, Any]
    condition: Optional[Condition]


def _do_not_matched_insert(
    *,
    target_identifier: str,
    source_ds,
    on: Sequence[str],
    merge_condition: Optional[Condition],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    catalog_options: Dict[str, str],
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> None:
    from pypaimon.ray.ray_paimon import read_paimon, write_paimon
    from pypaimon.ray.shuffle import _coerce_large_string_types

    needs_full_target = merge_condition is not None
    if needs_full_target:
        target_ds = read_paimon(target_identifier, catalog_options)
        target_by_key: Dict[tuple, List[Dict[str, Any]]] = {}
        for batch in target_ds.iter_batches(batch_format="pyarrow"):
            for row in batch.to_pylist():
                key = tuple(row.get(c) for c in on)
                target_by_key.setdefault(key, []).append(row)
    else:
        target_on_ds = read_paimon(
            target_identifier, catalog_options, projection=list(on)
        )
        target_keys: set = set()
        for batch in target_on_ds.iter_batches(batch_format="pyarrow"):
            cols = [batch.column(c).to_pylist() for c in on]
            for tup in zip(*cols):
                target_keys.add(tup)

    on_list = list(on)
    field_names = list(target_field_names)
    out_schema = target_pa_schema
    captured_clauses = clauses
    captured_merge_cond = merge_condition

    def _is_matched(s_row: Dict[str, Any]) -> bool:
        key = tuple(s_row.get(c) for c in on_list)
        if needs_full_target:
            t_rows = target_by_key.get(key)
            if not t_rows:
                return False
            for t_row in t_rows:
                if captured_merge_cond(_prefixed(s_row, t_row)):
                    return True
            return False
        return key in target_keys

    def _transform(batch: pa.Table) -> pa.Table:
        rows = batch.to_pylist()
        out = []
        for s_row in rows:
            if _is_matched(s_row):
                continue
            for clause in captured_clauses:
                cond = clause.condition
                if cond is not None and not cond(_prefixed(s_row, None)):
                    continue
                out.append(_apply_set(clause.spec, s_row, None, field_names))
                break
        aligned = [{name: r.get(name) for name in field_names} for r in out]
        result = pa.Table.from_pylist(aligned, schema=out_schema)
        return _coerce_large_string_types(result)

    transformed = source_ds.map_batches(_transform, batch_format="pyarrow")
    write_paimon(
        transformed,
        target_identifier,
        catalog_options,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )


def _do_matched_update(
    *,
    target_table,
    target_identifier: str,
    source_ds,
    on: Sequence[str],
    merge_condition: Optional[Condition],
    clauses: List[_NormalizedClause],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    catalog_options: Dict[str, str],
) -> None:
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    update_cols_union = _union_update_cols(clauses)
    needs_full_target = merge_condition is not None or any(
        c.condition is not None for c in clauses
    )
    if needs_full_target:
        needed_cols = list(target_field_names)
    else:
        needed_cols = _needed_target_cols(
            clauses, on, update_cols_union, target_field_names
        )
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]

    target_ds = read_paimon(target_identifier, catalog_options, projection=projection)
    target_by_key: Dict[tuple, List[Dict[str, Any]]] = {}
    for batch in target_ds.iter_batches(batch_format="pyarrow"):
        for row in batch.to_pylist():
            key = tuple(row.get(c) for c in on)
            target_by_key.setdefault(key, []).append(row)

    if not target_by_key:
        return

    field_names = list(target_field_names)
    output_row_ids: List[Any] = []
    output_cols: Dict[str, list] = {c: [] for c in update_cols_union}

    for batch in source_ds.iter_batches(batch_format="pyarrow"):
        for s_row in batch.to_pylist():
            key = tuple(s_row.get(c) for c in on)
            t_rows = target_by_key.get(key)
            if not t_rows:
                continue
            for t_row in t_rows:
                combined = _prefixed(s_row, t_row)
                if merge_condition is not None and not merge_condition(combined):
                    continue
                for clause in clauses:
                    if clause.condition is not None and not clause.condition(combined):
                        continue
                    new_values = _apply_set(clause.spec, s_row, t_row, field_names)
                    output_row_ids.append(t_row[row_id_name])
                    for col in update_cols_union:
                        output_cols[col].append(new_values.get(col, t_row.get(col)))
                    break

    if not output_row_ids:
        return

    pydict = {row_id_name: output_row_ids}
    pydict.update(output_cols)
    schema_fields = [pa.field(row_id_name, pa.int64(), nullable=False)]
    for col in update_cols_union:
        schema_fields.append(target_pa_schema.field(col))
    update_table = pa.Table.from_pydict(pydict, schema=pa.schema(schema_fields))

    wb = target_table.new_batch_write_builder()
    tu = wb.new_update().with_update_type(update_cols_union)
    msgs = tu.update_by_arrow_with_row_id(update_table)
    tc = wb.new_commit()
    tc.commit(msgs)
    tc.close()


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
) -> Dict[str, Any]:
    combined = _prefixed(s_row, t_row)
    base = t_row if t_row is not None else (s_row if s_row is not None else {})
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
