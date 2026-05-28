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

from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Union

import pyarrow as pa

SetSpec = Union[str, Dict[str, Any]]
Condition = Callable[[Mapping[str, Any]], bool]


def merge_into(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    on: Sequence[str],
    when_matched_update: Optional[SetSpec] = None,
    when_matched_update_condition: Optional[Condition] = None,
    when_not_matched_insert: Optional[SetSpec] = None,
    when_not_matched_insert_condition: Optional[Condition] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    if when_matched_update is None and when_not_matched_insert is None:
        raise ValueError(
            "At least one of when_matched_update or when_not_matched_insert "
            "must be provided."
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
    matched_update = _normalize_set_spec(when_matched_update, target_field_names)
    not_matched_insert = _normalize_set_spec(
        when_not_matched_insert, target_field_names
    )

    source_ds = _normalize_source(source, catalog_options)
    _validate_source_on_cols(source_ds, on)

    from pypaimon.schema.data_types import PyarrowFieldParser

    target_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )

    if matched_update is not None:
        _do_matched_update(
            target_table=table,
            target_identifier=target,
            source_ds=source_ds,
            on=list(on),
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            spec=matched_update,
            condition=when_matched_update_condition,
            catalog_options=catalog_options,
        )

    if not_matched_insert is not None:
        _do_not_matched_insert(
            target_identifier=target,
            source_ds=source_ds,
            on=list(on),
            target_field_names=target_field_names,
            target_pa_schema=target_pa_schema,
            spec=not_matched_insert,
            condition=when_not_matched_insert_condition,
            catalog_options=catalog_options,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )


def _do_not_matched_insert(
    *,
    target_identifier: str,
    source_ds,
    on: Sequence[str],
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    spec: Dict[str, Any],
    condition: Optional[Condition],
    catalog_options: Dict[str, str],
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> None:
    from pypaimon.ray.ray_paimon import read_paimon, write_paimon
    from pypaimon.ray.shuffle import _coerce_large_string_types

    target_on_ds = read_paimon(
        target_identifier, catalog_options, projection=list(on)
    )
    target_keys = set()
    for batch in target_on_ds.iter_batches(batch_format="pyarrow"):
        cols = [batch.column(c).to_pylist() for c in on]
        for tup in zip(*cols):
            target_keys.add(tup)

    on_list = list(on)
    field_names = list(target_field_names)
    insert_spec = spec
    insert_cond = condition
    out_schema = target_pa_schema

    def _transform(batch: pa.Table) -> pa.Table:
        rows = batch.to_pylist()
        out = []
        for s_row in rows:
            key = tuple(s_row.get(c) for c in on_list)
            if key in target_keys:
                continue
            if insert_cond is not None and not insert_cond(_prefixed(s_row, None)):
                continue
            out.append(_apply_set(insert_spec, s_row, None, field_names))
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
    target_field_names: Sequence[str],
    target_pa_schema: pa.Schema,
    spec: Dict[str, Any],
    condition: Optional[Condition],
    catalog_options: Dict[str, str],
) -> None:
    from pypaimon.ray.ray_paimon import read_paimon
    from pypaimon.table.special_fields import SpecialFields

    row_id_name = SpecialFields.ROW_ID.name
    update_cols = list(spec.keys())
    needed_cols = _needed_target_cols(spec, on, update_cols, target_field_names, condition)
    projection = [row_id_name] + [c for c in needed_cols if c != row_id_name]

    target_ds = read_paimon(target_identifier, catalog_options, projection=projection)
    target_by_key: Dict[tuple, Dict[str, Any]] = {}
    for batch in target_ds.iter_batches(batch_format="pyarrow"):
        for row in batch.to_pylist():
            key = tuple(row.get(c) for c in on)
            target_by_key[key] = row

    if not target_by_key:
        return

    field_names = list(target_field_names)
    output_row_ids: list = []
    output_cols: Dict[str, list] = {c: [] for c in update_cols}

    for batch in source_ds.iter_batches(batch_format="pyarrow"):
        for s_row in batch.to_pylist():
            key = tuple(s_row.get(c) for c in on)
            t_row = target_by_key.get(key)
            if t_row is None:
                continue
            if condition is not None and not condition(_prefixed(s_row, t_row)):
                continue
            new_values = _apply_set(spec, s_row, t_row, field_names)
            output_row_ids.append(t_row[row_id_name])
            for col in update_cols:
                output_cols[col].append(new_values[col])

    if not output_row_ids:
        return

    pydict = {row_id_name: output_row_ids}
    pydict.update(output_cols)
    schema_fields = [pa.field(row_id_name, pa.int64(), nullable=False)]
    for col in update_cols:
        schema_fields.append(target_pa_schema.field(col))
    update_table = pa.Table.from_pydict(pydict, schema=pa.schema(schema_fields))

    wb = target_table.new_batch_write_builder()
    tu = wb.new_update().with_update_type(update_cols)
    msgs = tu.update_by_arrow_with_row_id(update_table)
    tc = wb.new_commit()
    tc.commit(msgs)
    tc.close()


def _needed_target_cols(
    spec: Dict[str, Any],
    on: Sequence[str],
    update_cols: Sequence[str],
    all_target_cols: Sequence[str],
    condition: Optional[Condition],
) -> list:
    if condition is not None:
        return list(all_target_cols)
    needed = set(on) | set(update_cols)
    for value in spec.values():
        if callable(value):
            return list(all_target_cols)
        if isinstance(value, str) and value.startswith("t."):
            needed.add(value[2:])
    return [c for c in all_target_cols if c in needed]


def _normalize_set_spec(
    spec: Optional[SetSpec],
    target_field_names: Sequence[str],
) -> Optional[Dict[str, Any]]:
    if spec is None:
        return None
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
