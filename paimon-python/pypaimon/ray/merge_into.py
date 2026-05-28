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

"""MERGE INTO for Paimon primary-key tables, driven by Ray Datasets.

Mirrors the high-level semantics of
``paimon-flink/.../action/MergeIntoAction.java`` and
``paimon-spark/.../commands/MergeIntoPaimonTable.scala`` but exposes a
Pythonic API instead of SQL.

MVP scope: only upsert-flavored clauses (matched-update,
not-matched-insert, not-matched-by-source-update). DELETE clauses raise
``NotImplementedError`` because pypaimon's
``KeyValueDataWriter._add_system_fields`` still hardcodes ``_VALUE_KIND``
to INSERT — see
``paimon-python/pypaimon/write/writer/key_value_data_writer.py:53``.
"""

from dataclasses import dataclass, field
from functools import partial
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
import pyarrow.compute as pc

from pypaimon.ray.shuffle import (
    _coerce_large_string_types,
    _pick_collision_safe_col_name,
)

SetSpec = Union[str, Dict[str, Any]]
Condition = Callable[[Mapping[str, Any]], bool]

_SIDE_TARGET = "t"
_SIDE_SOURCE = "s"


@dataclass
class _MergeConfig:
    on: Tuple[str, ...]
    target_field_names: Tuple[str, ...]
    side_col: str
    matched_update: Optional[Dict[str, Any]]
    matched_update_condition: Optional[Condition]
    not_matched_insert: Optional[Dict[str, Any]]
    not_matched_insert_condition: Optional[Condition]
    not_matched_by_source_update: Optional[Dict[str, Any]]
    not_matched_by_source_update_condition: Optional[Condition]
    target_pa_schema: pa.Schema = field(repr=False)


def merge_paimon(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    on: Sequence[str],
    when_matched_update: Optional[SetSpec] = None,
    when_matched_update_condition: Optional[Condition] = None,
    when_matched_delete_condition: Optional[Condition] = None,
    when_not_matched_insert: Optional[SetSpec] = None,
    when_not_matched_insert_condition: Optional[Condition] = None,
    when_not_matched_by_source_update: Optional[Dict[str, Any]] = None,
    when_not_matched_by_source_update_condition: Optional[Condition] = None,
    when_not_matched_by_source_delete_condition: Optional[Condition] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """MERGE INTO ``target`` USING ``source`` for a Paimon primary-key table.

    Args:
        target: Full table identifier, e.g. ``"db.table"``.
        source: Right-hand side. One of ``ray.data.Dataset``, a Paimon
            table identifier string (read via :func:`read_paimon` with
            the same ``catalog_options``), a ``pyarrow.Table``, or a
            ``pandas.DataFrame``.
        catalog_options: Forwarded to ``CatalogFactory.create`` and to
            ``read_paimon`` / ``write_paimon``.
        on: Join keys. Must be a subset of the target table's primary
            keys. Rows are matched by equality on these columns.
        when_matched_update: SET spec for matched rows. Use ``"*"`` to
            copy all target columns from source (requires schema
            compatibility), or a dict ``{target_col: expr}`` where
            ``expr`` is ``"s.col"`` / ``"t.col"`` / a literal / a
            callable taking the combined row.
        when_matched_update_condition: Optional predicate over the
            combined row (keys prefixed ``s.`` / ``t.``); rows not
            satisfying it are left unchanged.
        when_matched_delete_condition: Not yet supported; raises
            ``NotImplementedError`` if non-None.
        when_not_matched_insert: SET spec for source rows with no
            matching target row. ``"*"`` copies all target-schema
            columns from source.
        when_not_matched_insert_condition: Optional predicate over the
            source row (keys prefixed ``s.``).
        when_not_matched_by_source_update: SET spec for target rows
            with no matching source row. Same dict format as above; only
            ``"t.col"`` / literal / callable values are meaningful since
            no source row exists.
        when_not_matched_by_source_update_condition: Optional predicate
            over the target row (keys prefixed ``t.``).
        when_not_matched_by_source_delete_condition: Not yet supported;
            raises ``NotImplementedError`` if non-None.
        ray_remote_args: Forwarded to ``write_paimon``.
        concurrency: Forwarded to ``write_paimon``.

    Notes:
        - The target table must be a primary-key table.
        - For HASH_FIXED bucket mode, the existing
          :func:`maybe_apply_repartition` is applied by ``write_paimon``
          before writing.
        - If the user passes any callable as a condition or as a SET
          value, the target read falls back to the full schema (callables
          are opaque to projection analysis).
        - User-supplied callables must be picklable since Ray ships them
          to workers.
        - If ``on`` is a strict subset of the primary keys and the SET
          expression rewrites the remaining PK columns, two PK rows may
          collide downstream of the merge engine; do not do this.
    """
    if when_matched_delete_condition is not None or when_not_matched_by_source_delete_condition is not None:
        raise NotImplementedError(
            "DELETE clauses are not supported yet: the pypaimon writer "
            "hardcodes _VALUE_KIND to INSERT. See "
            "paimon-python/pypaimon/write/writer/key_value_data_writer.py:53. "
            "Once that TODO is resolved, DELETE can be wired through."
        )

    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.schema.data_types import PyarrowFieldParser

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    if not table.is_primary_key_table:
        raise ValueError(
            f"merge_paimon requires a primary-key table; "
            f"'{target}' has no primary keys."
        )

    primary_keys = list(table.primary_keys)
    if not set(on).issubset(set(primary_keys)):
        raise ValueError(
            f"'on' columns {list(on)} must be a subset of target primary "
            f"keys {primary_keys}."
        )

    if (
        when_matched_update is None
        and when_not_matched_insert is None
        and when_not_matched_by_source_update is None
    ):
        raise ValueError(
            "At least one of when_matched_update, when_not_matched_insert, "
            "or when_not_matched_by_source_update must be provided."
        )

    target_field_names = list(table.field_names)
    matched_update = _normalize_set_spec(
        when_matched_update, target_field_names, allow_star=True, star_side=_SIDE_SOURCE
    )
    not_matched_insert = _normalize_set_spec(
        when_not_matched_insert, target_field_names, allow_star=True, star_side=_SIDE_SOURCE
    )
    not_matched_by_source_update = _normalize_set_spec(
        when_not_matched_by_source_update,
        target_field_names,
        allow_star=False,
        star_side=None,
    )

    target_pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)

    source_ds = _normalize_source(source, catalog_options)
    source_schema = source_ds.schema()
    source_col_names = list(source_schema.names) if source_schema is not None else []
    if source_col_names:
        for col in on:
            if col not in source_col_names:
                raise ValueError(
                    f"'on' column '{col}' is missing from source schema "
                    f"{source_col_names}."
                )

    target_projection = _compute_target_projection(
        on=on,
        target_field_names=target_field_names,
        matched_update=matched_update,
        not_matched_by_source_update=not_matched_by_source_update,
        matched_update_condition=when_matched_update_condition,
        not_matched_by_source_update_condition=when_not_matched_by_source_update_condition,
    )

    from pypaimon.ray.ray_paimon import read_paimon, write_paimon

    target_ds = read_paimon(target, catalog_options, projection=target_projection)

    side_col = _pick_collision_safe_col_name(
        set(target_field_names) | set(source_col_names), "_paimon_side"
    )
    aligned_target, aligned_source = _align_for_union(
        target_ds, source_ds, side_col=side_col
    )
    combined = aligned_target.union(aligned_source)

    cfg = _MergeConfig(
        on=tuple(on),
        target_field_names=tuple(target_field_names),
        side_col=side_col,
        matched_update=matched_update,
        matched_update_condition=when_matched_update_condition,
        not_matched_insert=not_matched_insert,
        not_matched_insert_condition=when_not_matched_insert_condition,
        not_matched_by_source_update=not_matched_by_source_update,
        not_matched_by_source_update_condition=when_not_matched_by_source_update_condition,
        target_pa_schema=target_pa_schema,
    )

    merged = (
        combined
        .groupby(list(on))
        .map_groups(partial(_merge_groups, cfg=cfg), batch_format="pyarrow")
    )

    write_paimon(
        merged,
        target,
        catalog_options,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )


def _normalize_set_spec(
    spec: Optional[SetSpec],
    target_field_names: Sequence[str],
    *,
    allow_star: bool,
    star_side: Optional[str],
) -> Optional[Dict[str, Any]]:
    if spec is None:
        return None
    if isinstance(spec, str):
        if not allow_star or spec != "*":
            raise ValueError(
                f"SET spec strings other than '*' are not supported here; got {spec!r}."
            )
        return {col: f"{star_side}.{col}" for col in target_field_names}
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
        "source must be a ray.data.Dataset, a Paimon table identifier "
        "string, a pyarrow.Table, or a pandas.DataFrame; got "
        f"{type(source).__name__}."
    )


def _compute_target_projection(
    *,
    on: Sequence[str],
    target_field_names: Sequence[str],
    matched_update: Optional[Dict[str, Any]],
    not_matched_by_source_update: Optional[Dict[str, Any]],
    matched_update_condition: Optional[Condition],
    not_matched_by_source_update_condition: Optional[Condition],
) -> Optional[List[str]]:
    if matched_update_condition is not None or not_matched_by_source_update_condition is not None:
        return None
    needed = set(on)
    for spec in (matched_update, not_matched_by_source_update):
        if not spec:
            continue
        needed.update(spec.keys())
        for value in spec.values():
            if callable(value):
                return None
            if isinstance(value, str) and value.startswith("t."):
                needed.add(value[2:])
    needed.update(_required_target_cols_for_passthrough(matched_update, target_field_names))
    needed.update(_required_target_cols_for_passthrough(not_matched_by_source_update, target_field_names))
    return [col for col in target_field_names if col in needed]


def _required_target_cols_for_passthrough(
    spec: Optional[Dict[str, Any]], target_field_names: Sequence[str]
) -> List[str]:
    if not spec:
        return list(target_field_names)
    return [col for col in target_field_names if col not in spec]


def _align_for_union(target_ds, source_ds, *, side_col: str):
    target_schema = target_ds.schema()
    source_schema = source_ds.schema()
    target_type_map = _schema_type_map(target_schema)
    source_type_map = _schema_type_map(source_schema)

    union_field_types: Dict[str, pa.DataType] = {}
    for name, t in target_type_map.items():
        union_field_types[name] = t
    for name, t in source_type_map.items():
        if name not in union_field_types:
            union_field_types[name] = t

    aligned_target = target_ds.map_batches(
        partial(
            _align_batch,
            union_field_types=union_field_types,
            side_value=_SIDE_TARGET,
            side_col=side_col,
        ),
        batch_format="pyarrow",
    )
    aligned_source = source_ds.map_batches(
        partial(
            _align_batch,
            union_field_types=union_field_types,
            side_value=_SIDE_SOURCE,
            side_col=side_col,
        ),
        batch_format="pyarrow",
    )
    return aligned_target, aligned_source


def _schema_type_map(schema: Optional[pa.Schema]) -> Dict[str, pa.DataType]:
    if schema is None:
        return {}
    return dict(zip(schema.names, schema.types))


def _align_batch(
    batch: pa.Table,
    *,
    union_field_types: Dict[str, pa.DataType],
    side_value: str,
    side_col: str,
) -> pa.Table:
    n = batch.num_rows
    arrays = []
    fields = []
    present = {name: batch.column(name) for name in batch.schema.names}
    for name, target_type in union_field_types.items():
        if name in present:
            col = present[name]
            if col.type != target_type:
                col = col.cast(target_type, safe=False)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(n, type=target_type))
        fields.append(pa.field(name, target_type))
    arrays.append(pa.array([side_value] * n, type=pa.string()))
    fields.append(pa.field(side_col, pa.string()))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _merge_groups(group: pa.Table, *, cfg: _MergeConfig) -> pa.Table:
    if group.num_rows == 0:
        return pa.Table.from_pylist([], schema=cfg.target_pa_schema)
    side_mask = pc.equal(group.column(cfg.side_col), _SIDE_TARGET)
    target_table = group.filter(side_mask).drop([cfg.side_col])
    source_table = group.filter(pc.invert(side_mask)).drop([cfg.side_col])
    target_rows = target_table.to_pylist()
    source_rows = source_table.to_pylist()
    output_rows: List[Dict[str, Any]] = []

    if target_rows and source_rows:
        if cfg.matched_update is not None:
            for t_row in target_rows:
                for s_row in source_rows:
                    combined = _prefixed(s_row, t_row)
                    if cfg.matched_update_condition is not None and not cfg.matched_update_condition(combined):
                        continue
                    output_rows.append(_apply_set(cfg.matched_update, s_row, t_row, cfg.target_field_names))
    elif source_rows and not target_rows:
        if cfg.not_matched_insert is not None:
            for s_row in source_rows:
                combined = _prefixed(s_row, None)
                if cfg.not_matched_insert_condition is not None and not cfg.not_matched_insert_condition(combined):
                    continue
                output_rows.append(_apply_set(cfg.not_matched_insert, s_row, None, cfg.target_field_names))
    elif target_rows and not source_rows:
        if cfg.not_matched_by_source_update is not None:
            for t_row in target_rows:
                combined = _prefixed(None, t_row)
                if (
                    cfg.not_matched_by_source_update_condition is not None
                    and not cfg.not_matched_by_source_update_condition(combined)
                ):
                    continue
                output_rows.append(_apply_set(cfg.not_matched_by_source_update, None, t_row, cfg.target_field_names))

    if not output_rows:
        return pa.Table.from_pylist([], schema=cfg.target_pa_schema)
    aligned = [{name: row.get(name) for name in cfg.target_field_names} for row in output_rows]
    arrow_table = pa.Table.from_pylist(aligned, schema=cfg.target_pa_schema)
    return _coerce_large_string_types(arrow_table)


def _prefixed(s_row: Optional[Dict[str, Any]], t_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if s_row is not None:
        for k, v in s_row.items():
            out[f"s.{k}"] = v
    if t_row is not None:
        for k, v in t_row.items():
            out[f"t.{k}"] = v
    return out


def _apply_set(
    spec: Dict[str, Any],
    s_row: Optional[Dict[str, Any]],
    t_row: Optional[Dict[str, Any]],
    target_field_names: Sequence[str],
) -> Dict[str, Any]:
    combined = _prefixed(s_row, t_row)
    out: Dict[str, Any] = {}
    base = t_row if t_row is not None else (s_row if s_row is not None else {})
    for col in target_field_names:
        if col in spec:
            out[col] = _eval_set_value(spec[col], combined, s_row, t_row)
        elif col in base:
            out[col] = base[col]
        else:
            out[col] = None
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
