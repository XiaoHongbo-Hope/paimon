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

from dataclasses import dataclass
from typing import (
    Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union,
)

import pyarrow as pa

SetSpec = str
OnSpec = Union[Sequence[str], Mapping[str, str]]


@dataclass
class WhenMatched:
    update: SetSpec


@dataclass
class WhenNotMatched:
    insert: SetSpec


@dataclass
class _NormalizedClause:
    spec: Dict[str, Any]


def clauses_use_vector_fast_path(
    clauses: List[_NormalizedClause],
) -> bool:
    if not clauses:
        return False
    for c in clauses:
        for v in c.spec.values():
            if callable(v):
                return False
    return True


def apply_matched_transform(
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
        for clause in clauses:
            new_values = _apply_set(clause.spec, s_row, t_row, field_names)
            out_row_ids.append(t_row[row_id_name])
            for col in update_cols:
                out_cols[col].append(new_values.get(col, t_row.get(col)))
            break
    return pa.Table.from_pydict(
        {row_id_name: out_row_ids, **out_cols},
        schema=update_schema,
    )


def vectorized_matched_transform(
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
            arrays.append(
                _resolve_spec_array(
                    spec[col], batch, available, on_pairs, out_type
                )
            )
        else:
            arrays.append(batch.column(f"t.{col}"))
    return pa.Table.from_arrays(arrays, schema=update_schema)


def apply_insert_transform(
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
        for clause in clauses:
            out.append(
                _apply_set(
                    clause.spec, s_row, None, field_names,
                    null_unspecified=True,
                )
            )
            break
    aligned = [{name: r.get(name) for name in field_names} for r in out]
    return _coerce_large_string_types(
        pa.Table.from_pylist(aligned, schema=out_schema)
    )


def vectorized_insert_transform(
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
            arrays.append(
                _resolve_spec_array(
                    spec[col], batch, available, (), out_type
                )
            )
        else:
            arrays.append(pa.nulls(batch.num_rows, type=out_type))
    return pa.Table.from_arrays(arrays, schema=target_pa_schema)


def build_update_schema(
    target_pa_schema: pa.Schema,
    update_cols: Sequence[str],
    row_id_name: str,
) -> pa.Schema:
    return pa.schema(
        [pa.field(row_id_name, pa.int64(), nullable=False)]
        + [target_pa_schema.field(col) for col in update_cols]
    )


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
