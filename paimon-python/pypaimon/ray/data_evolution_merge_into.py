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

from pypaimon.ray.data_evolution_merge_join import (
    build_matched_update_ds,
    build_not_matched_insert_ds,
    build_unified_both,
    distributed_update_apply,
    distributed_write_collect_msgs,
)

SetSpec = Union[str, Dict[str, Any]]
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
) -> Dict[str, int]:
    _require_ray_join()
    num_partitions = _resolve_num_partitions(num_partitions)

    table, source_ds, matched_specs, not_matched_specs, ctx = _prepare(
        target, source, catalog_options,
        list(when_matched), list(when_not_matched), on,
    )
    base_snapshot = table.snapshot_manager().get_latest_snapshot()

    update_ds, insert_ds, update_cols_union, num_matched = _build_datasets(
        target, source_ds, matched_specs, not_matched_specs,
        ctx, base_snapshot, num_partitions,
    )

    return _execute_and_commit(
        table, update_ds, insert_ds, update_cols_union,
        num_matched, base_snapshot, num_partitions,
        ray_remote_args, concurrency,
    )


def _prepare(target, source, catalog_options, when_matched, when_not_matched, on):
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
        )
        for c in when_matched
    ]
    not_matched_specs = [
        _NormalizedClause(
            spec=_normalize_set_spec(c.insert, target_field_names, on_map),
        )
        for c in when_not_matched
    ]

    update_cols: set = set()
    for clause in matched_specs:
        update_cols.update(clause.spec.keys())
    update_cols = _exclude_blob_cols(table, update_cols)

    source_ds = _normalize_source(source, catalog_options)
    _validate_source_on_cols(source_ds, source_on_cols)

    from pypaimon.schema.data_types import PyarrowFieldParser
    target_pa_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )
    ctx = (target_on_cols, source_on_cols, target_field_names,
           target_pa_schema, catalog_options)
    return table, source_ds, matched_specs, not_matched_specs, ctx


def _build_datasets(
    target, source_ds, matched_specs, not_matched_specs,
    ctx, base_snapshot, num_partitions,
):
    target_on_cols, source_on_cols, target_field_names, \
        target_pa_schema, catalog_options = ctx

    update_ds = None
    insert_ds = None
    update_cols_union: List[str] = []
    num_matched = 0

    # Both clauses → one LEFT_OUTER join feeds matched and not-matched.
    if matched_specs and not_matched_specs and base_snapshot is not None:
        update_cols_union = _union_update_cols(matched_specs)
        update_ds, insert_ds, num_matched = build_unified_both(
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
            resolve_target_projection=_resolve_target_projection,
        )
    else:
        if matched_specs and base_snapshot is not None:
            update_cols_union = _union_update_cols(matched_specs)
            update_ds, num_matched = build_matched_update_ds(
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
                resolve_target_projection=_resolve_target_projection,
            )

        if not_matched_specs:
            insert_ds = build_not_matched_insert_ds(
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

    return update_ds, insert_ds, update_cols_union, num_matched


def _execute_and_commit(
    table, update_ds, insert_ds, update_cols_union,
    num_matched, base_snapshot, num_partitions,
    ray_remote_args, concurrency,
):
    update_msgs: list = []
    num_updated = 0
    if update_ds is not None:
        update_msgs, num_updated = distributed_update_apply(
            update_ds, table, update_cols_union,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
        )

    all_msgs: list = list(update_msgs)
    num_inserted = 0
    if insert_ds is not None:
        insert_msgs = distributed_write_collect_msgs(
            insert_ds, table,
            ray_remote_args=ray_remote_args, concurrency=concurrency,
        )
        num_inserted = sum(
            f.row_count for m in insert_msgs for f in m.new_files
        )
        all_msgs.extend(insert_msgs)
    # TODO: add global-index update action check after PR #8045 merges
    if all_msgs:
        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.commit(all_msgs)
        tc.close()

    return {
        "num_matched": num_matched,
        "num_inserted": num_inserted,
        "num_unchanged": num_matched - num_updated,
    }


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


def _resolve_num_partitions(num_partitions: Optional[int]) -> int:
    if num_partitions is not None:
        return num_partitions
    try:
        import ray

        cpus = ray.cluster_resources().get("CPU", 16)
        return max(16, int(cpus) * 2)
    except Exception:
        return 16


def _require_ray_join() -> None:
    import ray
    from ray.data import Dataset

    if not hasattr(Dataset, "join"):
        raise RuntimeError(
            f"merge_into requires ray>=2.50 (Dataset.join); "
            f"installed ray is {ray.__version__}."
        )


def _exclude_blob_cols(table, update_cols: set) -> set:
    blob_cols = {
        f.name
        for f in table.table_schema.fields
        if getattr(f.type, "type", None) == "BLOB"
    }
    return update_cols - blob_cols


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
    needed = set(_needed_target_cols(
        clauses, target_on, update_cols, target_field_names,
    ))
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
