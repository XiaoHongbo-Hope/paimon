# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Resumable Ray writes for fixed-bucket primary-key tables."""

from typing import Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.ray.update_by_row_id import (
    _OffsetUpdateCommitter,
    _delete_checkpoint_tag,
    _ensure_source_tag,
    _get_checkpoint_tag,
    _has_offset_checkpoint_state,
    _load_offset_operation_checkpoint,
    _offset_checkpoint_state,
    _operation_checkpoint_tags,
    _operation_commit_user,
    _operation_source_tag,
)

__all__ = [
    "delete_write_paimon_checkpoint",
]

_CHECKPOINT_PROPERTY = "primary-key.write.checkpoint"
_CHECKPOINT_MODE = "primary-key-source-offset"


def _write_paimon_incremental(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    update_cols: Optional[List[str]],
    operation_id: Optional[str],
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Incrementally upsert columns by the target primary key.

    The target must use fixed buckets and ``merge-engine=partial-update``.
    ``source`` must be a :class:`PaimonOffsetSource`; committed source windows
    resume from ``operation_id`` after a driver restart.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.options.core_options import MergeEngine
    from pypaimon.ray.offset_source import PaimonOffsetSource
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.bucket_mode import BucketMode
    from pypaimon.write.ray_datasink import _prepare_primary_key_groups

    if not isinstance(source, PaimonOffsetSource):
        raise ValueError(
            "Incremental write_paimon requires a PaimonOffsetSource.")
    if getattr(source, "_needs_target_read_plan", False):
        raise ValueError(
            "Incremental write_paimon does not accept a co-bucketed join "
            "source.")
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id must be a non-empty string.")
    if len(operation_id) > 256:
        raise ValueError("operation_id must contain at most 256 characters.")
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    update_cols = list(dict.fromkeys(update_cols))

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    if not table.is_primary_key_table:
        raise ValueError(
            "Incremental write_paimon requires a primary-key target.")
    if table.bucket_mode() != BucketMode.HASH_FIXED:
        raise ValueError(
            "Incremental write_paimon requires a fixed-bucket target.")
    if table.options.merge_engine() != MergeEngine.PARTIAL_UPDATE:
        raise ValueError(
            "Incremental write_paimon requires "
            "'merge-engine'='partial-update'.")
    if table.options.sequence_field():
        raise ValueError(
            "Incremental write_paimon does not support sequence fields yet.")

    primary_keys = list(table.primary_keys)
    invalid = [col for col in update_cols if col not in table.field_names]
    if invalid:
        raise ValueError(
            "update column {!r} is not in target {!r}.".format(
                invalid[0], target))
    key_updates = [col for col in update_cols if col in primary_keys]
    if key_updates:
        raise ValueError(
            "primary-key column {!r} cannot be updated.".format(
                key_updates[0]))
    sparse_non_null = [
        field.name for field in table.table_schema.fields
        if (field.name not in primary_keys
            and field.name not in update_cols
            and not field.type.nullable)
    ]
    if sparse_non_null:
        raise ValueError(
            "unprovided partial-update column {!r} must be nullable."
            .format(sparse_non_null[0]))

    initial_snapshot = table.snapshot_manager().get_latest_snapshot()
    commit_user = _operation_commit_user(operation_id)
    checkpoint_tags = _operation_checkpoint_tags(operation_id)
    loaded = _load_offset_operation_checkpoint(
        catalog,
        target,
        table,
        operation_id,
        update_cols,
        commit_user,
        checkpoint_tags,
        checkpoint_property=_CHECKPOINT_PROPERTY,
        checkpoint_mode=_CHECKPOINT_MODE,
    )
    saved_plan = loaded[1]["source"] if loaded and loaded[1] else None
    source_tag = _operation_source_tag(operation_id, target)
    retained = _get_checkpoint_tag(
        catalog, source.table_identifier, source_tag)
    bind_kwargs = {
        "checkpoint_plan": saved_plan,
        "catalog_options": catalog_options,
    }
    if retained is not None:
        bind_kwargs["retention_tags"] = {"source": source_tag}
        bind_kwargs["retained_snapshot_ids"] = {
            "source": retained.snapshot.id,
        }
    bound_source = source._bind(catalog, **bind_kwargs)
    _ensure_source_tag(
        catalog,
        source.table_identifier,
        source_tag,
        bound_source.plan["snapshot_id"],
    )

    committer = _OffsetUpdateCommitter(
        table,
        catalog,
        target,
        operation_id,
        update_cols,
        bound_source.plan,
        initial_snapshot,
        loaded,
        checkpoint_property=_CHECKPOINT_PROPERTY,
        checkpoint_mode=_CHECKPOINT_MODE,
        protect_row_ids=False,
        reject_external_appends=True,
    )
    if committer.next_offset > bound_source.num_units:
        committer.close()
        raise RuntimeError("Offset checkpoint is beyond the source unit count.")

    target_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields)
    required = primary_keys + update_cols

    def _to_write_batch(batch: pa.Table) -> pa.Table:
        missing = [col for col in required if col not in batch.column_names]
        if missing:
            raise ValueError(
                "source is missing columns {}.".format(missing))
        arrays = []
        for field in target_schema:
            if field.name in required:
                arrays.append(batch.column(field.name).cast(field.type))
            else:
                arrays.append(pa.nulls(batch.num_rows, type=field.type))
        return pa.Table.from_arrays(arrays, schema=target_schema)

    try:
        for _, end in bound_source.windows(committer.next_offset):
            source_ds = bound_source.read_window(committer.next_offset, end)
            write_ds = source_ds.map_batches(
                _to_write_batch, batch_format="pyarrow")
            messages, num_rows = _prepare_primary_key_groups(
                write_ds,
                table,
                concurrency=concurrency,
                ray_remote_args=ray_remote_args,
            )
            committer.commit_window(messages, num_rows, end, set())
        committer.finish()
        return {"num_written": committer.num_updated}
    finally:
        committer.close()


def delete_write_paimon_checkpoint(
    target: str,
    catalog_options: Dict[str, str],
    operation_id: str,
) -> bool:
    """Delete a completed primary-key upsert checkpoint."""
    from pypaimon.catalog.catalog_factory import CatalogFactory

    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id must be a non-empty string.")
    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    checkpoint_tags = _operation_checkpoint_tags(operation_id)
    snapshots = []
    for checkpoint_tag in checkpoint_tags:
        tagged = _get_checkpoint_tag(catalog, target, checkpoint_tag)
        if (tagged is not None
                and _has_offset_checkpoint_state(
                    tagged.snapshot,
                    _CHECKPOINT_PROPERTY,
                    _CHECKPOINT_MODE)):
            snapshots.append(tagged.snapshot)

    if not snapshots:
        latest = table.snapshot_manager().get_latest_snapshot()
        if latest is not None:
            earliest = table.snapshot_manager().try_get_earliest_snapshot(
                latest.id)
            commit_user = _operation_commit_user(operation_id)
            for snapshot_id in range(latest.id, earliest.id - 1, -1):
                snapshot = table.snapshot_manager().get_snapshot_by_id(
                    snapshot_id)
                if (snapshot is not None
                        and snapshot.commit_user == commit_user
                        and _has_offset_checkpoint_state(
                            snapshot,
                            _CHECKPOINT_PROPERTY,
                            _CHECKPOINT_MODE)):
                    snapshots.append(snapshot)
                    break

    source_table = None
    if snapshots:
        state = _offset_checkpoint_state(
            max(snapshots, key=lambda snapshot: snapshot.id),
            _CHECKPOINT_PROPERTY,
            _CHECKPOINT_MODE,
        )
        source_table = state["source"]["table"]

    deleted = False
    for checkpoint_tag in checkpoint_tags:
        deleted = (_delete_checkpoint_tag(
            catalog, target, checkpoint_tag, ignore_missing=True) or deleted)
    if source_table is not None:
        deleted = (_delete_checkpoint_tag(
            catalog,
            source_table,
            _operation_source_tag(operation_id, target),
            ignore_missing=True,
        ) or deleted)
    return deleted
