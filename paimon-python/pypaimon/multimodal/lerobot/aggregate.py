# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Merge source coordinates at the write boundary of a new table group."""

import copy
import json
from contextlib import ExitStack

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.multimodal.lerobot.loader import (
    _abort_messages, _commit_messages, _prepare_dataset, _video_fps,
)
from pypaimon.multimodal.lerobot.metadata import (
    _append_arrow_tables, _commit_metadata, _load_dataset_metadata,
    _metadata_table, _positive_integer, _prepare_metadata_tables,
    _source_episode_tables, _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.schema import (
    _require_v3, _schema_from_info, _validate_v3_required_features,
    _video_feature_names,
)
from pypaimon.multimodal.lerobot.source import (
    _close_quietly, _import_lerobot_dataset, _load_hub_info,
    _open_resolved_dataset, _pandas_index_column, _resolved_source,
    _validate_info_paths,
)
from pypaimon.multimodal.source_utils import _validate_source_kerberos


_CONTROLS = ("index", "episode_index", "task_index", "subtask_index")
_STATS = ("min", "max", "mean", "std", "count")
_ENCODER_KEYS = {
    "video.codec", "video.pix_fmt", "video.gop_size", "video.crf",
    "video.encoder", "video.extra_options", "video.video_backend",
    "video.has_audio", "has_audio", "video.bit_rate",
}
# These properties describe source storage or are rebuilt for the merged dataset.
_REBUILT_INFO = {
    "total_frames", "total_episodes", "total_tasks", "total_subtasks", "total_videos",
    "total_chunks", "splits", "data_path", "video_path", "chunks_size",
    "data_files_size_in_mb", "video_files_size_in_mb", "features",
}


def _json_table(table):
    return {row["key"]: json.loads(row["value"]) for row in table.to_pylist()}


def _merge_stats(items):
    """Combine population moments; quantile summaries cannot be merged exactly."""
    if any(set(item) - set(_STATS) - {"q01", "q10", "q50", "q90", "q99"}
           for item in items):
        raise ValueError("Unsupported LeRobot statistics fields.")
    if any(not set(_STATS).issubset(item) for item in items):
        raise ValueError("LeRobot statistics require min/max/mean/std/count.")
    arrays = [{key: np.asarray(item[key], dtype=np.float64) for key in _STATS}
              for item in items]
    first = arrays[0]
    for item in arrays:
        if any(item[key].shape != first[key].shape for key in _STATS):
            raise ValueError("LeRobot statistics shapes must match.")
        count = item["count"]
        if count.shape != (1,) or not np.all(np.isfinite(count)) \
                or np.any(count <= 0) or np.any(count != np.floor(count)):
            raise ValueError("LeRobot statistics count must be one positive integer.")
        if any(item[key].shape != item["mean"].shape for key in ("min", "max", "std")):
            raise ValueError("LeRobot statistic shapes must match their mean.")
    count = sum(item["count"] for item in arrays)
    mean = sum(item["mean"] * item["count"] for item in arrays) / count
    variance = sum(item["count"] * (
        item["std"] ** 2 + (item["mean"] - mean) ** 2) for item in arrays) / count
    return {
        "min": np.minimum.reduce([item["min"] for item in arrays]).tolist(),
        "max": np.maximum.reduce([item["max"] for item in arrays]).tolist(),
        "mean": mean.tolist(), "std": np.sqrt(variance).tolist(),
        "count": count.astype(np.int64).tolist(),
    }


def _batch_stats(values):
    values = np.asarray(values, dtype=np.float64)
    return {"min": [values.min()], "max": [values.max()],
            "mean": [values.mean()], "std": [values.std()],
            "count": [len(values)]}


def _vocabulary(plans, kind):
    key = kind + "_table"
    tables = [plan["metadata"][key] for plan in plans]
    if all(table is None for table in tables):
        return None
    if any(table is None for table in tables):
        raise ValueError("LeRobot sources disagree on %s metadata." % kind)
    singular = kind[:-1]
    index_name = singular + "_index"
    schema = tables[0].schema
    label = _pandas_index_column(schema, singular)
    if set(schema.names) != {index_name, label}:
        raise ValueError("Multi-source %s metadata requires only index and text." % kind)
    rows, by_text = [], {}
    for ordinal, (plan, table) in enumerate(zip(plans, tables)):
        if not table.schema.equals(schema, check_metadata=True):
            raise ValueError("LeRobot source %d has incompatible %s schema." % (ordinal, kind))
        mapping = {}
        for row in sorted(table.to_pylist(), key=lambda row: row[index_name]):
            text = row[label]
            if text not in by_text:
                by_text[text] = len(rows)
                rows.append({index_name: len(rows), label: text})
            mapping[row[index_name]] = by_text[text]
        plan[index_name] = mapping
    return pa.Table.from_pylist(rows, schema=schema)


def _feature_contract(features):
    result = copy.deepcopy(features)
    for feature in result.values():
        if feature.get("dtype") == "video":
            for key in ("info", "video_info"):
                feature[key] = {k: v for k, v in feature.get(key, {}).items()
                                if k not in _ENCODER_KEYS}
    return result


def _plan_merge(plans):
    first = plans[0]
    info = copy.deepcopy(first["info"])
    metadata = dict(first["metadata"])
    frame_offset = episode_offset = 0
    for ordinal, plan in enumerate(plans):
        other = plan["info"]
        for key in (set(info) | set(other)) - _REBUILT_INFO:
            if info.get(key) != other.get(key):
                raise ValueError("LeRobot source %d has incompatible info.%s." % (ordinal, key))
        expected_features = _feature_contract(info["features"])
        actual_features = _feature_contract(other["features"])
        for name in set(expected_features) | set(actual_features):
            if expected_features.get(name) != actual_features.get(name):
                raise ValueError("LeRobot source %d has incompatible features.%s." % (ordinal, name))
        if not plan["schema"].equals(first["schema"], check_metadata=False):
            raise ValueError("LeRobot source %d has incompatible frame schema." % ordinal)
        if not plan["metadata"]["episodes_schema"].equals(
                metadata["episodes_schema"], check_metadata=True):
            raise ValueError("LeRobot source %d has incompatible Episode schema." % ordinal)
        # Validate every Episode before creating any target table.
        try:
            for name in plan["videos"]:
                _video_fps(other, other["features"][name], name)
            for unused in _validated_episode_tables(plan["metadata"], first["videos"]):
                pass
        except ValueError as error:
            raise ValueError("LeRobot source %d: %s" % (ordinal, error)) from error
        plan["frame_offset"] = frame_offset
        plan["episode_offset"] = episode_offset
        plan["control_stats"] = {}
        frame_offset += other["total_frames"]
        episode_offset += other["total_episodes"]
    metadata["tasks_table"] = _vocabulary(plans, "tasks")
    metadata["subtasks_table"] = _vocabulary(plans, "subtasks")
    info.update(total_frames=frame_offset, total_episodes=episode_offset,
                total_tasks=metadata["tasks_table"].num_rows,
                splits={"train": "0:%d" % episode_offset})
    if metadata["subtasks_table"] is not None:
        info["total_subtasks"] = metadata["subtasks_table"].num_rows
    for key in ("total_chunks", "total_videos", "data_path", "video_path",
                "chunks_size", "data_files_size_in_mb", "video_files_size_in_mb"):
        info.pop(key, None)
    # Encoder descriptions are not training features. Retain only shared values.
    for name in first["videos"]:
        for key in ("info", "video_info"):
            details = [p["info"]["features"][name].get(key, {}) for p in plans]
            info["features"][name][key] = {
                k: v for k, v in details[0].items()
                if all(d.get(k) == v for d in details[1:])}
    stats = [p["metadata"]["stats_table"] for p in plans]
    if any(s is None for s in stats) and not all(s is None for s in stats):
        raise ValueError("LeRobot stats must be present in all sources or absent in all.")
    merged_stats = None
    if stats[0] is not None:
        decoded = [_json_table(s) for s in stats]
        if any(set(s) != set(decoded[0]) for s in decoded[1:]):
            raise ValueError("LeRobot statistics features must match.")
        merged_stats = {name: _merge_stats([s[name] for s in decoded])
                        for name in decoded[0] if name not in _CONTROLS}
    metadata["info_table"] = _metadata_table(info)
    metadata["stats_table"] = (None if merged_stats is None
                               else _metadata_table(merged_stats))
    # Quantiles are omitted rather than presenting source summaries as merged values.
    metadata["episodes_schema"] = pa.schema([
        field for field in metadata["episodes_schema"]
        if not any(field.name.startswith("stats/%s/q" % name) for name in _CONTROLS)
    ], metadata=metadata["episodes_schema"].metadata)
    return metadata, merged_stats


def _transform_frames(plan, batch, episode):
    for name in _CONTROLS:
        if name not in batch.column_names:
            continue
        column = batch[name]
        if name in ("index", "episode_index"):
            offset = plan["frame_offset" if name == "index" else "episode_offset"]
            values = pc.add_checked(column, pa.scalar(offset, type=column.type))
        else:
            mapping = plan[name]
            values = pa.array([mapping[value] for value in column.to_pylist()], type=column.type)
        batch = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name), values)
        stats = _batch_stats(values.to_numpy())
        episode_stats = plan["control_stats"].setdefault(episode, {})
        episode_stats[name] = (_merge_stats([episode_stats[name], stats])
                               if name in episode_stats else stats)
    return batch


def _merged_episodes(plans, schema):
    for plan in plans:
        for table in _source_episode_tables(plan["metadata"]):
            records = table.select(schema.names).to_pylist()
            for row in records:
                episode = row["episode_index"]
                row["episode_index"] += plan["episode_offset"]
                for key in ("dataset_from_index", "dataset_to_index"):
                    row[key] += plan["frame_offset"]
                for name, stats in plan["control_stats"][episode].items():
                    for stat, value in stats.items():
                        key = "stats/%s/%s" % (name, stat)
                        if key in row:
                            original = np.asarray(row[key])
                            updated = np.full(original.shape, value[0])
                            row[key] = updated.tolist()
            yield pa.Table.from_pylist(records, schema=schema)


def _import_sources(connection, table_name, sources, batch_size, options,
                    source_options, tag_name):
    from functools import partial
    from pypaimon.multimodal.lerobot.api import _create_target_table, _validated_counts

    _validate_source_kerberos(sources, source_options, "LeRobot")
    with ExitStack() as stack:
        plans = []
        for ordinal, source in enumerate(sources):
            resolved, info = stack.enter_context(_resolved_source(source, source_options))
            if info is None:
                info = _load_hub_info(resolved)
            _require_v3(info, "source %d" % ordinal)
            _validate_info_paths(info)
            _positive_integer(info.get("fps"), "fps")
            _validated_counts(info, "source %d" % ordinal)
            _validate_v3_required_features(info)
            schema = _schema_from_info(info)
            videos = _video_feature_names(info)
            dataset = _open_resolved_dataset(
                None if resolved.file_io is not None else _import_lerobot_dataset(),
                resolved, info, download_videos=bool(videos))
            dataset_resources = stack.enter_context(ExitStack())
            if callable(getattr(dataset, "close", None)):
                dataset_resources.callback(_close_quietly, dataset, "dataset")
            plans.append({"dataset": dataset, "dataset_resources": dataset_resources,
                          "source": resolved, "info": info,
                          "schema": schema, "videos": videos,
                          "metadata": _load_dataset_metadata(dataset, info, resolved)})
        metadata, stats = _plan_merge(plans)
        table = _create_target_table(connection, table_name, plans[0]["schema"],
                                     options, metadata, plans[0]["videos"])
        tables = _prepare_metadata_tables(connection, table.raw_table, metadata)
        messages = []
        try:
            for plan in plans:
                messages.extend(_prepare_dataset(
                    table.raw_table, plan["dataset"], plan["info"], plan["source"],
                    plan["schema"], batch_size, plan["metadata"], plan["videos"],
                    transform=partial(_transform_frames, plan)))
                plan["dataset_resources"].close()
                plan["dataset"] = None
        except BaseException:
            _abort_messages(table.raw_table, messages)
            raise
        commit = table.raw_table.new_batch_write_builder().new_commit()
        try:
            frames_snapshot = _commit_messages(commit, messages)
        finally:
            commit.close()
        episodes_snapshot = _append_arrow_tables(
            tables["episodes"], _merged_episodes(plans, metadata["episodes_schema"]))
        if stats is not None:
            for name in _CONTROLS:
                parts = [s[name] for p in plans for s in p["control_stats"].values() if name in s]
                if parts:
                    stats[name] = _merge_stats(parts)
            metadata["stats_table"] = _metadata_table(stats)
        _commit_metadata(connection, tables, tag_name, metadata, table.identifier,
                         frames_snapshot, episodes_snapshot)
