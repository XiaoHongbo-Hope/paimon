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

import json
import sys
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon.catalog.catalog_exception import TableNotExistException
import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot.aggregate import _batch_stats, _transform_frames
from pypaimon.multimodal.lerobot.metadata import _restore_pandas_metadata


pytestmark = pytest.mark.skipif(sys.version_info < (3, 10), reason="LeRobot import requires Python 3.10+")


def source(root, tasks=("pick", "place"), subtasks=("reach", "grasp"), video=False, value=0):
    root.mkdir(parents=True)
    meta = root / "meta"
    (meta / "episodes/chunk-000").mkdir(parents=True)
    (root / "data/chunk-000").mkdir(parents=True)
    features = {key: {"dtype": "int64", "shape": [1], "names": None}
                for key in ("index", "episode_index", "frame_index", "task_index", "subtask_index")}
    features["timestamp"] = {"dtype": "float32", "shape": [1], "names": None}
    features["observation.state"] = {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    features["action"] = {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    if video:
        features["camera"] = {"dtype": "video", "shape": [16, 16, 3],
                              "names": ["height", "width", "channels"],
                              "info": {"video.fps": 10}}
        av = pytest.importorskip("av")
        path = root / "videos/camera/chunk-000/file-000.mp4"
        path.parent.mkdir(parents=True)
        with av.open(str(path), "w") as container:
            stream = container.add_stream("mpeg4", rate=10)
            stream.width = stream.height = 16
            stream.pix_fmt = "yuv420p"
            for i in range(5):
                frame = av.VideoFrame.from_ndarray(np.full((16, 16, 3), value + i * 10, np.uint8), format="rgb24")
                for packet in stream.encode(frame):
                    container.mux(packet)
            for packet in stream.encode():
                container.mux(packet)
    info = dict(codebase_version="v3.0", fps=10, robot_type="test", total_frames=5,
                total_episodes=2, total_tasks=2, features=features, splits={"train": "0:2"},
                data_path="data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
                video_path="videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4")
    (meta / "info.json").write_text(json.dumps(info))
    rows = []
    episodes = []
    for ep, (start, end) in enumerate(((0, 2), (2, 5))):
        ep_rows = []
        for index in range(start, end):
            row = dict(index=index, episode_index=ep, frame_index=index-start,
                       timestamp=(index-start)/10, task_index=ep, subtask_index=index % 2)
            row["observation.state"] = [float(value + index), float(index)]
            row["action"] = [float(index), -float(index)]
            ep_rows.append(row)
        rows.extend(ep_rows)
        episode = dict(episode_index=ep, dataset_from_index=start, dataset_to_index=end,
                       length=end-start, tasks=[tasks[ep]])
        episode.update({"data/chunk_index": 0, "data/file_index": 0})
        if video:
            episode.update({"videos/camera/chunk_index": 0, "videos/camera/file_index": 0,
                            "videos/camera/from_timestamp": start/10,
                            "videos/camera/to_timestamp": end/10})
        for name in ("index", "episode_index", "task_index", "subtask_index"):
            stats = _batch_stats([r[name] for r in ep_rows])
            for stat, v in stats.items():
                episode["stats/%s/%s" % (name, stat)] = v
            episode["stats/%s/q50" % name] = [0.0]
        episodes.append(episode)
    data = pa.Table.from_pylist(rows)
    # Source float32 timestamps must match the training feature contract.
    data = data.set_column(data.schema.get_field_index("timestamp"), "timestamp",
                           data["timestamp"].cast(pa.float32()))
    pq.write_table(data, root / "data/chunk-000/file-000.parquet")
    pq.write_table(pa.Table.from_pylist(episodes), meta / "episodes/chunk-000/file-000.parquet")
    for kind, texts in (("task", tasks), ("subtask", subtasks)):
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(
            {kind + "_index": [0, 1]}, index=pd.Index(texts, name=kind))), meta / (kind + "s.parquet"))
    stats = {name: _batch_stats([r[name] for r in rows])
             for name in ("index", "episode_index", "task_index", "subtask_index")}
    for name in ("observation.state", "action"):
        values = np.array([r[name] for r in rows])
        stats[name] = {"min": values.min(0).tolist(), "max": values.max(0).tolist(),
                       "mean": values.mean(0).tolist(), "std": values.std(0).tolist(), "count": [5],
                       "q50": np.median(values, axis=0).tolist()}
    (meta / "stats.json").write_text(json.dumps(stats))
    return root.as_uri()


@pytest.fixture
def setup(tmp_path):
    a = source(tmp_path / "a")
    b = source(tmp_path / "b", tasks=("place", "stack"), subtasks=("grasp", "release"), value=20)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    return conn, [a, b]


def rows(conn, name):
    return conn.get_table(name).scan().to_arrow()


def metadata(conn, suffix):
    return {r["key"]: json.loads(r["value"]) for r in rows(conn, "merged__" + suffix).to_pylist()}


def test_transform_frames_preserves_unlabelled_subtasks():
    plan = {
        "frame_offset": 5,
        "episode_offset": 2,
        "task_index": {0: 1},
        "subtask_index": {0: 3},
        "control_stats": {},
    }
    batch = pa.table({
        "index": pa.array([0, 1], type=pa.int64()),
        "episode_index": pa.array([0, 0], type=pa.int64()),
        "task_index": pa.array([0, 0], type=pa.int64()),
        "subtask_index": pa.array([0, None], type=pa.int64()),
    })

    converted = _transform_frames(plan, batch, 0)

    assert converted["index"].to_pylist() == [5, 6]
    assert converted["episode_index"].to_pylist() == [2, 2]
    assert converted["task_index"].to_pylist() == [1, 1]
    assert converted["subtask_index"].to_pylist() == [3, None]
    assert plan["control_stats"][0]["subtask_index"]["count"] == [1]


@pytest.fixture(scope="module")
def ray_cluster():
    ray = pytest.importorskip("ray")
    ray.init(address="local", num_cpus=2, include_dashboard=False,
             object_store_memory=100 * 1024 * 1024)
    yield ray
    ray.shutdown()


@pytest.fixture
def import_parallelism(request):
    if request.param > 1:
        request.getfixturevalue("ray_cluster")
    return request.param


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_numbering_stats_and_training(setup, import_parallelism):
    conn, sources = setup
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism)
    data = rows(conn, "merged").sort_by("index")
    assert data["index"].to_pylist() == list(range(10))
    assert data["episode_index"].to_pylist() == [0, 0, 1, 1, 1, 2, 2, 3, 3, 3]
    assert data["frame_index"].to_pylist() == [0, 1, 0, 1, 2] * 2
    assert data["task_index"].to_pylist() == [0, 0, 1, 1, 1, 1, 1, 2, 2, 2]
    assert data["subtask_index"].to_pylist() == [0, 1, 0, 1, 0, 1, 2, 1, 2, 1]
    episodes = rows(conn, "merged__episodes").sort_by("episode_index")
    assert episodes["dataset_from_index"].to_pylist() == [0, 2, 5, 7]
    assert episodes["dataset_to_index"].to_pylist() == [2, 5, 7, 10]
    assert episodes["stats/index/mean"].to_pylist() == [[0.5], [3.0], [5.5], [8.0]]
    assert not any(c.endswith("/q50") for c in episodes.column_names)
    info = metadata(conn, "info")
    assert (info["total_frames"], info["total_episodes"], info["total_tasks"]) == (10, 4, 3)
    assert info["splits"] == {"train": "0:4"}
    assert info["total_subtasks"] == 3
    stats = metadata(conn, "stats")
    for name in ("index", "episode_index", "task_index", "subtask_index", "observation.state", "action"):
        values = np.array(data[name].to_pylist())
        np.testing.assert_allclose(stats[name]["mean"], np.atleast_1d(values.mean(0)))
        np.testing.assert_allclose(stats[name]["std"], np.atleast_1d(values.std(0)))
        assert "q50" not in stats[name]
    tasks = conn.catalog.get_table(conn._identifier("merged__tasks"))
    table = rows(conn, "merged__tasks").sort_by("task_index")
    restored = _restore_pandas_metadata(tasks, table).to_pandas()
    assert restored.index.tolist() == ["pick", "place", "stack"]
    pytest.importorskip("datasets")
    torch = pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(
        conn.get_table("merged"), tag_name="train",
        delta_timestamps={"action": [-0.1, 0, 0.1]})
    assert dataset[5]["task"] == "place"
    assert dataset[7]["task"] == "stack"
    assert dataset[5]["action_is_pad"].tolist() == [True, False, False]
    batch = next(iter(dataset.to_dataloader(batch_size=3, num_workers=0)))
    assert batch["action"].shape == (3, 3, 2)
    assert batch["action"].dtype == torch.float32
    selected = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train", episodes=[2])
    assert len(selected) == 2
    assert selected[0]["episode_index"].item() == 2


@pytest.mark.parametrize("component", ["tasks", "subtasks", "episodes"])
def test_merge_ignores_producer_metadata(setup, component):
    conn, sources = setup
    root = Path(sources[1][7:]) / "meta"
    path = root / ("episodes/chunk-000/file-000.parquet" if component == "episodes"
                   else component + ".parquet")
    table = pq.read_table(path)
    schema_metadata = dict(table.schema.metadata or {})
    if component == "episodes":
        schema_metadata[b"producer"] = b"another-exporter"
    else:
        pandas_metadata = json.loads(schema_metadata[b"pandas"])
        pandas_metadata["pandas_version"] = "1.5.0"
        schema_metadata[b"pandas"] = json.dumps(pandas_metadata).encode()
    pq.write_table(table.replace_schema_metadata(schema_metadata), path)
    conn.load_from_lerobot("merged", sources)
    assert rows(conn, "merged").num_rows == 10
    assert rows(conn, "merged__episodes").num_rows == 4
    for kind, expected in (("task", ["pick", "place", "stack"]),
                           ("subtask", ["reach", "grasp", "release"])):
        target = conn.catalog.get_table(conn._identifier("merged__" + kind + "s"))
        data = rows(conn, "merged__" + kind + "s").sort_by(kind + "_index")
        restored = _restore_pandas_metadata(target, data).to_pandas()
        assert restored.index.tolist() == expected


@pytest.mark.parametrize("custom_source", [0, 1])
def test_merge_normalizes_task_text_columns(setup, custom_source):
    conn, sources = setup
    root = Path(sources[custom_source][7:]) / "meta"
    for kind in ("task", "subtask"):
        path = root / (kind + "s.parquet")
        frame = pq.read_table(path).to_pandas()
        frame.index.name = "instruction"
        pq.write_table(pa.Table.from_pandas(frame), path)
    conn.load_from_lerobot("merged", sources)
    data = rows(conn, "merged").sort_by("index")
    assert data["task_index"].to_pylist() == [0, 0, 1, 1, 1, 1, 1, 2, 2, 2]
    assert data["subtask_index"].to_pylist() == [0, 1, 0, 1, 0, 1, 2, 1, 2, 1]
    for kind, expected in (("task", ["pick", "place", "stack"]),
                           ("subtask", ["reach", "grasp", "release"])):
        target = conn.catalog.get_table(conn._identifier("merged__" + kind + "s"))
        table = rows(conn, "merged__" + kind + "s").sort_by(kind + "_index")
        restored = _restore_pandas_metadata(target, table).to_pandas()
        assert restored.index.tolist() == expected
        assert restored.index.name == ("instruction" if custom_source == 0 else kind)
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(conn.get_table("merged"))
    assert dataset[5]["task"] == "place"
    assert dataset[7]["task"] == "stack"


@pytest.mark.parametrize("field", ["fps", "robot_type", "features", "extension", "stats"])
def test_incompatible_sources_fail_before_creation(setup, field):
    conn, sources = setup
    root = Path(sources[1][7:])
    if field == "stats":
        (root / "meta/stats.json").unlink()
    else:
        path = root / "meta/info.json"
        info = json.loads(path.read_text())
        if field == "features":
            info[field]["action"]["names"] = ["different", "axes"]
        else:
            info[field] = 20 if field == "fps" else "different"
        path.write_text(json.dumps(info))
    with pytest.raises(ValueError):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged")


def test_missing_stats_and_existing_target(setup):
    conn, sources = setup
    for uri in sources:
        (Path(uri[7:]) / "meta/stats.json").unlink()
    conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged__stats")
    with pytest.raises(ValueError, match="already exists"):
        conn.load_from_lerobot("merged", sources)
    assert rows(conn, "merged").num_rows == 10


@pytest.mark.parametrize("sources,parallelism", [([], 1), (["unused", "unused"], 0)])
def test_invalid_arguments_before_io(tmp_path, sources, parallelism):
    conn = pm.connect(options={"warehouse": str(tmp_path)})
    with patch("pypaimon.multimodal.lerobot.source._resolved_source", side_effect=AssertionError("source accessed")):
        with pytest.raises(ValueError):
            conn.load_from_lerobot("merged", sources, parallelism=parallelism)


@pytest.mark.parametrize("failure", ["write", "tag"])
def test_failure_does_not_publish_root_tag(setup, failure):
    from pypaimon.multimodal.lerobot import aggregate
    conn, sources = setup
    if failure == "write":
        prepare = aggregate._prepare_dataset
        calls = []

        def fail_second(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("injected")
            return prepare(*args, **kwargs)
        target = patch.object(aggregate, "_prepare_dataset", side_effect=fail_second)
    else:
        target = patch("pypaimon.multimodal.lerobot.metadata._create_tag", side_effect=RuntimeError("injected"))
    with target, pytest.raises(RuntimeError, match="injected"):
        conn.load_from_lerobot("merged", sources, tag_name="train")
    table = conn.get_table("merged").raw_table
    assert not table.tag_manager().tag_exists("train")
    if failure == "write":
        assert table.snapshot_manager().get_latest_snapshot() is None
    else:
        assert rows(conn, "merged").num_rows == 10
    assert all(Path(uri[7:]).is_dir() for uri in sources)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_video_sources_with_identical_paths(tmp_path, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / "a", video=True, value=0),
               source(tmp_path / "b", video=True, value=100)]
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism)
    dataset = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train", video_backend="pyav")
    assert dataset[0]["camera"].mean().item() < 0.05
    assert dataset[5]["camera"].mean().item() > 0.3
    assert dataset[5]["frame_index"].item() == 0
    import av
    for source_index, uri in enumerate(sources):
        path = Path(uri[7:]) / "videos/camera/chunk-000/file-000.mp4"
        with av.open(str(path)) as container:
            expected = [frame.to_ndarray(format="rgb24") for frame in container.decode(video=0)]
        for frame_index in (0, 4):
            actual = dataset[source_index * 5 + frame_index]["camera"].numpy()
            np.testing.assert_allclose(actual, expected[frame_index].transpose(2, 0, 1) / 255.0)


def test_official_merge_reference_and_local_sources(tmp_path):
    module = pytest.importorskip("lerobot.datasets.lerobot_dataset")
    official = pytest.importorskip("lerobot.datasets.aggregate")
    roots = []
    for n in range(2):
        root = tmp_path / ("source%d" % n)
        dataset = module.LeRobotDataset.create(
            repo_id="test/source%d" % n, root=root, fps=10, use_videos=False,
            robot_type="test", features={
                "observation.state": {"dtype": "float32", "shape": (2,), "names": ["x", "y"]},
                "action": {"dtype": "float32", "shape": (2,), "names": ["x", "y"]},
            })
        for ep in range(2):
            for i in range(3):
                dataset.add_frame({
                    "observation.state": np.array([n, i], dtype=np.float32),
                    "action": np.array([i, -i], dtype=np.float32),
                    "task": ("pick", "place", "stack")[n + ep],
                })
            dataset.save_episode()
        dataset.finalize()
        roots.append(root)
    reference = tmp_path / "official"
    official.aggregate_datasets(
        repo_ids=["test/source0", "test/source1"], aggr_repo_id="test/merged",
        roots=roots, aggr_root=reference)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("merged", roots, tag_name="train", batch_size=2)
    expected = pa.concat_tables([pq.read_table(p) for p in sorted((reference / "data").rglob("*.parquet"))])
    actual = rows(conn, "merged").sort_by("index")
    for name in expected.column_names:
        np.testing.assert_allclose(actual[name].to_pylist(), expected[name].to_pylist())
    info = metadata(conn, "info")
    reference_info = json.loads((reference / "meta/info.json").read_text())
    for name in ("total_frames", "total_episodes", "total_tasks", "splits"):
        assert info[name] == reference_info[name]
    reference_stats = json.loads((reference / "meta/stats.json").read_text())
    for name in ("observation.state", "action"):
        for key in ("min", "max", "mean", "std", "count"):
            np.testing.assert_allclose(metadata(conn, "stats")[name][key], reference_stats[name][key])
    # A string source continues through the unchanged single-source importer.
    conn.load_from_lerobot("single", str(roots[0]))
    assert rows(conn, "single").num_rows == 6


def test_weighted_population_statistics():
    from pypaimon.multimodal.lerobot.aggregate import _merge_stats
    result = _merge_stats([_batch_stats([0, 2]), _batch_stats([10, 10, 10])])
    np.testing.assert_allclose(result["mean"], [6.4])
    np.testing.assert_allclose(result["std"], [np.std([0, 2, 10, 10, 10])])
    assert result["count"] == [5]


def test_invalid_episode_ranges_fail_before_creation(setup):
    conn, sources = setup
    path = Path(sources[1][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("dataset_from_index"),
                           "dataset_from_index", pa.array([1, 2], type=pa.int64()))
    pq.write_table(data, path)
    with pytest.raises(ValueError, match="source 1"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged")


def test_ray_merge_multifile_sources_matches_serial(setup, ray_cluster):
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        path = root / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        pq.write_table(data.slice(0, 2), path)
        pq.write_table(data.slice(2), path.with_name("file-001.parquet"))
        episodes_path = root / "meta/episodes/chunk-000/file-000.parquet"
        episodes = pq.read_table(episodes_path)
        episodes = episodes.set_column(
            episodes.schema.get_field_index("data/file_index"),
            "data/file_index", pa.array([0, 1], type=pa.int64()))
        pq.write_table(episodes, episodes_path)
    conn.load_from_lerobot("serial", sources, batch_size=1)
    conn.load_from_lerobot("parallel", sources, batch_size=1, parallelism=2)
    for suffix, key in (("", "index"), ("__episodes", "episode_index"),
                        ("__tasks", "task_index"), ("__subtasks", "subtask_index"),
                        ("__info", "key"), ("__stats", "key")):
        assert rows(conn, "serial" + suffix).sort_by(key).equals(
            rows(conn, "parallel" + suffix).sort_by(key))


def test_ray_worker_failure_does_not_commit(setup, ray_cluster):
    conn, sources = setup
    path = Path(sources[1][7:]) / "data/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("index"), "index",
                           pa.array([9, 1, 2, 3, 4], type=pa.int64()))
    pq.write_table(data, path)
    with pytest.raises(Exception, match="index"):
        conn.load_from_lerobot("merged", sources, parallelism=2, tag_name="train")
    table = conn.get_table("merged").raw_table
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert not table.tag_manager().tag_exists("train")
    assert all(Path(uri[7:]).is_dir() for uri in sources)
