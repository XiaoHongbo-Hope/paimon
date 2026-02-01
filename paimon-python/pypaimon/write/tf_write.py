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
from typing import TYPE_CHECKING, Any, Dict, List

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser

if TYPE_CHECKING:
    from pypaimon.table.table import Table
from pypaimon.write.commit_message import CommitMessage


def _tf_batch_to_arrow(batch: Any, pa_schema: pa.Schema) -> pa.RecordBatch:
    """
    Convert one batch from tf.data.Dataset (dict of tensors) to pyarrow.RecordBatch.
    Uses zero-copy from numpy to Arrow when types match (1D primitive columns).
    """
    import numpy as np

    names = pa_schema.names
    arrays = []
    for name in names:
        if name not in batch:
            raise ValueError(f"Batch missing column '{name}'; got keys: {list(batch.keys())}")
        t = batch[name]
        if hasattr(t, "numpy"):
            arr = t.numpy()
        else:
            arr = np.asarray(t)
        if arr.ndim == 0:
            arr = np.expand_dims(arr, 0)
        elif arr.ndim > 1:
            # Nested/high-dim: Arrow needs list-of-rows; tolist() is slow for large arrays
            pa_type = pa_schema.field(name).type
            try:
                pa_arr = pa.array(arr.tolist(), type=pa_type)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                pa_arr = pa.array(np.asarray(arr.tolist()), type=pa_type)
            arrays.append(pa_arr)
            continue

        pa_type = pa_schema.field(name).type
        # Zero-copy path: pa.array(arr) without type= lets PyArrow reuse numpy buffer when possible
        try:
            pa_arr = pa.array(arr)
            if pa_arr.type != pa_type:
                pa_arr = pa_arr.cast(pa_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            try:
                pa_arr = pa.array(arr, type=pa_type)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                pa_arr = pa.array(arr.tolist(), type=pa_type)
        arrays.append(pa_arr)
    return pa.RecordBatch.from_arrays(arrays, schema=pa_schema)


def write_tf_dataset(
    table: "Table",
    dataset: Any,  # tf.data.Dataset
    strategy: Any,  # tf.distribute.Strategy
    *,
    overwrite: bool = False,
) -> None:
    """
    Write a TensorFlow Dataset to a Paimon table.

    Each replica writes its shard via write_arrow_batch. The chief collects
    commit messages from all replicas and runs a single commit.
    Args:
        table: Paimon table (from catalog.get_table(...)).
        dataset: tf.data.Dataset whose elements are dicts of column name -> Tensor,
                 matching the table schema.
        strategy: tf.distribute.Strategy (e.g. from get_strategy(), MirroredStrategy()).
        overwrite: If True, overwrite existing data.
    """
    import tensorflow as tf

    pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)

    def _replica_id(ctx) -> int:
        x = ctx.replica_id_in_sync_group
        return int(x.numpy()) if hasattr(x, "numpy") else int(x)

    replica_writes: Dict[int, Any] = {}

    def write_batch_fn(batch: Any) -> None:
        ctx = tf.distribute.get_replica_context()
        rid = _replica_id(ctx)
        if rid not in replica_writes:
            builder = table.new_batch_write_builder()
            replica_writes[rid] = builder.new_write()
        pa_batch = _tf_batch_to_arrow(batch, pa_schema)
        replica_writes[rid].write_arrow_batch(pa_batch)

    def prepare_commit_per_replica() -> List[CommitMessage]:
        """Each replica returns its own commit messages (prepare_commit); chief will merge and commit once."""
        ctx = tf.distribute.get_replica_context()
        rid = _replica_id(ctx)
        if rid not in replica_writes:
            return []
        tw = replica_writes[rid]
        msgs = tw.prepare_commit()
        tw.close()
        return msgs

    dist_dataset = strategy.experimental_distribute_dataset(dataset)

    try:
        for batch in dist_dataset:
            strategy.run(write_batch_fn, args=(batch,))

        per_replica_messages = strategy.run(prepare_commit_per_replica)
        local_results = strategy.experimental_local_results(per_replica_messages)
        merged: List[CommitMessage] = []
        for lst in local_results:
            merged.extend(lst)

        builder = table.new_batch_write_builder()
        if overwrite:
            builder = builder.overwrite()
        table_commit = builder.new_commit()
        try:
            table_commit.commit(merged)
        finally:
            # On commit failure, TableCommit._commit already calls file_store_commit.abort(merged)
            table_commit.close()
    finally:
        for tw in replica_writes.values():
            try:
                tw.close()
            except Exception:
                pass
