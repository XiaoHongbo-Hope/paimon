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
"""
Module to write a Paimon table from a MaxFrame DataFrame.

Architecture
------------
MaxFrame is a lazy-evaluation distributed DataFrame engine backed by
MaxCompute.  Its ``apply_chunk`` API distributes a user function across
workers, where each worker receives a *chunk* of the DataFrame as a
regular ``pandas.DataFrame``.

The write is split into two phases so that data files can be written in
parallel while metadata is committed atomically:

Phase 1 – parallel on workers (via ``apply_chunk``)
    Each worker converts its pandas chunk to PyArrow, writes data files
    through PyPaimon, and calls ``prepare_commit()`` to obtain
    ``CommitMessage`` objects.  The commit messages are serialized with
    ``pickle`` and returned to the client inside a single-row DataFrame.

Phase 2 – serial on client
    The client deserializes all ``CommitMessage`` objects and calls
    ``table_commit.commit()`` exactly once, producing a single atomic
    Paimon snapshot.
"""

import logging
import pickle
import base64
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pypaimon.table.table import Table

logger = logging.getLogger(__name__)


def _serialize_commit_messages(messages):
    """Pickle + base64-encode a list of CommitMessage objects."""
    return base64.b64encode(pickle.dumps(messages)).decode("utf-8")


def _deserialize_commit_messages(data_str):
    """Decode a base64 string back into a list of CommitMessage objects."""
    return pickle.loads(base64.b64decode(data_str))


def maxframe_write(table: "Table", dataframe, overwrite: bool = False):
    """
    Distributed write of a MaxFrame DataFrame into a Paimon table.

    Parameters
    ----------
    table : Table
        The target Paimon table object.
    dataframe : maxframe.dataframe.DataFrame
        A MaxFrame distributed DataFrame.
    overwrite : bool
        If True, overwrite the existing data in the table.
    """
    try:
        import maxframe.dataframe  # noqa: F401
    except ImportError:
        raise ImportError(
            "MaxFrame is required for write_maxframe(). "
            "Install it with: pip install pypaimon[maxframe]"
        )

    import pandas as pd

    # --- Phase 1: parallel data-file writing via apply_chunk ---------------

    # The table object is picklable (verified by Ray datasink pattern).
    # We capture it in the closure so each worker can create its own writer.
    _table = table
    _overwrite = overwrite

    def _write_chunk_to_paimon(pdf):
        """Executed on each MaxFrame worker with a pandas chunk."""
        import os
        import pyarrow as pa
        from pypaimon.schema.data_types import PyarrowFieldParser

        if pdf.empty:
            return pd.DataFrame({"_paimon_commit_msg": [""], "_paimon_worker_info": ["empty"]})

        # Create a fresh writer per chunk (same pattern as Ray datasink)
        write_builder = _table.new_batch_write_builder()
        if _overwrite:
            write_builder = write_builder.overwrite()
        table_write = write_builder.new_write()
        try:
            pa_schema = PyarrowFieldParser.from_paimon_schema(
                _table.table_schema.fields
            )
            record_batch = pa.RecordBatch.from_pandas(pdf, schema=pa_schema)
            table_write.write_arrow_batch(record_batch)
            commit_messages = table_write.prepare_commit()
        finally:
            table_write.close()

        serialized = _serialize_commit_messages(commit_messages)
        worker_info = (
            f"pid={os.getpid()}, "
            f"rows={len(pdf)}, "
            f"files={sum(len(m.new_files) for m in commit_messages)}"
        )
        return pd.DataFrame({
            "_paimon_commit_msg": [serialized],
            "_paimon_worker_info": [worker_info],
        })

    logger.info("MaxFrame write phase 1: distributing data-file writes")

    commit_result_df = dataframe.mf.apply_chunk(
        _write_chunk_to_paimon,
        output_type="dataframe",
        dtypes={"_paimon_commit_msg": "object", "_paimon_worker_info": "object"},
        skip_infer=True,
    ).execute()

    # --- Phase 2: serial commit on client ----------------------------------

    # Log distributed execution summary
    worker_infos = commit_result_df.get("_paimon_worker_info", [])
    num_workers = sum(1 for w in worker_infos if w and w != "empty")
    logger.info(
        "MaxFrame write phase 1 completed: %d workers participated", num_workers
    )
    for i, info in enumerate(worker_infos):
        if info and info != "empty":
            logger.info("  chunk %d: %s", i, info)

    all_commit_messages = []
    for msg_str in commit_result_df["_paimon_commit_msg"]:
        if not msg_str:
            continue
        messages = _deserialize_commit_messages(msg_str)
        non_empty = [m for m in messages if not m.is_empty()]
        all_commit_messages.extend(non_empty)

    if not all_commit_messages:
        logger.info("No data to commit (all chunks were empty)")
        return

    logger.info(
        "MaxFrame write phase 2: committing %d commit messages",
        len(all_commit_messages),
    )

    write_builder = table.new_batch_write_builder()
    if overwrite:
        write_builder = write_builder.overwrite()
    table_commit = write_builder.new_commit()
    try:
        table_commit.commit(all_commit_messages)
        logger.info("Successfully committed MaxFrame write")
    except Exception:
        logger.error("Failed to commit MaxFrame write", exc_info=True)
        try:
            table_commit.abort(all_commit_messages)
        except Exception:
            logger.warning("Failed to abort commit messages", exc_info=True)
        raise
    finally:
        table_commit.close()
