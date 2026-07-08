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

"""Fault-tolerant, incrementally-committed column backfill for data-evolution
tables.

For long jobs (e.g. days of model-service inference) a single terminal commit
means a mid-way crash loses everything. This drives the update **per data
file** (the DE overlay granularity): infer one file's rows, buffer, and commit
every ``commit_every_files`` completed files. On restart it derives progress
from the target itself (which files already carry a partial overlay covering
``update_cols``) and skips them, so the expensive inference never re-runs for
committed files.

The caller supplies only the target table and an inference callback; batching,
committing and resume are handled here.
"""
import logging
import time
import uuid
from typing import Callable, Dict, List, Optional

import pyarrow as pa

logger = logging.getLogger(__name__)


def _completed_first_row_ids(files_info, update_cols: List[str],
                             full_col_count: int):
    """First-row-ids whose update columns are already committed.

    An ``update_by_row_id`` overlay writes exactly ``update_cols`` — a strict
    subset of the file schema — so a non-blob file that covers ``update_cols``
    but has fewer than ``full_col_count`` write columns is such an overlay. The
    base file carries the full schema and is excluded. (For a base written with
    a partial, update-covering column set — rare, via schema evolution — pass
    the pre-job snapshot to anchor instead; see module docstring.)
    """
    from pypaimon.manifest.schema.data_file_meta import DataFileMeta

    wanted = set(update_cols)
    done = set()
    for frid, entry in files_info.first_row_id_index.items():
        _split, files = entry
        for f in files:
            if DataFileMeta.is_blob_file(f.file_name):
                continue
            write_cols = f.write_cols
            if (write_cols
                    and wanted.issubset(set(write_cols))
                    and len(write_cols) < full_col_count):
                done.add(frid)
                break
    return done


def incremental_update_by_row_id(
    target: str,
    catalog_options: Dict[str, str],
    infer_fn: Callable[[pa.Table], pa.Table],
    update_cols: List[str],
    input_cols: List[str],
    *,
    commit_every_files: int = 8,
    commit_every_seconds: Optional[float] = None,
    resume: bool = True,
    progress_fn: Optional[Callable[[Dict], None]] = None,
) -> Dict:
    """Backfill ``update_cols`` file-by-file with incremental commits.

    Args:
        target: target table identifier.
        catalog_options: catalog connection options.
        infer_fn: ``(rows: pa.Table with _ROW_ID + input_cols) -> pa.Table with
            _ROW_ID + update_cols``. The only caller-supplied step (the model
            call). Rows may be returned in any order; they are matched by
            ``_ROW_ID``.
        update_cols: columns to write back.
        input_cols: columns the inference reads (``_ROW_ID`` is added).
        commit_every_files: commit after this many completed files.
        commit_every_seconds: also commit at least this often (whichever first).
        resume: skip files whose update columns are already committed.

    Returns a stats dict.
    """
    import ray

    from pypaimon import CatalogFactory
    from pypaimon.ray.update_by_row_id import update_by_row_id
    from pypaimon.read.split import DataSplit
    from pypaimon.read.table_read import TableRead
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.table.special_fields import SpecialFields
    from pypaimon.manifest.schema.data_file_meta import DataFileMeta
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    row_id_name = SpecialFields.ROW_ID.name
    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    full_col_count = len(table.fields)

    # One manifest scan: file list + resume set (progress derived from target).
    planner = TableUpdateByRowId(
        table, "_inc_planner_" + uuid.uuid4().hex[:8], BATCH_COMMIT_IDENTIFIER)
    files_info = planner._snapshot_files_info()
    done = (_completed_first_row_ids(files_info, update_cols, full_col_count)
            if resume else set())
    todo = [frid for frid in planner.first_row_ids if frid not in done]

    read_fields = [f for f in table.fields if f.name in set(input_cols)]

    stats = {
        "files_total": len(planner.first_row_ids),
        "resumed_skipped": len(done),
        "files_done": 0,
        "rows_updated": 0,
        "commits": 0,
    }

    logger.info(
        "incremental_update_by_row_id target=%s: %d data files, %d already "
        "committed (skipped), %d to process; commit every %s files / %s s",
        target, stats["files_total"], stats["resumed_skipped"], len(todo),
        commit_every_files, commit_every_seconds)

    buffer: List[pa.Table] = []
    last_commit = time.monotonic()
    t_start = time.monotonic()

    def _flush():
        if not buffer:
            return
        n_files = len(buffer)
        src = pa.concat_tables(buffer, promote_options="default")
        res = update_by_row_id(
            target, ray.data.from_arrow(src), catalog_options,
            update_cols=list(update_cols))
        stats["rows_updated"] += res.get("num_updated", 0)
        stats["commits"] += 1
        buffer.clear()
        elapsed = time.monotonic() - t_start
        logger.info(
            "committed %d files (%d rows): progress %d/%d files, %d rows, "
            "%d commits, %.0fs elapsed",
            n_files, res.get("num_updated", 0), stats["files_done"], len(todo),
            stats["rows_updated"], stats["commits"], elapsed)
        if progress_fn is not None:
            progress_fn(dict(stats, to_process=len(todo),
                             elapsed_s=round(elapsed, 1)))

    for frid in todo:
        owning_split, files = files_info.first_row_id_index[frid]
        data_files = [
            f for f in files if not DataFileMeta.is_blob_file(f.file_name)
        ]
        split = DataSplit(
            files=data_files, partition=owning_split.partition,
            bucket=owning_split.bucket, raw_convertible=True)
        rows = TableRead(
            table, predicate=None, read_type=read_fields).to_arrow([split])
        n = rows.num_rows
        # A data file's rows are in row-id order, so row i owns first_row_id + i.
        rows = rows.append_column(
            row_id_name,
            pa.array(range(frid, frid + n), type=pa.int64()))

        upd = infer_fn(rows)
        buffer.append(upd)
        stats["files_done"] += 1
        logger.debug("inferred file first_row_id=%d (%d rows); %d/%d done",
                     frid, n, stats["files_done"], len(todo))

        due_by_count = len(buffer) >= commit_every_files
        due_by_time = (commit_every_seconds is not None
                       and time.monotonic() - last_commit >= commit_every_seconds)
        if due_by_count or due_by_time:
            _flush()
            last_commit = time.monotonic()

    _flush()
    logger.info(
        "incremental_update_by_row_id done target=%s: processed %d/%d files, "
        "%d rows, %d commits, %d resumed-skipped, %.0fs",
        target, stats["files_done"], len(todo), stats["rows_updated"],
        stats["commits"], stats["resumed_skipped"], time.monotonic() - t_start)
    return stats
