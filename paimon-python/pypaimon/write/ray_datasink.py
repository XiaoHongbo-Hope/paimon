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
Module to write a Ray Dataset into a Paimon table, by using the Ray Datasink API.
"""

import logging
from typing import TYPE_CHECKING, Iterable, List, Optional

from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI
from ray.data.block import BlockAccessor, Block
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa

if TYPE_CHECKING:
    from pypaimon.table.table import Table
    from pypaimon.write.write_builder import WriteBuilder
    from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)


@DeveloperAPI
class PaimonDatasink(Datasink[List["CommitMessage"]]):
    """
    Paimon datasink to write a Ray Dataset into an existing Paimon table.
    This module heavily uses PyPaimon to write to Paimon table. All the routines
    in this class override `ray.data.Datasink`.
    """

    def __init__(
        self,
        table: "Table",
        overwrite: bool = False,
    ):
        """
        Initialize the PaimonDatasink.

        Args:
            table: The Paimon table to write to.
            overwrite: If True, overwrite existing data. Defaults to False.
        """
        self.table = table
        self.overwrite = overwrite
        self._writer_builder: Optional["WriteBuilder"] = None

    # Since Paimon table and writer_builder may not be pickle-able due to
    # file system connections and other resources, we need to handle
    # serialization carefully. The table will be recreated on worker nodes
    # if needed, but typically the table object is serializable.
    def __getstate__(self) -> dict:
        """Exclude non-serializable objects during pickling."""
        state = self.__dict__.copy()
        # writer_builder is created in on_write_start() on driver,
        # and will be recreated if needed. We keep it for now as it
        # should be serializable, but if issues arise, we can exclude it.
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state after unpickling."""
        self.__dict__.update(state)
        # Ensure writer_builder is None if it wasn't serialized properly
        if self._writer_builder is not None and not hasattr(self._writer_builder, 'table'):
            self._writer_builder = None

    def on_write_start(self, schema=None) -> None:
        """Prepare for the write job.

        This method is executed on the driver node before any write tasks begin.
        It initializes the BatchWriteBuilder that will be used by worker nodes.

        Args:
            schema: Optional schema information passed by Ray Data.
        """
        table_name = self._get_table_name()
        logger.info(f"Starting write job for table {table_name}")

        self._writer_builder = self.table.new_batch_write_builder()
        if self.overwrite:
            self._writer_builder = self._writer_builder.overwrite()

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["CommitMessage"]:
        """Write blocks. This is used by a single write task.

        This method is executed on worker nodes. Each worker:
        1. Creates a BatchTableWrite instance
        2. Writes all blocks assigned to it
        3. Prepares commit and returns CommitMessage list
        4. Closes the writer (ensured by try-finally)

        Args:
            blocks: Generator of data blocks to write.
            ctx: TaskContext for the write task.

        Returns:
            List of CommitMessage objects containing metadata about written files.
            These will be collected by the driver and committed atomically.

        Raises:
            Exception: If writing fails, the exception is propagated and
                table_write.close() is called in the finally block.
        """
        commit_messages_list: List["CommitMessage"] = []
        table_write = None

        try:
            table_write = self._writer_builder.new_write()

            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()

                # Skip empty blocks
                if block_arrow.num_rows == 0:
                    continue

                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            commit_messages_list.extend(commit_messages)

        finally:
            # Ensure table_write is closed even if an exception occurs
            if table_write is not None:
                try:
                    table_write.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_write: {e}",
                        exc_info=e
                    )

        return commit_messages_list

    def on_write_complete(
        self, write_result: WriteResult[List["CommitMessage"]]
    ):
        """Callback for when a write job completes.

        This method is executed on the driver node after all write tasks complete.
        It collects all CommitMessages from workers and commits them atomically.

        Args:
            write_result: Aggregated result containing all CommitMessages from workers
                and write statistics.

        Raises:
            RuntimeError: If commit fails, the exception is propagated and
                table_commit.close() is called in the finally block.
                The TableCommit._commit() method will call abort() on failure.
        """
        table_commit = None
        try:
            # Collect all commit messages from all workers
            all_commit_messages = [
                commit_message
                for commit_messages in write_result.write_returns
                for commit_message in commit_messages
            ]

            # Filter out empty messages (TableCommit._commit() also does this,
            # but we can optimize by doing it here)
            non_empty_messages = [
                msg for msg in all_commit_messages if not msg.is_empty()
            ]

            if not non_empty_messages:
                logger.info("No data to commit (all commit messages are empty)")
                return

            table_name = self._get_table_name()
            logger.info(
                f"Committing {len(non_empty_messages)} commit messages "
                f"for table {table_name}"
            )

            table_commit = self._writer_builder.new_commit()
            table_commit.commit(non_empty_messages)

            logger.info(f"Successfully committed write job for table {table_name}")
        finally:
            # Ensure table_commit is closed even if an exception occurs
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_commit: {e}",
                        exc_info=e
                    )

    def on_write_failed(self, error: Exception) -> None:
        """Callback for when a write job fails.

        This method is called on a best-effort basis when a write job fails.
        It logs the error for debugging purposes.

        Note: We cannot abort commit messages here because we don't have
        access to them. The abort is handled in TableCommit._commit() if
        commit fails.

        Args:
            error: The first error encountered during the write job.
        """
        table_name = self._get_table_name()
        logger.warning(
            f"Write job failed for table {table_name}. Error: {error}",
            exc_info=error
        )
    
    def _get_table_name(self) -> str:
        """Get the table name for logging purposes.
        
        Returns:
            The full table identifier (e.g., "db.table") or "unknown" if not available.
        """
        if hasattr(self.table, 'identifier'):
            return self.table.identifier.get_full_name()
        return 'unknown'


