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
Module to write Ray Dataset to a Paimon table, by using the Ray Datasink API.
"""
import logging
from typing import Dict, Optional, Iterable, Any, List

from pypaimon import CatalogFactory
from pypaimon.common.identifier import Identifier
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.batch_table_write import BatchTableWrite
from pypaimon.write.commit_message import CommitMessage
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource import Datasink, WriteReturnType, WriteResult

logger = logging.getLogger(__name__)


class PaimonDatasink(Datasink):

    def __init__(
        self,
        catalog_id: str,
        database: str,
        table: str,
        options: Optional[Dict[str, str]] = None,
        overwrite: bool = False,
    ):
        self.catalog_id = catalog_id
        self.database = database
        self.table = table
        self.options = options or {}
        self.overwrite = overwrite

        # Validate options
        if not self.options:
            raise ValueError("Options for PaimonDatasink cannot be empty")
        
        # Set metastore from catalog_id if not already specified
        from pypaimon.common.config import CatalogOptions
        if CatalogOptions.METASTORE not in self.options:
            self.options[CatalogOptions.METASTORE] = catalog_id

    def on_write_start(self) -> None:

        logger.info(
            "Paimon write started for table %s.%s (overwrite=%s)",
            self.database,
            self.table,
            self.overwrite
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> WriteReturnType:
        catalog = CatalogFactory.create(self.options)
        table_id = Identifier.create(self.database, self.table)
        paimon_table: FileStoreTable = catalog.get_table(table_id)
        
        writer = None
        total_rows = 0
        
        try:
            # Create write builder
            write_builder = paimon_table.new_batch_write_builder()
            if self.overwrite:
                write_builder = write_builder.overwrite()
            
            # Create writer
            writer = write_builder.new_write()
            
            # Write all blocks
            for block in blocks:
                rows_written = self._write_block(block, writer)
                total_rows += rows_written
            
            commit_messages = writer.prepare_commit()
            
            logger.info(
                "Worker wrote %d rows, prepared %d commit messages",
                total_rows,
                len(commit_messages)
            )
            
            return commit_messages
            
        except Exception as e:
            logger.error(
                "Failed to write blocks in worker: %s",
                e,
                exc_info=True
            )
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            raise
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception as e:
                    logger.warning("Error closing writer: %s", e)

    def _write_block(self, block: Block, writer: BatchTableWrite) -> int:

        block_accessor = BlockAccessor.for_block(block)
        num_rows = block_accessor.num_rows()
        
        if num_rows == 0:
            logger.debug("Skipped writing empty block")
            return 0
        
        try:
            writer.write_arrow(block_accessor.to_arrow())
            return num_rows
        except Exception as e:
            logger.error("Failed to write block with %d rows: %s", num_rows, e)
            raise

    def on_write_complete(self, write_result: WriteResult[WriteReturnType]) -> None:
        all_commit_messages: List[CommitMessage] = []
        for worker_result in write_result.write_returns:
            if isinstance(worker_result, list):
                # Each worker returns a list of CommitMessage
                all_commit_messages.extend(worker_result)
            elif worker_result is not None:
                # Handle single CommitMessage (shouldn't happen, but be safe)
                if isinstance(worker_result, CommitMessage):
                    all_commit_messages.append(worker_result)
                else:
                    logger.warning(
                        "Unexpected return type from worker: %s",
                        type(worker_result)
                    )
        
        if not all_commit_messages:
            logger.warning("No commit messages to commit")
            return
        
        # Create catalog and table on driver
        catalog = CatalogFactory.create(self.options)
        table_id = Identifier.create(self.database, self.table)
        paimon_table: FileStoreTable = catalog.get_table(table_id)
        
        write_builder = paimon_table.new_batch_write_builder()
        if self.overwrite:
            write_builder = write_builder.overwrite()
        
        commit = None
        try:
            commit = write_builder.new_commit()
            commit.commit(all_commit_messages)
            
            logger.info(
                "Successfully committed %d commit messages to table %s.%s",
                len(all_commit_messages),
                self.database,
                self.table
            )
        except Exception as e:
            logger.error(
                "Failed to commit to table %s.%s: %s",
                self.database,
                self.table,
                e,
                exc_info=True
            )
            # Abort on failure
            if commit is not None:
                try:
                    commit.abort(all_commit_messages)
                except Exception as abort_error:
                    logger.warning("Error during abort: %s", abort_error)
            raise
        finally:
            if commit is not None:
                try:
                    commit.close()
                except Exception as e:
                    logger.warning("Error closing commit: %s", e)

    def on_write_failed(self, error: Exception) -> None:
        """Callback for when a write job fails.
        
        This is called on a best-effort basis on write failures.
        
        Args:
            error: The first error encountered.
        """
        logger.error(
            "Write failed for table %s.%s: %s",
            self.database,
            self.table,
            error,
            exc_info=True
        )

    def get_name(self) -> str:
        """Return the name of this datasink.
        
        Returns:
            A human-readable name for this datasink.
        """
        return f"PaimonDatasink({self.database}.{self.table})"

