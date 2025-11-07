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
from typing import List, Optional, Iterable

import pyarrow

from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import PyarrowFieldParser

try:
    from ray.data.datasource import Datasource
except ImportError:
    # Fallback for environments without Ray Data
    Datasource = object


class PaimonDatasource(Datasource):
    """
    Ray Data Datasource implementation for reading Paimon tables.
    
    This datasource enables distributed parallel reading of Paimon table splits,
    allowing Ray to read multiple splits concurrently across the cluster.
    """

    def __init__(self, table_read: TableRead, splits: List[Split]):
        """
        Initialize PaimonDatasource.
        
        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
        """
        self.table_read = table_read
        self.splits = splits
        self._schema = None

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        identifier = self.table_read.table.identifier
        table_name = identifier.get_full_name() if hasattr(identifier, 'get_full_name') else str(identifier)
        return f"PaimonTable({table_name})"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate the in-memory data size.
        
        Returns:
            Estimated size in bytes, or None if unknown
        """
        if not self.splits:
            return 0
        
        # Sum up file sizes from all splits
        # Note: This is an estimate, actual in-memory size may be different
        total_size = sum(split.file_size for split in self.splits)
        return total_size if total_size > 0 else None

    def get_read_tasks(self, parallelism: int, per_task_row_limit: Optional[int] = None) -> List:
        """
        Execute the read and return read tasks.
        
        Args:
            parallelism: The requested read parallelism
            per_task_row_limit: The per-task row limit for the read tasks
            
        Returns:
            A list of read tasks that can be executed to read blocks in parallel
        """
        try:
            from ray.data.datasource import ReadTask
            from ray.data.block import BlockMetadata
        except ImportError:
            raise ImportError(
                "Ray Data is not installed. Please install it with: pip install ray[data]"
            )

        # Get schema for metadata
        if self._schema is None:
            self._schema = PyarrowFieldParser.from_paimon_schema(self.table_read.read_type)

        read_tasks = []
        
        # Store necessary information for creating readers in Ray workers
        # We need to store table, predicate, and read_type separately to ensure
        # they are properly serialized and available in Ray workers
        table = self.table_read.table
        predicate = self.table_read.predicate
        read_type = self.table_read.read_type
        
        # Create a read task for each split
        # This allows Ray to parallelize reading across multiple splits
        for split in self.splits:
            # Create read function for this split
            # Capture all necessary data in the closure to ensure proper serialization
            def create_read_fn(
                split=split,
                table=table,
                predicate=predicate,
                read_type=read_type,
                schema=self._schema
            ):
                """Read function that will be executed by Ray workers."""
                # Recreate TableRead in the worker to ensure predicate is available
                # This ensures that all components are properly available after Ray serialization
                from pypaimon.read.table_read import TableRead
                worker_table_read = TableRead(table, predicate, read_type)
                
                # Read the split and return as a list of tables (batches)
                # This allows Ray to handle large splits by breaking them into blocks
                arrow_table = worker_table_read.to_arrow([split])
                
                # Return as a list to allow Ray to split into multiple blocks if needed
                if arrow_table is not None and arrow_table.num_rows > 0:
                    return [arrow_table]
                else:
                    # Return empty table with correct schema
                    empty_table = pyarrow.Table.from_arrays(
                        [pyarrow.array([], type=field.type) for field in schema],
                        schema=schema
                    )
                    return [empty_table]

            # Create metadata for this split
            # Use split's row_count and file_size if available
            # If predicate exists, we can't accurately estimate num_rows before filtering
            # Set to None to let Ray calculate the actual row count from the data
            if predicate is not None:
                num_rows = None  # Let Ray calculate from actual data after filtering
            else:
                num_rows = split.row_count if hasattr(split, 'row_count') and split.row_count > 0 else None
            size_bytes = split.file_size if hasattr(split, 'file_size') and split.file_size > 0 else None
            
            # Get file paths for metadata
            input_files = split.file_paths if hasattr(split, 'file_paths') else None

            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=size_bytes,
                exec_stats=None,  # Will be populated by Ray during execution
                input_files=input_files
            )

            # Create ReadTask
            read_task = ReadTask(
                read_fn=create_read_fn,
                metadata=metadata,
                schema=self._schema,
                per_task_row_limit=per_task_row_limit
            )
            
            read_tasks.append(read_task)

        return read_tasks

