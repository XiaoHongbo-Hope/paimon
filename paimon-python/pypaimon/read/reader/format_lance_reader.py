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
#  limitations under the License.
################################################################################

import logging
from typing import List, Optional, Any

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance_utils import to_lance_specified

logger = logging.getLogger(__name__)


class FormatLanceReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Lance file using PyArrow,
    and filters it based on the provided predicate and projection.
    
    Uses LanceFileReader with lazy initialization to avoid loading entire file into memory.
    """

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 push_down_predicate: Any, batch_size: int = 1024):
        """Initialize Lance reader with lazy loading."""

        file_path_for_lance, storage_options = to_lance_specified(file_io, file_path)

        self._file_io = file_io
        self._file_path = file_path
        self._file_path_for_lance = file_path_for_lance
        self._storage_options = storage_options
        self._read_fields = read_fields
        self._push_down_predicate = push_down_predicate
        self._batch_size = batch_size
        
        self._lance_file_reader = None
        self._reader = None
        self._initialized = False

    def _ensure_initialized(self):
        """Lazy initialization: create reader only when needed."""
        if self._initialized:
            return
        
        if hasattr(self, '_file_io') and hasattr(self._file_io, 'try_to_refresh_token'):
            self._file_io.try_to_refresh_token()
            if hasattr(self._file_io, 'token') and self._file_io.token:
                self._file_io.properties.update(self._file_io.token.token)
            if hasattr(self, '_file_path'):
                self._file_path_for_lance, self._storage_options = to_lance_specified(
                    self._file_io, self._file_path
                )
        
        import lance
        
        self._lance_file_reader = lance.file.LanceFileReader(
            path=self._file_path_for_lance,
            storage_options=self._storage_options,
            columns=self._read_fields if self._read_fields else None
        )
        
        reader_results = self._lance_file_reader.read_all(batch_size=self._batch_size)
        self._reader = reader_results.to_batches()  # Returns RecordBatchReader (streaming)
        
        self._initialized = True

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        """Read next batch, with lazy initialization."""
        self._ensure_initialized()
        
        while True:
            try:
                if hasattr(self._reader, 'read_next_batch'):
                    batch = self._reader.read_next_batch()
                else:
                    batch = next(self._reader)
                
                if batch is None:
                    return None
                
                if self._push_down_predicate is not None:
                    table = pa.Table.from_batches([batch])
                    in_memory_dataset = ds.InMemoryDataset(table)
                    scanner = in_memory_dataset.scanner(filter=self._push_down_predicate)
                    filtered_table = scanner.to_table().combine_chunks()
                    if filtered_table.num_rows > 0:
                        # Convert back to RecordBatch
                        return filtered_table.to_batches()[0]
                    else:
                        # No rows match predicate, continue to next batch
                        continue
                
                return batch
            except StopIteration:
                return None

    def close(self):
        """Close reader and release resources."""
        if self._reader is not None:
            try:
                self._reader.close()
            except Exception:
                pass
            self._reader = None
        
        if self._lance_file_reader is not None:
            try:
                self._lance_file_reader.close()
            except Exception:
                pass
            self._lance_file_reader = None
        
        self._initialized = False
