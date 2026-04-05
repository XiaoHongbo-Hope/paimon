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

from typing import List, Optional, Any

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance_utils import to_lance_specified


class FormatLanceReader(RecordBatchReader):
    """Streaming Lance file reader with lazy initialization."""

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 push_down_predicate: Any, batch_size: int = 1024):
        self._file_io = file_io
        self._file_path = file_path
        self._read_fields = read_fields
        self._push_down_predicate = push_down_predicate
        self._batch_size = batch_size

        self._reader = None
        self._lance_file_reader = None
        self._initialized = False

    def _ensure_initialized(self):
        if self._initialized:
            return

        import lance

        file_path_for_lance, storage_options = to_lance_specified(
            self._file_io, self._file_path)

        columns = self._read_fields if self._read_fields else None
        self._lance_file_reader = lance.file.LanceFileReader(
            file_path_for_lance,
            storage_options=storage_options,
            columns=columns,
        )

        reader_results = self._lance_file_reader.read_all(
            batch_size=self._batch_size)
        self._reader = reader_results.to_batches()
        self._initialized = True

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        if not self._initialized:
            self._ensure_initialized()

        while True:
            try:
                if hasattr(self._reader, 'read_next_batch'):
                    batch = self._reader.read_next_batch()
                else:
                    batch = next(self._reader)
            except StopIteration:
                return None

            if batch is None:
                return None

            if self._push_down_predicate is not None:
                table = pa.Table.from_batches([batch])
                filtered = ds.InMemoryDataset(table).scanner(
                    filter=self._push_down_predicate).to_table()
                if filtered.num_rows == 0:
                    continue
                return filtered.combine_chunks().to_batches()[0]

            return batch

    def close(self):
        if self._lance_file_reader is not None:
            self._lance_file_reader.close()
            self._lance_file_reader = None
        self._reader = None
        self._initialized = False
