###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

from typing import List, Optional

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.table.row.offset_row import OffsetRow


class FilterRecordBatchReader(RecordBatchReader):
    def __init__(
        self,
        reader: RecordBatchReader,
        predicate: Predicate,
        field_names: Optional[List[str]] = None,
    ):
        self.reader = reader
        self.predicate = predicate
        self.field_names = field_names

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        while True:
            batch = self.reader.read_arrow_batch()
            if batch is None:
                return None
            if batch.num_rows == 0:
                return batch
            nrows = batch.num_rows
            if self.field_names is not None:
                col_indices = []
                batch_schema_names = set(batch.schema.names)
                for name in self.field_names:
                    if name in batch_schema_names:
                        col_indices.append(batch.schema.get_field_index(name))
                    else:
                        col_indices.append(None)
                ncols = len(self.field_names)
            else:
                col_indices = list(range(batch.num_columns))
                ncols = batch.num_columns
            mask = []
            row_tuple = [None] * ncols
            offset_row = OffsetRow(row_tuple, 0, ncols)
            for i in range(nrows):
                for j in range(ncols):
                    if col_indices[j] is not None:
                        row_tuple[j] = batch.column(col_indices[j])[i].as_py()
                    else:
                        row_tuple[j] = None
                offset_row.replace(tuple(row_tuple))
                try:
                    mask.append(self.predicate.test(offset_row))
                except Exception:
                    mask.append(False)
            if any(mask):
                filtered = batch.filter(pa.array(mask))
                if filtered.num_rows > 0:
                    return filtered
            continue

    def return_batch_pos(self) -> int:
        pos = getattr(self.reader, 'return_batch_pos', lambda: 0)()
        return pos if pos is not None else 0

    def close(self) -> None:
        self.reader.close()
