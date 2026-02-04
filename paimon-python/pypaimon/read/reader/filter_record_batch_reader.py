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
import pyarrow.compute as pc
import pyarrow.dataset as ds

from pypaimon.common.predicate import Predicate
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.table.row.offset_row import OffsetRow


class FilterRecordBatchReader(RecordBatchReader):
    """
    Wraps a RecordBatchReader and filters each batch by predicate.
    Used for data evolution read where predicate cannot be pushed down to
    individual file readers.
    """

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
            filtered = self._filter_batch(batch)
            if filtered is not None and filtered.num_rows > 0:
                return filtered
            continue

    @staticmethod
    def _predicate_has_null_check(predicate: Predicate) -> bool:
        if predicate.method in ('isNull', 'isNotNull'):
            return True
        if predicate.method in ('and', 'or') and predicate.literals:
            return any(
                FilterRecordBatchReader._predicate_has_null_check(p)
                for p in predicate.literals
            )
        return False

    def _filter_batch_simple_null(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        if self.predicate.method not in ('isNull', 'isNotNull') or not self.predicate.field:
            return None
        if self.predicate.field not in batch.schema.names:
            return None
        col = batch.column(batch.schema.get_field_index(self.predicate.field))
        if self.predicate.method == 'isNull':
            mask = pc.is_null(col)
        else:
            mask = pc.is_valid(col)
        result = batch.filter(mask)
        return result if result.num_rows > 0 else None

    def _filter_batch(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        simple_null = self._filter_batch_simple_null(batch)
        if simple_null is not None:
            return simple_null
        use_vectorized = not self._predicate_has_null_check(self.predicate)
        if use_vectorized:
            try:
                expr = self.predicate.to_arrow()
                table = pa.Table.from_batches([batch])
                dataset = ds.InMemoryDataset(table)
                result = dataset.scanner(filter=expr).to_table()
                if result.num_rows == 0:
                    return None
                return pa.RecordBatch.from_arrays(
                    [result.column(i) for i in range(result.num_columns)],
                    schema=result.schema)
            except Exception:
                pass
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
        if not any(mask):
            return None
        return batch.filter(pa.array(mask))

    def return_batch_pos(self) -> int:
        pos = getattr(self.reader, 'return_batch_pos', lambda: 0)()
        return pos if pos is not None else 0

    def close(self) -> None:
        self.reader.close()
