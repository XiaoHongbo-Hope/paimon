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
from pypaimon.schema.data_types import DataField
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
        schema_fields: Optional[List[DataField]] = None,
    ):
        self.reader = reader
        self.predicate = predicate
        self.field_names = field_names
        self.schema_fields = schema_fields

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

    def _build_col_indices(self, batch: pa.RecordBatch) -> tuple:
        names = set(batch.schema.names)
        if self.schema_fields is not None:
            fields = self.schema_fields
        elif self.field_names is not None:
            fields = self.field_names
        else:
            return list(range(batch.num_columns)), batch.num_columns
        indices = []
        for f in fields:
            name = f.name if hasattr(f, 'name') else f
            indices.append(batch.schema.get_field_index(name) if name in names else None)
        return indices, len(indices)

    def _filter_batch_simple_null(
        self, batch: pa.RecordBatch
    ) -> Optional[pa.RecordBatch]:
        if (self.predicate.method not in ('isNull', 'isNotNull')
                or not self.predicate.field):
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
        if not self.predicate.has_null_check():
            try:
                expr = self.predicate.to_arrow()
                result = ds.InMemoryDataset(pa.Table.from_batches([batch])).scanner(
                    filter=expr
                ).to_table()
                if result.num_rows == 0:
                    return None
                batches = result.to_batches()
                return batches[0] if len(batches) == 1 else pa.RecordBatch.from_arrays(
                    [result.column(i) for i in range(result.num_columns)],
                    schema=result.schema,
                )
            except (TypeError, ValueError):
                pass
        nrows = batch.num_rows
        col_indices, ncols = self._build_col_indices(batch)
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
            except (TypeError, ValueError):
                mask.append(False)
        if not any(mask):
            return None
        return batch.filter(pa.array(mask))

    def return_batch_pos(self) -> int:
        pos = getattr(self.reader, 'return_batch_pos', lambda: 0)()
        return pos if pos is not None else 0

    def close(self) -> None:
        self.reader.close()
