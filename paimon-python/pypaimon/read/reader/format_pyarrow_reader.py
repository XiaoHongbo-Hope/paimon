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

from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.
    """

    def __init__(self, file_io: FileIO, file_format: str, file_path: str, read_fields: List[str],
                 push_down_predicate: Any, batch_size: int = 1024,
                 read_schema_fields: Optional[List[DataField]] = None):
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        self.dataset = ds.dataset(file_path_for_pyarrow, format=file_format, filesystem=file_io.filesystem)
        self.read_fields = read_fields
        self.read_schema_fields = read_schema_fields

        # Identify which fields exist in the file and which are missing
        file_schema_names = set(self.dataset.schema.names)
        self.existing_fields = [f for f in read_fields if f in file_schema_names]
        self.missing_fields = [f for f in read_fields if f not in file_schema_names]

        # Only pass existing fields to PyArrow scanner to avoid errors
        self.reader = self.dataset.scanner(
            columns=self.existing_fields,
            filter=push_down_predicate,
            batch_size=batch_size
        ).to_reader()

    def _build_output_schema(self) -> Optional[pa.Schema]:
        """Build PyArrow schema for type lookup when filling missing columns."""
        if self.read_schema_fields is None:
            return None
        name_to_field = {f.name: f for f in self.read_schema_fields}
        ordered_fields = [name_to_field[n] for n in self.read_fields if n in name_to_field]
        if not ordered_fields:
            return None
        return PyarrowFieldParser.from_paimon_schema(ordered_fields)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            batch = self.reader.read_next_batch()

            if not self.missing_fields:
                return batch

            output_schema = self._build_output_schema()

            def _type_for_missing(name: str) -> pa.DataType:
                if output_schema is not None:
                    idx = output_schema.get_field_index(name)
                    if idx >= 0:
                        return output_schema.field(idx).type
                return pa.null()

            missing_columns = [
                pa.nulls(batch.num_rows, type=_type_for_missing(name))
                for name in self.missing_fields
            ]

            # Reconstruct the batch with all fields in the correct order
            all_columns = []
            out_fields = []
            for field_name in self.read_fields:
                if field_name in self.existing_fields:
                    # Get the column from the existing batch
                    column_idx = self.existing_fields.index(field_name)
                    all_columns.append(batch.column(column_idx))
                    out_fields.append(batch.schema.field(column_idx))
                else:
                    # Get the column from missing fields
                    column_idx = self.missing_fields.index(field_name)
                    col_type = _type_for_missing(field_name)
                    all_columns.append(missing_columns[column_idx])
                    nullable = not SpecialFields.is_system_field(field_name)
                    out_fields.append(pa.field(field_name, col_type, nullable=nullable))
            # Create a new RecordBatch with all columns
            return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields))

        except StopIteration:
            return None

    def close(self):
        if self.reader is not None:
            self.reader = None
