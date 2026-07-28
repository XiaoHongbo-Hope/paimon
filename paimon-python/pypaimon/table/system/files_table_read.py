# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Projection-aware, batched read pipeline for ``$files``."""

from typing import Iterator, List

import pyarrow

from pypaimon.read.split import Split
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.system.files_table_scan import (
    FilesSystemSplit,
    FilesTableScan,
)
from pypaimon.table.system.system_table import SystemReadBuilder
from pypaimon.table.system.system_table_read import (
    _PREDICATE_NOT_SUPPORTED,
    SystemTableRead,
)


_BATCH_SIZE = 1024


class FilesReadBuilder(SystemReadBuilder):
    """Keep ``$files`` metadata out of scan planning."""

    def new_scan(self) -> FilesTableScan:
        return FilesTableScan(self.system_table)

    def new_read(self) -> "FilesTableRead":
        return FilesTableRead(
            system_table=self.system_table,
            read_type=self.read_type(),
            predicate=self._predicate,
            limit=self._limit,
        )


class FilesTableRead(SystemTableRead):
    """Read active file entries in projected Arrow record batches."""

    def to_arrow(self, splits: List[Split]) -> pyarrow.Table:
        batches = list(self.to_record_batch_iterator(splits))
        schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        if not batches:
            return pyarrow.Table.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema],
                schema=schema,
            )
        return pyarrow.Table.from_batches(batches, schema=schema)

    def to_iterator(self, splits: List[Split]) -> Iterator[dict]:
        for batch in self.to_record_batch_iterator(splits):
            column_names = batch.schema.names
            py_columns = [
                batch.column(i).to_pylist()
                for i in range(batch.num_columns)
            ]
            for row_idx in range(batch.num_rows):
                yield {
                    column_names[column_idx]: py_columns[column_idx][row_idx]
                    for column_idx in range(batch.num_columns)
                }

    def to_record_batch_iterator(
        self,
        splits: List[Split],
    ) -> Iterator[pyarrow.RecordBatch]:
        if self.predicate is not None:
            raise NotImplementedError(_PREDICATE_NOT_SUPPORTED)
        if self.limit is not None and self.limit <= 0:
            return

        projected_names = [field.name for field in self.read_type]
        remaining = self.limit
        for split in splits:
            if not isinstance(split, FilesSystemSplit):
                raise TypeError(
                    "FilesTableRead expects FilesSystemSplit but received "
                    + type(split).__name__
                )
            entries = self.system_table.read_entries(
                split.manifest_files, projected_names)
            offset = 0
            while offset < len(entries):
                batch_size = min(_BATCH_SIZE, len(entries) - offset)
                if remaining is not None:
                    batch_size = min(batch_size, remaining)
                if batch_size <= 0:
                    return
                batch_entries = entries[offset:offset + batch_size]
                yield self.system_table.entries_to_record_batch(
                    batch_entries, projected_names)
                offset += batch_size
                if remaining is not None:
                    remaining -= batch_size
                    if remaining <= 0:
                        return
