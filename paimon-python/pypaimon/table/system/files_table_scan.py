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

"""Deferred scan plan for the ``$files`` system table."""

from typing import List, Optional, TYPE_CHECKING

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.table.row.generic_row import GenericRow

if TYPE_CHECKING:  # pragma: no cover
    from pypaimon.table.system.files_table import FilesTable


class FilesSystemSplit(Split):
    """Manifest-list split whose entries are decoded by the table read."""

    def __init__(self, manifest_files):
        self.manifest_files = manifest_files

    @property
    def row_count(self) -> int:
        return max(
            0,
            sum(
                manifest.num_added_files - manifest.num_deleted_files
                for manifest in self.manifest_files
            ),
        )

    @property
    def files(self) -> List[DataFileMeta]:
        return []

    @property
    def partition(self) -> Optional[GenericRow]:
        return None

    @property
    def bucket(self) -> int:
        return -1


class FilesTableScan:
    """Plan ``$files`` without materialising all rows into Arrow."""

    def __init__(self, table: "FilesTable"):
        self.table = table

    def plan(self) -> Plan:
        snapshot, manifest_files = self.table.manifest_files()
        if snapshot is None or not manifest_files:
            return Plan(_splits=[])
        return Plan(
            _splits=[FilesSystemSplit(manifest_files)],
            snapshot_id=snapshot.id,
        )
