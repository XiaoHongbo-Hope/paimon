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

import logging
import pyarrow as pa
from pathlib import Path
from typing import Optional, Tuple

from pypaimon.common.core_options import CoreOptions
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.write.writer.blob_file_writer import BlobFileWriter

logger = logging.getLogger(__name__)

# Constant for checking rolling condition periodically (aligned with Java)
CHECK_ROLLING_RECORD_CNT = 1000


class BlobWriter(AppendOnlyDataWriter):
    """
    Blob writer that handles rolling based on target file size.
    Aligned with Java RollingBlobFileWriter implementation.
    
    In blob-as-descriptor mode, writes each row and checks file size after each write.
    When target size is exceeded, automatically closes current file and creates a new one.
    """

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int, blob_column: str):
        super().__init__(table, partition, bucket, max_seq_number, [blob_column])

        # Override file format to "blob"
        self.file_format = CoreOptions.FILE_FORMAT_BLOB

        # Use blobTargetFileSize (aligned with Java)
        options = self.table.options
        self.blob_target_file_size = CoreOptions.get_blob_target_file_size(options)
        
        # Current writer state (aligned with RollingFileWriterImpl)
        self.current_writer: Optional[BlobFileWriter] = None
        self.current_file_path: Optional[Path] = None
        self.record_count = 0

        logger.info(f"Initialized BlobWriter with blob file format, blob_target_file_size={self.blob_target_file_size}")

    def write(self, data: pa.RecordBatch):
        """
        Write data row by row, checking file size after each write (aligned with Java RollingFileWriterImpl).
        """
        try:
            processed_data = self._process_data(data)
            
            if self.pending_data is None:
                self.pending_data = processed_data
            else:
                self.pending_data = self._merge_data(self.pending_data, processed_data)
            
            # In blob-as-descriptor mode, write row by row and check size after each write
            if self.blob_as_descriptor and self.pending_data is not None and self.pending_data.num_rows > 0:
                # Write each row individually, checking size after each write
                for i in range(self.pending_data.num_rows):
                    row_data = self.pending_data.slice(i, 1)
                    self._write_row(row_data)
                    self.record_count += 1
                    
                    # Check rolling condition (aligned with Java: check every CHECK_ROLLING_RECORD_CNT records)
                    if self._should_roll_file():
                        self._close_current_writer()
                self.pending_data = None
            else:
                # Normal mode: use parent class logic
                self._check_and_roll_if_needed()
        except Exception as e:
            logger.warning("Exception occurs when writing data. Cleaning up.", exc_info=e)
            self.abort()
            raise e

    def _write_row(self, row_data: pa.Table):
        """
        Write a single row to the current blob file (aligned with Java RollingFileWriterImpl.write).
        Opens a new file if needed.
        """
        if row_data.num_rows == 0:
            return
        
        # Open current writer if needed (aligned with Java: openCurrentWriter)
        if self.current_writer is None:
            self._open_current_writer()
        
        # Write the row
        self.current_writer.write_row(row_data)
    
    def _open_current_writer(self):
        """Open a new blob file writer (aligned with Java: openCurrentWriter)."""
        import uuid
        file_name = f"data-{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)
        self.current_file_path = file_path
        self.current_writer = BlobFileWriter(self.file_io, file_path, self.blob_as_descriptor)
    
    def _should_roll_file(self) -> bool:
        """
        Check if current file should be rolled (aligned with Java: rollingFile).
        Checks every CHECK_ROLLING_RECORD_CNT records or when reachTargetSize returns true.
        """
        if self.current_writer is None:
            return False
        
        # Check every CHECK_ROLLING_RECORD_CNT records (aligned with Java)
        force_check = (self.record_count % CHECK_ROLLING_RECORD_CNT == 0)
        return self.current_writer.reach_target_size(force_check, self.blob_target_file_size)
    
    def _close_current_writer(self):
        """
        Close current writer and create metadata (aligned with Java: closeCurrentWriter).
        """
        if self.current_writer is None:
            return
        
        # Close writer and get file size
        file_size = self.current_writer.close()
        file_name = self.current_file_path.name
        
        # Create metadata
        # Read the written data to get row count (we need to track this)
        # For now, we'll use the record count from the writer
        row_count = self.current_writer.row_count
        
        # Create file metadata
        self._add_file_metadata(file_name, self.current_file_path, row_count, file_size)
        
        # Reset current writer
        self.current_writer = None
        self.current_file_path = None
    
    def _write_data_to_file(self, data):
        """Override for normal mode (non-blob-as-descriptor)."""
        if data.num_rows == 0:
            return
        
        # In normal mode (non-blob-as-descriptor), use parent class logic
        # But we need to handle blob format specifically
        import uuid
        file_name = f"data-{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)
        
        # Write blob file (normal mode, no rolling)
        self.file_io.write_blob(file_path, data, self.blob_as_descriptor)
        
        file_size = self.file_io.get_file_size(file_path)
        
        # Create metadata using parent class logic (but adapted for blob)
        from datetime import datetime
        from pypaimon.manifest.schema.data_file_meta import DataFileMeta
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.schema.data_types import PyarrowFieldParser
        
        # Get column stats
        data_fields = PyarrowFieldParser.to_paimon_schema(data.schema)
        column_stats = {
            field.name: self._get_column_stats(data.to_batches()[0], field.name)
            for field in data_fields
        }
        min_value_stats = [column_stats[field.name]['min_values'] for field in data_fields]
        max_value_stats = [column_stats[field.name]['max_values'] for field in data_fields]
        value_null_counts = [column_stats[field.name]['null_counts'] for field in data_fields]
        
        min_seq = self.sequence_generator.start
        max_seq = self.sequence_generator.current
        self.sequence_generator.start = self.sequence_generator.current
        
        self.committed_files.append(DataFileMeta(
            file_name=file_name,
            file_size=file_size,
            row_count=data.num_rows,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats(GenericRow([], []), GenericRow([], []), []),
            value_stats=SimpleStats(
                GenericRow(min_value_stats, data_fields),
                GenericRow(max_value_stats, data_fields),
                value_null_counts),
            min_sequence_number=min_seq,
            max_sequence_number=max_seq,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=datetime.now(),
            delete_row_count=0,
            file_source="APPEND",
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=self.write_cols,
            file_path=str(file_path),
        ))

    def _add_file_metadata(self, file_name: str, file_path: Path, data_or_row_count, file_size: int):
        """Add file metadata to committed_files."""
        from datetime import datetime
        from pypaimon.manifest.schema.data_file_meta import DataFileMeta
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.schema.data_types import PyarrowFieldParser

        # Handle both Table and row_count
        if isinstance(data_or_row_count, pa.Table):
            data = data_or_row_count
            row_count = data.num_rows
            data_fields = PyarrowFieldParser.to_paimon_schema(data.schema)
            column_stats = {
                field.name: self._get_column_stats(data.to_batches()[0], field.name)
                for field in data_fields
            }
            min_value_stats = [column_stats[field.name]['min_values'] for field in data_fields]
            max_value_stats = [column_stats[field.name]['max_values'] for field in data_fields]
            value_null_counts = [column_stats[field.name]['null_counts'] for field in data_fields]
        else:
            # row_count only (from BlobFileWriter)
            row_count = data_or_row_count
            # For blob files, we don't have stats
            data_fields = [PyarrowFieldParser.to_paimon_schema(pa.schema([('blob_data', pa.large_binary())]))[0]]
            min_value_stats = [None]
            max_value_stats = [None]
            value_null_counts = [0]

        min_seq = self.sequence_generator.start
        max_seq = self.sequence_generator.current
        self.sequence_generator.start = self.sequence_generator.current
        
        self.committed_files.append(DataFileMeta(
            file_name=file_name,
            file_size=file_size,
            row_count=row_count,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats(GenericRow([], []), GenericRow([], []), []),
            value_stats=SimpleStats(
                GenericRow(min_value_stats, data_fields),
                GenericRow(max_value_stats, data_fields),
                value_null_counts),
            min_sequence_number=min_seq,
            max_sequence_number=max_seq,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=datetime.now(),
            delete_row_count=0,
            file_source="APPEND",
            value_stats_cols=None,
            external_path=None,
            first_row_id=None,
            write_cols=self.write_cols,
            file_path=str(file_path),
        ))
    
    def prepare_commit(self):
        """Prepare commit, ensuring all data is written."""
        # In blob-as-descriptor mode, close current writer if open
        if self.current_writer is not None:
            self._close_current_writer()
        
        # In normal mode, ensure pending_data is written
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            if not self.blob_as_descriptor:
                self._write_data_to_file(self.pending_data)
                self.pending_data = None
        
        return self.committed_files.copy()
    
    def close(self):
        """Close current writer if open (aligned with Java: close)."""
        # Close blob-as-descriptor writer if open
        if self.current_writer is not None:
            self._close_current_writer()
        
        # In normal mode, ensure pending_data is written
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            if not self.blob_as_descriptor:
                # Normal mode: write pending data
                self._write_data_to_file(self.pending_data)
                self.pending_data = None
        
        # Call parent close to handle any remaining logic
        super().close()
    
    def abort(self):
        """Abort current writer if open (aligned with Java: abort)."""
        if self.current_writer is not None:
            try:
                self.current_writer.abort()
            except Exception as e:
                logger.warning(f"Error aborting blob writer: {e}", exc_info=e)
            self.current_writer = None
            self.current_file_path = None
        super().abort()

    @staticmethod
    def _get_column_stats(record_batch, column_name: str):
        column_array = record_batch.column(column_name)
        # For blob data, don't generate min/max values
        return {
            "min_values": None,
            "max_values": None,
            "null_counts": column_array.null_count,
        }
