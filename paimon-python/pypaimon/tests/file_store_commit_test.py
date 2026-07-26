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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from pypaimon.api.rest_exception import BadRequestException, RESTException
from pypaimon.catalog.catalog_exception import (
    TableNoPermissionException,
    TableNotExistException,
)
from pypaimon.common.identifier import Identifier
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit.conflict_detection import (
    CommitConflictError,
    RowIdPlanningConflictError,
)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import (
    CommitOutcomeUnknownError,
    FileStoreCommit,
    RetryResult,
    SuccessResult,
)


@patch('pypaimon.write.file_store_commit.ManifestFileManager')
@patch('pypaimon.write.file_store_commit.ManifestListManager')
class TestFileStoreCommit(unittest.TestCase):
    """Test cases for FileStoreCommit class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock table with required attributes
        self.mock_table = Mock()
        self.mock_table.partition_keys = ['dt', 'region']
        self.mock_table.current_branch.return_value = 'main'
        self.mock_table.table_path = '/test/table/path'
        self.mock_table.file_io = Mock()
        self.mock_table.options.manifest_target_size.return_value = 8 * 1024 * 1024
        self.mock_table.options.manifest_merge_min_count.return_value = 30

        # Mock snapshot commit
        self.mock_snapshot_commit = Mock()

    def _create_file_store_commit(self):
        """Helper method to create FileStoreCommit instance."""
        return FileStoreCommit(
            snapshot_commit=self.mock_snapshot_commit,
            table=self.mock_table,
            commit_user='test_user'
        )

    def _prepare_atomic_attempt(self, atomic_error=None, close_error=None):
        file_store_commit = self._create_file_store_commit()
        self.mock_table.identifier = 'default.test_table'
        self.mock_table.table_schema = Mock(id=7)
        self.mock_table.options.row_tracking_enabled.return_value = False

        snapshot_commit = MagicMock()
        snapshot_commit.__enter__.return_value = snapshot_commit
        snapshot_commit.__exit__.return_value = False
        snapshot_commit.__exit__.side_effect = close_error
        if atomic_error is None:
            snapshot_commit.commit.return_value = True
        else:
            snapshot_commit.commit.side_effect = atomic_error
        file_store_commit.snapshot_commit = snapshot_commit
        file_store_commit._write_manifest_files = Mock(return_value=[Mock()])
        file_store_commit._generate_partition_statistics = Mock(return_value=[])
        file_store_commit.manifest_list_manager.read_all.return_value = []

        commit_entry = Mock(kind=0)
        commit_entry.file = Mock(row_count=1)
        return file_store_commit, snapshot_commit, commit_entry

    def test_generate_partition_statistics_single_partition_single_file(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation with single partition and single file."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        # Create test data
        creation_time_dt = datetime(2024, 1, 15, 10, 30, 0)
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time = Timestamp.from_local_date_time(creation_time_dt)
        file_meta = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,  # 1MB
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        self.assertIsInstance(stat, PartitionStatistics)
        self.assertEqual(stat.spec, {'dt': '2024-01-15', 'region': 'us-east-1'})
        self.assertEqual(stat.record_count, 10000)
        self.assertEqual(stat.file_count, 1)
        self.assertEqual(stat.file_size_in_bytes, 1024 * 1024)
        expected_time = file_meta.creation_time_epoch_millis()
        self.assertEqual(stat.last_file_creation_time, expected_time)

    def test_generate_partition_statistics_multiple_files_same_partition(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation with multiple files in same partition."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time_1 = Timestamp.from_local_date_time(datetime(2024, 1, 15, 10, 30, 0))
        creation_time_2 = Timestamp.from_local_date_time(datetime(2024, 1, 15, 11, 30, 0))  # Later time

        file_meta_1 = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,  # 1MB
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time_1
        )

        file_meta_2 = DataFileMeta.create(
            file_name="test_file_2.parquet",
            file_size=2 * 1024 * 1024,  # 2MB
            row_count=15000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time_2
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta_1, file_meta_2]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        self.assertEqual(stat.spec, {'dt': '2024-01-15', 'region': 'us-east-1'})
        self.assertEqual(stat.record_count, 25000)  # 10000 + 15000
        self.assertEqual(stat.file_count, 2)
        self.assertEqual(stat.file_size_in_bytes, 3 * 1024 * 1024)  # 1MB + 2MB
        expected_time = file_meta_2.creation_time_epoch_millis()
        self.assertEqual(stat.last_file_creation_time, expected_time)

    def test_generate_partition_statistics_multiple_partitions(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation with multiple different partitions."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        creation_time_dt = datetime(2024, 1, 15, 10, 30, 0)
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time = Timestamp.from_local_date_time(creation_time_dt)

        # File for partition 1
        file_meta_1 = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        # File for partition 2
        file_meta_2 = DataFileMeta.create(
            file_name="test_file_2.parquet",
            file_size=2 * 1024 * 1024,
            row_count=20000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        commit_message_1 = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta_1]
        )

        commit_message_2 = CommitMessage(
            partition=('2024-01-15', 'us-west-2'),
            bucket=0,
            new_files=[file_meta_2]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(
            self._to_entries([commit_message_1, commit_message_2]))

        # Verify results
        self.assertEqual(len(statistics), 2)

        # Sort statistics by partition spec for consistent testing
        statistics.sort(key=lambda s: s.spec['region'])

        # Check first partition (us-east-1)
        stat_1 = statistics[0]
        self.assertEqual(stat_1.spec, {'dt': '2024-01-15', 'region': 'us-east-1'})
        self.assertEqual(stat_1.record_count, 10000)
        self.assertEqual(stat_1.file_count, 1)
        self.assertEqual(stat_1.file_size_in_bytes, 1024 * 1024)

        # Check second partition (us-west-2)
        stat_2 = statistics[1]
        self.assertEqual(stat_2.spec, {'dt': '2024-01-15', 'region': 'us-west-2'})
        self.assertEqual(stat_2.record_count, 20000)
        self.assertEqual(stat_2.file_count, 1)
        self.assertEqual(stat_2.file_size_in_bytes, 2 * 1024 * 1024)

    def test_generate_partition_statistics_unpartitioned_table(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation for unpartitioned table."""
        # Update mock table to have no partition keys
        self.mock_table.partition_keys = []

        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        creation_time_dt = datetime(2024, 1, 15, 10, 30, 0)
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time = Timestamp.from_local_date_time(creation_time_dt)
        file_meta = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        commit_message = CommitMessage(
            partition=(),  # Empty partition for unpartitioned table
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        self.assertEqual(stat.spec, {})  # Empty spec for unpartitioned table
        self.assertEqual(stat.record_count, 10000)
        self.assertEqual(stat.file_count, 1)
        self.assertEqual(stat.file_size_in_bytes, 1024 * 1024)

    def test_generate_partition_statistics_no_creation_time(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation when file has no creation time."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        file_meta = DataFileMeta(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=None,
            max_key=None,
            key_stats=None,
            value_stats=None,
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=None,
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        # Should have a valid timestamp (current time)
        self.assertGreater(stat.last_file_creation_time, 0)

    def test_generate_partition_statistics_mismatched_partition_keys(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation when partition tuple doesn't match partition keys."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        # Table has 2 partition keys but partition tuple has 3 values
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        file_meta = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=Timestamp.from_local_date_time(datetime(2024, 1, 15, 10, 30, 0))
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1', 'extra-value'),  # 3 values but table has 2 keys
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results - should fallback to index-based naming
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        expected_spec = {
            'partition_0': '2024-01-15',
            'partition_1': 'us-east-1',
            'partition_2': 'extra-value'
        }
        self.assertEqual(stat.spec, expected_spec)

    def test_generate_partition_statistics_empty_commit_messages(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        """Test partition statistics generation with empty commit messages list."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        # Test method
        statistics = file_store_commit._generate_partition_statistics([])

        # Verify results
        self.assertEqual(len(statistics), 0)

    def test_append_commit_inherits_index_manifest(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()

        self.mock_table.identifier = 'default.test_table'
        self.mock_table.table_schema = Mock()
        self.mock_table.table_schema.id = 7
        self.mock_table.options.row_tracking_enabled.return_value = False

        snapshot_commit = MagicMock()
        snapshot_commit.__enter__.return_value = snapshot_commit
        snapshot_commit.__exit__.return_value = False
        snapshot_commit.commit.return_value = True
        file_store_commit.snapshot_commit = snapshot_commit

        file_store_commit._write_manifest_files = Mock(return_value=[Mock()])
        file_store_commit._generate_partition_statistics = Mock(return_value=[])
        file_store_commit.manifest_list_manager.read_all.return_value = []

        latest_snapshot = Mock()
        latest_snapshot.id = 3
        latest_snapshot.total_record_count = 10
        latest_snapshot.index_manifest = "index-manifest-existing"

        commit_entry = Mock()
        commit_entry.kind = 0
        commit_entry.file = Mock()
        commit_entry.file.row_count = 2

        result = file_store_commit._try_commit_once(
            retry_result=None,
            commit_kind="APPEND",
            commit_entries=[commit_entry],
            changelog_entries=[],
            commit_identifier=11,
            latest_snapshot=latest_snapshot
        )

        self.assertTrue(result.is_success())
        self.assertEqual(
            "index-manifest-existing",
            snapshot_commit.commit.call_args[0][0].index_manifest
        )

    def test_atomic_exception_has_unknown_outcome(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 0
        file_store_commit.commit_timeout = 1000
        file_store_commit.snapshot_manager.get_latest_snapshot.return_value = None
        atomic_error = RuntimeError("atomic commit failed")
        file_store_commit._try_commit_once = Mock(return_value=RetryResult(
            None, atomic_error, outcome_unknown=True))

        with self.assertRaises(CommitOutcomeUnknownError) as raised:
            file_store_commit._try_commit(
                "APPEND", 1, lambda _snapshot: [Mock()])

        self.assertIs(atomic_error, raised.exception.__cause__)

    def test_atomic_commit_exception_marks_retry_unknown(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        self.mock_table.identifier = 'default.test_table'
        self.mock_table.table_schema = Mock(id=7)
        self.mock_table.options.row_tracking_enabled.return_value = False
        atomic_error = RuntimeError("atomic commit failed")
        snapshot_commit = MagicMock()
        snapshot_commit.__enter__.return_value = snapshot_commit
        snapshot_commit.__exit__.return_value = False
        snapshot_commit.commit.side_effect = atomic_error
        file_store_commit.snapshot_commit = snapshot_commit
        file_store_commit._write_manifest_files = Mock(return_value=[Mock()])
        file_store_commit._generate_partition_statistics = Mock(return_value=[])
        file_store_commit.manifest_list_manager.read_all.return_value = []
        commit_entry = Mock(kind=0)
        commit_entry.file = Mock(row_count=1)

        result = file_store_commit._try_commit_once(
            retry_result=None,
            commit_kind="APPEND",
            commit_entries=[commit_entry],
            changelog_entries=[],
            commit_identifier=1,
            latest_snapshot=None,
        )

        self.assertFalse(result.is_success())
        self.assertTrue(result.outcome_unknown)
        self.assertIs(atomic_error, result.exception)

    def test_deterministic_atomic_rejections_are_not_retried(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        identifier = Identifier.create("default", "test_table")
        wrapped_bad_request = RuntimeError("Failed to commit snapshot")
        wrapped_bad_request.__cause__ = BadRequestException("bad request")
        errors = [
            TableNotExistException(identifier),
            TableNoPermissionException(identifier),
            wrapped_bad_request,
        ]

        for atomic_error in errors:
            with self.subTest(error=type(atomic_error).__name__):
                file_store_commit, snapshot_commit, commit_entry = (
                    self._prepare_atomic_attempt(atomic_error=atomic_error)
                )
                file_store_commit.commit_max_retries = 3
                file_store_commit.commit_timeout = 10_000
                file_store_commit._commit_retry_wait = Mock()
                file_store_commit.snapshot_manager.get_latest_snapshot.return_value = None

                with self.assertRaises(type(atomic_error)) as raised:
                    file_store_commit._try_commit(
                        "APPEND", 1, lambda _snapshot: [commit_entry])

                self.assertIs(atomic_error, raised.exception)
                snapshot_commit.commit.assert_called_once()
                file_store_commit._commit_retry_wait.assert_not_called()

    def test_atomic_transport_exception_remains_unknown(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        transport_error = RESTException(
            "response lost", cause=TimeoutError("read timed out"))
        file_store_commit, _, commit_entry = self._prepare_atomic_attempt(
            atomic_error=transport_error)

        result = file_store_commit._try_commit_once(
            retry_result=None,
            commit_kind="APPEND",
            commit_entries=[commit_entry],
            changelog_entries=[],
            commit_identifier=1,
            latest_snapshot=None,
        )

        self.assertFalse(result.is_success())
        self.assertTrue(result.outcome_unknown)
        self.assertIs(transport_error, result.exception)

    def test_exception_after_atomic_call_returns_remains_unknown(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        close_error = TableNoPermissionException(
            Identifier.create("default", "test_table"))
        file_store_commit, _, commit_entry = self._prepare_atomic_attempt(
            close_error=close_error)

        result = file_store_commit._try_commit_once(
            retry_result=None,
            commit_kind="APPEND",
            commit_entries=[commit_entry],
            changelog_entries=[],
            commit_identifier=1,
            latest_snapshot=None,
        )

        self.assertFalse(result.is_success())
        self.assertTrue(result.outcome_unknown)
        self.assertIs(close_error, result.exception)

    def test_false_commit_has_deterministic_outcome(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 0
        file_store_commit.commit_timeout = 1000
        file_store_commit.snapshot_manager.get_latest_snapshot.return_value = None
        file_store_commit._try_commit_once = Mock(
            return_value=RetryResult(None))

        with self.assertRaises(CommitConflictError) as raised:
            file_store_commit._try_commit(
                "APPEND", 1, lambda _snapshot: [Mock()])

        self.assertNotIsInstance(
            raised.exception, CommitOutcomeUnknownError)

    def test_false_atomic_commit_cleans_attempt_manifests(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit, snapshot_commit, commit_entry = (
            self._prepare_atomic_attempt())
        snapshot_commit.commit.return_value = False
        file_store_commit._clean_up_reuse_tmp_manifests = Mock()
        file_store_commit._clean_up_no_reuse_tmp_manifests = Mock()

        result = file_store_commit._try_commit_once(
            retry_result=None,
            commit_kind="APPEND",
            commit_entries=[commit_entry],
            changelog_entries=[],
            commit_identifier=1,
            latest_snapshot=None,
        )

        self.assertFalse(result.is_success())
        self.assertFalse(result.outcome_unknown)
        file_store_commit._clean_up_reuse_tmp_manifests.assert_called_once()
        file_store_commit._clean_up_no_reuse_tmp_manifests.assert_called_once()

    def test_unknown_outcome_survives_later_failure(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 2
        file_store_commit.commit_timeout = 1000
        file_store_commit.snapshot_manager.get_latest_snapshot.return_value = None
        file_store_commit._commit_retry_wait = Mock()
        atomic_error = RuntimeError("atomic commit failed")
        file_store_commit._try_commit_once = Mock(side_effect=[
            RetryResult(None, atomic_error, outcome_unknown=True),
            RuntimeError("conflict"),
        ])

        with self.assertRaises(CommitOutcomeUnknownError) as raised:
            file_store_commit._try_commit(
                "APPEND", 1, lambda _snapshot: [Mock()])

        self.assertIs(atomic_error, raised.exception.__cause__)

    def test_atomic_failure_remains_unknown(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 0
        file_store_commit.commit_timeout = 1000
        file_store_commit.snapshot_manager.get_latest_snapshot.return_value = None
        atomic_error = RuntimeError("response lost")
        file_store_commit._try_commit_once = Mock(return_value=RetryResult(
            None, atomic_error, outcome_unknown=True))

        with self.assertRaises(CommitOutcomeUnknownError) as raised:
            file_store_commit._try_commit(
                "APPEND", 1, lambda _snapshot: [Mock()])

        self.assertIs(atomic_error, raised.exception.__cause__)

    def test_unknown_duplicate_resolves_to_success(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        atomic_error = RuntimeError("response lost")
        retry_result = RetryResult(
            None, atomic_error, outcome_unknown=True)
        latest_snapshot = Mock(
            id=1,
            commit_user="test_user",
            commit_identifier=1,
            commit_kind="APPEND",
        )
        file_store_commit.snapshot_manager.get_snapshot_by_id.return_value = (
            latest_snapshot
        )

        result = file_store_commit._try_commit_once(
            retry_result=retry_result,
            commit_kind="APPEND",
            commit_entries=[Mock()],
            changelog_entries=[],
            commit_identifier=1,
            latest_snapshot=latest_snapshot,
        )

        self.assertTrue(result.is_success())

    def test_unknown_retry_resolves_when_duplicate_is_found(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 1
        file_store_commit.commit_timeout = 1000
        file_store_commit._commit_retry_wait = Mock()
        file_store_commit.snapshot_manager.get_latest_snapshot.side_effect = [
            None,
            Mock(id=1),
        ]
        atomic_error = RuntimeError("response lost")
        file_store_commit._try_commit_once = Mock(side_effect=[
            RetryResult(None, atomic_error, outcome_unknown=True),
            SuccessResult(),
        ])

        file_store_commit._try_commit(
            "APPEND", 1, lambda _snapshot: [Mock()])

        self.assertEqual(2, file_store_commit._try_commit_once.call_count)

    def test_unknown_retry_resolves_duplicate_before_replanning(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 1
        file_store_commit.commit_timeout = 1000
        file_store_commit._commit_retry_wait = Mock()
        committed_snapshot = Mock(
            id=1,
            commit_user="test_user",
            commit_identifier=1,
            commit_kind="OVERWRITE",
        )
        file_store_commit.snapshot_manager.get_latest_snapshot.side_effect = [
            None,
            committed_snapshot,
        ]
        file_store_commit.snapshot_manager.get_snapshot_by_id.return_value = (
            committed_snapshot
        )
        atomic_error = RuntimeError("response lost")
        file_store_commit._try_commit_once = Mock(return_value=RetryResult(
            None, atomic_error, outcome_unknown=True))
        commit_entries_plan = Mock(side_effect=[[Mock()], []])

        file_store_commit._try_commit(
            "OVERWRITE", 1, commit_entries_plan)

        self.assertEqual(1, file_store_commit._try_commit_once.call_count)
        self.assertEqual(1, commit_entries_plan.call_count)
        get_snapshot_by_id = (
            file_store_commit.snapshot_manager.get_snapshot_by_id
        )
        get_snapshot_by_id.assert_called_once_with(1)

    def test_unknown_retry_with_empty_plan_and_no_duplicate_remains_unknown(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit.commit_max_retries = 1
        file_store_commit.commit_timeout = 1000
        file_store_commit._commit_retry_wait = Mock()
        other_snapshot = Mock(
            id=1,
            commit_user="other_user",
            commit_identifier=1,
            commit_kind="OVERWRITE",
        )
        file_store_commit.snapshot_manager.get_latest_snapshot.side_effect = [
            None,
            other_snapshot,
        ]
        file_store_commit.snapshot_manager.get_snapshot_by_id.return_value = (
            other_snapshot
        )
        atomic_error = RuntimeError("response lost")
        file_store_commit._try_commit_once = Mock(return_value=RetryResult(
            None, atomic_error, outcome_unknown=True))
        commit_entries_plan = Mock(side_effect=[[Mock()], []])

        with self.assertRaises(CommitOutcomeUnknownError) as raised:
            file_store_commit._try_commit(
                "OVERWRITE", 1, commit_entries_plan)

        self.assertIs(atomic_error, raised.exception.__cause__)
        self.assertEqual(1, file_store_commit._try_commit_once.call_count)
        self.assertEqual(2, commit_entries_plan.call_count)

    def test_empty_plan_without_retry_remains_no_op(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        file_store_commit._try_commit_once = Mock()
        file_store_commit._is_duplicate_commit = Mock()

        file_store_commit._try_commit(
            "OVERWRITE", 1, lambda _snapshot: [])

        file_store_commit._try_commit_once.assert_not_called()
        file_store_commit._is_duplicate_commit.assert_not_called()

    def test_collects_row_id_window_base_entries(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        self.mock_table.partition_keys_fields = []
        self.mock_table.total_buckets = 1
        base_file = Mock(
            file_name="base.parquet",
            level=0,
            extra_files=[],
            embedded_index=None,
            external_path=None,
        )
        identity = (7, "user", 1, "APPEND", 1, "base", "delta", None, None)
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[Mock()],
            check_from_snapshot=7,
            row_id_base_files=[base_file],
            row_id_base_snapshot_identity=identity,
        )

        entries, snapshot = file_store_commit._collect_row_id_base_entries(
            [message])

        self.assertEqual(identity, snapshot)
        self.assertEqual([base_file], [entry.file for entry in entries])

    def test_first_row_id_planning_conflict_is_safe_to_abort(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        conflict = RowIdPlanningConflictError("row ID planning conflict")
        file_store_commit.conflict_detection.has_row_id_check_from_snapshot = (
            Mock(return_value=True))
        file_store_commit.conflict_detection.read_row_id_base_entries = Mock(
            side_effect=conflict)
        clear_window = Mock()
        file_store_commit.conflict_detection.clear_row_id_window_changes = (
            clear_window)

        with self.assertRaises(CommitConflictError) as raised:
            file_store_commit._try_commit_once(
                retry_result=None,
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                detect_conflicts=True,
            )

        self.assertIs(conflict, raised.exception.__cause__)
        clear_window.assert_called_once()

    def test_retried_row_id_planning_conflict_preserves_error(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        conflict = RowIdPlanningConflictError("row ID planning conflict")
        file_store_commit.conflict_detection.has_row_id_check_from_snapshot = (
            Mock(return_value=True))
        file_store_commit.conflict_detection.read_row_id_base_entries = Mock(
            side_effect=conflict)
        clear_window = Mock()
        file_store_commit.conflict_detection.clear_row_id_window_changes = (
            clear_window)

        with self.assertRaises(RuntimeError) as raised:
            file_store_commit._try_commit_once(
                retry_result=RetryResult(Mock(id=0), outcome_unknown=True),
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                detect_conflicts=True,
            )

        self.assertIs(conflict, raised.exception)
        self.assertNotIsInstance(raised.exception, CommitConflictError)
        clear_window.assert_called_once()

    def test_false_retry_row_id_planning_conflict_is_safe_to_abort(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        conflict = RowIdPlanningConflictError("row ID planning conflict")
        file_store_commit.conflict_detection.has_row_id_check_from_snapshot = (
            Mock(return_value=True))
        file_store_commit.conflict_detection.read_row_id_base_entries = Mock(
            side_effect=conflict)

        with self.assertRaises(CommitConflictError) as raised:
            file_store_commit._try_commit_once(
                retry_result=RetryResult(Mock(id=0), outcome_unknown=False),
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                detect_conflicts=True,
            )

        self.assertIs(conflict, raised.exception.__cause__)

    def test_cumulative_unknown_preserves_row_id_planning_conflict(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        conflict = RowIdPlanningConflictError("row ID planning conflict")
        file_store_commit.conflict_detection.has_row_id_check_from_snapshot = (
            Mock(return_value=True))
        file_store_commit.conflict_detection.read_row_id_base_entries = Mock(
            side_effect=conflict)

        with self.assertRaises(RowIdPlanningConflictError) as raised:
            file_store_commit._try_commit_once(
                retry_result=RetryResult(Mock(id=0), outcome_unknown=False),
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                detect_conflicts=True,
                previous_outcome_unknown=True,
            )

        self.assertIs(conflict, raised.exception)

    def test_false_retry_generic_conflict_is_safe_to_abort(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        conflict = RuntimeError("commit conflict")
        file_store_commit.conflict_detection.has_row_id_check_from_snapshot = (
            Mock(return_value=False))
        file_store_commit.commit_scanner.read_incremental_changes = Mock(
            return_value=[])
        file_store_commit.conflict_detection.check_conflicts = Mock(
            return_value=conflict)

        with self.assertRaises(CommitConflictError) as raised:
            file_store_commit._try_commit_once(
                retry_result=RetryResult(
                    Mock(id=0), base_data_files=[], outcome_unknown=False),
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                detect_conflicts=True,
            )

        self.assertIs(conflict, raised.exception.__cause__)

    def test_false_retry_hash_conflict_is_safe_to_abort(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()

        with self.assertRaises(CommitConflictError) as raised:
            file_store_commit._try_commit_once(
                retry_result=RetryResult(Mock(id=0), outcome_unknown=False),
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                hash_index_base_snapshot=0,
            )

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)

    def test_row_id_planning_io_error_preserves_error(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        error = OSError("row ID planning failed")
        file_store_commit.conflict_detection.has_row_id_check_from_snapshot = (
            Mock(return_value=True))
        file_store_commit.conflict_detection.read_row_id_base_entries = Mock(
            side_effect=error)
        clear_window = Mock()
        file_store_commit.conflict_detection.clear_row_id_window_changes = (
            clear_window)

        with self.assertRaises(OSError) as raised:
            file_store_commit._try_commit_once(
                retry_result=None,
                commit_kind="APPEND",
                commit_entries=[Mock()],
                changelog_entries=[],
                commit_identifier=1,
                latest_snapshot=Mock(id=1),
                detect_conflicts=True,
            )

        self.assertIs(error, raised.exception)
        clear_window.assert_called_once()

    def test_mixed_messages_keep_row_id_base_evidence(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        self.mock_table.partition_keys_fields = []
        self.mock_table.total_buckets = 1
        identity = (7, "user", 1, "APPEND", 1, "base", "delta", None, None)
        base_file = Mock(
            file_name="base.parquet",
            level=0,
            extra_files=[],
            embedded_index=None,
            external_path=None,
        )
        complete = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[Mock()],
            check_from_snapshot=7,
            row_id_base_files=[base_file],
            row_id_base_snapshot_identity=identity,
        )
        legacy = CommitMessage(
            partition=(), bucket=0, new_files=[Mock()])

        entries, snapshot = file_store_commit._collect_row_id_base_entries(
            [complete, legacy])

        self.assertEqual(identity, snapshot)
        self.assertEqual([base_file], [entry.file for entry in entries])

    def test_index_only_message_keeps_row_id_base_evidence(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        self.mock_table.partition_keys_fields = []
        self.mock_table.total_buckets = 1
        identity = (7, "user", 1, "APPEND", 1, "base", "delta", None, None)
        base_file = Mock(
            file_name="base.parquet",
            level=0,
            extra_files=[],
            embedded_index=None,
            external_path=None,
        )
        update = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[Mock()],
            check_from_snapshot=7,
            row_id_base_files=[base_file],
            row_id_base_snapshot_identity=identity,
        )
        row_delete = CommitMessage(
            partition=("other",),
            bucket=0,
            new_files=[],
            check_from_snapshot=7,
            index_adds=[Mock()],
        )

        entries, snapshot = file_store_commit._collect_row_id_base_entries(
            [update, row_delete])

        self.assertEqual(identity, snapshot)
        self.assertEqual([base_file], [entry.file for entry in entries])

    def test_incomplete_row_id_base_evidence_falls_back(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        message = CommitMessage(
            partition=(), bucket=0, new_files=[Mock()],
            check_from_snapshot=7)

        self.assertEqual(
            (None, None),
            file_store_commit._collect_row_id_base_entries([message]),
        )

    def test_abort_does_not_delete_row_id_base_files(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        file_store_commit = self._create_file_store_commit()
        new_file = Mock(external_path="/new", file_path=None)
        base_file = Mock(external_path="/base", file_path=None)
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[new_file],
            row_id_base_files=[base_file],
        )

        file_store_commit.abort([message])

        self.mock_table.file_io.delete_quietly.assert_called_once_with("/new")

    def test_null_partition_value(
            self, mock_manifest_list_manager, mock_manifest_file_manager):
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        from pypaimon.schema.data_types import DataField, AtomicType

        file_store_commit = self._create_file_store_commit()
        self.mock_table.partition_keys = ['dt']
        self.mock_table.partition_keys_fields = [
            DataField(0, 'dt', AtomicType('STRING'))
        ]
        self.mock_table.table_schema = Mock()
        self.mock_table.table_schema.id = 0

        file_store_commit.manifest_file_manager = Mock()
        file_store_commit.manifest_file_manager.manifest_path = '/test/manifest'
        self.mock_table.file_io.get_file_size.return_value = 1024

        creation_time = Timestamp.from_local_date_time(datetime(2024, 1, 15, 10, 30, 0))

        def make_file(name):
            return DataFileMeta.create(
                file_name=name,
                file_size=1024,
                row_count=100,
                min_key=GenericRow([], []),
                max_key=GenericRow([], []),
                key_stats=SimpleStats.empty_stats(),
                value_stats=SimpleStats.empty_stats(),
                min_sequence_number=1,
                max_sequence_number=10,
                schema_id=0,
                level=0,
                extra_files=[],
                creation_time=creation_time,
            )

        entries = [
            ManifestEntry(kind=0, partition=GenericRow([None], None), bucket=0, total_buckets=None,
                          file=make_file("f1.parquet")),
            ManifestEntry(kind=0, partition=GenericRow(['2024-01-15'], None), bucket=0, total_buckets=None,
                          file=make_file("f2.parquet")),
        ]

        result = file_store_commit._write_manifest_files(entries, "manifest-test")
        self.assertIsNotNone(result)

    @staticmethod
    def _to_entries(commit_messages):
        commit_entries = []
        for msg in commit_messages:
            partition = GenericRow(list(msg.partition), None)
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=None,
                    file=file
                ))
        return commit_entries
