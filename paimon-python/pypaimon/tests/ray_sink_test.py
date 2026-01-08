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

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
from ray.data._internal.execution.interfaces import TaskContext

from pypaimon import CatalogFactory, Schema
from pypaimon.write.ray_datasink import PaimonDatasink
from pypaimon.write.commit_message import CommitMessage


class RaySinkTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse_path = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(self.warehouse_path, exist_ok=True)

        catalog_options = {
            "warehouse": self.warehouse_path
        }
        self.catalog = CatalogFactory.create(catalog_options)
        self.catalog.create_database("test_db", ignore_if_exists=True)

        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.float64())
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema=pa_schema,
            partition_keys=None,
            primary_keys=['id'],
            options={'bucket': '2'},  # Use fixed bucket mode for testing
            comment='test table'
        )

        self.table_identifier = "test_db.test_table"
        self.catalog.create_table(self.table_identifier, schema, ignore_if_exists=False)
        self.table = self.catalog.get_table(self.table_identifier)

    def tearDown(self):
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_init_and_serialization(self):
        """Test initialization, serialization, and table name."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        self.assertEqual(datasink.table, self.table)
        self.assertFalse(datasink.overwrite)
        self.assertIsNone(datasink._writer_builder)
        self.assertEqual(datasink._get_table_name(), "test_db.test_table")

        datasink_overwrite = PaimonDatasink(self.table, overwrite=True)
        self.assertTrue(datasink_overwrite.overwrite)

        # Test serialization
        datasink._writer_builder = Mock()
        state = datasink.__getstate__()
        self.assertIn('table', state)
        self.assertIn('overwrite', state)
        self.assertIn('_writer_builder', state)

        new_datasink = PaimonDatasink.__new__(PaimonDatasink)
        new_datasink.__setstate__(state)
        self.assertEqual(new_datasink.table, self.table)
        self.assertFalse(new_datasink.overwrite)

    def test_on_write_start(self):
        """Test on_write_start with normal and overwrite modes."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        self.assertIsNotNone(datasink._writer_builder)
        self.assertFalse(datasink._writer_builder.static_partition)

        datasink_overwrite = PaimonDatasink(self.table, overwrite=True)
        datasink_overwrite.on_write_start()
        self.assertIsNotNone(datasink_overwrite._writer_builder.static_partition)

    def test_write_blocks(self):
        """Test write method with empty, single, and multiple blocks."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        ctx = Mock(spec=TaskContext)

        # Test empty block
        empty_table = pa.table({
            'id': pa.array([], type=pa.int64()),
            'name': pa.array([], type=pa.string()),
            'value': pa.array([], type=pa.float64())
        })
        result = datasink.write([empty_table], ctx)
        self.assertEqual(result, [])

        # Test single block
        single_block = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [1.1, 2.2, 3.3]
        })
        result = datasink.write([single_block], ctx)
        self.assertIsInstance(result, list)
        if result:
            self.assertTrue(all(isinstance(msg, CommitMessage) for msg in result))

        # Test multiple blocks
        block1 = pa.table({
            'id': [4, 5],
            'name': ['David', 'Eve'],
            'value': [4.4, 5.5]
        })
        block2 = pa.table({
            'id': [6, 7],
            'name': ['Frank', 'Grace'],
            'value': [6.6, 7.7]
        })
        result = datasink.write([block1, block2], ctx)
        self.assertIsInstance(result, list)
        if result:
            self.assertTrue(all(isinstance(msg, CommitMessage) for msg in result))

    def test_write_creates_writer_builder_on_worker(self):
        """Test that write method creates WriteBuilder on worker (not using driver's builder)."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.prepare_commit.return_value = []
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            ctx = Mock(spec=TaskContext)

            datasink.write([data_table], ctx)

            mock_builder.assert_called_once()

    def test_on_write_complete(self):
        """Test on_write_complete with empty messages, normal messages, and filtering."""
        from ray.data.datasource.datasink import WriteResult

        # Test empty messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[], []]  # Empty commit messages from workers
        )
        datasink.on_write_complete(write_result)

        # Test with messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False
        commit_msg2 = Mock(spec=CommitMessage)
        commit_msg2.is_empty.return_value = False

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1], [commit_msg2]]
        )

        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        datasink.on_write_complete(write_result)

        mock_commit.commit.assert_called_once()
        commit_args = mock_commit.commit.call_args[0][0]
        self.assertEqual(len(commit_args), 2)

        # Test filtering empty messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        empty_msg = Mock(spec=CommitMessage)
        empty_msg.is_empty.return_value = True
        non_empty_msg = Mock(spec=CommitMessage)
        non_empty_msg.is_empty.return_value = False

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[empty_msg], [non_empty_msg]]
        )

        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        datasink.on_write_complete(write_result)

        mock_commit.commit.assert_called_once()
        commit_args = mock_commit.commit.call_args[0][0]
        self.assertEqual(len(commit_args), 1)
        self.assertEqual(commit_args[0], non_empty_msg)

    def test_error_handling(self):
        """Test error handling in write method and on_write_failed."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()

        # Test write error handling
        invalid_table = pa.table({
            'wrong_column': [1, 2, 3]  # Wrong schema
        })
        ctx = Mock(spec=TaskContext)

        with self.assertRaises(Exception):
            datasink.write([invalid_table], ctx)

        # Test that table_write is closed on error
        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.write_arrow.side_effect = Exception("Write error")
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })

            with self.assertRaises(Exception):
                datasink.write([data_table], ctx)

            mock_write.close.assert_called_once()

        # Test on_write_failed
        datasink = PaimonDatasink(self.table, overwrite=False)
        error = Exception("Test error")
        datasink.on_write_failed(error)  # Should not raise exception


if __name__ == '__main__':
    unittest.main()

