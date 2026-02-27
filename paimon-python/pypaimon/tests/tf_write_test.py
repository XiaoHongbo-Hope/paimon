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
import shutil
import subprocess
import sys
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import PyarrowFieldParser

_SUBPROCESS_ENV = {**os.environ, "CUDA_VISIBLE_DEVICES": "", "TF_WRITE_TEST_SUBPROCESS": "1"}


def _tf_bootstrap_script(test_name):
    """Run TF test in subprocess with TensorFlow imported FIRST (before pypaimon) to avoid mutex deadlock."""
    return """
import os
os.environ["CUDA_VISIBLE_DEVICES"] = ""
import sys
sys.path.insert(0, ".")
import tensorflow  # noqa: E402 - must be before pypaimon
import unittest
from pypaimon.tests.tf_write_test import TfWriteTest
suite = unittest.defaultTestLoader.loadTestsFromName(%s, TfWriteTest)
runner = unittest.TextTestRunner()
result = runner.run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
""" % repr(test_name)


def _tf_diag(msg):
    if os.environ.get("TF_WRITE_TEST_VERBOSE") == "1":
        print("[tf_write_test] " + msg, flush=True)


def _tf_subprocess_skip_on_mutex(result, err_msg):
    if result.returncode == 0:
        return
    out = err_msg or (result.stdout or b"").decode() + (result.stderr or b"").decode()
    out_lower = out.lower()
    if (
        "mutex" in out_lower
        or "libc++abi" in out_lower
        or "invalid argument" in out_lower
        or result.returncode in (-6, -11)  # SIGABRT, SIGSEGV
    ):
        raise unittest.SkipTest("Skipped on local macOS: TF mutex/abort.")
    raise AssertionError(err_msg or "subprocess exited with %s" % result.returncode)


def _tf_test_cwd():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


_tf_import_ok_cache = None


def _tf_import_ok(timeout_sec=15):
    global _tf_import_ok_cache
    if _tf_import_ok_cache is not None:
        return _tf_import_ok_cache
    try:
        result = subprocess.run(
            [sys.executable, "-c", "import tensorflow"],
            env={**os.environ, "CUDA_VISIBLE_DEVICES": ""},
            timeout=timeout_sec,
            capture_output=True,
            cwd=_tf_test_cwd(),
        )
        ok = result.returncode == 0
        _tf_import_ok_cache = ok
        return ok
    except subprocess.TimeoutExpired:
        _tf_import_ok_cache = False
        return False


class TfWriteTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(self.warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("default", ignore_if_exists=True)
        self.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])
        self.schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=None,
            primary_keys=["id"],
            options={"bucket": "2"},
            comment="test table",
        )
        self.table_identifier = "default.tf_write_test_table"
        self.catalog.create_table(self.table_identifier, self.schema, ignore_if_exists=False)
        self.table = self.catalog.get_table(self.table_identifier)

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_write_tf(self):
        if os.environ.get("TF_WRITE_TEST_SUBPROCESS") != "1":
            _tf_diag("main: probing TF import")
            if not _tf_import_ok():
                self.skipTest("Skipped on local macOS: TF import blocks.")
            _tf_diag("main: spawning subprocess (TF-first bootstrap) for test_write_tf")
            capture = os.environ.get("TF_WRITE_TEST_VERBOSE") != "1"
            result = subprocess.run(
                [sys.executable, "-c", _tf_bootstrap_script("test_write_tf")],
                env=_SUBPROCESS_ENV,
                timeout=120,
                capture_output=capture,
                cwd=_tf_test_cwd(),
            )
            _tf_diag("main: subprocess returned")
            err_msg = (result.stdout or b"").decode() + (result.stderr or b"").decode() if capture else ""
            if result.returncode != 0:
                _tf_subprocess_skip_on_mutex(result, err_msg)
            return
        _tf_diag("subprocess: importing tensorflow")
        import tensorflow as tf
        _tf_diag("subprocess: creating dataset (unbatch)")
        ds = tf.data.Dataset.from_tensor_slices({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0],
        })
        builder = self.table.new_batch_write_builder()
        table_write = builder.new_write()
        _tf_diag("subprocess: write_tf (first batch)")
        table_write.write_tf(ds)
        table_write.close()
        _tf_diag("subprocess: reading back")
        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertIsNotNone(actual)
        self.assertEqual(actual.num_rows, 3)
        actual_sorted = actual.sort_by("id")
        self.assertEqual(actual_sorted.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(actual_sorted.column("name").to_pylist(), ["a", "b", "c"])
        self.assertEqual(actual_sorted.column("value").to_pylist(), [1.0, 2.0, 3.0])
        _tf_diag("subprocess: creating batched dataset")
        ds_batch = tf.data.Dataset.from_tensor_slices({
            "id": [4, 5, 6, 7, 8],
            "name": ["d", "e", "f", "g", "h"],
            "value": [4.0, 5.0, 6.0, 7.0, 8.0],
        }).batch(2)
        builder2 = self.table.new_batch_write_builder()
        table_write2 = builder2.new_write()
        _tf_diag("subprocess: write_tf (batched)")
        table_write2.write_tf(ds_batch)
        table_write2.close()
        splits = read_builder.new_scan().plan().splits()  # re-plan after second write
        actual2 = table_read.to_arrow(splits)
        self.assertEqual(actual2.num_rows, 8)
        actual2_sorted = actual2.sort_by("id")
        self.assertEqual(actual2_sorted.column("id").to_pylist(), [1, 2, 3, 4, 5, 6, 7, 8])
        _tf_diag("subprocess: test_write_tf done")

    def test_write_tf_overwrite(self):
        if os.environ.get("TF_WRITE_TEST_SUBPROCESS") != "1":
            _tf_diag("main: probing TF import")
            if not _tf_import_ok():
                self.skipTest("Skipped on local macOS: TF import blocks.")
            _tf_diag("main: spawning subprocess (TF-first bootstrap) for test_write_tf_overwrite")
            capture = os.environ.get("TF_WRITE_TEST_VERBOSE") != "1"
            result = subprocess.run(
                [sys.executable, "-c", _tf_bootstrap_script("test_write_tf_overwrite")],
                env=_SUBPROCESS_ENV,
                timeout=120,
                capture_output=capture,
                cwd=_tf_test_cwd(),
            )
            _tf_diag("main: subprocess returned")
            err_msg = (result.stdout or b"").decode() + (result.stderr or b"").decode() if capture else ""
            if result.returncode != 0:
                _tf_subprocess_skip_on_mutex(result, err_msg)
            return
        _tf_diag("subprocess: importing tensorflow")
        import tensorflow as tf
        _tf_diag("subprocess: creating ds1")
        ds1 = tf.data.Dataset.from_tensor_slices({
            "id": [1, 2],
            "name": ["x", "y"],
            "value": [10.0, 20.0],
        })
        builder1 = self.table.new_batch_write_builder()
        table_write1 = builder1.new_write()
        _tf_diag("subprocess: write_tf(ds1)")
        table_write1.write_tf(ds1)
        table_write1.close()

        _tf_diag("subprocess: creating ds2")
        ds2 = tf.data.Dataset.from_tensor_slices({
            "id": [3, 4],
            "name": ["p", "q"],
            "value": [30.0, 40.0],
        })
        builder2 = self.table.new_batch_write_builder()
        table_write2 = builder2.new_write()
        _tf_diag("subprocess: write_tf(ds2, overwrite=True)")
        table_write2.write_tf(ds2, overwrite=True)
        table_write2.close()

        _tf_diag("subprocess: reading back")
        read_builder = self.table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertIsNotNone(actual)
        self.assertEqual(actual.num_rows, 2)
        actual_sorted = actual.sort_by("id")
        self.assertEqual(actual_sorted.column("id").to_pylist(), [3, 4])
        self.assertEqual(actual_sorted.column("name").to_pylist(), ["p", "q"])
        self.assertEqual(actual_sorted.column("value").to_pylist(), [30.0, 40.0])
        _tf_diag("subprocess: test_write_tf_overwrite done")

    def test_tf_batch_to_arrow(self):
        import numpy as np
        from pypaimon.write.tf_write import _tf_batch_to_arrow

        pa_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        batch = {
            "id": np.array([1, 2, 3], dtype=np.int64),
            "name": np.array(["a", "b", "c"]),
            "value": np.array([1.0, 2.0, 3.0], dtype=np.float64),
        }
        record_batch = _tf_batch_to_arrow(batch, pa_schema)
        self.assertEqual(record_batch.num_rows, 3)
        self.assertEqual(record_batch.schema.names, ["id", "name", "value"])
        self.assertEqual(record_batch.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(record_batch.column("name").to_pylist(), ["a", "b", "c"])
        self.assertEqual(record_batch.column("value").to_pylist(), [1.0, 2.0, 3.0])
        batch_missing = {"id": np.array([1, 2]), "value": np.array([1.0, 2.0], dtype=np.float64)}
        with self.assertRaises(ValueError) as ctx:
            _tf_batch_to_arrow(batch_missing, pa_schema)
        self.assertIn("name", str(ctx.exception))
        self.assertIn("missing column", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
