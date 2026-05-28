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
import tempfile
import unittest

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import merge_paimon


class RayMergeIntoTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        try:
            shutil.rmtree(cls.tempdir)
        except OSError:
            pass

    def _make_pk_table(self, name: str, extra_options=None):
        pa_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            ("name", pa.string()),
            ("value", pa.int64()),
        ])
        options = {"bucket": "2"}
        if extra_options:
            options.update(extra_options)
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=["id"], options=options
        )
        full = f"default.{name}"
        self.catalog.create_table(full, schema, False)
        return full, pa_schema

    def _write(self, full_name, data: pa.Table):
        table = self.catalog.get_table(full_name)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

    def _read_sorted(self, full_name):
        table = self.catalog.get_table(full_name)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        return result.sort_by("id").to_pydict()

    def test_basic_upsert_star_set(self):
        full, pa_schema = self._make_pk_table("merge_basic")
        self._write(full, pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }, schema=pa_schema))

        source = pa.Table.from_pydict({
            "id": [2, 3, 4],
            "name": ["Bob-Updated", "Charlie-Updated", "Dan"],
            "value": [250, 350, 400],
        }, schema=pa_schema)

        merge_paimon(
            target=full,
            source=source,
            catalog_options=self.catalog_options,
            on=["id"],
            when_matched_update="*",
            when_not_matched_insert="*",
        )

        out = self._read_sorted(full)
        self.assertEqual(out["id"], [1, 2, 3, 4])
        self.assertEqual(out["name"], ["Alice", "Bob-Updated", "Charlie-Updated", "Dan"])
        self.assertEqual(out["value"], [100, 250, 350, 400])

    def test_set_dict_with_literal_and_source_ref(self):
        full, pa_schema = self._make_pk_table("merge_set_dict")
        self._write(full, pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "value": [10, 20],
        }, schema=pa_schema))

        source = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["A-new", "B-new"],
            "value": pa.array([99, 99], type=pa.int64()),
        })

        merge_paimon(
            target=full,
            source=source,
            catalog_options=self.catalog_options,
            on=["id"],
            when_matched_update={"name": "s.name", "value": 777},
        )

        out = self._read_sorted(full)
        self.assertEqual(out["id"], [1, 2])
        self.assertEqual(out["name"], ["A-new", "B-new"])
        self.assertEqual(out["value"], [777, 777])

    def test_matched_update_condition(self):
        full, pa_schema = self._make_pk_table("merge_condition")
        self._write(full, pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["Old1", "Old2"],
            "value": [50, 50],
        }, schema=pa_schema))

        source = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["New1", "New2"],
            "value": pa.array([100, 10], type=pa.int64()),
        })

        merge_paimon(
            target=full,
            source=source,
            catalog_options=self.catalog_options,
            on=["id"],
            when_matched_update="*",
            when_matched_update_condition=lambda r: r["s.value"] > r["t.value"],
        )

        out = self._read_sorted(full)
        self.assertEqual(out["id"], [1, 2])
        self.assertEqual(out["name"], ["New1", "Old2"])
        self.assertEqual(out["value"], [100, 50])

    def test_not_matched_by_source_update(self):
        full, pa_schema = self._make_pk_table_with_flag("merge_soft_delete")
        target = self.catalog.get_table(full)
        write_builder = target.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(pa.Table.from_pydict({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": ["A", "B", "C"],
            "deleted": [False, False, False],
        }))
        commits = writer.prepare_commit()
        write_builder.new_commit().commit(commits)
        writer.close()

        source = pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["A"],
            "deleted": [False],
        })

        merge_paimon(
            target=full,
            source=source,
            catalog_options=self.catalog_options,
            on=["id"],
            when_not_matched_by_source_update={"deleted": True},
        )

        out = self._read_sorted(full)
        self.assertEqual(out["id"], [1, 2, 3])
        self.assertEqual(out["name"], ["A", "B", "C"])
        self.assertEqual(out["deleted"], [False, True, True])

    def _make_pk_table_with_flag(self, name: str):
        pa_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            ("name", pa.string()),
            ("deleted", pa.bool_()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=["id"], options={"bucket": "2"}
        )
        full = f"default.{name}"
        self.catalog.create_table(full, schema, False)
        return full, pa_schema

    def test_delete_raises_not_implemented(self):
        full, pa_schema = self._make_pk_table("merge_delete_unsupported")
        with self.assertRaises(NotImplementedError) as ctx:
            merge_paimon(
                target=full,
                source=pa.Table.from_pydict({"id": pa.array([1], type=pa.int32())}),
                catalog_options=self.catalog_options,
                on=["id"],
                when_matched_delete_condition=lambda r: True,
            )
        self.assertIn("DELETE", str(ctx.exception))

        with self.assertRaises(NotImplementedError):
            merge_paimon(
                target=full,
                source=pa.Table.from_pydict({"id": pa.array([1], type=pa.int32())}),
                catalog_options=self.catalog_options,
                on=["id"],
                when_not_matched_by_source_delete_condition=lambda r: True,
            )

    def test_duplicate_source_rows_raise(self):
        full, pa_schema = self._make_pk_table("merge_dup_source")
        self._write(full, pa.Table.from_pydict({
            "id": [1],
            "name": ["Old"],
            "value": [10],
        }, schema=pa_schema))

        source = pa.Table.from_pydict({
            "id": [1, 1],
            "name": ["A", "B"],
            "value": [100, 200],
        }, schema=pa_schema)

        with self.assertRaises(ValueError) as ctx:
            merge_paimon(
                target=full,
                source=source,
                catalog_options=self.catalog_options,
                on=["id"],
                when_matched_update="*",
            )
        self.assertIn("source must be unique", str(ctx.exception))

    def test_validation_errors(self):
        # append-only table (no PK) → ValueError
        ao_schema = pa.schema([("id", pa.int32()), ("v", pa.int64())])
        ao = Schema.from_pyarrow_schema(ao_schema)
        self.catalog.create_table("default.merge_append_only", ao, False)
        with self.assertRaises(ValueError):
            merge_paimon(
                target="default.merge_append_only",
                source=pa.Table.from_pydict({"id": [1], "v": [1]}),
                catalog_options=self.catalog_options,
                on=["id"],
                when_matched_update="*",
            )

        full, pa_schema = self._make_pk_table("merge_validation")

        # on not subset of PKs → ValueError
        with self.assertRaises(ValueError):
            merge_paimon(
                target=full,
                source=pa.Table.from_pydict({"id": pa.array([1], type=pa.int32())}),
                catalog_options=self.catalog_options,
                on=["name"],
                when_matched_update="*",
            )

        # no when_* clause → ValueError
        with self.assertRaises(ValueError):
            merge_paimon(
                target=full,
                source=pa.Table.from_pydict({"id": pa.array([1], type=pa.int32())}),
                catalog_options=self.catalog_options,
                on=["id"],
            )

        # source missing `on` column → ValueError
        with self.assertRaises(ValueError):
            merge_paimon(
                target=full,
                source=pa.Table.from_pydict({"name": ["x"], "value": pa.array([1], type=pa.int64())}),
                catalog_options=self.catalog_options,
                on=["id"],
                when_matched_update="*",
            )

        # SET key not a target column → ValueError
        with self.assertRaises(ValueError):
            merge_paimon(
                target=full,
                source=pa.Table.from_pydict({"id": pa.array([1], type=pa.int32())}),
                catalog_options=self.catalog_options,
                on=["id"],
                when_matched_update={"not_a_real_column": "x"},
            )

    def test_source_type_normalization(self):
        import pandas as pd

        # pyarrow.Table source
        full_a, pa_schema = self._make_pk_table("merge_src_arrow")
        self._write(full_a, pa.Table.from_pydict({
            "id": [1], "name": ["a"], "value": [1],
        }, schema=pa_schema))
        merge_paimon(
            target=full_a,
            source=pa.Table.from_pydict({
                "id": pa.array([2], type=pa.int32()),
                "name": ["b"],
                "value": pa.array([2], type=pa.int64()),
            }),
            catalog_options=self.catalog_options,
            on=["id"],
            when_not_matched_insert="*",
        )
        self.assertEqual(self._read_sorted(full_a)["id"], [1, 2])

        # pandas.DataFrame source
        full_b, pa_schema_b = self._make_pk_table("merge_src_pandas")
        self._write(full_b, pa.Table.from_pydict({
            "id": [1], "name": ["a"], "value": [1],
        }, schema=pa_schema_b))
        merge_paimon(
            target=full_b,
            source=pd.DataFrame({
                "id": pd.array([2], dtype="int32"),
                "name": ["b"],
                "value": pd.array([2], dtype="int64"),
            }),
            catalog_options=self.catalog_options,
            on=["id"],
            when_not_matched_insert="*",
        )
        self.assertEqual(self._read_sorted(full_b)["id"], [1, 2])

        # Paimon table identifier source
        full_c, pa_schema_c = self._make_pk_table("merge_src_target")
        src_name, _ = self._make_pk_table("merge_src_source")
        self._write(full_c, pa.Table.from_pydict({
            "id": [1], "name": ["a"], "value": [1],
        }, schema=pa_schema_c))
        self._write(src_name, pa.Table.from_pydict({
            "id": [2], "name": ["b"], "value": [2],
        }, schema=pa_schema_c))
        merge_paimon(
            target=full_c,
            source=src_name,
            catalog_options=self.catalog_options,
            on=["id"],
            when_not_matched_insert="*",
        )
        self.assertEqual(self._read_sorted(full_c)["id"], [1, 2])


if __name__ == "__main__":
    unittest.main()
