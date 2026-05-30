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
import uuid

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import WhenMatched, WhenNotMatched, merge_into


class RayDataEvolutionMergeIntoTest(unittest.TestCase):

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
    ])

    de_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database('default', True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, options=None):
        opts = options if options is not None else self.de_options
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(self.pa_schema, options=opts)
        self.catalog.create_table(name, s, False)
        return name

    def _source(self, ids=(1,)):
        return pa.Table.from_pydict(
            {
                'id': pa.array(list(ids), type=pa.int32()),
                'name': ['x'] * len(ids),
                'age': [10] * len(ids),
            },
            schema=self.pa_schema,
        )

    def _write(self, target, data):
        table = self.catalog.get_table(target)
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(data)
        wb.new_commit().commit(writer.prepare_commit())
        writer.close()

    def _read_sorted(self, target):
        table = self.catalog.get_table(target)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits).sort_by('id').to_pydict()

    def _snapshot_id(self, target):
        table = self.catalog.get_table(target)
        snap = table.snapshot_manager().get_latest_snapshot()
        return snap.id if snap is not None else None

    def test_no_clause_raises(self):
        target = self._create_table()
        with self.assertRaises(ValueError):
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
            )

    def test_non_de_table_rejected(self):
        target = self._create_table(options={'row-tracking.enabled': 'true'})
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
            )
        self.assertIn('data-evolution.enabled', str(ctx.exception))

    def test_no_row_tracking_rejected(self):
        target = self._create_table(options={'data-evolution.enabled': 'true'})
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
            )
        self.assertIn('row-tracking.enabled', str(ctx.exception))

    def test_source_missing_on_col_raises(self):
        target = self._create_table()
        bad_source = pa.Table.from_pydict(
            {'name': ['x'], 'age': [10]},
            schema=pa.schema([('name', pa.string()), ('age', pa.int32())]),
        )
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=bad_source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
            )
        self.assertIn("'id'", str(ctx.exception))

    def test_matched_update_star(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3, 4], type=pa.int32()),
                'name': ['b2', 'c2', 'd'],
                'age': pa.array([22, 33, 40], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c2'])
        self.assertEqual(out['age'], [10, 22, 33])

    def test_matched_update_dict(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2], type=pa.int32()),
                'name': ['ignored'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={'age': 's.age'})],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])
        self.assertEqual(out['age'], [10, 99])

    def test_matched_update_with_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['a', 'b', 'c'],
                'age': pa.array([5, 100, 50], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched(
                    update={'age': 's.age'},
                    condition=lambda r: r['s.age'] > r['t.age'],
                ),
            ],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['age'], [10, 100, 50])

    def test_matched_multiple_clauses_first_match_wins(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['s1', 's2', 's3'],
                'age': pa.array([5, 25, 100], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched(
                    update={'age': 1},
                    condition=lambda r: r['s.age'] < r['t.age'],
                ),
                WhenMatched(update={'age': 999}),
            ],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['age'], [1, 999, 999])

    def test_matched_partial_clause_falls_back_to_target(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([42], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )
        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['new'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )
        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched(update={'name': 's.name'}),
                WhenMatched(update={'age': 's.age'}),
            ],
        )
        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['new'])
        self.assertEqual(out['age'], [42])

    def test_not_matched_insert_appends_unmatched(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3, 4], type=pa.int32()),
                'name': ['b2', 'c2', 'd'],
                'age': pa.array([22, 33, 40], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3, 4])
        self.assertEqual(out['name'], ['a', 'b', 'c', 'd'])
        self.assertEqual(out['age'], [10, 20, 30, 40])

    def test_not_matched_insert_with_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3, 4], type=pa.int32()),
                'name': ['b', 'c', 'd'],
                'age': pa.array([5, 50, 100], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(
                    insert='*',
                    condition=lambda r: r['s.age'] >= 50,
                ),
            ],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 3, 4])
        self.assertEqual(out['age'], [10, 50, 100])

    def test_not_matched_multiple_clauses_first_match_wins(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3], type=pa.int32()),
                'name': ['b', 'c'],
                'age': pa.array([5, 99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(
                    insert={'id': 's.id', 'name': 'small', 'age': 1},
                    condition=lambda r: r['s.age'] < 10,
                ),
                WhenNotMatched(insert={'id': 's.id', 'name': 'big', 'age': 2}),
            ],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'small', 'big'])
        self.assertEqual(out['age'], [10, 1, 2])

    def test_merge_condition_filters_matched_update(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a2', 'b2'],
                'age': pa.array([100, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            merge_condition=lambda r: r['s.age'] > r['t.age'],
            when_matched=[WhenMatched(update={'name': 's.name'})],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a2', 'b'])
        self.assertEqual(out['age'], [10, 20])

    def test_merge_condition_failure_routes_to_insert(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['new1', 'new2'],
                'age': pa.array([5, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            merge_condition=lambda r: r['s.age'] > r['t.age'],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        ids_sorted = sorted(out['id'])
        self.assertEqual(ids_sorted, [1, 1, 2])
        rows = sorted(zip(out['id'], out['name'], out['age']))
        self.assertEqual(rows, [(1, 'new1', 5), (1, 'old', 20), (2, 'new2', 30)])

    def test_combined_update_and_insert(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c'])
        self.assertEqual(out['age'], [10, 22, 30])

    def test_combined_update_and_insert_with_merge_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['n1', 'n2', 'n3'],
                'age': pa.array([100, 5, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            merge_condition=lambda r: r['s.age'] > r['t.age'],
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(sorted(out['id']), [1, 2, 2, 3])
        rows = sorted(zip(out['id'], out['name'], out['age']))
        self.assertEqual(
            rows,
            [(1, 'n1', 100), (2, 'b', 20), (2, 'n2', 5), (3, 'n3', 30)],
        )

    def test_combined_matched_clause_condition_no_merge_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['n1', 'n2', 'n3'],
                'age': pa.array([100, 5, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched(
                    update={'name': 's.name'},
                    condition=lambda r: r['s.age'] > 50,
                )
            ],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        rows = sorted(zip(out['id'], out['name'], out['age']))
        self.assertEqual(rows, [(1, 'n1', 10), (2, 'b', 20), (3, 'n3', 30)])

    def test_on_with_renamed_columns(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source_schema = pa.schema([
            ('uid', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        source = pa.Table.from_pydict(
            {
                'uid': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=source_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on={'id': 'uid'},
            when_matched=[WhenMatched(update={'age': 's.age'})],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['age'], [10, 22])

    def test_on_with_renamed_columns_star(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source_schema = pa.schema([
            ('uid', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        source = pa.Table.from_pydict(
            {
                'uid': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=source_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on={'id': 'uid'},
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c'])
        self.assertEqual(out['age'], [10, 22, 30])

    def test_insert_into_empty_target(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['a', 'b', 'c'],
                'age': pa.array([10, 20, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b', 'c'])
        self.assertEqual(out['age'], [10, 20, 30])

    def test_insert_into_empty_target_with_merge_condition(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([10, 20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            merge_condition=lambda r: r['s.age'] > 0,
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])
        self.assertEqual(out['age'], [10, 20])

    def test_insert_dict_fills_unspecified_with_null(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2], type=pa.int32()),
                'name': ['source-name'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert={'id': 's.id', 'age': 99})],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', None])
        self.assertEqual(out['age'], [10, 99])

    def test_multi_source_match_raises_by_default(self):
        # One target row matched by several source rows: the winning value is
        # undefined (Spark DE's checkCardinality=false), so we refuse by default.
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 1], type=pa.int32()),
                'name': ['x', 'y'],
                'age': pa.array([100, 200], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        with self.assertRaises(Exception) as ctx:
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
            )
        self.assertIn("multiple source rows", str(ctx.exception))

    def test_multi_source_match_allow_keeps_first(self):
        # Opt-in: allow_multiple_matches keeps the first match deterministically.
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 1], type=pa.int32()),
                'name': ['x', 'y'],
                'age': pa.array([100, 200], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            allow_multiple_matches=True,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1])
        # One source row wins; we don't pin which.
        self.assertIn(out['name'][0], ['x', 'y'])
        self.assertIn(out['age'][0], [100, 200])

    def test_blob_update_is_rejected(self):
        import types

        from pypaimon.ray.data_evolution_merge_into import _reject_blob_updates
        from pypaimon.schema.data_types import AtomicType, DataField

        fake_table = types.SimpleNamespace(
            table_schema=types.SimpleNamespace(
                fields=[
                    DataField(0, 'id', AtomicType('INT')),
                    DataField(1, 'payload', AtomicType('BLOB')),
                ]
            )
        )
        with self.assertRaises(NotImplementedError):
            _reject_blob_updates(fake_table, {'payload'})
        _reject_blob_updates(fake_table, {'id'})

    def test_add_paimon_src_idx_pandas_blocks(self):
        import pandas as pd

        from pypaimon.ray.data_evolution_merge_into import (
            PAIMON_SRC_IDX_COL,
            _add_paimon_src_idx,
        )

        pdf = pd.DataFrame(
            {
                'id': pd.array(list(range(20)), dtype='int32'),
                'name': ['x'] * 20,
                'age': pd.array(list(range(20)), dtype='int64'),
            }
        )
        ds = ray.data.from_pandas(pdf).repartition(4)
        out = _add_paimon_src_idx(ds).to_pandas()
        ids = sorted(out[PAIMON_SRC_IDX_COL].tolist())
        self.assertEqual(len(out), 20)
        self.assertEqual(ids, list(range(20)))

    def test_combined_writes_single_snapshot(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )
        before = self._snapshot_id(target)

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        after = self._snapshot_id(target)
        self.assertEqual(after, before + 1)

    def test_self_merge_via_normal_join(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched(
                    update={'age': lambda r: r['t.age'] + 1},
                ),
            ],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['age'], [11, 21, 31])

    def test_matched_update_can_change_on_column(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['x'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['y'],
                'age': pa.array([20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={'id': 999, 'name': 'y'})],
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [999])
        self.assertEqual(out['name'], ['y'])
        self.assertEqual(out['age'], [10])

    def test_empty_target_matched_update_is_noop(self):
        target = self._create_table()
        before = self._snapshot_id(target)

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([10, 20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
        )

        self.assertEqual(self._snapshot_id(target), before)

    def test_duplicate_identical_source_rows_route_separately(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['t1'],
                    'age': pa.array([100], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 2], type=pa.int32()),
                'name': ['dup', 'dup'],
                'age': pa.array([5, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            merge_condition=lambda r: r['s.age'] > r['t.age'],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        rows = sorted(zip(out['id'], out['name'], out['age']))
        self.assertEqual(
            rows,
            [(1, 't1', 100), (2, 'dup', 5), (2, 'dup', 5)],
        )

    def test_merge_condition_routes_per_source_row(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['original'],
                    'age': pa.array([100], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 1], type=pa.int32()),
                'name': ['high', 'low'],
                'age': pa.array([200, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            merge_condition=lambda r: r['s.age'] > r['t.age'],
            when_not_matched=[WhenNotMatched(insert='*')],
        )

        out = self._read_sorted(target)
        rows = sorted(zip(out['id'], out['name'], out['age']))
        self.assertEqual(
            rows,
            [(1, 'low', 5), (1, 'original', 100)],
        )


class RayMergeIntoGlobalIndexGateTest(unittest.TestCase):

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
    ])

    de_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _table(self):
        name = f'default.gidx_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(self.pa_schema, options=self.de_options)
        self.catalog.create_table(name, s, False)
        return self.catalog.get_table(name)

    def _entry(self, table, column, partition_values=()):
        from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
        from pypaimon.index.index_file_meta import IndexFileMeta
        from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
        from pypaimon.table.row.generic_row import GenericRow

        field_id = next(f.id for f in table.fields if f.name == column)
        index_file = IndexFileMeta(
            index_type='BTREE', file_name=f'idx-{column}', file_size=1, row_count=1,
            global_index_meta=GlobalIndexMeta(
                row_range_start=0, row_range_end=1, index_field_id=field_id,
            ),
        )
        return IndexManifestEntry(
            kind=0, partition=GenericRow(list(partition_values), []),
            bucket=0, index_file=index_file,
        )

    def _update_msg(self, partition=()):
        from pypaimon.write.commit_message import CommitMessage
        return CommitMessage(partition=partition, bucket=0, new_files=[])

    def test_update_throw_error_raises(self):
        from unittest.mock import patch
        from pypaimon.ray import data_evolution_merge_into as m

        table = self._table()
        with patch.object(m, '_scan_global_index_entries',
                          return_value=[self._entry(table, 'age')]):
            with self.assertRaises(NotImplementedError):
                m._apply_global_index_update_action(
                    table, object(), ['age'], [self._update_msg()],
                    m.GLOBAL_INDEX_ACTION_THROW_ERROR,
                )

    def test_update_drop_returns_delete_msgs(self):
        from unittest.mock import patch
        from pypaimon.ray import data_evolution_merge_into as m

        table = self._table()
        with patch.object(m, '_scan_global_index_entries',
                          return_value=[self._entry(table, 'age')]):
            msgs = m._apply_global_index_update_action(
                table, object(), ['age'], [self._update_msg()],
                m.GLOBAL_INDEX_ACTION_DROP_PARTITION_INDEX,
            )
        self.assertEqual(1, len(msgs))
        self.assertFalse(msgs[0].is_empty())
        self.assertEqual('idx-age', msgs[0].index_files[0].index_file.file_name)
        self.assertEqual(1, msgs[0].index_files[0].kind)

    def test_update_unaffected_column_is_noop(self):
        from unittest.mock import patch
        from pypaimon.ray import data_evolution_merge_into as m

        table = self._table()
        with patch.object(m, '_scan_global_index_entries',
                          return_value=[self._entry(table, 'age')]):
            msgs = m._apply_global_index_update_action(
                table, object(), ['name'], [self._update_msg()],
                m.GLOBAL_INDEX_ACTION_DROP_PARTITION_INDEX,
            )
        self.assertEqual([], msgs)

    def test_update_untouched_partition_is_noop(self):
        from unittest.mock import patch
        from pypaimon.ray import data_evolution_merge_into as m

        table = self._table()
        entry = self._entry(table, 'age', partition_values=('EU',))
        with patch.object(m, '_scan_global_index_entries', return_value=[entry]):
            msgs = m._apply_global_index_update_action(
                table, object(), ['age'], [self._update_msg(partition=('US',))],
                m.GLOBAL_INDEX_ACTION_DROP_PARTITION_INDEX,
            )
        self.assertEqual([], msgs)


if __name__ == '__main__':
    unittest.main()
