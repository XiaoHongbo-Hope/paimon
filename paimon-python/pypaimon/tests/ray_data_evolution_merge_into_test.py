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

    def test_multi_source_match_silently_picks_first(self):
        # Spark DE sets checkCardinality=false: silently dedupe target _ROW_IDs
        # rather than raising when source has multiple rows for the same key.
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
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1])
        # Exactly one of the source rows wins; we don't pin which.
        self.assertIn(out['name'][0], ['x', 'y'])
        self.assertIn(out['age'][0], [100, 200])

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

    def test_strict_mode_rejects_when_snapshot_advances(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['x'],
                    'age': pa.array([1], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )
        current_id = self._snapshot_id(target)

        table = self.catalog.get_table(target).copy(
            {"commit.strict-mode.last-safe-snapshot": str(current_id - 1)}
        )
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_arrow(
            pa.Table.from_pydict(
                {
                    'id': pa.array([2], type=pa.int32()),
                    'name': ['y'],
                    'age': pa.array([2], type=pa.int32()),
                },
                schema=self.pa_schema,
            )
        )
        msgs = tw.prepare_commit()
        tw.close()

        with self.assertRaises(RuntimeError) as ctx:
            wb.new_commit().commit(msgs)
        self.assertIn("strict-mode", str(ctx.exception).lower())

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


if __name__ == '__main__':
    unittest.main()
