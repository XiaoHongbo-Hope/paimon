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

import re
from typing import Set

import pyarrow as pa


_COL_REF_PATTERN = re.compile(r'\b([st])\.(\w+)\b')


def _require_datafusion():
    try:
        import datafusion
        return datafusion
    except ImportError:
        raise ImportError(
            "merge_into condition expressions require the 'datafusion' "
            "package. Install it with: pip install pypaimon[sql]"
        )


def rewrite_condition(condition: str) -> str:
    return _COL_REF_PATTERN.sub(r'"\1.\2"', condition)


def filter_batch(batch: pa.Table, condition: str) -> pa.Table:
    datafusion = _require_datafusion()
    rewritten = rewrite_condition(condition)
    ctx = datafusion.SessionContext()
    ctx.register_record_batches("_merge_batch", [batch.to_batches()])
    result = ctx.sql(
        f'SELECT * FROM _merge_batch WHERE {rewritten}'
    )
    return result.to_arrow_table()


def extract_columns(condition: str) -> Set[str]:
    return {f"{m.group(1)}.{m.group(2)}"
            for m in _COL_REF_PATTERN.finditer(condition)}


def extract_target_columns(condition: str) -> Set[str]:
    return {m.group(2) for m in _COL_REF_PATTERN.finditer(condition)
            if m.group(1) == "t"}
