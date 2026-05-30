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

import ast
import operator
from typing import Mapping, Set

_PREFIXES = ("s", "t")

_COMPARE_OPS = {
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}

_ALLOWED_NODES = (
    ast.BoolOp, ast.And, ast.Or,
    ast.UnaryOp, ast.Not, ast.USub, ast.UAdd,
    ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.Constant, ast.Attribute, ast.Name, ast.Load,
)


class ConditionExpr:
    """A parsed merge condition over the joined row.

    Columns are referenced as ``s.col`` / ``t.col``; the evaluator reads them
    from a combined ``{"s.col": ..., "t.col": ...}`` mapping. Only comparisons,
    boolean and/or/not, and literals are supported, so the expression is safe to
    evaluate (no ``eval``) and its referenced columns can be extracted statically.
    """

    def __init__(self, source: str, body: ast.AST):
        self.source = source
        self._body = body

    def eval(self, combined: Mapping) -> bool:
        return bool(_eval(self._body, combined))

    def target_columns(self) -> Set[str]:
        return self._columns("t")

    def source_columns(self) -> Set[str]:
        return self._columns("s")

    def _columns(self, prefix: str) -> Set[str]:
        cols = set()
        for node in ast.walk(self._body):
            if (isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == prefix):
                cols.add(node.attr)
        return cols


def parse(source: str) -> ConditionExpr:
    try:
        tree = ast.parse(source, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid merge condition {source!r}: {e}")
    for node in ast.walk(tree):
        if isinstance(node, ast.Expression):
            continue
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(
                f"Unsupported syntax in merge condition {source!r}: "
                f"{type(node).__name__}. Only comparisons of s./t. columns and "
                f"literals combined with and/or/not are allowed."
            )
        if isinstance(node, ast.Attribute):
            if not (isinstance(node.value, ast.Name) and node.value.id in _PREFIXES):
                raise ValueError(
                    f"Column reference in merge condition {source!r} must be "
                    f"'s.<col>' or 't.<col>'."
                )
        if isinstance(node, ast.Name) and node.id not in _PREFIXES:
            raise ValueError(
                f"Unknown name {node.id!r} in merge condition {source!r}; "
                f"only 's' and 't' are allowed."
            )
    return ConditionExpr(source, tree.body)


def _eval(node, combined):
    if isinstance(node, ast.BoolOp):
        values = (_eval(v, combined) for v in node.values)
        if isinstance(node.op, ast.And):
            return all(values)
        return any(values)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return not _eval(node.operand, combined)
    if isinstance(node, ast.Compare):
        left = _operand(node.left, combined)
        ok = True
        for op, comparator in zip(node.ops, node.comparators):
            right = _operand(comparator, combined)
            ok = ok and _apply(op, left, right)
            left = right
        return ok
    return _operand(node, combined)


def _operand(node, combined):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Attribute):
        return combined.get(f"{node.value.id}.{node.attr}")
    if isinstance(node, ast.UnaryOp):
        value = _operand(node.operand, combined)
        return -value if isinstance(node.op, ast.USub) else value
    return _eval(node, combined)


def _apply(op, left, right):
    if isinstance(op, ast.Eq):
        return left == right
    if isinstance(op, ast.NotEq):
        return left != right
    if left is None or right is None:
        return False
    return _COMPARE_OPS[type(op)](left, right)
