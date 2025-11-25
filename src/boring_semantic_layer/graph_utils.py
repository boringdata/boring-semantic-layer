"""Graph utilities with functional programming support."""

import contextlib
from typing import Any

from ibis.expr.operations.core import Node as IbisNode
from ibis.expr.types import Expr as IbisExpr
from returns.maybe import Maybe, Nothing, Some
from returns.result import Failure, Result, Success
from xorq.common.utils.graph_utils import (
    replace_nodes as _xorq_replace_nodes,
)
from xorq.common.utils.graph_utils import (
    to_node as _xorq_to_node,
)
from xorq.vendor.ibis.common.graph import Graph
from xorq.vendor.ibis.expr.operations.core import Node
from xorq.vendor.ibis.expr.types import Expr as XorqExpr

__all__ = [
    "bfs",
    "gen_children_of",
    "replace_nodes",
    "to_node",
    "walk_nodes",
    "to_node_safe",
    "try_to_node",
    "find_dimensions_and_measures",
    "Graph",
    "Node",
]


def to_node(maybe_expr: Any) -> Node:
    if isinstance(maybe_expr, IbisNode):
        return maybe_expr
    if isinstance(maybe_expr, IbisExpr):
        return maybe_expr.op()
    return _xorq_to_node(maybe_expr)


def gen_children_of(node: Node) -> tuple[Node, ...]:
    children = getattr(node, "__children__", ())
    result = []
    for child in children:
        with contextlib.suppress(ValueError, AttributeError):
            result.append(to_node(child))
    return tuple(result)


def bfs(expr) -> Graph:
    from collections import deque

    start = to_node(expr)
    queue = deque([start])
    graph_dict = {}
    while queue:
        node = queue.popleft()
        if node in graph_dict:
            continue
        children = gen_children_of(node)
        graph_dict[node] = children
        for child in children:
            if child not in graph_dict:
                queue.append(child)
    return Graph(graph_dict)


def walk_nodes(node_types, expr):
    start = to_node(expr)
    visited = set()
    stack = [start]
    types = node_types if isinstance(node_types, tuple) else (node_types,)
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        if isinstance(node, types):
            yield node
        stack.extend(c for c in gen_children_of(node) if c not in visited)


def replace_nodes(replacer, expr):
    node = to_node(expr)
    result = _xorq_replace_nodes(replacer, node)
    return result.to_expr()


def to_node_safe(maybe_expr: Any) -> Result[Node, ValueError]:
    try:
        return Success(to_node(maybe_expr))
    except ValueError as e:
        return Failure(e)


def try_to_node(child: Any) -> Maybe[Node]:
    return to_node_safe(child).map(Some).value_or(Nothing)


def find_dimensions_and_measures(
    expr: IbisExpr | XorqExpr,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from .ops import (
        _find_all_root_models,
        _get_field_dict,
        _merge_fields_with_prefixing,
    )

    roots = _find_all_root_models(to_node(expr))
    return (
        _merge_fields_with_prefixing(roots, lambda r: _get_field_dict(r, "dimensions")),
        _merge_fields_with_prefixing(roots, lambda r: _get_field_dict(r, "measures")),
    )
