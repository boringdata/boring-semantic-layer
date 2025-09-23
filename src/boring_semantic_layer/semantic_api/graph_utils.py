"""
Graph-traversal utilities for the semantic API, adapted from xorq.common.utils.graph_utils.
Provides BFS, node walking, and simple node inspection over Ibis expression trees.
"""

from collections import deque
from typing import Any, Dict, Iterable, Tuple, Type

from ibis.expr.types import Expr
from ibis.expr.operations.core import Node


def to_node(maybe_expr: Any) -> Node:
    """Unwrap an Expr or Node into a raw Node for traversal."""
    if isinstance(maybe_expr, Node):
        return maybe_expr
    if isinstance(maybe_expr, Expr):
        return maybe_expr.op()
    raise ValueError(f"Cannot convert type {type(maybe_expr)} to Node")


def gen_children_of(node: Node) -> Tuple[Node, ...]:
    """Yield the direct child nodes of a given Node."""
    raw = getattr(node, "__children__", ())
    children = []
    for child in raw:
        try:
            ch_node = to_node(child)
        except ValueError:
            continue
        children.append(ch_node)
    return tuple(children)


def bfs(expr: Expr) -> Dict[Node, Tuple[Node, ...]]:
    """Perform a breadth-first traversal, returning a map of each node to its children."""
    start = to_node(expr)
    queue = deque([start])
    graph: Dict[Node, Tuple[Node, ...]] = {}
    while queue:
        node = queue.popleft()
        if node in graph:
            continue
        children = gen_children_of(node)
        graph[node] = children
        for c in children:
            if c not in graph:
                queue.append(c)
    return graph


def walk_nodes(
    node_types: Type[Any] | Tuple[Type[Any], ...], expr: Expr
) -> Iterable[Node]:
    """
    Yield all nodes of the given types in a depth-first walk of the expression.
    """
    start = to_node(expr)
    visited = set()
    stack = [start]
    if not isinstance(node_types, tuple):
        node_types = (node_types,)
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        if isinstance(node, node_types):
            yield node
        for child in gen_children_of(node):
            if child not in visited:
                stack.append(child)


def replace_nodes(replacer, expr: Expr) -> Expr:
    """
    Walk the expression tree and apply a replacer(op, kwargs) to each Node.
    The replacer should return either the original op or a recreated op.
    """
    initial = to_node(expr)
    new_op = initial.replace(lambda op, kwargs: replacer(op, kwargs))
    return new_op.to_expr()


def find_dimensions_and_measures(expr: Expr) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Traverse the expression tree and collect dimensions and measures from all SemanticTable nodes.

    Returns:
        A tuple of two dictionaries (dimensions, measures), where keys are prefixed field names
        and values are the Dimension or Measure objects attached to each table node.
    """
    from .ops import _find_all_root_models, _merge_fields_with_prefixing

    node = to_node(expr)
    roots = _find_all_root_models(node)
    dimensions = _merge_fields_with_prefixing(
        roots, lambda r: getattr(r, "dimensions", {})
    )
    measures = _merge_fields_with_prefixing(roots, lambda r: getattr(r, "measures", {}))
    return dimensions, measures
