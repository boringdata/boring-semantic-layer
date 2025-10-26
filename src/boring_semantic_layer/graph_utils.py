from collections import deque
from collections.abc import Iterable
from typing import Any

from ibis.expr.operations.core import Node
from ibis.expr.types import Expr


def to_node(maybe_expr: Any) -> Node:
    return (
        maybe_expr
        if isinstance(maybe_expr, Node)
        else maybe_expr.op()
        if isinstance(maybe_expr, Expr)
        else maybe_expr.op()
        if hasattr(maybe_expr, "op") and callable(maybe_expr.op)
        else (_ for _ in ()).throw(
            ValueError(f"Cannot convert type {type(maybe_expr)} to Node"),
        )
    )


def gen_children_of(node: Node) -> tuple[Node, ...]:
    def try_to_node(child):
        try:
            return to_node(child)
        except ValueError:
            return None

    return tuple(
        n for n in (try_to_node(c) for c in getattr(node, "__children__", ())) if n is not None
    )


def bfs(expr: Expr) -> dict[Node, tuple[Node, ...]]:
    """Perform a breadth-first traversal, returning a map of each node to its children."""
    start = to_node(expr)
    queue = deque([start])
    graph: dict[Node, tuple[Node, ...]] = {}
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
    node_types: type[Any] | tuple[type[Any], ...],
    expr: Expr,
) -> Iterable[Node]:
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


def replace_nodes(replacer, expr: Expr) -> Expr:
    return to_node(expr).replace(lambda op, kwargs: replacer(op, kwargs)).to_expr()


def find_dimensions_and_measures(expr: Expr) -> tuple[dict[str, Any], dict[str, Any]]:
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
