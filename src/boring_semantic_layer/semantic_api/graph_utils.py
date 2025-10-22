from collections import deque
from typing import Any, Dict, Iterable, Tuple, Type
from ibis.expr.types import Expr
from ibis.expr.operations.core import Node


def to_node(maybe_expr: Any) -> Node:
    return (maybe_expr if isinstance(maybe_expr, Node)
            else maybe_expr.op() if isinstance(maybe_expr, Expr)
            else (_ for _ in ()).throw(ValueError(f"Cannot convert type {type(maybe_expr)} to Node")))


def gen_children_of(node: Node) -> Tuple[Node, ...]:
    def try_to_node(child):
        try:
            return to_node(child)
        except ValueError:
            return None
    return tuple(n for n in (try_to_node(c) for c in getattr(node, "__children__", ())) if n is not None)


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


def walk_nodes(node_types: Type[Any] | Tuple[Type[Any], ...], expr: Expr) -> Iterable[Node]:
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


def find_dimensions_and_measures(expr: Expr) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    from .ops import _find_all_root_models, _merge_fields_with_prefixing
    roots = _find_all_root_models(to_node(expr))
    get_dims = lambda r: r._dims_dict() if hasattr(r, '_dims_dict') else getattr(r, "dimensions", {})
    get_meas = lambda r: r._measures_dict() if hasattr(r, '_measures_dict') else getattr(r, "measures", {})
    return (_merge_fields_with_prefixing(roots, get_dims),
            _merge_fields_with_prefixing(roots, get_meas))
