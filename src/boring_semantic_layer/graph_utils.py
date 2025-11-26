from collections import deque
from collections.abc import Iterable
from typing import Any

from ibis.expr.operations.core import Node
from ibis.expr.types import Expr
from returns.maybe import Maybe, Nothing, Some
from returns.result import Failure, Success

__all__ = [
    "bfs",
    "gen_children_of",
    "replace_nodes",
    "to_node",
    "walk_nodes",
    "to_node_safe",
    "try_to_node",
    "find_dimensions_and_measures",
    "graph_predecessors",
    "graph_successors",
    "graph_bfs",
    "graph_invert",
    "graph_to_dict",
    "build_dependency_graph",
]


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


def to_node_safe(maybe_expr: Any) -> Success[Node] | Failure[ValueError]:
    """Convert expression to Node, returning Result instead of raising."""
    try:
        return Success(to_node(maybe_expr))
    except ValueError as e:
        return Failure(e)


def try_to_node(child: Any) -> Maybe[Node]:
    """Try to convert child to Node, returning Maybe."""
    return to_node_safe(child).map(Some).value_or(Nothing)


def gen_children_of(node: Node) -> tuple[Node, ...]:
    """Generate children nodes, filtering out conversion failures."""
    children = (try_to_node(c) for c in getattr(node, "__children__", ()))
    return tuple(child.unwrap() for child in children if isinstance(child, Some))


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


def graph_predecessors(graph: dict[str, dict], node: str) -> set[str]:
    """Get direct dependencies of a node."""
    return set(graph.get(node, {}).get("deps", {}).keys())


def graph_successors(graph: dict[str, dict], node: str) -> set[str]:
    """Get direct dependents of a node."""
    return {field for field, meta in graph.items() if node in meta["deps"]}


def graph_bfs(
    graph: dict[str, dict],
    start: str | list[str],
):
    """
    Perform BFS on a dependency graph, yielding node names in order.

    Args:
        graph: Dictionary mapping node names to metadata dicts with "deps" key
        start: Starting node name(s)

    Yields:
        Node names in breadth-first order
    """
    from collections import deque

    start_names = [start] if isinstance(start, str) else start
    queue = deque(start_names)
    visited = set()

    while queue:
        node_name = queue.popleft()
        if node_name in visited:
            continue
        visited.add(node_name)
        yield node_name

        # Add dependencies to queue
        if node_name in graph:
            deps = graph[node_name].get("deps", {})
            for dep_name in deps:
                if dep_name not in visited:
                    queue.append(dep_name)


def graph_invert(graph: dict[str, dict]) -> dict[str, dict]:
    """
    Invert a dependency graph (reverse all edges).

    Args:
        graph: Dependency graph to invert

    Returns:
        Inverted graph where dependencies become dependents
    """
    inverted = {}

    # Get all nodes (including dependencies that might not be in main graph)
    all_nodes = set(graph.keys())
    for field_meta in graph.values():
        all_nodes.update(field_meta["deps"].keys())

    # Initialize all nodes in the inverted graph
    for node_name in all_nodes:
        inverted[node_name] = {
            "deps": {},
            "type": graph[node_name]["type"] if node_name in graph else "column",
        }

    # Invert the edges: if A depends on B, then in inverted graph B depends on A
    for node_name, metadata in graph.items():
        for dep_name, _dep_type in metadata["deps"].items():
            # In original: node_name -> dep_name
            # In inverted: dep_name -> node_name
            inverted[dep_name]["deps"][node_name] = metadata["type"]

    return inverted


def graph_to_dict(graph: dict[str, dict]) -> dict:
    """
    Export graph to JSON-serializable dictionary format.

    Args:
        graph: Dependency graph

    Returns:
        Dictionary with "nodes" and "edges" arrays
    """
    # Get all nodes
    all_nodes = set(graph.keys())
    for field_meta in graph.values():
        all_nodes.update(field_meta["deps"].keys())

    nodes = [
        {"id": node, "type": graph[node]["type"] if node in graph else "column"}
        for node in sorted(all_nodes)
    ]
    edges = [
        {"source": source, "target": target, "type": dep_type}
        for target, metadata in graph.items()
        for source, dep_type in metadata["deps"].items()
    ]
    return {"nodes": nodes, "edges": edges}


def build_dependency_graph(
    dimensions: dict, measures: dict, calc_measures: dict, base_table
) -> dict[str, dict]:
    """
    Build a dependency graph from semantic model fields.

    Args:
        dimensions: Dictionary of dimension objects
        measures: Dictionary of measure objects
        calc_measures: Dictionary of calculated measure expressions
        base_table: The base Ibis table

    Returns:
        Dictionary mapping field names to metadata with "deps" and "type" keys
    """
    from xorq.vendor.ibis.expr.operations.relations import Field as XorqField

    try:
        from ibis.expr.operations.relations import Field as IbisField
    except ImportError:
        IbisField = None

    from .ops import _collect_measure_refs

    graph = {}

    # Build extended table with all dimensions for measure analysis
    extended_table = _build_extended_table(base_table, dimensions)

    # Extract dependencies for dimensions and measures
    for name, obj in {**dimensions, **measures}.items():
        try:
            table = extended_table if name in measures else base_table
            if name in dimensions:
                table = _add_previous_dimensions(table, dimensions, name)

            resolved = _resolve_expr(obj.expr, table)
            table_op = to_node(table)

            # Collect Field nodes from both ibis and xorq
            fields = []
            for f in walk_nodes((XorqField,), resolved):
                if hasattr(f, "name") and hasattr(f, "rel") and f.rel == table_op:
                    fields.append(f)

            # Also try ibis Field if available
            if IbisField is not None:
                for f in walk_nodes((IbisField,), resolved):
                    if hasattr(f, "name") and hasattr(f, "rel") and f.rel == table_op:
                        fields.append(f)

            deps_with_types = _classify_dependencies(
                fields, dimensions, measures, calc_measures, current_field=name
            )
            graph[name] = {
                "deps": deps_with_types,
                "type": "dimension" if name in dimensions else "measure",
            }
        except Exception:
            graph[name] = {"deps": {}, "type": "dimension" if name in dimensions else "measure"}

    # Extract dependencies for calculated measures
    for name, calc_expr in calc_measures.items():
        refs = set()
        _collect_measure_refs(calc_expr, refs)
        graph[name] = {"deps": {ref: "measure" for ref in refs}, "type": "calc_measure"}

    return graph


def _resolve_expr(expr, table):
    """Resolve an expression against a table."""
    if hasattr(expr, "resolve"):
        return expr.resolve(table)
    elif callable(expr):
        return expr(table)
    return expr


def _build_extended_table(base_table, dimensions: dict):
    """Build a table with all dimensions added."""
    extended_table = base_table
    for dim_name, dim in dimensions.items():
        try:
            resolved = _resolve_expr(dim.expr, extended_table)
            extended_table = extended_table.mutate(**{dim_name: resolved})
        except Exception:
            pass
    return extended_table


def _add_previous_dimensions(table, dimensions: dict, current_name: str):
    """Add all dimensions defined before current_name to the table."""
    for prev_name, prev_dim in dimensions.items():
        if prev_name == current_name:
            break
        try:
            resolved = _resolve_expr(prev_dim.expr, table)
            table = table.mutate(**{prev_name: resolved})
        except Exception:
            pass
    return table


def _classify_dependencies(
    fields: list,
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    current_field: str | None = None,
) -> dict[str, str]:
    """Classify field dependencies as dimension, measure, or column."""
    return {
        f.name: (
            "dimension"
            if f.name in dimensions and f.name != current_field
            else "measure"
            if f.name in measures or f.name in calc_measures
            else "column"
        )
        for f in fields
    }


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
