"""Graph utilities with functional programming support."""

from functools import reduce as functools_reduce
from operator import methodcaller
from typing import Any, Callable, Sequence

from ibis.expr.operations.core import Node as IbisNode
from ibis.expr.types import Expr as IbisExpr
from returns.curry import partial
from returns.maybe import Maybe, Nothing, Some
from returns.result import Failure, Result, Success, safe
from toolz import compose
from xorq.common.utils.graph_utils import (
    replace_nodes as _xorq_replace_nodes,
)
from xorq.common.utils.graph_utils import (
    to_node as _xorq_to_node,
)
from xorq.vendor.ibis.common.graph import Graph
from xorq.vendor.ibis.expr.operations.core import Node
from xorq.vendor.ibis.expr.types import Expr as XorqExpr


def _collect_field_types() -> tuple[type, ...]:
    """Collect all Field types that may appear in expressions."""
    from xorq.vendor.ibis.expr.operations.relations import Field as XorqField

    types = [XorqField]
    try:
        from ibis.expr.operations.relations import Field as IbisField

        types.append(IbisField)
    except ImportError:
        pass

    return tuple(types)


# Initialize field types once at module load
FIELD_TYPES = _collect_field_types()


def is_field(node: Any) -> bool:
    """Check if node is a Field from either ibis or xorq."""
    return isinstance(node, FIELD_TYPES)


def is_table_field(table_op: Node) -> Callable[[Any], bool]:
    """Create a predicate for fields belonging to a specific table."""

    def check(node: Any) -> bool:
        return (
            is_field(node)
            and hasattr(node, "name")
            and hasattr(node, "rel")
            and node.rel == table_op
        )

    return check


__all__ = [
    "bfs",
    "gen_children_of",
    "replace_nodes",
    "to_node",
    "walk_nodes",
    "to_node_safe",
    "try_to_node",
    "find_dimensions_and_measures",
    "find_entity_dimensions",
    "find_event_timestamp_dimensions",
    "Graph",
    "Node",
    "graph_predecessors",
    "graph_successors",
    "graph_bfs",
    "graph_invert",
    "graph_to_dict",
    "build_dependency_graph",
    "extract_column_from_dimension",
    "build_column_index_from_roots",
    "traverse_roots_with",
]


def to_node(maybe_expr: Any) -> Node:
    """Convert expression to node, handling various types."""
    if isinstance(maybe_expr, IbisNode):
        return maybe_expr
    if isinstance(maybe_expr, IbisExpr):
        return maybe_expr.op()
    return _xorq_to_node(maybe_expr)


def gen_children_of(node: Node) -> tuple[Node, ...]:
    """Generate child nodes from a node."""
    children = getattr(node, "__children__", ())
    return tuple(to_node(child) for child in children)


def bfs(expr) -> Graph:
    """
    Build a graph using breadth-first search.

    This is fundamentally imperative - keep it simple and clear.
    """
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
    """
    Walk nodes in depth-first order, yielding nodes of specified types.

    This is also fundamentally imperative - keep it clear.
    """
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

        for child in gen_children_of(node):
            if child not in visited:
                stack.append(child)


def replace_nodes(replacer, expr):
    """Replace nodes in expression tree using functional composition."""
    return compose(
        methodcaller("to_expr"),
        partial(_xorq_replace_nodes, replacer),
        to_node,
    )(expr)


def to_node_safe(maybe_expr: Any) -> Result[Node, ValueError]:
    """
    Safely convert to node, returning Result.

    Public API that only catches ValueError since that's the expected
    error type for invalid expression inputs from user code.
    """
    try:
        return Success(to_node(maybe_expr))
    except ValueError as e:
        return Failure(e)


def try_to_node(child: Any) -> Maybe[Node]:
    """Try to convert to node, returning Maybe."""
    return to_node_safe(child).map(Some).value_or(Nothing)


def find_dimensions_and_measures(
    expr: IbisExpr | XorqExpr,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Find dimensions and measures in expression.

    Uses functional composition for field extraction.
    """
    from .ops import (
        _find_all_root_models,
        _get_field_dict,
        _merge_fields_with_prefixing,
    )

    roots = _find_all_root_models(to_node(expr))

    dimensions = _merge_fields_with_prefixing(
        roots, lambda r: _get_field_dict(r, "dimensions")
    )
    measures = _merge_fields_with_prefixing(roots, lambda r: _get_field_dict(r, "measures"))

    return (dimensions, measures)


def _filter_by_attribute(items: dict[str, Any], attr: str) -> dict[str, Any]:
    """Filter dictionary items by attribute value."""
    return {name: item for name, item in items.items() if getattr(item, attr, False)}


def find_entity_dimensions(expr: IbisExpr | XorqExpr) -> dict[str, Any]:
    """Find all entity dimensions in the expression tree."""
    dimensions, _ = find_dimensions_and_measures(expr)
    return _filter_by_attribute(dimensions, "is_entity")


def find_event_timestamp_dimensions(expr: IbisExpr | XorqExpr) -> dict[str, Any]:
    """Find all event timestamp dimensions in the expression tree."""
    dimensions, _ = find_dimensions_and_measures(expr)
    return _filter_by_attribute(dimensions, "is_event_timestamp")


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
    all_nodes = set(graph.keys())
    for field_meta in graph.values():
        all_nodes.update(field_meta["deps"].keys())

    inverted = {
        node_name: {
            "deps": {},
            "type": graph[node_name]["type"] if node_name in graph else "column",
        }
        for node_name in all_nodes
    }

    for node_name, metadata in graph.items():
        for dep_name in metadata["deps"]:
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
    from .ops import _collect_measure_refs

    graph = {}
    extended_table = _build_extended_table(base_table, dimensions)

    for name, obj in {**dimensions, **measures}.items():
        try:
            table = extended_table if name in measures else base_table
            if name in dimensions:
                table = _add_previous_dimensions(table, dimensions, name)

            resolved = _resolve_expr(obj.expr, table)
            table_op = to_node(table)

            # Extract fields belonging to this table
            fields = list(filter(is_table_field(table_op), walk_nodes(FIELD_TYPES, resolved)))

            deps_with_types = _classify_dependencies(
                fields, dimensions, measures, calc_measures, current_field=name
            )
            graph[name] = {
                "deps": deps_with_types,
                "type": "dimension" if name in dimensions else "measure",
            }
        except Exception:
            graph[name] = {"deps": {}, "type": "dimension" if name in dimensions else "measure"}

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
    """
    Build a table with all dimensions added.

    Uses safe operations from returns library.
    """

    @safe
    def add_dimension(table, dim_item):
        """Safely add a dimension to the table."""
        dim_name, dim = dim_item
        resolved = _resolve_expr(dim.expr, table)
        return table.mutate(**{dim_name: resolved})

    extended_table = base_table
    for dim_item in dimensions.items():
        result = add_dimension(extended_table, dim_item)
        extended_table = result.value_or(extended_table)

    return extended_table


def _add_previous_dimensions(table, dimensions: dict, current_name: str):
    """
    Add all dimensions defined before current_name to the table.

    Uses safe operations from returns library.
    """

    @safe
    def add_dimension(tbl, dim_item):
        """Safely add a dimension to the table."""
        prev_name, prev_dim = dim_item
        resolved = _resolve_expr(prev_dim.expr, tbl)
        return tbl.mutate(**{prev_name: resolved})

    result_table = table
    for prev_name, prev_dim in dimensions.items():
        if prev_name == current_name:
            break
        result = add_dimension(result_table, (prev_name, prev_dim))
        result_table = result.value_or(result_table)

    return result_table


def _classify_dependencies(
    fields: list,
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    current_field: str | None = None,
) -> dict[str, str]:
    """
    Classify field dependencies as dimension, measure, or column.
    """

    def classify_field(f):
        if f.name in dimensions and f.name != current_field:
            return "dimension"
        elif f.name in measures or f.name in calc_measures:
            return "measure"
        else:
            return "column"

    return {f.name: classify_field(f) for f in fields}


def traverse_roots_with(
    roots: Sequence[Any], transform: Callable[[Any], Result[Any, Exception]]
) -> Result[list[Any], Exception]:
    """
    Traverse semantic table roots and apply a transformation function to each.

    This is a generic traversal utility that handles errors safely using the
    returns library. Short-circuits on first error.

    Args:
        roots: Sequence of semantic table roots
        transform: Function to apply to each root (root -> Result[T, Exception])

    Returns:
        Result containing list of successful transformations or first error
    """
    # Use railway-oriented programming with .bind for proper error propagation
    def accumulate_result(acc_result: Result[list[Any], Exception], root: Any) -> Result[list[Any], Exception]:
        # Short-circuit if already failed
        return acc_result.bind(
            lambda acc_list: transform(root).map(lambda value: acc_list + [value])
        )

    return functools_reduce(accumulate_result, roots, Success([]))


def extract_column_from_dimension(dimension: Any, table: Any) -> Maybe[str]:
    """
    Extract the column name accessed by a dimension expression.

    Handles both Deferred expressions (_.column) and regular callables (lambda t: t.column).
    Uses the returns library for safe extraction without exceptions.

    Args:
        dimension: The dimension object or callable
        table: The table to resolve against

    Returns:
        Maybe[str] containing the column name if successful, Nothing otherwise
    """
    from .ops import _extract_columns_from_callable, _is_deferred

    expr = dimension.expr if hasattr(dimension, "expr") else dimension

    if _is_deferred(expr):
        return _safe_extract_from_deferred(expr, table)

    if callable(expr):
        extraction_result = _extract_columns_from_callable(expr, table)
        if extraction_result.is_success() and extraction_result.columns:
            return Some(next(iter(extraction_result.columns)))

    return Nothing


@safe
def _extract_from_deferred(deferred_expr: Any, table: Any) -> str:
    resolved = deferred_expr.resolve(table)
    if hasattr(resolved, "get_name"):
        return resolved.get_name()
    raise ValueError("No get_name method")


def _safe_extract_from_deferred(deferred_expr: Any, table: Any) -> Maybe[str]:
    """Safely extract column name from deferred expression."""
    result = _extract_from_deferred(deferred_expr, table)
    return result.map(Some).value_or(Nothing)


def build_column_index_from_roots(
    roots: Sequence[Any],
) -> Result[dict[str, list[int]], Exception]:
    """
    Build an index of which columns appear in which tables.

    This is a generic utility for tracking column occurrences across joined tables,
    which is essential for determining when Ibis will rename columns with _right suffix.

    Args:
        roots: Sequence of semantic table roots in join order

    Returns:
        Result containing dict mapping column names to list of table indices,
        or Failure if table extraction fails
    """

    def process_root(acc_result, idx_root_pair):
        idx, root = idx_root_pair

        if not hasattr(root, "name") or not root.name:
            return acc_result

        table = root.to_untagged()
        return acc_result.map(
            lambda column_index: _update_column_index(column_index, table, idx)
        )

    def _update_column_index(column_index, table, idx):
        """Update column index with table columns."""
        for col in table.columns:
            if col not in column_index:
                column_index[col] = []
            column_index[col].append(idx)
        return column_index

    return functools_reduce(process_root, enumerate(roots), Success({}))
