"""Graph utilities with functional programming support.

This module re-exports xorq's graph utilities and adds functional wrappers
using the returns library for safer error handling.
"""

from typing import Any

# Import from xorq (we'll wrap some of these)
from xorq.common.utils.graph_utils import (
    gen_children_of as _xorq_gen_children_of,
    replace_nodes as _xorq_replace_nodes,
    to_node as _xorq_to_node,
)
from xorq.vendor.ibis.common.graph import Graph
from xorq.vendor.ibis.expr.operations.core import Node
from xorq.vendor.ibis.expr.types import Expr as XorqExpr

# Regular ibis types (optional - for backward compatibility)
try:
    from ibis.expr.operations.core import Node as IbisNode
    from ibis.expr.types import Expr as IbisExpr
    _REGULAR_IBIS_AVAILABLE = True
except ImportError:
    IbisNode = None
    IbisExpr = None
    _REGULAR_IBIS_AVAILABLE = False

# Functional programming utilities
from returns.maybe import Maybe, Nothing, Some
from returns.result import Failure, Result, Success

# Re-export for compatibility
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
    "is_feature_view",
    "Graph",
    "Node",
]


def to_node(maybe_expr: Any) -> Node:
    """Convert expression to Node, handling both ibis and xorq types.

    This wrapper extends xorq's to_node to also handle regular ibis types.

    Args:
        maybe_expr: Value to convert to a Node (can be Node, Expr from ibis or xorq)

    Returns:
        A Node instance

    Raises:
        ValueError: If the type cannot be converted to a Node
    """
    # First check if it's regular ibis types (since xorq's to_node doesn't handle them)
    if _REGULAR_IBIS_AVAILABLE:
        if isinstance(maybe_expr, IbisNode):
            return maybe_expr
        if isinstance(maybe_expr, IbisExpr):
            return maybe_expr.op()

    # Fall back to xorq's implementation for xorq types
    return _xorq_to_node(maybe_expr)


def gen_children_of(node: Node) -> tuple[Node, ...]:
    """Generate children nodes from a node.

    This wraps the node's __children__ and converts them to Nodes.

    Args:
        node: A Node to get children from

    Returns:
        Tuple of child Nodes
    """
    children = getattr(node, "__children__", ())
    result = []
    for child in children:
        try:
            result.append(to_node(child))
        except (ValueError, AttributeError):
            # Skip children that can't be converted
            pass
    return tuple(result)


def bfs(expr) -> Graph:
    """Perform breadth-first search traversal.

    Handles both ibis and xorq types by using our wrapped functions.

    Args:
        expr: An expression or node to traverse

    Returns:
        Graph mapping each node to its children
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
    """Walk the expression tree yielding nodes of specified types.

    Handles both ibis and xorq types.

    Args:
        node_types: Type or tuple of types to match
        expr: Expression to walk

    Yields:
        Nodes matching the specified types
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
        stack.extend(c for c in gen_children_of(node) if c not in visited)


def replace_nodes(replacer, expr):
    """Replace nodes in expression tree using a replacer function.

    This wrapper ensures the result is converted back to an Expr.

    Args:
        replacer: Function taking (op, kwargs) and returning replacement op
        expr: Expression to transform

    Returns:
        Transformed expression
    """
    node = to_node(expr)
    result = _xorq_replace_nodes(replacer, node)
    # xorq's replace_nodes returns a Node, we need to convert to Expr
    return result.to_expr()


def to_node_safe(maybe_expr: Any) -> Result[Node, ValueError]:
    """Convert expression to Node, returning Result instead of raising.

    This is a functional wrapper around to_node that returns a Result type
    for safer error handling without exceptions.

    Args:
        maybe_expr: Value to convert to a Node

    Returns:
        Success[Node] if conversion succeeds, Failure[ValueError] otherwise
    """
    try:
        return Success(to_node(maybe_expr))
    except ValueError as e:
        return Failure(e)


def try_to_node(child: Any) -> Maybe[Node]:
    """Try to convert child to Node, returning Maybe.

    This is a functional wrapper that uses Maybe monad for optional values.

    Args:
        child: Value to try converting to a Node

    Returns:
        Some[Node] if conversion succeeds, Nothing otherwise
    """
    return to_node_safe(child).map(Some).value_or(Nothing)


def find_dimensions_and_measures(expr: IbisExpr | XorqExpr) -> tuple[dict[str, Any], dict[str, Any]]:
    """Find all dimensions and measures in a semantic model expression.

    Args:
        expr: An ibis expression containing semantic model references

    Returns:
        Tuple of (dimensions_dict, measures_dict) extracted from the expression
    """
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


def find_entity_dimensions(expr: IbisExpr | XorqExpr) -> dict[str, Any]:
    """Find all entity dimensions in a semantic model expression.

    Entity dimensions are those marked with is_entity=True, representing
    join keys or primary identifiers (e.g., business_id, user_id).

    Args:
        expr: An ibis expression containing semantic model references

    Returns:
        Dictionary of entity dimension names to Dimension objects

    Examples:
        >>> from boring_semantic_layer import entity_dimension, to_semantic_table
        >>> model = to_semantic_table(tbl).with_dimensions(
        ...     business_id=entity_dimension(lambda t: t.business_id),
        ...     user_id=entity_dimension(lambda t: t.user_id),
        ... )
        >>> entities = find_entity_dimensions(model.to_expr())
        >>> list(entities.keys())
        ['business_id', 'user_id']
    """
    from .ops import (
        _find_all_root_models,
        _get_field_dict,
        _merge_fields_with_prefixing,
    )

    roots = _find_all_root_models(to_node(expr))
    all_dimensions = _merge_fields_with_prefixing(roots, lambda r: _get_field_dict(r, "dimensions"))

    # Filter to only entity dimensions
    return {name: dim for name, dim in all_dimensions.items() if dim.is_entity}


def find_event_timestamp_dimensions(expr: IbisExpr | XorqExpr) -> dict[str, Any]:
    """Find all event timestamp dimensions in a semantic model expression.

    Event timestamp dimensions are those marked with is_event_timestamp=True,
    representing the primary temporal field for point-in-time correctness.

    Args:
        expr: An ibis expression containing semantic model references

    Returns:
        Dictionary of event timestamp dimension names to Dimension objects

    Examples:
        >>> from boring_semantic_layer import time_dimension, to_semantic_table
        >>> model = to_semantic_table(tbl).with_dimensions(
        ...     statement_date=time_dimension(lambda t: t.statement_date),
        ... )
        >>> timestamps = find_event_timestamp_dimensions(model.to_expr())
        >>> list(timestamps.keys())
        ['statement_date']
    """
    from .ops import (
        _find_all_root_models,
        _get_field_dict,
        _merge_fields_with_prefixing,
    )

    roots = _find_all_root_models(to_node(expr))
    all_dimensions = _merge_fields_with_prefixing(roots, lambda r: _get_field_dict(r, "dimensions"))

    # Filter to only event timestamp dimensions
    return {name: dim for name, dim in all_dimensions.items() if dim.is_event_timestamp}


def is_feature_view(expr: IbisExpr | XorqExpr) -> bool:
    """Check if a semantic model is a valid FeatureView.

    A FeatureView is defined as a semantic model with:
    - Exactly 1 entity dimension (join key)
    - Exactly 1 event timestamp dimension (for point-in-time correctness)

    This matches the Feast FeatureView concept where features are organized
    around a single entity and timestamp for feature engineering.

    Args:
        expr: An ibis expression containing semantic model references

    Returns:
        True if the model has exactly 1 entity and 1 event timestamp, False otherwise

    Examples:
        >>> from boring_semantic_layer import entity_dimension, time_dimension, to_semantic_table
        >>> # Valid FeatureView
        >>> model = (
        ...     to_semantic_table(tbl, name="balance_features")
        ...     .with_dimensions(
        ...         business_id=entity_dimension(lambda t: t.business_id),
        ...         statement_date=time_dimension(lambda t: t.statement_date),
        ...     )
        ... )
        >>> is_feature_view(model)
        True

        >>> # Not a FeatureView (multiple entities)
        >>> model2 = (
        ...     to_semantic_table(tbl)
        ...     .with_dimensions(
        ...         business_id=entity_dimension(lambda t: t.business_id),
        ...         user_id=entity_dimension(lambda t: t.user_id),
        ...         statement_date=time_dimension(lambda t: t.statement_date),
        ...     )
        ... )
        >>> is_feature_view(model2)
        False
    """
    entities = find_entity_dimensions(expr)
    timestamps = find_event_timestamp_dimensions(expr)

    # Must have exactly 1 of each
    return len(entities) == 1 and len(timestamps) == 1
