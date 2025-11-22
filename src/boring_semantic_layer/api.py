"""Public API for boring-semantic-layer.

This module provides functional-style convenience functions for working with
semantic tables. All functions are thin wrappers around SemanticModel methods.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ibis.expr import types as ir

from .expr import SemanticModel


def to_semantic_table(
    ibis_table: ir.Table, name: str | None = None, description: str | None = None
) -> SemanticModel:
    """Create a SemanticModel from an Ibis table.

    Args:
        ibis_table: An Ibis table expression (can be regular ibis or xorq vendored ibis)
        name: Optional name for the semantic table
        description: Optional description for the semantic table

    Returns:
        A new SemanticModel wrapping the table

    Note:
        Tables are kept in their original form (regular ibis or xorq vendored ibis)
        throughout semantic operations. Conversion only happens if needed at execution time.
    """
    return SemanticModel(
        table=ibis_table,
        dimensions=None,
        measures=None,
        calc_measures=None,
        name=name,
        description=description,
    )


def join_one(
    left: SemanticModel,
    other: SemanticModel,
    on: Callable[[Any, Any], ir.BooleanValue],
    how: str = "inner",
) -> SemanticModel:
    """Join two semantic tables with a one-to-one relationship.

    Args:
        left: Left semantic table
        other: Right semantic table
        on: Lambda function taking (left, right) tables and returning a boolean condition
        how: Join type - "inner", "left", "right", or "outer" (default: "inner")

    Returns:
        Joined SemanticModel

    Examples:
        >>> join_one(orders, customers, lambda o, c: o.customer_id == c.customer_id)
        >>> join_one(orders, customers, lambda o, c: o.customer_id == c.customer_id, how="left")
        >>> join_one(orders, customers, lambda o, c: o.customer_id == c.customer_id, how="outer")
    """
    return left.join_one(other, on, how)


def join_many(
    left: SemanticModel,
    other: SemanticModel,
    on: Callable[[Any, Any], ir.BooleanValue],
    how: str = "left",
) -> SemanticModel:
    """Join two semantic tables with a one-to-many relationship.

    Args:
        left: Left semantic table
        other: Right semantic table
        on: Lambda function taking (left, right) tables and returning a boolean condition
        how: Join type - "inner", "left", "right", or "outer" (default: "left")

    Returns:
        Joined SemanticModel

    Examples:
        >>> join_many(customer, orders, lambda c, o: c.customer_id == o.customer_id)
        >>> join_many(customer, orders, lambda c, o: c.customer_id == o.customer_id, how="inner")
        >>> join_many(customer, orders, lambda c, o: c.customer_id == o.customer_id, how="right")
    """
    return left.join_many(other, on, how)


def filter_(
    table: SemanticModel,
    predicate: Callable[[ir.Table], ir.BooleanValue],
) -> SemanticModel:
    """Filter a semantic table by a predicate.

    Args:
        table: Semantic table to filter
        predicate: Function that takes a table and returns a boolean expression

    Returns:
        Filtered SemanticModel
    """
    return table.filter(predicate)


def group_by_(table: SemanticModel, *dims: str) -> SemanticModel:
    """Group a semantic table by dimensions.

    Args:
        table: Semantic table to group
        *dims: Dimension names to group by

    Returns:
        Grouped SemanticModel
    """
    return table.group_by(*dims)


def aggregate_(
    table: SemanticModel,
    *measure_names: str,
    **aliased: str,
) -> SemanticModel:
    """Aggregate measures in a semantic table.

    Args:
        table: Semantic table to aggregate
        *measure_names: Names of measures to aggregate
        **aliased: Aliased measure aggregations

    Returns:
        Aggregated SemanticModel
    """
    return table.aggregate(*measure_names, **aliased)


def mutate_(
    table: SemanticModel,
    **kwargs: Callable[[ir.Table], ir.Value],
) -> SemanticModel:
    """Add computed columns to a semantic table.

    Args:
        table: Semantic table to mutate
        **kwargs: Named column expressions (xorq vendored ibis expressions)

    Returns:
        Mutated SemanticModel
    """
    return table.mutate(**kwargs)


def order_by_(table: SemanticModel, *keys: str | ir.Value) -> SemanticModel:
    """Order a semantic table by keys.

    Args:
        table: Semantic table to order
        *keys: Column names or expressions to order by

    Returns:
        Ordered SemanticModel
    """
    return table.order_by(*keys)


def limit_(table: SemanticModel, n: int) -> SemanticModel:
    """Limit the number of rows in a semantic table.

    Args:
        table: Semantic table to limit
        n: Maximum number of rows

    Returns:
        Limited SemanticModel
    """
    return table.limit(n)
