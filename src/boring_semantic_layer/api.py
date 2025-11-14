"""Public API for boring-semantic-layer.

This module provides functional-style convenience functions for working with
semantic tables. All functions are thin wrappers around SemanticModel methods.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ibis.expr.types as ir

from .expr import SemanticModel


def to_semantic_table(
    ibis_table: ir.Table, name: str | None = None, description: str | None = None
) -> SemanticModel:
    """Create a SemanticModel from an Ibis table.

    Args:
        ibis_table: An Ibis table expression
        name: Optional name for the semantic table
        description: Optional description for the semantic table

    Returns:
        A new SemanticModel wrapping the table
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
    left_on: str,
    right_on: str,
) -> SemanticModel:
    """Join two semantic tables with a one-to-one relationship.

    Args:
        left: Left semantic table
        other: Right semantic table
        left_on: Column name in left table
        right_on: Column name in right table

    Returns:
        Joined SemanticModel
    """
    return left.join_one(other, left_on, right_on)


def join_many(
    left: SemanticModel,
    other: SemanticModel,
    left_on: str,
    right_on: str,
) -> SemanticModel:
    """Join two semantic tables with a one-to-many relationship.

    Args:
        left: Left semantic table
        other: Right semantic table
        left_on: Column name in left table
        right_on: Column name in right table

    Returns:
        Joined SemanticModel
    """
    return left.join_many(other, left_on, right_on)


def join_cross(left: SemanticModel, other: SemanticModel) -> SemanticModel:
    """Cross join two semantic tables.

    Args:
        left: Left semantic table
        other: Right semantic table

    Returns:
        Cross-joined SemanticModel
    """
    return left.join_cross(other)


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
        **kwargs: Named column expressions

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
