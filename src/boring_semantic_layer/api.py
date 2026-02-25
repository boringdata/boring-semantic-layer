"""Public API for boring-semantic-layer.

This module provides functional-style convenience functions for working with
semantic tables. All functions are thin wrappers around SemanticModel methods.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ibis.common.deferred import Deferred
    from ibis.expr import types as ir

from .expr import SemanticModel
from .ops import Dimension


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
    on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    how: str = "left",
) -> SemanticModel:
    """Join two semantic tables with a one-to-one relationship.

    Args:
        left: Left semantic table
        other: Right semantic table
        on: Join predicate. Accepts a lambda ``(left, right) -> bool``, a column
            name string, a Deferred ``_.col``, or a list of strings/Deferred for
            compound equi-joins.
        how: Join type - "inner", "left", "right", or "outer" (default: "left")

    Returns:
        Joined SemanticModel

    Examples:
        >>> join_one(orders, customers, on="customer_id")
        >>> join_one(orders, customers, on=_.customer_id)
        >>> join_one(orders, customers, on=lambda o, c: o.customer_id == c.customer_id)
    """
    return left.join_one(other, on, how)


def join_many(
    left: SemanticModel,
    other: SemanticModel,
    on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    how: str = "left",
) -> SemanticModel:
    """Join two semantic tables with a one-to-many relationship.

    Args:
        left: Left semantic table
        other: Right semantic table
        on: Join predicate. Accepts a lambda ``(left, right) -> bool``, a column
            name string, a Deferred ``_.col``, or a list of strings/Deferred for
            compound equi-joins.
        how: Join type - "inner", "left", "right", or "outer" (default: "left")

    Returns:
        Joined SemanticModel

    Examples:
        >>> join_many(customer, orders, on="customer_id")
        >>> join_many(customer, orders, on=_.customer_id)
        >>> join_many(customer, orders, on=lambda c, o: c.customer_id == o.customer_id)
    """
    return left.join_many(other, on, how)


def join_cross(left: SemanticModel, other: SemanticModel) -> SemanticModel:
    """Cross join (Cartesian product) two semantic tables.

    Args:
        left: Left semantic table
        other: Right semantic table

    Returns:
        Joined SemanticModel (Cartesian product of all rows)

    Examples:
        >>> join_cross(table_a, table_b)  # All combinations of rows
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


def group_by_(table: SemanticModel, *dims: str | Deferred) -> SemanticModel:
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
    *measure_names: str | Callable | Deferred,
    **aliased,
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


def entity_dimension(
    expr: Callable[[ir.Table], ir.Value],
    description: str | None = None,
) -> Dimension:
    """Create an entity dimension (join key/identifier).

    Entity dimensions represent the primary entities in your feature view,
    similar to Feast's entity concept. These are typically used as join keys
    (e.g., business_id, user_id, customer_id).

    Args:
        expr: Lambda function that extracts the entity column from a table
        description: Optional description of the entity dimension

    Returns:
        Dimension marked as an entity

    Examples:
        >>> from boring_semantic_layer import entity_dimension, to_semantic_table
        >>> model = (
        ...     to_semantic_table(table, name="features")
        ...     .with_dimensions(
        ...         business_id=entity_dimension(lambda t: t.business_id),
        ...         user_id=entity_dimension(lambda t: t.user_id, "User identifier"),
        ...     )
        ... )
    """
    return Dimension(
        expr=expr,
        description=description,
        is_entity=True,
    )


def time_dimension(
    expr: Callable[[ir.Table], ir.Value],
    description: str | None = None,
    smallest_time_grain: str | None = None,
) -> Dimension:
    """Create an event timestamp dimension for point-in-time correctness.

    Event timestamp dimensions represent the primary temporal field for
    feature engineering and point-in-time joins, similar to Feast's
    event_timestamp. Unlike regular time dimensions (is_time_dimension),
    this marks THE canonical timestamp for the feature view.

    Args:
        expr: Lambda function that extracts the timestamp column from a table
        description: Optional description of the time dimension
        smallest_time_grain: Optional time granularity (e.g., "TIME_GRAIN_DAY", "TIME_GRAIN_HOUR")

    Returns:
        Dimension marked as an event timestamp

    Examples:
        >>> from boring_semantic_layer import time_dimension, to_semantic_table
        >>> model = (
        ...     to_semantic_table(table, name="features")
        ...     .with_dimensions(
        ...         statement_date=time_dimension(
        ...             lambda t: t.statement_date,
        ...             "Statement date for balance features",
        ...             smallest_time_grain="TIME_GRAIN_DAY",
        ...         ),
        ...     )
        ... )
    """
    return Dimension(
        expr=expr,
        description=description,
        is_event_timestamp=True,
        is_time_dimension=bool(smallest_time_grain),
        smallest_time_grain=smallest_time_grain,
    )
