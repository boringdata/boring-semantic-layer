"""Public API for boring-semantic-layer.

This module provides functional-style convenience functions for working with
semantic tables. All functions are thin wrappers around SemanticModel methods.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
    derived_dimensions: tuple[str, ...] | list[str] = (),
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
        derived_dimensions: Optional derived dimension parts to auto-generate from this
            dimension when added to a model (supported: year, month, day).

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
        ...             derived_dimensions=("year", "month", "day"),
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
        derived_dimensions=tuple(derived_dimensions or ()),
    )
