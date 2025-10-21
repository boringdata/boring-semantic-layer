"""
API utilities for chainable semantic table operations.
"""

from typing import Any, Callable

from .table import SemanticTable


def join_one(
    left: SemanticTable, other: SemanticTable, left_on: str, right_on: str
) -> SemanticTable:
    """Inner join one-to-one or many-to-one on primary/foreign keys."""
    return left.join_one(other, left_on, right_on)


def join_many(
    left: SemanticTable, other: SemanticTable, left_on: str, right_on: str
) -> SemanticTable:
    """Left join one-to-many on primary/foreign keys."""
    return left.join_many(other, left_on, right_on)


def join_cross(left: SemanticTable, other: SemanticTable) -> SemanticTable:
    """Cross join two semantic tables."""
    return left.join_cross(other)


def filter_(table: SemanticTable, predicate: Callable[[Any], Any]) -> Any:
    """Filter rows in a semantic table based on a predicate."""
    return table.filter(predicate)


def group_by_(table: SemanticTable, *dims: str) -> SemanticTable:
    """Group a semantic table by one or more dimensions."""
    return table.group_by(*dims)


def aggregate_(table: SemanticTable, *measure_names: str, **aliased: str) -> Any:
    """Aggregate a grouped semantic table by measures."""
    return table.aggregate(*measure_names, **aliased)


def mutate_(table: Any, **kwargs: Callable[[Any], Any]) -> Any:
    """Add or modify columns in a semantic or ibis table expression."""
    return table.mutate(**kwargs)


def order_by_(table: Any, *keys: Any) -> Any:
    """Order rows in a semantic or ibis table expression."""
    return table.order_by(*keys)


def limit_(table: Any, n: int) -> Any:
    """Limit number of rows in a semantic or ibis table expression."""
    return table.limit(n)
