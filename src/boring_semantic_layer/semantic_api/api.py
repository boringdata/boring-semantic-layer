"""
Port of xorq.semantic.api to standalone ibis package.
"""

from __future__ import annotations

from typing import Any, Callable

from boring_semantic_layer.semantic_api.ops import (
    SemanticAggregate,
    SemanticFilter,
    SemanticGroupBy,
    SemanticMutate,
    SemanticProject,
    SemanticTable,
)
from ibis.expr.api import Table as IbisTable


def to_semantic_table(table: IbisTable) -> IbisTable:
    """Initialize an empty SemanticTable over an Ibis table."""
    return SemanticTable(table=table, dimensions={}, measures={}).to_expr()


def with_dimensions(table: IbisTable, **dimensions: Callable) -> IbisTable:
    """Attach or extend dimension lambdas (name -> fn(table) -> column)."""
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_dims = {**getattr(node, "dimensions", {}), **dimensions}
    return SemanticTable(
        table=node.table.to_expr(),
        dimensions=new_dims,
        measures=getattr(node, "measures", {}),
    ).to_expr()


def with_measures(table: IbisTable, **measures: Callable) -> IbisTable:
    """Attach or extend measure lambdas (name -> fn(table) -> scalar/agg)."""
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_meas = {**getattr(node, "measures", {}), **measures}
    return SemanticTable(
        table=node.table.to_expr(),
        dimensions=getattr(node, "dimensions", {}),
        measures=new_meas,
    ).to_expr()


def where_(table: IbisTable, predicate: Callable) -> IbisTable:
    """Add a semantic filter node to the AST."""
    return SemanticFilter(source=table.op(), predicate=predicate).to_expr()


def select_(table: IbisTable, *fields: str) -> IbisTable:
    """Add a semantic projection of named dimensions/measures."""
    return SemanticProject(source=table.op(), fields=fields).to_expr()


def group_by_(table: IbisTable, *keys: str) -> IbisTable:
    """Add a semantic GROUP BY marker."""
    return SemanticGroupBy(source=table.op(), keys=keys).to_expr()


def aggregate_(table: IbisTable, **measures: Callable) -> IbisTable:
    """Add a semantic AGGREGATE node."""
    node = table.op()
    keys = getattr(node, "keys", ())  # inherit keys if called after group_by_
    return SemanticAggregate(source=node, keys=keys, aggs=measures).to_expr()


def mutate_(table: IbisTable, **post_aggs: Callable) -> IbisTable:
    """Add a post-aggregation semantic MUTATE node."""
    return SemanticMutate(source=table.op(), post=post_aggs).to_expr()


def join_(
    left: IbisTable,
    right: IbisTable,
    how: str = "inner",
    on: Callable[[Any, Any], Any] | None = None,
) -> IbisTable:
    """Add a semantic JOIN between two semantic tables."""
    from boring_semantic_layer.semantic_api.ops import SemanticJoin

    lnode = left.op()
    rnode = right.op()
    return SemanticJoin(left=lnode, right=rnode, how=how, on=on).to_expr()


def join_one(
    left: IbisTable,
    right: IbisTable,
    left_on: str,
    right_on: str,
) -> IbisTable:
    """Declare a one-to-one (inner) join on specified keys."""
    return join_(
        left,
        right,
        how="inner",
        on=lambda l, r: getattr(l, left_on) == getattr(r, right_on),
    )


def join_many(
    left: IbisTable,
    right: IbisTable,
    left_on: str,
    right_on: str,
) -> IbisTable:
    """Declare a one-to-many (left) join on specified keys."""
    return join_(
        left,
        right,
        how="left",
        on=lambda l, r: getattr(l, left_on) == getattr(r, right_on),
    )


def join_cross(left: IbisTable, right: IbisTable) -> IbisTable:
    """Declare a cross join (cartesian product)."""
    return join_(left, right, how="cross", on=None)