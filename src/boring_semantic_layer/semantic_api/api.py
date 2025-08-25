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


class SemanticTableExpr(IbisTable):
    """Wrapper around a semantic-table Ibis Expr enabling the semantic-DSL methods."""

    __slots__ = ("_expr",)

    def __init__(self, expr: IbisTable) -> None:
        # Bypass immutability to set the underlying expr
        object.__setattr__(self, "_expr", expr)

    def op(self):
        return self._expr.op()

    def to_expr(self) -> IbisTable:
        return self._expr

    def __repr__(self) -> str:
        return repr(self._expr)

    def __getattr__(self, name: str):
        return getattr(self._expr, name)

    # Semantic-DSL instance methods:
    def with_dimensions(self, **dims: Callable) -> SemanticTableExpr:
        return with_dimensions(self, **dims)

    def with_measures(self, **meas: Callable) -> SemanticTableExpr:
        return with_measures(self, **meas)

    def group_by(self, *keys: str) -> SemanticTableExpr:
        return group_by_(self, *keys)

    def aggregate(self, **aggs: Callable) -> SemanticTableExpr:
        return aggregate_(self, **aggs)

    def mutate(self, **post_aggs: Callable) -> SemanticTableExpr:
        return mutate_(self, **post_aggs)

    def filter(self, predicate: Callable) -> SemanticTableExpr:
        return where_(self, predicate)

    def where(self, predicate: Callable) -> SemanticTableExpr:
        return where_(self, predicate)

    def select(self, *fields: str) -> SemanticTableExpr:
        return select_(self, *fields)

    def join_one(
        self, other: IbisTable, left_on: str, right_on: str
    ) -> SemanticTableExpr:
        return join_one(self, other, left_on, right_on)

    def join_many(
        self, other: IbisTable, left_on: str, right_on: str
    ) -> SemanticTableExpr:
        return join_many(self, other, left_on, right_on)

    def join_cross(self, other: IbisTable) -> SemanticTableExpr:
        return join_cross(self, other)

    def join(
        self, other: IbisTable, how: str = "inner", on: Callable | None = None
    ) -> SemanticTableExpr:
        return join_(self, other, how=how, on=on)


def to_semantic_table(table: IbisTable) -> IbisTable:
    """Initialize an empty SemanticTable over an Ibis table."""
    expr = SemanticTable(table=table, dimensions={}, measures={}).to_expr()
    return SemanticTableExpr(expr)


def with_dimensions(table: IbisTable, **dimensions: Callable) -> IbisTable:
    """Attach or extend dimension lambdas (name -> fn(table) -> column)."""
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_dims = {**getattr(node, "dimensions", {}), **dimensions}
    expr = SemanticTable(
        table=node.table.to_expr(),
        dimensions=new_dims,
        measures=getattr(node, "measures", {}),
    ).to_expr()
    return SemanticTableExpr(expr)


def with_measures(table: IbisTable, **measures: Callable) -> IbisTable:
    """Attach or extend measure lambdas (name -> fn(table) -> scalar/agg)."""
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_meas = {**getattr(node, "measures", {}), **measures}
    expr = SemanticTable(
        table=node.table.to_expr(),
        dimensions=getattr(node, "dimensions", {}),
        measures=new_meas,
    ).to_expr()
    return SemanticTableExpr(expr)


def where_(table: IbisTable, predicate: Callable) -> IbisTable:
    """Add a semantic filter node to the AST."""
    expr = SemanticFilter(source=table.op(), predicate=predicate).to_expr()
    return SemanticTableExpr(expr)


def select_(table: IbisTable, *fields: str) -> IbisTable:
    """Add a semantic projection of named dimensions/measures."""
    expr = SemanticProject(source=table.op(), fields=fields).to_expr()
    return SemanticTableExpr(expr)


def group_by_(table: IbisTable, *keys: str) -> IbisTable:
    """Add a semantic GROUP BY marker."""
    expr = SemanticGroupBy(source=table.op(), keys=keys).to_expr()
    return SemanticTableExpr(expr)


def aggregate_(table: IbisTable, **measures: Callable) -> IbisTable:
    """Add a semantic AGGREGATE node."""
    node = table.op()
    keys = getattr(node, "keys", ())
    expr = SemanticAggregate(source=node, keys=keys, aggs=measures).to_expr()
    return SemanticTableExpr(expr)


def mutate_(table: IbisTable, **post_aggs: Callable) -> IbisTable:
    """Add a post-aggregation semantic MUTATE node."""
    expr = SemanticMutate(source=table.op(), post=post_aggs).to_expr()
    return SemanticTableExpr(expr)


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
    expr = SemanticJoin(left=lnode, right=rnode, how=how, on=on).to_expr()
    return SemanticTableExpr(expr)


def join_one(
    left: IbisTable,
    right: IbisTable,
    left_on: str,
    right_on: str,
) -> IbisTable:
    """Declare a one-to-one (inner) join on specified keys."""
    expr = join_(
        left,
        right,
        how="inner",
        on=lambda left_tbl, right_tbl: getattr(left_tbl, left_on)
        == getattr(right_tbl, right_on),
    )
    return expr


def join_many(
    left: IbisTable,
    right: IbisTable,
    left_on: str,
    right_on: str,
) -> IbisTable:
    """Declare a one-to-many (left) join on specified keys."""
    expr = join_(
        left,
        right,
        how="left",
        on=lambda left_tbl, right_tbl: getattr(left_tbl, left_on)
        == getattr(right_tbl, right_on),
    )
    return expr


def join_cross(left: IbisTable, right: IbisTable) -> IbisTable:
    """Declare a cross join (cartesian product)."""
    expr = join_(left, right, how="cross", on=None)
    return expr
