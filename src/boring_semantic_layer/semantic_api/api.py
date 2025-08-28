from __future__ import annotations

from typing import Any, Callable

import ibis as ibis_mod
from ibis.expr.format import fmt as _fmt
from ibis.expr.sql import convert


from boring_semantic_layer.semantic_api.ops import (
    SemanticAggregate,
    SemanticFilter,
    SemanticGroupBy,
    SemanticMutate,
    SemanticProject,
    SemanticTable,
    SemanticJoin,
    SemanticOrderBy,
)


@_fmt.register(SemanticTable)
def _format_semantic_table(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticFilter)
def _format_semantic_filter(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticProject)
def _format_semantic_project(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticGroupBy)
def _format_semantic_group_by(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticAggregate)
def _format_semantic_aggregate(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticMutate)
def _format_semantic_mutate(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticJoin)
def _format_semantic_join(op, **kwargs):
    return op.__class__.__name__


@_fmt.register(SemanticOrderBy)
def _format_semantic_orderby(op, **kwargs):
    return op.__class__.__name__


IbisTable = ibis_mod.expr.api.Table


class SemanticTableExpr(IbisTable):
    __slots__ = ("_node",)

    def __init__(self, node: Any) -> None:
        object.__setattr__(self, "_node", node)

    def op(self):
        return self.to_expr().op()

    def to_expr(self) -> IbisTable:
        node = self._node
        if hasattr(node, "to_expr"):
            return node.to_expr()
        return node

    def to_ibis(self, catalog: dict[str, Any] | None = None) -> IbisTable:
        return convert(self.to_expr(), catalog=catalog or {})

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return self.to_ibis().execute(*args, **kwargs)

    def __repr__(self) -> str:
        try:
            return repr(self.to_expr())
        except AttributeError:
            return repr(self.to_expr().op())

    def __getattr__(self, name: str):
        return getattr(self.to_expr(), name)

    def with_dimensions(self, **dims: Callable) -> SemanticTableExpr:
        return with_dimensions(self, **dims)

    def with_measures(self, **meas: Callable) -> SemanticTableExpr:
        return with_measures(self, **meas)

    def group_by(self, *keys: str, **inline_dims: Callable) -> SemanticTableExpr:
        return group_by_(self, *keys, **inline_dims)

    def aggregate(self, *fns: Callable, **aggs: Callable) -> SemanticTableExpr:
        from .api import _infer_measure_name  # avoid circular

        if fns:
            if aggs:
                raise ValueError
            if len(fns) != 1:
                raise ValueError

            name = _infer_measure_name(fns[0])
            aggs = {name: fns[0]}
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

    def order_by(self, *keys: str) -> SemanticTableExpr:
        return order_by_(self, *keys)


def to_semantic_table(table: IbisTable) -> SemanticTableExpr:
    node = SemanticTable(table=table, dimensions={}, measures={})
    return SemanticTableExpr(node)


def with_dimensions(table: IbisTable, **dimensions: Callable) -> SemanticTableExpr:
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_dims = {**getattr(node, "dimensions", {}), **dimensions}
    node = SemanticTable(
        table=node.table.to_expr(),
        dimensions=new_dims,
        measures=getattr(node, "measures", {}),
    )
    return SemanticTableExpr(node)


def with_measures(table: IbisTable, **measures: Callable) -> SemanticTableExpr:
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_meas = {**getattr(node, "measures", {}), **measures}
    node = SemanticTable(
        table=node.table.to_expr(),
        dimensions=getattr(node, "dimensions", {}),
        measures=new_meas,
    )
    return SemanticTableExpr(node)


def where_(table: IbisTable, predicate: Callable) -> SemanticTableExpr:
    node = SemanticFilter(source=table.op(), predicate=predicate)
    return SemanticTableExpr(node)


def select_(table: IbisTable, *fields: str) -> SemanticTableExpr:
    node = SemanticProject(source=table.op(), fields=fields)
    return SemanticTableExpr(node)


def group_by_(
    table: IbisTable, *keys: str, **inline_dims: Callable
) -> SemanticTableExpr:
    # Detect if we have inline dimensions (regular Ibis style) or semantic dimensions
    if inline_dims:
        # Regular Ibis group_by with inline expressions
        # Add inline dimensions to the semantic table first, then group by them
        semantic_table = table
        for name, expr_fn in inline_dims.items():
            semantic_table = semantic_table.with_dimensions(**{name: expr_fn})

        # Collect all keys (both string keys and inline dimension names)
        all_keys = list(keys) + list(inline_dims.keys())

        # Use semantic group_by with all keys
        node = SemanticGroupBy(source=semantic_table.op(), keys=all_keys)
        return SemanticTableExpr(node)
    else:
        # Semantic group_by with predefined dimensions
        node = SemanticGroupBy(source=table.op(), keys=keys)
        return SemanticTableExpr(node)


def aggregate_(
    table: IbisTable, *fns: Callable, **measures: Callable
) -> SemanticTableExpr:
    from .api import _infer_measure_name

    if fns:
        if measures:
            raise ValueError("Cannot mix positional and named measure lambdas")
        if len(fns) != 1:
            raise ValueError(
                f"Expected exactly 1 positional measure lambda, got {len(fns)}"
            )
        measures = {_infer_measure_name(fns[0]): fns[0]}

    node = table.op()
    keys = getattr(node, "keys", ())
    node = SemanticAggregate(source=node, keys=keys, aggs=measures)
    return SemanticTableExpr(node)


def mutate_(table: IbisTable, **post_aggs: Callable) -> SemanticTableExpr:
    node = SemanticMutate(source=table.op(), post=post_aggs)
    return SemanticTableExpr(node)


def join_(
    left: IbisTable,
    right: IbisTable,
    how: str = "inner",
    on: Callable[[Any, Any], Any] | None = None,
) -> SemanticTableExpr:
    node = SemanticJoin(left=left.op(), right=right.op(), how=how, on=on)
    return SemanticTableExpr(node)


def join_one(
    left: IbisTable,
    right: IbisTable,
    left_on: str,
    right_on: str,
) -> SemanticTableExpr:
    return join_(
        left,
        right,
        how="inner",
        on=lambda left_tbl, right_tbl: getattr(left_tbl, left_on)
        == getattr(right_tbl, right_on),
    )


def join_many(
    left: IbisTable,
    right: IbisTable,
    left_on: str,
    right_on: str,
) -> SemanticTableExpr:
    return join_(
        left,
        right,
        how="left",
        on=lambda left_tbl, right_tbl: getattr(left_tbl, left_on)
        == getattr(right_tbl, right_on),
    )


def join_cross(left: IbisTable, right: IbisTable) -> SemanticTableExpr:
    return join_(left, right, how="cross", on=None)


def order_by_(table: IbisTable, *keys: str) -> SemanticTableExpr:
    node = SemanticOrderBy(source=table.op(), keys=keys)
    return SemanticTableExpr(node)


def _infer_measure_name(fn: Callable) -> str:
    names = fn.__code__.co_names
    unique = set(names)
    if len(unique) != 1:
        raise ValueError
    return next(iter(unique))


@convert.register(SemanticTableExpr)
def _lower_semantic_tableexpr(node: SemanticTableExpr, catalog, *args):
    return convert(node.to_expr(), catalog=catalog)
