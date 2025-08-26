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


IbisTable = ibis_mod.expr.api.Table


class SemanticTableExpr(IbisTable):
    __slots__ = ("_expr",)

    def __init__(self, expr: IbisTable) -> None:
        # FIXME: HACK to work around immutability of ibis expressions
        object.__setattr__(self, "_expr", expr)

    def op(self):
        return self._expr.op()

    def to_expr(self) -> IbisTable:
        return self._expr

    def to_ibis(self, catalog: dict[str, Any] | None = None) -> IbisTable:
        return convert(self._expr, catalog=catalog or {})

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return self.to_ibis().execute(*args, **kwargs)

    def __repr__(self) -> str:
        # FIXME
        try:
            return repr(self._expr)
        except AttributeError:
            return repr(self._expr.op())

    def __getattr__(self, name: str):
        return getattr(self._expr, name)

    def with_dimensions(self, **dims: Callable) -> SemanticTableExpr:
        return with_dimensions(self, **dims)

    def with_measures(self, **meas: Callable) -> SemanticTableExpr:
        return with_measures(self, **meas)

    def group_by(self, *keys: str) -> SemanticTableExpr:
        return group_by_(self, *keys)

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


def to_semantic_table(table: IbisTable) -> SemanticTableExpr:
    expr = SemanticTable(table=table, dimensions={}, measures={}).to_expr()
    return SemanticTableExpr(expr)


def with_dimensions(table: IbisTable, **dimensions: Callable) -> IbisTable:
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
    expr = SemanticFilter(source=table.op(), predicate=predicate).to_expr()
    return SemanticTableExpr(expr)


def select_(table: IbisTable, *fields: str) -> IbisTable:
    expr = SemanticProject(source=table.op(), fields=fields).to_expr()
    return SemanticTableExpr(expr)


def group_by_(table: IbisTable, *keys: str) -> IbisTable:
    expr = SemanticGroupBy(source=table.op(), keys=keys).to_expr()
    return SemanticTableExpr(expr)


def aggregate_(table: IbisTable, *fns: Callable, **measures: Callable) -> IbisTable:
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
    expr = SemanticAggregate(source=node, keys=keys, aggs=measures).to_expr()
    return SemanticTableExpr(expr)


def mutate_(table: IbisTable, **post_aggs: Callable) -> IbisTable:
    expr = SemanticMutate(source=table.op(), post=post_aggs).to_expr()
    return SemanticTableExpr(expr)


def join_(
    left: IbisTable,
    right: IbisTable,
    how: str = "inner",
    on: Callable[[Any, Any], Any] | None = None,
) -> IbisTable:

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
    expr = join_(
        left,
        right,
        how="left",
        on=lambda left_tbl, right_tbl: getattr(left_tbl, left_on)
        == getattr(right_tbl, right_on),
    )
    return expr


def join_cross(left: IbisTable, right: IbisTable) -> IbisTable:
    expr = join_(left, right, how="cross", on=None)
    return expr


def _infer_measure_name(fn: Callable) -> str:
    names = fn.__code__.co_names
    unique = set(names)
    if len(unique) != 1:
        raise ValueError
    return next(iter(unique))


@convert.register(SemanticTableExpr)
def _lower_semantic_tableexpr(node: SemanticTableExpr, catalog, *args):
    return convert(node._expr, catalog=catalog)
