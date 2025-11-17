from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import reduce
from operator import attrgetter
from typing import Any

import ibis
from ibis.common.collections import FrozenDict
from ibis.expr import types as ir
from ibis.expr.types import Table
from ibis.expr.types.groupby import GroupedTable
from returns.result import Success, safe

from .chart import chart as create_chart
from .measure_scope import AggregationExpr, MeasureScope
from .ops import (
    Dimension,
    Measure,
    SemanticAggregateOp,
    SemanticFilterOp,
    SemanticGroupByOp,
    SemanticIndexOp,
    SemanticJoinOp,
    SemanticLimitOp,
    SemanticMutateOp,
    SemanticOrderByOp,
    SemanticProjectOp,
    SemanticTableOp,
    SemanticUnnestOp,
    _classify_measure,
    _find_all_root_models,
    _get_merged_fields,
)
from .query import query as build_query


def to_ibis(expr):
    if isinstance(expr, SemanticTable):
        return expr.op().to_ibis()

    result = safe(lambda: expr.to_ibis())()
    if isinstance(result, Success):
        return result.unwrap()

    raise TypeError(f"Cannot convert {type(expr)} to Ibis expression")


class SemanticTable(ir.Table):
    @property
    def graph(self):
        """Get the dependency graph for this semantic table.

        Returns the dependency graph showing how dimensions and measures
        relate to each other. This works on all semantic table types
        (SemanticModel, joins, filters, group_by, etc.).

        For joins, this merges graphs from both left and right sides.
        For filters/limits/ordering, this returns the graph from the source.

        Returns:
            DependencyGraph with predecessors() and successors() methods
        """
        from .dependency_graph import DependencyGraph

        op = self.op()

        # For SemanticModel, get cached graph from the op
        if hasattr(op, "graph"):
            return op.graph

        # For joins, merge graphs from left and right ops with prefixing
        if hasattr(op, "left") and hasattr(op, "right"):
            merged = DependencyGraph()

            # Add left graph with prefixes (both field names and their dependencies)
            if hasattr(op.left, "graph") and hasattr(op.left, "name"):
                left_name = op.left.name
                for field_name, field_data in op.left.graph.items():
                    prefixed_name = f"{left_name}.{field_name}" if left_name else field_name
                    # Also prefix the dependencies
                    prefixed_deps = {
                        f"{left_name}.{dep_name}" if left_name else dep_name: dep_type
                        for dep_name, dep_type in field_data["deps"].items()
                    }
                    merged[prefixed_name] = {
                        "deps": prefixed_deps,
                        "type": field_data["type"]
                    }

            # Add right graph with prefixes (both field names and their dependencies)
            if hasattr(op.right, "graph") and hasattr(op.right, "name"):
                right_name = op.right.name
                for field_name, field_data in op.right.graph.items():
                    prefixed_name = f"{right_name}.{field_name}" if right_name else field_name
                    # Also prefix the dependencies
                    prefixed_deps = {
                        f"{right_name}.{dep_name}" if right_name else dep_name: dep_type
                        for dep_name, dep_type in field_data["deps"].items()
                    }
                    merged[prefixed_name] = {
                        "deps": prefixed_deps,
                        "type": field_data["type"]
                    }

            return merged

        # For pass-through nodes (filter, limit, order_by, group_by, aggregate), get graph from source
        # Walk the node tree to find any node with a graph attribute
        from .graph_utils import walk_nodes
        from .ops import SemanticTableOp

        for node in walk_nodes((SemanticTableOp,), self):
            if hasattr(node, "graph"):
                return node.graph

        # Fallback to empty graph
        return DependencyGraph()

    def filter(self, predicate: Callable) -> SemanticFilter:
        return SemanticFilter(source=self.op(), predicate=predicate)

    def group_by(self, *keys: str):
        return SemanticGroupBy(source=self.op(), keys=keys)

    def aggregate(self, *measure_names, nest: dict[str, Callable] | None = None, **aliased):
        """Aggregate measures without grouping (produces a single row result).

        This is a convenience method that delegates to group_by().aggregate().

        Args:
            *measure_names: Measure names to aggregate
            nest: Optional nested aggregations
            **aliased: Optional aliased aggregations

        Returns:
            SemanticAggregate with no grouping keys
        """
        return self.group_by().aggregate(*measure_names, nest=nest, **aliased)

    def mutate(self, **post) -> SemanticMutate:
        return SemanticMutate(source=self.op(), post=post)

    def order_by(self, *keys: str | ir.Value | Callable):
        return SemanticOrderBy(source=self.op(), keys=keys)

    def limit(self, n: int, offset: int = 0):
        return SemanticLimit(source=self.op(), n=n, offset=offset)

    def unnest(self, column: str) -> SemanticUnnest:
        return SemanticUnnest(source=self.op(), column=column)

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def to_ibis(self):
        return self.op().to_ibis()

    def execute(self):
        return to_ibis(self).execute()

    def compile(self, **kwargs):
        return to_ibis(self).compile(**kwargs)

    def sql(self, **kwargs):
        return ibis.to_sql(to_ibis(self), **kwargs)


def _create_dimension(expr: Dimension | Callable | dict) -> Dimension:
    if isinstance(expr, Dimension):
        return expr
    if isinstance(expr, dict):
        return Dimension(
            expr=expr["expr"],
            description=expr.get("description"),
            is_time_dimension=expr.get("is_time_dimension", False),
            smallest_time_grain=expr.get("smallest_time_grain"),
        )
    return Dimension(expr=expr, description=None)


def _derive_name(table: Any) -> str | None:
    expr = safe(lambda: table.to_expr())().value_or(table)
    return safe(lambda: expr.get_name())().value_or(None)


def _build_semantic_model_from_roots(
    ibis_table: ir.Table,
    all_roots: tuple,
    field_filter: set | None = None,
) -> SemanticModel:
    if not all_roots:
        return SemanticModel(
            table=ibis_table,
            dimensions={},
            measures={},
            calc_measures={},
        )

    all_dims = _get_merged_fields(all_roots, "dimensions")
    all_measures = _get_merged_fields(all_roots, "measures")
    all_calc = _get_merged_fields(all_roots, "calc_measures")

    if field_filter is not None:
        all_dims = {k: v for k, v in all_dims.items() if k in field_filter}
        all_measures = {k: v for k, v in all_measures.items() if k in field_filter}
        all_calc = {k: v for k, v in all_calc.items() if k in field_filter}

    return SemanticModel(
        table=ibis_table,
        dimensions=all_dims,
        measures=all_measures,
        calc_measures=all_calc,
    )


class SemanticModel(SemanticTable):
    def __init__(
        self,
        table: Any,
        dimensions: Mapping[str, Dimension | Callable | dict] | None = None,
        measures: Mapping[str, Measure | Callable] | None = None,
        calc_measures: Mapping[str, Any] | None = None,
        name: str | None = None,
        description: str | None = None,
        _source_join: Any | None = None,
    ) -> None:
        dims = FrozenDict(
            {dim_name: _create_dimension(dim) for dim_name, dim in (dimensions or {}).items()},
        )

        meas = FrozenDict(
            {
                meas_name: measure
                if isinstance(measure, Measure)
                else Measure(expr=measure, description=None)
                for meas_name, measure in (measures or {}).items()
            },
        )

        calc_meas = FrozenDict(calc_measures or {})

        derived_name = name or _derive_name(table)

        op = SemanticTableOp(
            table=table,
            dimensions=dims,
            measures=meas,
            calc_measures=calc_meas,
            name=derived_name,
            description=description,
            _source_join=_source_join,
        )

        super().__init__(op)

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    @property
    def json_definition(self):
        return self.op().json_definition

    @property
    def measures(self):
        return self.op().measures

    @property
    def name(self):
        return self.op().name

    @property
    def description(self):
        return self.op().description

    @property
    def dimensions(self):
        return self.op().dimensions

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    @property
    def _dims(self):
        return self.op()._dims

    @property
    def _base_measures(self):
        return self.op()._base_measures

    @property
    def _calc_measures(self):
        return self.op()._calc_measures

    @property
    def table(self):
        return self.op().table

    def with_dimensions(self, **dims) -> SemanticModel:
        return SemanticModel(
            table=self.op().table,
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            name=self.name,
            description=self.description,
        )

    def with_measures(self, **meas) -> SemanticModel:
        new_base_meas = dict(self.get_measures())
        new_calc_meas = dict(self.get_calculated_measures())

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        base_tbl = self.op().table
        scope = MeasureScope(_tbl=base_tbl, _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == "calc" else new_base_meas)[name] = value

        return SemanticModel(
            table=self.op().table,
            dimensions=self.get_dimensions(),
            measures=new_base_meas,
            calc_measures=new_calc_meas,
            name=self.name,
            description=self.description,
        )

    def join(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
    ) -> SemanticJoin:
        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoin(left=self.op(), right=other_op, on=on, how=how)

    def join_one(self, other: SemanticModel, left_on: str, right_on: str) -> SemanticJoin:
        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoin(
            left=self.op(),
            right=other_op,
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="inner",
        )

    def join_many(self, other: SemanticModel, left_on: str, right_on: str) -> SemanticJoin:
        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoin(
            left=self.op(),
            right=other_op,
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="left",
        )

    def join_cross(self, other: SemanticModel) -> SemanticJoin:
        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoin(left=self.op(), right=other_op, on=None, how="cross")

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ):
        processed_selector = selector
        if selector is not None and "ibis.selectors" in str(type(selector).__module__):
            if type(selector).__name__ == "AllColumns":
                processed_selector = None
            elif type(selector).__name__ == "Cols":
                processed_selector = sorted(selector.names)
            else:
                processed_selector = selector

        return SemanticIndexOp(
            source=self.op(),
            selector=processed_selector,
            by=by,
            sample=sample,
        )

    def to_ibis(self):
        return self.op().to_ibis()

    def as_expr(self):
        return self

    def __getitem__(self, key):
        dims_dict = self.get_dimensions()
        if key in dims_dict:
            return dims_dict[key]

        meas_dict = self.get_measures()
        if key in meas_dict:
            return meas_dict[key]

        calc_meas_dict = self.get_calculated_measures()
        if key in calc_meas_dict:
            return calc_meas_dict[key]

        raise KeyError(
            f"'{key}' not found in dimensions, measures, or calculated measures",
        )

    def query(
        self,
        dimensions: Sequence[str] | None = None,
        measures: Sequence[str] | None = None,
        filters: list | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
        time_grain: str | None = None,
        time_range: dict[str, str] | None = None,
    ):
        return build_query(
            semantic_table=self,
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            order_by=order_by,
            limit=limit,
            time_grain=time_grain,
            time_range=time_range,
        )


class SemanticJoin(SemanticTable):
    def __init__(
        self,
        left: SemanticTableOp,
        right: SemanticTableOp,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
    ) -> None:
        op = SemanticJoinOp(left=left, right=right, on=on, how=how)
        super().__init__(op)

    @property
    def left(self):
        return self.op().left

    @property
    def right(self):
        return self.op().right

    @property
    def on(self):
        return self.op().on

    @property
    def how(self):
        return self.op().how

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    @property
    def name(self):
        return getattr(self.op(), "name", None)

    @property
    def table(self):
        return self.op().to_ibis()

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ):
        processed_selector = selector
        if selector is not None and "ibis.selectors" in str(type(selector).__module__):
            if type(selector).__name__ == "AllColumns":
                processed_selector = None
            elif type(selector).__name__ == "Cols":
                processed_selector = sorted(selector.names)
            else:
                processed_selector = selector

        return SemanticIndexOp(
            source=self.op(),
            selector=processed_selector,
            by=by,
            sample=sample,
        )

    def to_ibis(self):
        return self.op().to_ibis()

    def as_expr(self):
        return self

    def __getitem__(self, key):
        dims_dict = self.get_dimensions()
        if key in dims_dict:
            return dims_dict[key]

        meas_dict = self.get_measures()
        if key in meas_dict:
            return meas_dict[key]

        calc_meas_dict = self.get_calculated_measures()
        if key in calc_meas_dict:
            return calc_meas_dict[key]

        raise KeyError(
            f"'{key}' not found in dimensions, measures, or calculated measures",
        )

    @property
    def dimensions(self):
        return self.op().dimensions

    @property
    def measures(self):
        return self.op().measures

    @property
    def _dims(self):
        return self.op()._dims

    @property
    def _base_measures(self):
        return self.op()._base_measures

    @property
    def _calc_measures(self):
        return self.op()._calc_measures

    @property
    def calc_measures(self):
        return self.op().calc_measures

    @property
    def json_definition(self):
        return self.op().json_definition

    def query(
        self,
        dimensions: list[str] | None = None,
        measures: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        time_grain: str | None = None,
        time_range: dict[str, str] | None = None,
    ):
        return build_query(
            semantic_table=self,
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            order_by=order_by,
            limit=limit,
            time_grain=time_grain,
            time_range=time_range,
        )

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.op())
        return _build_semantic_model_from_roots(self.op().to_ibis(), all_roots)

    def with_dimensions(self, **dims) -> SemanticModel:
        """Add or update dimensions."""
        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            _source_join=self.op(),  # Pass join reference for projection pushdown
        )

    def with_measures(self, **meas) -> SemanticModel:
        from .measure_scope import MeasureScope
        from .ops import _classify_measure

        joined_tbl = self.op().to_ibis()
        all_known = (
            list(self.get_measures().keys())
            + list(self.get_calculated_measures().keys())
            + list(meas.keys())
        )
        scope = MeasureScope(_tbl=joined_tbl, _known=all_known)

        new_base, new_calc = (
            dict(self.get_measures()),
            dict(self.get_calculated_measures()),
        )
        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc if kind == "calc" else new_base)[name] = value

        return SemanticModel(
            table=joined_tbl,
            dimensions=self.get_dimensions(),
            measures=new_base,
            calc_measures=new_calc,
            _source_join=self.op(),  # Pass join reference for projection pushdown
        )

    def join(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
    ) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=on,
            how=how,
        )

    def join_one(
        self,
        other: SemanticModel,
        left_on: str,
        right_on: str,
    ) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="inner",
        )

    def join_many(
        self,
        other: SemanticModel,
        left_on: str,
        right_on: str,
    ) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="left",
        )

    def join_cross(self, other: SemanticModel) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=None,
            how="cross",
        )

    def group_by(self, *keys: str):
        return self.op().group_by(*keys)

    def filter(self, predicate: Callable):
        return self.op().filter(predicate)


class SemanticFilter(SemanticTable):
    def __init__(self, source: SemanticTableOp, predicate: Callable) -> None:
        op = SemanticFilterOp(source=source, predicate=predicate)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def predicate(self):
        return self.op().predicate

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.op().source)
        return _build_semantic_model_from_roots(self.op().to_ibis(), all_roots)

    def with_dimensions(self, **dims) -> SemanticModel:
        """Add or update dimensions after filtering."""
        all_roots = _find_all_root_models(self.op().source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions={**existing_dims, **dims},
            measures=existing_meas,
            calc_measures=existing_calc,
        )

    def with_measures(self, **meas) -> SemanticModel:
        """Add or update measures after filtering."""
        all_roots = _find_all_root_models(self.op().source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        new_base_meas = dict(existing_meas)
        new_calc_meas = dict(existing_calc)

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        scope = MeasureScope(_tbl=self.op().to_ibis(), _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == "calc" else new_base_meas)[name] = value

        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions=existing_dims,
            measures=new_base_meas,
            calc_measures=new_calc_meas,
        )


class SemanticGroupBy(SemanticTable):
    def __init__(self, source: SemanticTableOp, keys: tuple[str, ...]) -> None:
        op = SemanticGroupByOp(source=source, keys=keys)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def keys(self):
        return self.op().keys

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    def aggregate(
        self,
        *measure_names,
        nest: dict[str, Callable] | None = None,
        **aliased,
    ):
        aggs = {}
        for item in measure_names:
            if isinstance(item, str):
                aggs[item] = lambda t, n=item: t[n]
            elif callable(item):
                aggs[f"_measure_{id(item)}"] = item
            else:
                raise TypeError(
                    f"measure_names must be strings or callables, got {type(item)}",
                )

        def wrap_aggregation_expr(expr):
            if isinstance(expr, AggregationExpr):

                def wrapped(t):
                    if expr.operation == "count":
                        return t.count()
                    return getattr(t[expr.column], expr.operation)()

                return wrapped
            return expr

        aliased = {k: wrap_aggregation_expr(v) for k, v in aliased.items()}
        aggs.update(aliased)

        if nest:

            def make_nest_agg(fn):
                def build_struct_dict(columns, source_tbl):
                    return {col: source_tbl[col] for col in columns}

                def collect_struct(struct_dict):
                    return ibis.struct(struct_dict).collect()

                def handle_grouped_table(result, ibis_tbl):
                    group_cols = tuple(map(attrgetter("name"), result.groupings))
                    return collect_struct(build_struct_dict(group_cols, ibis_tbl))

                def handle_table(result, ibis_tbl):
                    return collect_struct(build_struct_dict(result.columns, ibis_tbl))

                def nest_agg(ibis_tbl):
                    result = fn(ibis_tbl)

                    if isinstance(result, SemanticTable):
                        return to_ibis(result)

                    if isinstance(result, GroupedTable):
                        return handle_grouped_table(result, ibis_tbl)

                    if isinstance(result, Table):
                        return handle_table(result, ibis_tbl)

                    raise TypeError(
                        f"Nest lambda must return GroupedTable, Table, or SemanticExpression, "
                        f"got {type(result).__module__}.{type(result).__name__}",
                    )

                return nest_agg

            nest_aggs = {name: make_nest_agg(fn) for name, fn in nest.items()}
            aggs = {**aggs, **nest_aggs}
            nested_columns = tuple(nest.keys())
        else:
            nested_columns = ()

        return SemanticAggregate(
            source=self.op(),
            keys=self.keys,
            aggs=aggs,
            nested_columns=nested_columns,
        )


class SemanticAggregate(SemanticTable):
    def __init__(
        self,
        source: SemanticTableOp,
        keys: tuple[str, ...],
        aggs: dict[str, Any],
        nested_columns: list[str] | None = None,
    ) -> None:
        op = SemanticAggregateOp(
            source=source,
            keys=keys,
            aggs=aggs,
            nested_columns=nested_columns or [],
        )
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def keys(self):
        return self.op().keys

    @property
    def aggs(self):
        return self.op().aggs

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    @property
    def measures(self):
        return self.op().measures

    @property
    def nested_columns(self):
        return self.op().nested_columns

    def mutate(self, **post) -> SemanticMutate:
        return SemanticMutate(source=self.op(), post=post)

    def join(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
    ) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op(),
            on=on,
            how=how,
        )

    def join_one(
        self,
        other: SemanticModel,
        left_on: str,
        right_on: str,
    ) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op(),
            on=lambda left, right: left[left_on] == right[right_on],
            how="inner",
        )

    def join_many(
        self,
        other: SemanticModel,
        left_on: str,
        right_on: str,
    ) -> SemanticJoin:
        return SemanticJoin(
            left=self.op(),
            right=other.op(),
            on=lambda left, right: left[left_on] == right[right_on],
            how="left",
        )

    def as_table(self) -> SemanticModel:
        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions={},
            measures={},
            calc_measures={},
        )

    def chart(
        self,
        spec: dict[str, Any] | None = None,
        backend: str = "altair",
        format: str = "static",
    ):
        return create_chart(self, spec=spec, backend=backend, format=format)


class SemanticOrderBy(SemanticTable):
    def __init__(
        self, source: SemanticTableOp, keys: tuple[str | ir.Value | Callable, ...]
    ) -> None:
        op = SemanticOrderByOp(source=source, keys=keys)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def keys(self):
        return self.op().keys

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(self.op().to_ibis(), all_roots)

    def chart(
        self,
        spec: dict[str, Any] | None = None,
        backend: str = "altair",
        format: str = "static",
    ):
        """Create a chart from the ordered aggregate."""
        # Pass the expression to preserve order_by in the chart
        return create_chart(self, spec=spec, backend=backend, format=format)


class SemanticLimit(SemanticTable):
    def __init__(self, source: SemanticTableOp, n: int, offset: int = 0) -> None:
        op = SemanticLimitOp(source=source, n=n, offset=offset)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def n(self):
        return self.op().n

    @property
    def offset(self):
        return self.op().offset

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(self.op().to_ibis(), all_roots)

    def chart(
        self,
        spec: dict[str, Any] | None = None,
        backend: str = "altair",
        format: str = "static",
    ):
        """Create a chart from the limited aggregate."""
        # Pass the expression to preserve limit in the chart
        return create_chart(self, spec=spec, backend=backend, format=format)


class SemanticUnnest(SemanticTable):
    def __init__(self, source: SemanticTableOp, column: str) -> None:
        op = SemanticUnnestOp(source=source, column=column)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def column(self):
        return self.op().column

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(self.op().to_ibis(), all_roots)

    def with_dimensions(self, **dims) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        return SemanticModel(
            table=self,
            dimensions={**existing_dims, **dims},
            measures=existing_meas,
            calc_measures=existing_calc,
        )

    def with_measures(self, **meas) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        new_base_meas = dict(existing_meas)
        new_calc_meas = dict(existing_calc)

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        scope = MeasureScope(_tbl=self, _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == "calc" else new_base_meas)[name] = value

        return SemanticModel(
            table=self,
            dimensions=existing_dims,
            measures=new_base_meas,
            calc_measures=new_calc_meas,
        )


class SemanticMutate(SemanticTable):
    def __init__(self, source: SemanticTableOp, post: dict[str, Any] | None = None) -> None:
        op = SemanticMutateOp(source=source, post=post)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def post(self):
        return self.op().post

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    @property
    def nested_columns(self):
        return self.op().nested_columns

    def mutate(self, **post) -> SemanticMutate:
        return SemanticMutate(source=self.op(), post=post)

    def with_dimensions(self, **dims) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        return SemanticModel(
            table=self,
            dimensions={**existing_dims, **dims},
            measures=existing_meas,
            calc_measures=existing_calc,
        )

    def with_measures(self, **meas) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        new_base_meas = dict(existing_meas)
        new_calc_meas = dict(existing_calc)

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        scope = MeasureScope(_tbl=self, _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == "calc" else new_base_meas)[name] = value

        return SemanticModel(
            table=self,
            dimensions=existing_dims,
            measures=new_base_meas,
            calc_measures=new_calc_meas,
        )

    def group_by(self, *keys: str) -> SemanticGroupBy:
        source_with_unnests = reduce(
            lambda src, col: SemanticUnnestOp(source=src, column=col),
            self.nested_columns,
            self.op(),
        )

        return SemanticGroupBy(source=source_with_unnests, keys=keys)

    def chart(
        self,
        spec: dict[str, Any] | None = None,
        backend: str = "altair",
        format: str = "static",
    ):
        """Create a chart from the mutated aggregate."""
        # Pass the expression to preserve mutations in the chart
        return create_chart(self, spec=spec, backend=backend, format=format)

    def as_table(self) -> SemanticModel:
        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions={},
            measures={},
            calc_measures={},
        )


class SemanticProject(SemanticTable):
    def __init__(self, source: SemanticTableOp, fields: tuple[str, ...]) -> None:
        op = SemanticProjectOp(source=source, fields=fields)
        super().__init__(op)

    @property
    def source(self):
        return self.op().source

    @property
    def fields(self):
        return self.op().fields

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(
            self.op().to_ibis(), all_roots, field_filter=set(self.fields)
        )
