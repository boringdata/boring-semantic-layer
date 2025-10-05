from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Union

import ibis as ibis_mod
from ibis.expr.format import fmt as _fmt
from ibis.expr.sql import convert


from boring_semantic_layer.semantic_api.ops import (
    Dimension,
    Measure,
    SemanticAggregate,
    SemanticFilter,
    SemanticGroupBy,
    SemanticMutate,
    SemanticProject,
    SemanticTable,
    SemanticJoin,
    SemanticOrderBy,
    SemanticLimit,
)
from boring_semantic_layer.semantic_api.chart import (
    AltairChartRenderer,
    PlotlyChartRenderer,
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


@_fmt.register(SemanticLimit)
def _format_semantic_limit(op, **kwargs):
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
        dims = getattr(self._node, "dimensions", {})
        if name in dims:
            return dims[name](self.to_expr())
        meas = getattr(self._node, "measures", {})
        if name in meas:
            return meas[name](self.to_expr())
        if hasattr(self._node, name):
            return getattr(self._node, name)
        return getattr(self.to_expr(), name)

    def with_dimensions(self, **dims: Union[Callable, Dimension]) -> SemanticTableExpr:
        return with_dimensions(self, **dims)

    def with_measures(self, **meas: Union[Callable, Measure]) -> SemanticTableExpr:
        return with_measures(self, **meas)

    @property
    def dimensions(self) -> dict:
        """Get dimensions from the semantic operation tree."""
        return self._get_semantic_metadata("dimensions")

    @property
    def measures(self) -> dict:
        """Get measures from the semantic operation tree."""
        return self._get_semantic_metadata("measures")

    @property
    def time_dimensions(self) -> dict:
        """Get time dimensions by checking dimension attributes directly."""
        # Check dimensions directly for is_time_dimension attribute
        time_dims = {
            name: dim
            for name, dim in self.dimensions.items()
            if hasattr(dim, "is_time_dimension") and dim.is_time_dimension
        }

        # Fallback: check Ibis column types for temporal data
        if not time_dims:
            try:
                ibis_table = self.to_ibis()
                for dim_name, dim_def in self.dimensions.items():
                    if (
                        dim_name in ibis_table.columns
                        and ibis_table[dim_name].type().is_temporal()
                    ):
                        time_dims[dim_name] = dim_def
            except Exception:
                pass

        return time_dims

    def _get_semantic_metadata(self, attr_name: str) -> dict:
        """
        Get semantic metadata (dimensions/measures) using the existing root model functions.

        This leverages _find_all_root_models and _merge_fields_with_prefixing to handle
        both single tables and joined tables consistently.
        """
        from .ops import _find_all_root_models, _merge_fields_with_prefixing

        node = self._node

        # For aggregated results, return the actual column names from the result
        if hasattr(node, "__class__") and "Aggregate" in node.__class__.__name__:
            if attr_name == "dimensions":
                # For aggregated results, group-by keys become the available dimensions
                if hasattr(node, "keys"):
                    return {key: key for key in node.keys}
                return {}
            elif attr_name == "measures":
                # For aggregated results, aggregated columns become the available measures
                if hasattr(node, "aggs"):
                    return {name: name for name in node.aggs.keys()}
                return {}

        # Use the existing functions to find all root models and merge fields
        try:
            all_roots = _find_all_root_models(node)
            if not all_roots:
                return {}

            # Use the centralized merging logic that handles prefixing
            merged_metadata = _merge_fields_with_prefixing(
                all_roots, lambda r: getattr(r, attr_name, {})
            )
            return merged_metadata

        except Exception:
            # Fallback: return empty dict if root model extraction fails
            return {}

    def group_by(self, *keys: str, **inline_dims: Callable) -> SemanticTableExpr:
        return group_by_(self, *keys, **inline_dims)

    def aggregate(self, *fns, **aggs: Callable) -> SemanticTableExpr:
        from .api import _infer_measure_name  # avoid circular

        if fns:
            if aggs:
                raise ValueError(
                    "Cannot mix positional and named arguments in aggregate"
                )

            # Handle mixed string and lambda function arguments
            inferred_aggs = {}
            for i, fn in enumerate(fns):
                if isinstance(fn, str):
                    # String-based predefined measure reference
                    # Create a lambda that accesses the predefined measure via the resolver
                    measure_name = fn
                    inferred_aggs[measure_name] = lambda t, name=measure_name: getattr(
                        t, name
                    )
                elif callable(fn):
                    # Lambda function - try to infer name
                    try:
                        name = _infer_measure_name(fn)
                        inferred_aggs[name] = fn
                    except ValueError:
                        # If name inference fails, create a unique name
                        fallback_name = f"measure_{i}"
                        inferred_aggs[fallback_name] = fn
                else:
                    raise ValueError(
                        f"Aggregate arguments must be strings or callable functions, got {type(fn)}"
                    )

            aggs = inferred_aggs
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

    def order_by(self, *keys: Any) -> SemanticTableExpr:
        return order_by_(self, *keys)

    def limit(self, n: int, offset: int = 0) -> SemanticTableExpr:
        return limit_(self, n, offset)

    def chart(
        self,
        spec: Optional[Dict[str, Any]] = None,
        backend: str = "altair",
        format: str = "static",
    ) -> Union["altair.Chart", "go.Figure", Dict[str, Any], bytes, str]:
        if backend not in ["altair", "plotly"]:
            raise ValueError(
                f"Unsupported backend: {backend}. Supported backends: 'altair', 'plotly'"
            )

        if format not in ["static", "interactive", "json", "png", "svg"]:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )

        if backend == "altair":
            renderer = AltairChartRenderer(self)
            return renderer.render(spec=spec, format=format)
        elif backend == "plotly":
            renderer = PlotlyChartRenderer(self)
            return renderer.render(spec=spec, format=format)


def to_semantic_table(
    table: IbisTable, name: Optional[str] = None
) -> SemanticTableExpr:
    node = SemanticTable(
        table=table,
        dimensions={},
        measures={},
        name=name,
    )
    return SemanticTableExpr(node)


def with_dimensions(
    table: IbisTable, **dimensions: Union[Callable, Dimension, dict]
) -> SemanticTableExpr:
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_dims = {**getattr(node, "dimensions", {}), **dimensions}
    node = SemanticTable(
        table=node.table.to_expr(),
        dimensions=new_dims,
        measures=getattr(node, "measures", {}),
        name=getattr(node, "name", None),
    )
    return SemanticTableExpr(node)


def with_measures(
    table: IbisTable, **measures: Union[Callable, Measure]
) -> SemanticTableExpr:
    node = table.op()
    if not isinstance(node, SemanticTable):
        node = SemanticTable(table=table, dimensions={}, measures={})
    new_meas = {**getattr(node, "measures", {}), **measures}
    node = SemanticTable(
        table=node.table.to_expr(),
        dimensions=getattr(node, "dimensions", {}),
        measures=new_meas,
        name=getattr(node, "name", None),
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


def order_by_(table: IbisTable, *keys: Any) -> SemanticTableExpr:
    # Convert deferred expressions to a serializable form
    processed_keys = []
    for key in keys:
        from ibis.common.deferred import Deferred

        if isinstance(key, Deferred):
            # Store deferred expressions as a callable that can be applied to the table
            processed_keys.append(
                ("__deferred__", lambda t, deferred=key: deferred.resolve(t))
            )
        else:
            processed_keys.append(key)

    node = SemanticOrderBy(source=table.op(), keys=processed_keys)
    return SemanticTableExpr(node)


def limit_(table: IbisTable, n: int, offset: int = 0) -> SemanticTableExpr:
    node = SemanticLimit(source=table.op(), n=n, offset=offset)
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
