"""Expression wrappers providing user-facing API.

Following Ibis patterns, Expressions are user-facing wrappers around Operations.
Operations (in ops.py) are internal IR with strict types.
Expressions provide flexible APIs and handle type transformations.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import reduce
from operator import attrgetter
from typing import TYPE_CHECKING, Any

from ibis.common.collections import FrozenDict
from ibis.expr import types as ir

from .ops import Dimension, Measure, SemanticTableOp

if TYPE_CHECKING:
    from .ops import SemanticJoinOp


def to_ibis(expr):
    """Convert semantic expression or operation to Ibis expression.

    This is the top-level conversion function matching ibis.to_sql() style.
    Works with both Expression and Operation objects.

    Args:
        expr: SemanticTable expression or Semantic operation

    Returns:
        Ibis Table expression

    Examples:
        >>> result = flights.group_by("carrier").aggregate("flight_count")
        >>> ibis_expr = to_ibis(result)
        >>> df = ibis_expr.execute()
    """
    # If it's an Expression, get its operation
    if hasattr(expr, "op"):
        return expr.op().to_ibis()
    # If it's an Operation, call its to_ibis method
    elif hasattr(expr, "to_ibis"):
        return expr.to_ibis()
    else:
        raise TypeError(f"Cannot convert {type(expr)} to Ibis expression")


class SemanticTable(ir.Table):
    """Base class for semantic tables with common fluent API methods.

    This provides the shared interface for all semantic tables including
    filter, group_by, order_by, limit, and other fluent methods.
    """

    def filter(self, predicate: Callable) -> SemanticFilter:
        """Apply a filter predicate."""
        return SemanticFilter(source=self.op(), predicate=predicate)

    def group_by(self, *keys: str):
        """Group by dimensions."""
        return SemanticGroupBy(source=self.op(), keys=keys)

    def mutate(self, **post) -> SemanticMutate:
        """Add or update columns."""
        return SemanticMutate(source=self.op(), post=post)

    def order_by(self, *keys: str | ir.Value | Callable):
        """Order by fields."""
        return SemanticOrderBy(source=self.op(), keys=keys)

    def limit(self, n: int, offset: int = 0):
        """Limit results."""
        return SemanticLimit(source=self.op(), n=n, offset=offset)

    def unnest(self, column: str) -> SemanticUnnest:
        """Unnest an array column, expanding rows.

        This is useful for working with nested data structures like Google Analytics
        sessions with nested hits, where you want to expand the array into separate rows.

        Args:
            column: Name of the array column to unnest

        Returns:
            SemanticUnnest expression with the array expanded into rows

        Example:
            >>> ga_sessions = to_semantic_table(ga_raw, name="ga")
            >>> ga_with_hits = ga_sessions.unnest("hits")
            >>> # Now each hit becomes its own row
        """
        return SemanticUnnest(source=self.op(), column=column)

    def pipe(self, func, *args, **kwargs):
        """Apply a function to self."""
        return func(self, *args, **kwargs)

    def execute(self):
        """Execute via to_ibis() to ensure proper lowering."""
        return to_ibis(self).execute()


def _create_dimension(expr: Dimension | Callable | dict) -> Dimension:
    """Transform various input types to Dimension."""
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
    """Derive table name from table expression."""
    try:
        table_expr = table.to_expr() if hasattr(table, "to_expr") else table
        return table_expr.get_name() if hasattr(table_expr, "get_name") else None
    except Exception:
        return None


class SemanticModel(SemanticTable):
    """User-facing semantic model with dimensions and measures.

    This is an Expression wrapper around SemanticTableOp. It provides:
    - Flexible input types (Callable, dict, Dimension)
    - User-facing methods (with_dimensions, with_measures, etc.)
    - Type transformations from flexible inputs to strict Operation types
    - Full Ibis Table API (execute, compile, etc.) inherited from SemanticTable

    The underlying Operation (SemanticTableOp) is the internal IR with strict,
    concrete types.
    """

    def __init__(
        self,
        table: Any,
        dimensions: Mapping[str, Dimension | Callable | dict] | None = None,
        measures: Mapping[str, Measure | Callable] | None = None,
        calc_measures: Mapping[str, Any] | None = None,
        name: str | None = None,
    ) -> None:
        """Create a semantic table with dimensions and measures.

        Args:
            table: Underlying Ibis table or Relation
            dimensions: Dimension definitions (Dimension, Callable, or dict)
            measures: Measure definitions (Measure or Callable)
            calc_measures: Calculated measure expressions
            name: Optional table name
        """
        # Transform flexible inputs to strict types
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
        # Store table expression directly (no .op() conversion)
        # This avoids redundant .op() → .to_expr() roundtrips

        # Create the Operation with strict types
        op = SemanticTableOp(
            table=table,  # Pass expression as-is
            dimensions=dims,
            measures=meas,
            calc_measures=calc_meas,
            name=derived_name,
        )

        # Initialize parent Table class with our operation
        super().__init__(op)

    # Note: .op() is inherited from ir.Table parent class
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
    def dimensions(self):
        """Get tuple of dimension names."""
        return self.op().dimensions

    def get_dimensions(self):
        """Get dictionary of dimensions with metadata."""
        return self.op().get_dimensions()

    def get_measures(self):
        """Get dictionary of base measures with metadata."""
        return self.op().get_measures()

    def get_calculated_measures(self):
        """Get dictionary of calculated measures with metadata."""
        return self.op().get_calculated_measures()

    @property
    def _dims(self):
        """Internal: Forward to Operation._dims (dict of Dimension objects)."""
        return self.op()._dims

    @property
    def _base_measures(self):
        """Internal: Forward to Operation._base_measures."""
        return self.op()._base_measures

    @property
    def _calc_measures(self):
        """Internal: Forward to Operation._calc_measures."""
        return self.op()._calc_measures

    @property
    def table(self):
        return self.op().table

    # User-facing methods
    def with_dimensions(self, **dims) -> SemanticModel:
        """Add or update dimensions."""
        return SemanticModel(
            table=self.op().table,
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            name=self.name,
        )

    def with_measures(self, **meas) -> SemanticModel:
        """Add or update measures."""
        from .measure_scope import MeasureScope
        from .ops import _classify_measure

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
        )

    def join(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
    ):
        """Join with another semantic table."""
        from .ops import SemanticJoinOp

        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoinOp(left=self.op(), right=other_op, on=on, how=how)

    def join_one(self, other: SemanticModel, left_on: str, right_on: str):
        """Inner join one-to-one or many-to-one."""
        from .ops import SemanticJoinOp

        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoinOp(
            left=self.op(),
            right=other_op,
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="inner",
        )

    def join_many(self, other: SemanticModel, left_on: str, right_on: str):
        """Left join one-to-many."""
        from .ops import SemanticJoinOp

        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoinOp(
            left=self.op(),
            right=other_op,
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="left",
        )

    def join_cross(self, other: SemanticModel):
        """Cross join."""
        from .ops import SemanticJoinOp

        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoinOp(left=self.op(), right=other_op, on=None, how="cross")

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ):
        """Create an index for search/discovery."""
        from .ops import SemanticIndexOp

        return SemanticIndexOp(
            source=self.op(),
            selector=selector,
            by=by,
            sample=sample,
        )

    def to_ibis(self):
        """Convert to Ibis expression."""
        return self.op().to_ibis()

    def as_expr(self):
        """Return self as expression."""
        return self

    def __getitem__(self, key):
        """Get dimension or measure by name."""
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
        """Query using parameter-based interface.

        Args:
            dimensions: List of dimension names to group by
            measures: List of measure names to aggregate
            filters: List of filters (dict, str, callable, or Filter objects)
            order_by: List of (field, direction) tuples
            limit: Maximum number of rows to return
            time_grain: Optional time grain (e.g., "TIME_GRAIN_MONTH")
            time_range: Optional time range with 'start' and 'end' keys

        Returns:
            SemanticAggregate or SemanticTable ready for execution
        """
        from .query import query as build_query

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

    # __repr__ inherited from ir.Table, uses custom formatter registered in convert.py


class SemanticFilter(SemanticTable):
    """User-facing filter expression wrapping SemanticFilterOp Operation."""

    def __init__(self, source: SemanticTableOp, predicate: Callable) -> None:
        from .ops import SemanticFilterOp

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

    def as_table(self) -> SemanticModel:
        from .ops import _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.op().source)
        return (
            SemanticModel(
                table=self.op().to_ibis(),
                dimensions=_get_merged_fields(all_roots, "dims"),
                measures=_get_merged_fields(all_roots, "measures"),
                calc_measures=_get_merged_fields(all_roots, "calc_measures"),
            )
            if all_roots
            else SemanticModel(
                table=self.op().to_ibis(),
                dimensions={},
                measures={},
                calc_measures={},
            )
        )


class SemanticGroupBy(SemanticTable):
    """User-facing group by expression wrapping SemanticGroupByOp Operation."""

    def __init__(self, source: SemanticTableOp, keys: tuple[str, ...]) -> None:
        from .ops import SemanticGroupByOp

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
        """Aggregate measures over grouped dimensions.

        Args:
            *measure_names: Measure names to aggregate
            nest: Dict mapping nest name to lambda that specifies columns to collect:
                  - Lambda with group_by: nest={"data": lambda t: t.group_by(["code", "elevation"])}
                  - Lambda with select: nest={"data": lambda t: t.select("code", "elevation")}
            **aliased: Additional aggregations with custom names
        """
        aggs = {}
        for item in measure_names:
            if isinstance(item, str):
                aggs[item] = lambda t, n=item: getattr(t, n)
            elif callable(item):
                aggs[f"_measure_{id(item)}"] = item
            else:
                raise TypeError(
                    f"measure_names must be strings or callables, got {type(item)}",
                )
        aggs.update(aliased)

        if nest:
            import ibis
            from ibis.expr.types import Table
            from ibis.expr.types.groupby import GroupedTable

            def make_nest_agg(fn):
                """Create a nested aggregation that collects rows as structs.

                Functional approach: Use pattern-based dispatch with early returns
                to handle different result types from the nest lambda.
                """

                def build_struct_dict(columns, source_tbl):
                    """Pure function: build struct dict from column names."""
                    return {col: source_tbl[col] for col in columns}

                def collect_struct(struct_dict):
                    """Pure function: create and collect struct from dict."""
                    return ibis.struct(struct_dict).collect()

                def handle_grouped_table(result, ibis_tbl):
                    """Handle GroupedTable: extract group columns and create struct."""
                    group_cols = tuple(map(attrgetter("name"), result.groupings))
                    return collect_struct(build_struct_dict(group_cols, ibis_tbl))

                def handle_table(result, ibis_tbl):
                    """Handle Table/Selection: extract all columns and create struct."""
                    return collect_struct(build_struct_dict(result.columns, ibis_tbl))

                def nest_agg(ibis_tbl):
                    """Apply nest function and dispatch based on result type."""
                    result = fn(ibis_tbl)

                    if hasattr(result, "to_ibis"):
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
    """User-facing aggregate expression wrapping SemanticAggregateOp Operation."""

    def __init__(
        self,
        source: SemanticTableOp,
        keys: tuple[str, ...],
        aggs: dict[str, Any],
        nested_columns: list[str] | None = None,
    ) -> None:
        from .ops import SemanticAggregateOp

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
        """Add or update columns after aggregation."""
        return SemanticMutate(source=self.op(), post=post)

    def join(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
    ) -> SemanticJoinOp:
        """Join with another semantic table."""
        from .ops import SemanticJoinOp, _unwrap_semantic_table

        return SemanticJoinOp(
            left=self.op(),
            right=_unwrap_semantic_table(other),
            on=on,
            how=how,
        )

    def join_one(
        self,
        other: SemanticModel,
        left_on: str,
        right_on: str,
    ) -> SemanticJoinOp:
        """Join with a one-to-one relationship."""
        from .ops import SemanticJoinOp, _unwrap_semantic_table

        return SemanticJoinOp(
            left=self.op(),
            right=_unwrap_semantic_table(other),
            on=lambda left, right: left[left_on] == right[right_on],
            how="inner",
        )

    def join_many(
        self,
        other: SemanticModel,
        left_on: str,
        right_on: str,
    ) -> SemanticJoinOp:
        """Join with a one-to-many relationship."""
        from .ops import SemanticJoinOp, _unwrap_semantic_table

        return SemanticJoinOp(
            left=self.op(),
            right=_unwrap_semantic_table(other),
            on=lambda left, right: left[left_on] == right[right_on],
            how="left",
        )

    def as_table(self) -> SemanticModel:
        """Convert to SemanticModel with no semantic metadata (columns are materialized)."""
        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions={},
            measures={},
            calc_measures={},
        )

    def chart(self, backend: str = "altair", chart_type: str | None = None):
        """Create a chart from the aggregate."""
        from .chart import chart as create_chart

        return create_chart(self.op(), backend=backend, chart_type=chart_type)


class SemanticOrderBy(SemanticTable):
    """User-facing order by expression wrapping SemanticOrderByOp Operation."""

    def __init__(
        self, source: SemanticTableOp, keys: tuple[str | ir.Value | Callable, ...]
    ) -> None:
        from .ops import SemanticOrderByOp

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
        """Convert to SemanticModel, preserving semantic metadata from source."""
        from .ops import _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.source)
        if all_roots:
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions=_get_merged_fields(all_roots, "dims"),
                measures=_get_merged_fields(all_roots, "measures"),
                calc_measures=_get_merged_fields(all_roots, "calc_measures"),
            )
        else:
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions={},
                measures={},
                calc_measures={},
            )

    def chart(self, backend: str = "altair", chart_type: str | None = None):
        """Create a chart from the ordered aggregate."""
        from .chart import chart as create_chart

        # Get the original aggregate to extract dimensions/measures
        source = self.source
        while hasattr(source, "source") and not hasattr(source, "aggs"):
            source = source.source
        if hasattr(source, "aggs"):
            return create_chart(source, backend=backend, chart_type=chart_type)
        raise ValueError("Cannot create chart: no aggregate found in query chain")


class SemanticLimit(SemanticTable):
    """User-facing limit expression wrapping SemanticLimitOp Operation."""

    def __init__(self, source: SemanticTableOp, n: int, offset: int = 0) -> None:
        from .ops import SemanticLimitOp

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
        """Convert to SemanticModel, preserving semantic metadata from source."""
        from .ops import _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.source)
        if all_roots:
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions=_get_merged_fields(all_roots, "dims"),
                measures=_get_merged_fields(all_roots, "measures"),
                calc_measures=_get_merged_fields(all_roots, "calc_measures"),
            )
        else:
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions={},
                measures={},
                calc_measures={},
            )

    def chart(self, backend: str = "altair", chart_type: str | None = None):
        """Create a chart from the limited aggregate."""
        from .chart import chart as create_chart

        source = self.source
        while hasattr(source, "source") and not hasattr(source, "aggs"):
            source = source.source
        if hasattr(source, "aggs"):
            return create_chart(source, backend=backend, chart_type=chart_type)
        raise ValueError("Cannot create chart: no aggregate found in query chain")


class SemanticUnnest(SemanticTable):
    """User-facing unnest expression wrapping SemanticUnnestOp Operation."""

    def __init__(self, source: SemanticTableOp, column: str) -> None:
        from .ops import SemanticUnnestOp

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
        """Convert to SemanticModel, preserving semantic metadata from source."""
        from .ops import _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.source)
        if all_roots:
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions=_get_merged_fields(all_roots, "dims"),
                measures=_get_merged_fields(all_roots, "measures"),
                calc_measures=_get_merged_fields(all_roots, "calc_measures"),
            )
        else:
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions={},
                measures={},
                calc_measures={},
            )

    def with_dimensions(self, **dims) -> SemanticModel:
        """Add or update dimensions on the unnested table."""
        from .ops import _find_all_root_models, _get_merged_fields

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
        """Add or update measures on the unnested table."""
        from .measure_scope import MeasureScope
        from .ops import _classify_measure, _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        # Process new measures through _classify_measure to extract metadata
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
    """User-facing mutate expression wrapping SemanticMutateOp Operation."""

    def __init__(self, source: SemanticTableOp, post: dict[str, Any] | None = None) -> None:
        from .ops import SemanticMutateOp

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
        """Add or update columns (supports chaining)."""
        return SemanticMutate(source=self.op(), post=post)

    def with_dimensions(self, **dims) -> SemanticModel:
        """Add or update dimensions after mutation."""
        from .ops import _find_all_root_models, _get_merged_fields

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
        """Add or update measures after mutation."""
        from .measure_scope import MeasureScope
        from .ops import _classify_measure, _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        # Process new measures through _classify_measure to extract metadata
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
        """Group by dimensions after mutation (enables re-aggregation).

        Automatically unnests any nested columns from prior aggregations.
        Uses reduce to fold unnest operations for functional composition.
        """
        from .ops import SemanticUnnestOp

        # Functional: fold over nested columns to build unnest operation chain
        source_with_unnests = reduce(
            lambda src, col: SemanticUnnestOp(source=src, column=col),
            self.nested_columns,
            self.op(),
        )

        return SemanticGroupBy(source=source_with_unnests, keys=keys)

    def as_table(self) -> SemanticModel:
        """Convert to SemanticModel with no semantic metadata (columns are materialized)."""
        return SemanticModel(
            table=self.op().to_ibis(),
            dimensions={},
            measures={},
            calc_measures={},
        )


class SemanticProject(SemanticTable):
    """User-facing project expression wrapping SemanticProjectOp Operation."""

    def __init__(self, source: SemanticTableOp, fields: tuple[str, ...]) -> None:
        from .ops import SemanticProjectOp

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
        """Convert to SemanticModel, preserving only projected fields' metadata."""
        from .ops import _find_all_root_models, _get_merged_fields

        all_roots = _find_all_root_models(self.source)

        if all_roots:
            # Get all available semantic metadata
            all_dims = _get_merged_fields(all_roots, "dims")
            all_measures = _get_merged_fields(all_roots, "measures")
            all_calc_measures = _get_merged_fields(all_roots, "calc_measures")

            # Filter to only include projected fields
            projected_fields = set(self.fields)
            filtered_dims = {k: v for k, v in all_dims.items() if k in projected_fields}
            filtered_measures = {k: v for k, v in all_measures.items() if k in projected_fields}
            filtered_calc_measures = {
                k: v for k, v in all_calc_measures.items() if k in projected_fields
            }

            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions=filtered_dims,
                measures=filtered_measures,
                calc_measures=filtered_calc_measures,
            )
        else:
            # No semantic metadata in source
            return SemanticModel(
                table=self.op().to_ibis(),
                dimensions={},
                measures={},
                calc_measures={},
            )
