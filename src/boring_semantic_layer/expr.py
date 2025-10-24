"""Expression wrappers providing user-facing API.

Following Ibis patterns, Expressions are user-facing wrappers around Operations.
Operations (in ops.py) are internal IR with strict types.
Expressions provide flexible APIs and handle type transformations.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Optional, Sequence

from ibis.common.collections import FrozenDict
from ibis.expr import types as ir

from .ops import Dimension, Measure, SemanticTableRelation


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


def _derive_name(table: Any) -> Optional[str]:
    """Derive table name from table expression."""
    try:
        table_expr = table.to_expr() if hasattr(table, "to_expr") else table
        return table_expr.get_name() if hasattr(table_expr, "get_name") else None
    except Exception:
        return None


class SemanticTable(ir.Table):
    """User-facing semantic table with dimensions and measures.

    This is an Expression wrapper around SemanticTableRelation. It provides:
    - Flexible input types (Callable, dict, Dimension)
    - User-facing methods (with_dimensions, with_measures, etc.)
    - Type transformations from flexible inputs to strict Operation types
    - Full Ibis Table API (execute, compile, etc.) inherited from ir.Table

    The underlying Operation (SemanticTableRelation) is the internal IR with strict,
    concrete types.
    """

    def __init__(
        self,
        table: Any,
        dimensions: Mapping[str, Dimension | Callable | dict] | None = None,
        measures: Mapping[str, Measure | Callable] | None = None,
        calc_measures: Mapping[str, Any] | None = None,
        name: Optional[str] = None,
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
        dims = FrozenDict({
            dim_name: _create_dimension(dim)
            for dim_name, dim in (dimensions or {}).items()
        })

        meas = FrozenDict({
            meas_name: measure if isinstance(measure, Measure) else Measure(expr=measure, description=None)
            for meas_name, measure in (measures or {}).items()
        })

        calc_meas = FrozenDict(calc_measures or {})

        derived_name = name or _derive_name(table)
        # Store table expression directly (no .op() conversion)
        # This avoids redundant .op() → .to_expr() roundtrips

        # Create the Operation with strict types
        op = SemanticTableRelation(
            table=table,  # Pass expression as-is
            dimensions=dims,
            measures=meas,
            calc_measures=calc_meas,
            name=derived_name,
        )

        # Initialize parent Table class with our operation
        super().__init__(op)

    # Forward property accessors to Operation
    # Note: .op() is inherited from ir.Table parent class
    @property
    def values(self):
        """Forward to Operation."""
        return self.op().values

    @property
    def schema(self):
        """Forward to Operation."""
        return self.op().schema

    @property
    def json_definition(self):
        """Forward to Operation."""
        return self.op().json_definition

    @property
    def dims(self):
        """Forward to Operation."""
        return self.op().dims

    @property
    def _dims(self):
        """Forward to Operation."""
        return self.op()._dims

    @property
    def _base_measures(self):
        """Forward to Operation."""
        return self.op()._base_measures

    @property
    def _calc_measures(self):
        """Forward to Operation."""
        return self.op()._calc_measures

    @property
    def measures(self):
        """Forward to Operation."""
        return self.op().measures

    @property
    def name(self):
        """Forward to Operation."""
        return self.op().name

    @property
    def dimensions(self):
        """Forward to Operation."""
        return self.op().dimensions

    @property
    def table(self):
        """Forward to Operation."""
        return self.op().table

    def _dims_dict(self):
        """Forward to Operation."""
        return self.op()._dims_dict()

    def _measures_dict(self):
        """Forward to Operation."""
        return self.op()._measures_dict()

    def _calc_measures_dict(self):
        """Forward to Operation."""
        return self.op()._calc_measures_dict()

    # User-facing methods
    def with_dimensions(self, **dims) -> "SemanticTable":
        """Add or update dimensions."""
        return SemanticTable(
            table=self.op().table,  # Already an expression
            dimensions={**self._dims_dict(), **dims},
            measures=self._measures_dict(),
            calc_measures=self._calc_measures_dict(),
            name=self.name
        )

    def with_measures(self, **meas) -> "SemanticTable":
        """Add or update measures."""
        from .measure_scope import MeasureScope
        from .ops import _classify_measure

        new_base_meas = dict(self._measures_dict())
        new_calc_meas = dict(self._calc_measures_dict())

        all_measure_names = tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        base_tbl = self.op().table  # Already an expression
        scope = MeasureScope(_tbl=base_tbl, _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == 'calc' else new_base_meas)[name] = value

        return SemanticTable(
            table=self.op().table,  # Already an expression
            dimensions=self._dims_dict(),
            measures=new_base_meas,
            calc_measures=new_calc_meas,
            name=self.name
        )

    def filter(self, predicate: Callable):
        """Filter rows using a predicate."""
        from .ops import SemanticFilter
        return SemanticFilter(source=self.op(), predicate=predicate)

    def group_by(self, *keys: str):
        """Group by dimensions."""
        from .ops import SemanticGroupBy
        return SemanticGroupBy(source=self.op(), keys=keys)

    def join(self, other: "SemanticTable", on: Callable[[Any, Any], Any] | None = None, how: str = "inner"):
        """Join with another semantic table."""
        from .ops import SemanticJoin
        other_op = other.op() if isinstance(other, SemanticTable) else other
        return SemanticJoin(left=self.op(), right=other_op, on=on, how=how)

    def join_one(self, other: "SemanticTable", left_on: str, right_on: str):
        """Inner join one-to-one or many-to-one."""
        from .ops import SemanticJoin
        other_op = other.op() if isinstance(other, SemanticTable) else other
        return SemanticJoin(
            left=self.op(),
            right=other_op,
            on=lambda l, r: getattr(l, left_on) == getattr(r, right_on),
            how="inner"
        )

    def join_many(self, other: "SemanticTable", left_on: str, right_on: str):
        """Left join one-to-many."""
        from .ops import SemanticJoin
        other_op = other.op() if isinstance(other, SemanticTable) else other
        return SemanticJoin(
            left=self.op(),
            right=other_op,
            on=lambda l, r: getattr(l, left_on) == getattr(r, right_on),
            how="left"
        )

    def join_cross(self, other: "SemanticTable"):
        """Cross join."""
        from .ops import SemanticJoin
        other_op = other.op() if isinstance(other, SemanticTable) else other
        return SemanticJoin(left=self.op(), right=other_op, on=None, how="cross")

    def index(self, selector: Any = None, by: Optional[str] = None, sample: Optional[int] = None):
        """Create an index for search/discovery."""
        from .ops import SemanticIndex
        return SemanticIndex(source=self.op(), selector=selector, by=by, sample=sample)

    def to_ibis(self):
        """Convert to Ibis expression."""
        return self.op().to_ibis()

    def as_expr(self):
        """Return self as expression."""
        return self

    # execute(), compile() are inherited from ir.Table
    # But we can override if needed for custom behavior

    def sql(self, **kwargs):
        """Generate SQL string."""
        import ibis
        return ibis.to_sql(self.to_ibis(), **kwargs)

    def __getitem__(self, key):
        """Get dimension or measure by name."""
        dims_dict = self._dims_dict()
        if key in dims_dict:
            return dims_dict[key]

        meas_dict = self._measures_dict()
        if key in meas_dict:
            return meas_dict[key]

        calc_meas_dict = self._calc_measures_dict()
        if key in calc_meas_dict:
            return calc_meas_dict[key]

        raise KeyError(f"'{key}' not found in dimensions, measures, or calculated measures")

    def pipe(self, func, *args, **kwargs):
        """Apply a function to self."""
        return func(self, *args, **kwargs)

    def query(
        self,
        dimensions: Optional[Sequence[str]] = None,
        measures: Optional[Sequence[str]] = None,
        filters: Optional[list] = None,
        order_by: Optional[Sequence[tuple[str, str]]] = None,
        limit: Optional[int] = None,
        time_grain: Optional[str] = None,
        time_range: Optional[dict[str, str]] = None,
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
            semantic_table=self,  # Pass Expression, not Operation
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            order_by=order_by,
            limit=limit,
            time_grain=time_grain,
            time_range=time_range,
        )

    # __repr__ inherited from ir.Table, uses custom formatter registered in lower.py
