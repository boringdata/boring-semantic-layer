from __future__ import annotations
from typing import Any, Callable, Dict, Optional, Union

from attrs import frozen

from .measure_scope import MeasureScope, ColumnScope
from .measure_nodes import MeasureRef, AllOf, BinOp, MeasureExpr
from .compile_all import compile_grouped_with_all


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[Any], Any]
    description: Optional[str] = None
    is_time_dimension: bool = False
    smallest_time_grain: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        """Make Dimension callable - transparently calls the expr."""
        return self.expr(table)

    @classmethod
    def from_value(cls, value: Union['Dimension', dict, Callable]) -> 'Dimension':
        """Convert various input formats to Dimension."""
        if isinstance(value, cls):
            return value
        elif isinstance(value, dict):
            return cls(**value)
        elif callable(value):
            return cls(expr=value)
        else:
            raise ValueError(f"Dimension must be callable, dict, or Dimension instance, got {type(value)}")


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[Any], Any]
    description: Optional[str] = None

    def __call__(self, *args, **kwargs) -> Any:
        """Make Measure callable - transparently calls the expr."""
        return self.expr(*args, **kwargs)

    @classmethod
    def from_value(cls, value: Union['Measure', dict, Callable]) -> 'Measure':
        """Convert various input formats to Measure."""
        if isinstance(value, cls):
            return value
        elif isinstance(value, dict):
            return cls(**value)
        elif callable(value):
            return cls(expr=value)
        else:
            raise ValueError(f"Measure must be callable, dict, or Measure instance, got {type(value)}")


class SemanticTable:
    def __init__(self, ibis_table, name: str):
        self._name = name
        self._base_tbl = ibis_table
        self._dims: Dict[str, Dimension] = {}
        self._base_measures: Dict[str, Measure] = {}
        self._calc_measures: Dict[str, MeasureExpr] = {}

    def with_dimensions(self, **defs):
        defs = {name: Dimension.from_value(value) for name, value in defs.items()}
        self._dims.update(defs)
        return self

    def with_measures(self, **defs):
        defs = {name: Measure.from_value(value) for name, value in defs.items()}

        known = set(self._base_measures) | set(self._calc_measures) | set(defs.keys())
        scope = MeasureScope(self._base_tbl, known_measures=known)
        for name, fn in defs.items():
            val = fn(scope)
            if isinstance(val, (MeasureRef, AllOf, BinOp, int, float)):
                self._calc_measures[name] = val
            else:
                # Create a closure that returns the aggregation function, preserving metadata
                self._base_measures[name] = Measure(
                    expr=(lambda _fn=fn.expr: (lambda base_tbl: _fn(ColumnScope(base_tbl))))(),
                    description=fn.description
                )
        return self

    def _join(self, other: "SemanticTable", cond, how: str) -> "SemanticTable":
        joined_tbl = self._base_tbl.join(other._base_tbl, cond, how=how)
        out = SemanticTable(joined_tbl, name=f"{self._name}_{how}_{other._name}")

        # Prefix all dimensions and measures with table names to avoid conflicts
        # This allows accessing them as table__dimension or table__measure
        out._dims = {}
        out._base_measures = {}
        out._calc_measures = {}

        # Add left table's fields with prefixes
        if self._name:
            for name, fn in self._dims.items():
                out._dims[f"{self._name}__{name}"] = fn
            for name, fn in self._base_measures.items():
                out._base_measures[f"{self._name}__{name}"] = fn
            for name, expr in self._calc_measures.items():
                # Need to rename MeasureRef in calc_measures
                out._calc_measures[f"{self._name}__{name}"] = self._rename_measure_refs(expr, self._name)
        else:
            # No prefix if table has no name
            out._dims.update(self._dims)
            out._base_measures.update(self._base_measures)
            out._calc_measures.update(self._calc_measures)

        # Add right table's fields with prefixes
        if other._name:
            for name, fn in other._dims.items():
                out._dims[f"{other._name}__{name}"] = fn
            for name, fn in other._base_measures.items():
                out._base_measures[f"{other._name}__{name}"] = fn
            for name, expr in other._calc_measures.items():
                out._calc_measures[f"{other._name}__{name}"] = self._rename_measure_refs(expr, other._name)
        else:
            # No prefix if table has no name - may cause conflicts!
            out._dims.update(other._dims)
            out._base_measures.update(other._base_measures)
            out._calc_measures.update(other._calc_measures)

        return out

    def _rename_measure_refs(self, expr: MeasureExpr, prefix: str) -> MeasureExpr:
        """Recursively rename MeasureRef names in calculated measure expressions."""
        from .measure_nodes import MeasureRef, AllOf, BinOp

        if isinstance(expr, MeasureRef):
            return MeasureRef(f"{prefix}__{expr.name}")
        elif isinstance(expr, AllOf):
            return AllOf(self._rename_measure_refs(expr.ref, prefix))
        elif isinstance(expr, BinOp):
            return BinOp(
                expr.op,
                self._rename_measure_refs(expr.left, prefix) if isinstance(expr.left, (MeasureRef, AllOf, BinOp)) else expr.left,
                self._rename_measure_refs(expr.right, prefix) if isinstance(expr.right, (MeasureRef, AllOf, BinOp)) else expr.right,
            )
        else:
            return expr

    def join(
        self,
        other: "SemanticTable",
        on: Optional[Callable[[Any, Any], Any]] = None,
        how: str = "inner",
    ) -> "SemanticTable":
        """
        Generic join on two semantic tables using a predicate or cross join.
        """
        if on is None:
            cond = None
        else:
            # predicate receives table scopes for left and right
            cond = on(ColumnScope(self._base_tbl), ColumnScope(other._base_tbl))
        return self._join(other, cond, how=how)

    def filter(self, predicate: Callable[[Any], Any]) -> "SemanticTable":
        """
        Filter rows in the semantic table, returning a new SemanticTable with the same dimensions and measures.
        """
        cond = predicate(ColumnScope(self._base_tbl))
        filtered_tbl = self._base_tbl.filter(cond)

        # Create new SemanticTable with filtered data but same semantic definitions
        out = SemanticTable(filtered_tbl, name=self._name)
        out._dims = self._dims.copy()
        out._base_measures = self._base_measures.copy()
        out._calc_measures = self._calc_measures.copy()
        return out

    def mutate(self, **defs: Callable[[Any], Any]) -> Any:
        """
        Add or modify columns in the semantic table using measure-based lambdas.

        For post-aggregation tables (where measures have been materialized as columns),
        use MeasureScope with only the aggregated column names as known measures.
        This allows t.all() to work on aggregated results.
        """
        base = self._base_tbl
        # Check if we have any base measures - if not, this is likely a post-aggregation table
        # In that case, treat column names as known measures for t.all() support
        if not self._base_measures and not self._calc_measures:
            # Post-aggregation: all columns are available, t.all() works on them
            known_measures = set(base.columns)
            post_agg = True
        else:
            # Pre-aggregation: use defined measures
            known_measures = set(self._base_measures) | set(self._calc_measures)
            post_agg = False

        scope = MeasureScope(base, known_measures=known_measures, post_aggregation=post_agg)
        cols = {name: fn(scope) for name, fn in defs.items()}
        return base.mutate(**cols)

    def order_by(self, *keys: Union[str, Any]) -> Any:
        """
        Order rows in the semantic table or expression.
        """
        return self._base_tbl.order_by(*keys)

    def limit(self, n: int) -> Any:
        """
        Limit number of rows in the semantic table or expression.
        """
        return self._base_tbl.limit(n)

    def to_ibis(self):
        """
        Get the underlying ibis table expression.
        Useful for executing queries: .to_ibis().execute()
        """
        return self._base_tbl

    def execute(self):
        """
        Execute the query and return results as a pandas DataFrame.
        Internally converts to ibis and executes.
        """
        return self.to_ibis().execute()

    def join_one(
        self, other: "SemanticTable", left_on: str, right_on: str
    ) -> "SemanticTable":
        cond = getattr(self._base_tbl, left_on) == getattr(other._base_tbl, right_on)
        return self._join(other, cond, how="inner")

    def join_many(
        self, other: "SemanticTable", left_on: str, right_on: str
    ) -> "SemanticTable":
        cond = getattr(self._base_tbl, left_on) == getattr(other._base_tbl, right_on)
        return self._join(other, cond, how="left")

    def join_cross(self, other: "SemanticTable") -> "SemanticTable":
        return self._join(other, cond=None, how="cross")

    def group_by(self, *dims: str):
        self._group_dims = list(dims)
        return self

    def aggregate(self, *measure_names: str, **aliased: str):
        if not hasattr(self, "_group_dims"):
            raise ValueError("Call .group_by(...) before .aggregate(...)")

        # allow defining new measures inline via callables (both positional and aliased)
        base = self._materialize_base_with_dims()
        inline_defs: dict[str, Callable] = {}
        # positional callables: derive measure names from MeasureRef
        from ibis.common.deferred import Deferred

        for fn in [m for m in measure_names if callable(m)]:
            val = fn(
                MeasureScope(
                    base,
                    known_measures=set(self._base_measures) | set(self._calc_measures),
                )
            )
            if not isinstance(val, MeasureRef):
                raise TypeError(
                    "aggregate() expects positional callables to return MeasureRef"
                )
            inline_defs[val.name] = fn
        # keyword callables - collect them first
        for name, fn in list(aliased.items()):
            if callable(fn):
                inline_defs[name] = fn

        if inline_defs:
            # Register inline measure definitions WITHOUT including the new names in scope
            # This prevents issues when measures reference columns with same names
            # Use MeasureScope so inline measure lambdas can access existing measures via MeasureRef
            known = set(self._base_measures) | set(self._calc_measures)
            for name, fn in inline_defs.items():
                val = fn(MeasureScope(base, known_measures=known))
                # Check if this is a MeasureRef (calculated measure) or ibis expression (base measure)
                if isinstance(val, MeasureRef):
                    # This is a reference to an existing measure - store as calc measure
                    self._calc_measures[name] = val
                else:
                    # This is a direct ibis aggregation - store as base measure
                    self._base_measures[name] = Measure(
                        expr=(lambda _fn=fn: (lambda base_tbl: _fn(ColumnScope(base_tbl))))()
                    )
            # use only string names for aggregation specs
            measure_names = tuple(inline_defs.keys())
            aliased = {}

        # only string measure names are supported in chainable DSL
        if any(callable(m) or isinstance(m, Deferred) for m in measure_names) or any(
            callable(f) or isinstance(f, Deferred) for f in aliased.values()
        ):
            raise ValueError(
                "aggregate() only accepts string measure names in the chainable DSL"
            )

        select_measures = list(measure_names) + list(aliased.values())
        base = self._materialize_base_with_dims()

        grouped = compile_grouped_with_all(
            base_tbl=base,
            by_cols=self._group_dims,
            agg_specs=self._base_measures,
            calc_specs=self._calc_measures,
        )

        proj_cols = {d: grouped[d] for d in self._group_dims}
        for m in select_measures:
            # Try to resolve measure name (handles both prefixed and short names)
            resolved_name = self._resolve_measure_name(m, grouped.columns)
            if resolved_name != m:
                # Short name was resolved to prefixed name - add as alias
                proj_cols[m] = grouped[resolved_name].name(m)
            else:
                proj_cols[m] = grouped[m]
        for alias, m in aliased.items():
            resolved_name = self._resolve_measure_name(m, grouped.columns)
            proj_cols[alias] = grouped[resolved_name]
        # build the result table expression
        result = grouped.select(**proj_cols)
        # wrap in SemanticTable to support further chainable DSL (e.g., mutate with t.all)
        out = SemanticTable(result, name=self._name)
        # DO NOT propagate semantic measure definitions - after aggregation,
        # the measures have been materialized as columns, not semantic measures.
        # This allows mutate() to work correctly with aggregated columns.
        out._dims = self._dims.copy()
        out._base_measures = {}
        out._calc_measures = {}
        return out

    def _resolve_measure_name(self, name: str, available_columns: list[str]) -> str:
        """Resolve a measure name to its actual column name.

        Handles both full prefixed names (table__measure) and short names (measure).
        For short names, finds the first matching prefixed column.
        """
        # 1. Try exact match first
        if name in available_columns:
            return name
        # 2. Try to find prefixed version (table__name format)
        for col in available_columns:
            if col.endswith(f"__{name}"):
                return col
        # 3. If not found, return original (will likely cause error downstream)
        return name

    def _materialize_base_with_dims(self):
        if not self._dims:
            return self._base_tbl
        cols = {name: fn(self._base_tbl) for name, fn in self._dims.items()}
        return self._base_tbl.mutate(**cols)


def to_semantic_table(ibis_table, name: Optional[str] = None) -> SemanticTable:
    return SemanticTable(ibis_table, name=name)
