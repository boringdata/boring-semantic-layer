from __future__ import annotations
from typing import Any, Callable, Dict, Optional, Union, Tuple
from attrs import frozen, field


from .measure_scope import MeasureScope, ColumnScope
from .measure_nodes import MeasureRef, AllOf, BinOp, MeasureExpr
from .compile_all import compile_grouped_with_all


def _resolve_expression(expr_or_callable, scope_or_table):
    """
    Pure helper to resolve expressions that can be callables, deferred expressions, or direct values.

    Supports:
    - Callables (lambdas): lambda t: t.distance.sum()
    - Ibis Deferred expressions: _.distance.sum()
    - Direct values (strings, MeasureRef, etc.): returned as-is

    Args:
        expr_or_callable: Expression, callable, or direct value
        scope_or_table: MeasureScope, ColumnScope, or Ibis table to resolve against

    Returns:
        Resolved expression value
    """
    from ibis.common.deferred import Deferred

    # If it's a Deferred expression, resolve it
    if isinstance(expr_or_callable, Deferred):
        # Resolve directly against the scope or table
        # MeasureScope.__getattr__ handles returning MeasureRef for known measures
        # and falls back to table columns for unknown attributes
        return expr_or_callable.resolve(scope_or_table)

    # If it's callable, call it with the scope
    if callable(expr_or_callable):
        return expr_or_callable(scope_or_table)

    # Otherwise return as-is (strings, MeasureRef, etc.)
    return expr_or_callable


def _make_immutable_dict(d: Dict) -> Tuple[Tuple[str, Any], ...]:
    """
    Convert a dict to an immutable tuple of key-value pairs.
    Preserves insertion order (important for join prefix resolution).
    """
    return tuple(d.items())


def _to_dict(immutable_dict: Tuple[Tuple[str, Any], ...]) -> Dict:
    """Convert immutable tuple of pairs back to dict."""
    return dict(immutable_dict)


@frozen(kw_only=True)
class SemanticTable:
    """
    Immutable semantic table with functional composition patterns.

    All transformation methods return new instances rather than modifying in place.
    Uses frozen dataclass pattern with tuple-based storage for dimensions and measures.
    """
    name: str
    base_tbl: Any  # Ibis table expression (already immutable)
    dim_defs: Tuple[Tuple[str, Callable], ...] = field(factory=tuple)
    base_measure_defs: Tuple[Tuple[str, Callable], ...] = field(factory=tuple)
    calc_measure_defs: Tuple[Tuple[str, MeasureExpr], ...] = field(factory=tuple)
    group_dim_names: Optional[Tuple[str, ...]] = field(default=None)

    def clone(self, **changes) -> "SemanticTable":
        """
        Create a new SemanticTable instance with specified changes.
        Pure function that returns a new instance without modifying self.
        """
        current_attrs = {
            "name": self.name,
            "base_tbl": self.base_tbl,
            "dim_defs": self.dim_defs,
            "base_measure_defs": self.base_measure_defs,
            "calc_measure_defs": self.calc_measure_defs,
            "group_dim_names": self.group_dim_names,
        }
        return SemanticTable(**(current_attrs | changes))

    @property
    def dims(self) -> list[str]:
        """Return list of dimension names defined on this semantic table."""
        return [name for name, _ in self.dim_defs]

    @property
    def measures(self) -> list[str]:
        """Return list of measure names defined on this semantic table (both base and calculated)."""
        base_names = {name for name, _ in self.base_measure_defs}
        calc_names = {name for name, _ in self.calc_measure_defs}
        return list(base_names | calc_names)

    @property
    def _dims(self) -> Dict[str, Callable]:
        """Return dict view of dimension definitions (for test compatibility)."""
        return _to_dict(self.dim_defs)

    @property
    def _base_measures(self) -> Dict[str, Callable]:
        """Return dict view of base measure definitions (for test compatibility)."""
        return _to_dict(self.base_measure_defs)

    @property
    def _calc_measures(self) -> Dict[str, MeasureExpr]:
        """Return dict view of calculated measure definitions (for test compatibility)."""
        return _to_dict(self.calc_measure_defs)

    def with_dimensions(self, **defs) -> "SemanticTable":
        """
        Define dimensions on the semantic table.
        Supports both lambdas (lambda t: t.col.op()) and deferred expressions (_.col.op()).

        Returns a new SemanticTable with added dimensions (immutable transformation).
        """
        from ibis.common.deferred import Deferred

        # Convert deferred expressions to callables
        resolved_defs = {
            name: (lambda t, expr=fn_or_expr: expr.resolve(t))
            if isinstance(fn_or_expr, Deferred)
            else fn_or_expr
            for name, fn_or_expr in defs.items()
        }

        # Merge with existing dimensions
        current_dims = _to_dict(self.dim_defs)
        current_dims.update(resolved_defs)

        return self.clone(dim_defs=_make_immutable_dict(current_dims))

    def with_measures(self, **defs) -> "SemanticTable":
        """
        Define measures on the semantic table.
        Returns a new SemanticTable with added measures (immutable transformation).
        """
        current_base = _to_dict(self.base_measure_defs)
        current_calc = _to_dict(self.calc_measure_defs)
        known = set(current_base) | set(current_calc) | set(defs.keys())
        scope = MeasureScope(self.base_tbl, known_measures=known)

        new_base = current_base.copy()
        new_calc = current_calc.copy()

        for name, fn_or_expr in defs.items():
            val = _resolve_expression(fn_or_expr, scope)
            if isinstance(val, (MeasureRef, AllOf, BinOp, int, float)):
                new_calc[name] = val
            else:
                new_base[name] = (
                    lambda _fn=fn_or_expr: (lambda base_tbl: _resolve_expression(_fn, ColumnScope(base_tbl)))
                )()

        return self.clone(
            base_measure_defs=_make_immutable_dict(new_base),
            calc_measure_defs=_make_immutable_dict(new_calc)
        )

    def _join(self, other: "SemanticTable", cond, how: str) -> "SemanticTable":
        """Pure function that creates a new joined semantic table."""
        joined_tbl = self.base_tbl.join(other.base_tbl, cond, how=how)

        # Build new dimension and measure dictionaries with prefixes
        new_dims = {}
        new_base_measures = {}
        new_calc_measures = {}

        # Helper to add fields with optional prefix
        def add_with_prefix(source_defs, target_dict, table_name, rename_fn=None):
            for name, value in _to_dict(source_defs).items():
                prefixed_name = f"{table_name}__{name}" if table_name else name
                target_dict[prefixed_name] = rename_fn(value, table_name) if rename_fn and table_name else value

        # Add left table's fields
        add_with_prefix(self.dim_defs, new_dims, self.name)
        add_with_prefix(self.base_measure_defs, new_base_measures, self.name)
        add_with_prefix(self.calc_measure_defs, new_calc_measures, self.name,
                       lambda expr, prefix: self._rename_measure_refs(expr, prefix))

        # Add right table's fields
        add_with_prefix(other.dim_defs, new_dims, other.name)
        add_with_prefix(other.base_measure_defs, new_base_measures, other.name)
        add_with_prefix(other.calc_measure_defs, new_calc_measures, other.name,
                       lambda expr, prefix: self._rename_measure_refs(expr, prefix))

        return SemanticTable(
            name=f"{self.name}_{how}_{other.name}",
            base_tbl=joined_tbl,
            dim_defs=_make_immutable_dict(new_dims),
            base_measure_defs=_make_immutable_dict(new_base_measures),
            calc_measure_defs=_make_immutable_dict(new_calc_measures),
        )

    def _rename_measure_refs(self, expr: MeasureExpr, prefix: str) -> MeasureExpr:
        """Pure function that recursively renames MeasureRef names in calculated measure expressions."""
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
        Pure function returning new SemanticTable.
        """
        cond = on(ColumnScope(self.base_tbl), ColumnScope(other.base_tbl)) if on else None
        return self._join(other, cond, how=how)

    def filter(self, predicate: Callable[[Any], Any]) -> "SemanticTable":
        """
        Filter rows in the semantic table, returning a new SemanticTable with the same dimensions and measures.
        Supports both lambdas and Ibis deferred expressions.
        Pure function - returns new instance.
        """
        cond = _resolve_expression(predicate, ColumnScope(self.base_tbl))
        filtered_tbl = self.base_tbl.filter(cond)
        return self.clone(base_tbl=filtered_tbl)

    def mutate(self, **defs: Callable[[Any], Any]) -> Any:
        """
        Add or modify columns in the semantic table using measure-based lambdas or deferred expressions.

        For post-aggregation tables (where measures have been materialized as columns),
        use MeasureScope with only the aggregated column names as known measures.
        This allows t.all() to work on aggregated results.

        Supports both lambdas (lambda t: t.col.sum()) and deferred expressions (_.col.sum()).

        Pure function - does not modify self, returns Ibis expression.
        """
        base_measures_dict = _to_dict(self.base_measure_defs)
        calc_measures_dict = _to_dict(self.calc_measure_defs)

        # Check if we have any base measures - if not, this is likely a post-aggregation table
        if not base_measures_dict and not calc_measures_dict:
            # Post-aggregation: all columns are available, t.all() works on them
            known_measures = set(self.base_tbl.columns)
            post_agg = True
        else:
            # Pre-aggregation: use defined measures
            known_measures = set(base_measures_dict) | set(calc_measures_dict)
            post_agg = False

        scope = MeasureScope(self.base_tbl, known_measures=known_measures, post_aggregation=post_agg)
        cols = {name: _resolve_expression(fn_or_expr, scope) for name, fn_or_expr in defs.items()}
        return self.base_tbl.mutate(**cols)

    def order_by(self, *keys: Union[str, Any]) -> Any:
        """Order rows in the semantic table or expression. Pure function - returns Ibis expression."""
        return self.base_tbl.order_by(*keys)

    def limit(self, n: int) -> Any:
        """Limit number of rows in the semantic table or expression. Pure function - returns Ibis expression."""
        return self.base_tbl.limit(n)

    def to_ibis(self):
        """Get the underlying ibis table expression. Pure function - returns Ibis expression."""
        return self.base_tbl

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()

    def join_one(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticTable":
        """Pure function for one-to-one join."""
        cond = getattr(self.base_tbl, left_on) == getattr(other.base_tbl, right_on)
        return self._join(other, cond, how="inner")

    def join_many(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticTable":
        """Pure function for one-to-many join."""
        cond = getattr(self.base_tbl, left_on) == getattr(other.base_tbl, right_on)
        return self._join(other, cond, how="left")

    def join_cross(self, other: "SemanticTable") -> "SemanticTable":
        """Pure function for cross join."""
        return self._join(other, cond=None, how="cross")

    def group_by(self, *dims: str) -> "SemanticTable":
        """Set grouping dimensions for aggregation. Pure function - returns new SemanticTable with group_dims set."""
        return self.clone(group_dim_names=tuple(dims))

    def aggregate(self, *measure_names: str, **aliased: str) -> "SemanticTable":
        """
        Aggregate measures by the grouped dimensions.
        Pure function - returns new SemanticTable with aggregated results.
        """
        if self.group_dim_names is None:
            raise ValueError("Call .group_by(...) before .aggregate(...)")

        base_measures_dict = _to_dict(self.base_measure_defs)
        calc_measures_dict = _to_dict(self.calc_measure_defs)

        # Process inline measure definitions (callables and deferred expressions)
        inline_defs = self._process_inline_measures(
            measure_names, aliased, base_measures_dict, calc_measures_dict
        )

        # Update measure dictionaries with inline definitions
        new_base_measures = base_measures_dict.copy()
        new_calc_measures = calc_measures_dict.copy()

        if inline_defs:
            measure_names, aliased = self._register_inline_measures(
                inline_defs, new_base_measures, new_calc_measures,
                measure_names, aliased, base_measures_dict, calc_measures_dict
            )

        # Compile and execute aggregation
        select_measures = list(measure_names) + list(aliased.values())
        base = self._materialize_base_with_dims()

        grouped = compile_grouped_with_all(
            base_tbl=base,
            by_cols=list(self.group_dim_names),
            agg_specs=new_base_measures,
            calc_specs=new_calc_measures,
        )

        # Build projection with resolved column names
        proj_cols = {d: grouped[d] for d in self.group_dim_names}
        for m in select_measures:
            resolved_name = self._resolve_measure_name(m, grouped.columns)
            proj_cols[m] = grouped[resolved_name].name(m) if resolved_name != m else grouped[m]
        for alias, m in aliased.items():
            resolved_name = self._resolve_measure_name(m, grouped.columns)
            proj_cols[alias] = grouped[resolved_name]

        result = grouped.select(**proj_cols)

        # Return new SemanticTable with materialized measures (not semantic definitions)
        return SemanticTable(
            name=self.name,
            base_tbl=result,
            dim_defs=self.dim_defs,
            base_measure_defs=tuple(),  # Clear - measures now materialized as columns
            calc_measure_defs=tuple(),
        )

    def _process_inline_measures(self, measure_names, aliased, base_measures_dict, calc_measures_dict):
        """Extract and validate inline measure definitions from aggregate() arguments."""
        from ibis.common.deferred import Deferred

        inline_defs: dict[str, Callable] = {}
        base = self._materialize_base_with_dims()

        # Process positional callables/deferred - derive names from MeasureRef
        for fn_or_expr in [m for m in measure_names if callable(m) or isinstance(m, Deferred)]:
            val = _resolve_expression(
                fn_or_expr,
                MeasureScope(base, known_measures=set(base_measures_dict) | set(calc_measures_dict))
            )
            if not isinstance(val, MeasureRef):
                raise TypeError("aggregate() expects positional callables/deferred to return MeasureRef")
            inline_defs[val.name] = fn_or_expr

        # Process keyword callables/deferred
        for name, fn_or_expr in aliased.items():
            if callable(fn_or_expr) or isinstance(fn_or_expr, Deferred):
                inline_defs[name] = fn_or_expr

        return inline_defs

    def _register_inline_measures(self, inline_defs, new_base_measures, new_calc_measures,
                                  measure_names, aliased, base_measures_dict, calc_measures_dict):
        """Register inline measure definitions and update measure name lists."""
        from ibis.common.deferred import Deferred

        base = self._materialize_base_with_dims()
        known = set(base_measures_dict) | set(calc_measures_dict)

        for name, fn_or_expr in inline_defs.items():
            val = _resolve_expression(fn_or_expr, MeasureScope(base, known_measures=known))
            if isinstance(val, (MeasureRef, AllOf, BinOp, int, float)):
                new_calc_measures[name] = val
            else:
                new_base_measures[name] = (
                    lambda _fn=fn_or_expr: (lambda base_tbl: _resolve_expression(_fn, ColumnScope(base_tbl)))
                )()

        # Update measure_names and aliased to only include strings
        measure_names = tuple([m for m in measure_names if isinstance(m, str)]) + tuple(inline_defs.keys())
        aliased = {k: v for k, v in aliased.items() if not callable(v) and not isinstance(v, Deferred)}

        return measure_names, aliased

    def _resolve_measure_name(self, name: str, available_columns: list[str]) -> str:
        """
        Pure function to resolve a measure name to its actual column name.
        Handles both full prefixed names (table__measure) and short names (measure).
        """
        if name in available_columns:
            return name
        for col in available_columns:
            if col.endswith(f"__{name}"):
                return col
        return name

    def _materialize_base_with_dims(self):
        """Pure function to materialize dimensions on the base table. Returns Ibis expression."""
        if not self.dim_defs:
            return self.base_tbl
        dims_dict = _to_dict(self.dim_defs)
        cols = {name: fn(self.base_tbl) for name, fn in dims_dict.items()}
        return self.base_tbl.mutate(**cols)


def to_semantic_table(ibis_table, name: Optional[str] = None) -> SemanticTable:
    """Factory function to create a SemanticTable from an Ibis table."""
    return SemanticTable(name=name, base_tbl=ibis_table)
