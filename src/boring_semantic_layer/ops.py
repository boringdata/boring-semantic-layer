from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from operator import methodcaller
from typing import TYPE_CHECKING, Any

from attrs import frozen
from ibis.common.collections import FrozenDict, FrozenOrderedDict
from ibis.common.deferred import Deferred
from ibis.expr import types as ir
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema

if TYPE_CHECKING:
    from .expr import (
        SemanticFilter,
        SemanticGroupBy,
        SemanticLimit,
        SemanticOrderBy,
        SemanticTable,
    )


def _to_ibis(source: Any) -> ir.Table:
    """Convert semantic or Ibis object to Ibis table expression."""
    return source.to_ibis() if hasattr(source, "to_ibis") else source.to_expr()


def _semantic_table(*args, **kwargs) -> SemanticTable:
    """Late-binding import to avoid circular dependency."""
    from .expr import SemanticModel

    return SemanticModel(*args, **kwargs)


def _unwrap_semantic_table(other: Any) -> Any:
    """Unwrap SemanticTable Expression to SemanticTableOp Operation."""
    # Use methodcaller to get .op() if it exists, otherwise return as-is
    return methodcaller("op")(other) if hasattr(other, "op") and callable(other.op) else other


def _unwrap(wrapped: Any) -> Any:
    """Extract the underlying callable/Deferred from wrapper."""
    return wrapped.unwrap if isinstance(wrapped, _CallableWrapper) else wrapped


def _resolve_expr(expr: Deferred | Callable | Any, scope: ir.Table) -> ir.Value:
    """Resolve deferred expressions or callables to Ibis values."""
    return (
        expr.resolve(scope)
        if isinstance(expr, Deferred)
        else expr(scope)
        if callable(expr)
        else expr
    )


def _get_field_dict(root: Any, field_type: str) -> dict:
    """Get field dict from SemanticOp using public methods."""
    method_map = {
        "dimensions": "get_dimensions",
        "measures": "get_measures",
        "calc_measures": "get_calculated_measures",
    }
    method_name = method_map[field_type]
    return dict(getattr(root, method_name)())


def _get_merged_fields(all_roots: list, field_type: str) -> dict:
    return (
        _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, field_type),
        )
        if len(all_roots) > 1
        else _get_field_dict(all_roots[0], field_type)
        if all_roots
        else {}
    )


def _collect_measure_refs(expr, refs_out: set):
    from .measure_scope import AllOf, BinOp, MeasureRef

    if isinstance(expr, MeasureRef):
        refs_out.add(expr.name)
    elif isinstance(expr, AllOf):
        refs_out.add(expr.ref.name)
    elif isinstance(expr, BinOp):
        _collect_measure_refs(expr.left, refs_out)
        _collect_measure_refs(expr.right, refs_out)


@frozen
class _CallableWrapper:
    """Hashable wrapper for Callable and Deferred.

    Both raw callables (lambda) and user Deferred (_.foo) are not hashable
    and cannot be stored in FrozenDict. This wrapper provides hashability
    using identity-based hashing.
    """

    _fn: Any

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def __hash__(self):
        return hash(id(self._fn))

    @property
    def unwrap(self):
        return self._fn


def _ensure_wrapped(fn: Any) -> _CallableWrapper:
    """Wrap Callable or Deferred for hashability."""
    return fn if isinstance(fn, _CallableWrapper) else _CallableWrapper(fn)


def _infer_locality(fn: Callable, table: Any) -> tuple[str | None, tuple[str, ...]]:
    """Infer locality from the table's unnest operations.

    Checks if the table is a SemanticUnnest operation and uses the unnested
    column name as the locality. This is more reliable than bytecode analysis
    since it reflects the actual data transformations.

    Examples:
        to_semantic_table(tbl).with_measures(...) -> (None, ())  # Session level
        to_semantic_table(tbl).unnest("hits").with_measures(...) -> ("hits", ("hits",))
        unnested.unnest("product").with_measures(...) -> ("product", ("product",))
    """
    # Check if table is a SemanticUnnest (has an operation with unnesting)
    from .expr import SemanticUnnest

    if isinstance(table, SemanticUnnest):
        # Get the unnest operation from the expression
        op = table.op()
        if hasattr(op, "column"):
            locality = op.column  # e.g., "hits"
            requires_unnest = (op.column,)
            return (locality, requires_unnest)

    # No unnest operation found - base/session level
    return (None, ())


def _classify_measure(fn_or_expr: Any, scope: Any):
    from .measure_scope import AllOf, BinOp, ColumnScope, MeasureRef

    # Handle dict inputs: extract expr (must be lambda/Deferred) and description
    if isinstance(fn_or_expr, dict):
        description = fn_or_expr.get("description")
        # locality and requires_unnest are internal-only, not user-facing
        locality = fn_or_expr.get("locality")
        requires_unnest = tuple(fn_or_expr.get("requires_unnest", []))
        fn_or_expr = fn_or_expr["expr"]  # Extract the lambda/Deferred
    elif isinstance(fn_or_expr, Measure):
        # Handle Measure objects (when copying/preserving existing measures)
        description = fn_or_expr.description
        locality = fn_or_expr.locality
        requires_unnest = fn_or_expr.requires_unnest
        fn_or_expr = fn_or_expr.expr
    else:
        description = None
        locality = None
        requires_unnest = ()

    # Try to resolve the expression to see if it's a calculated measure
    # If it fails (e.g., accessing nested arrays that need proxies), treat as base measure
    try:
        val = _resolve_expr(fn_or_expr, scope)
        is_calc = isinstance(val, MeasureRef | AllOf | BinOp | int | float)
    except (AttributeError, TypeError):
        # Failed to resolve - likely needs proxy for nested access
        # Treat as base measure
        is_calc = False
        val = None

    if is_calc:
        return ("calc", val)
    else:
        # Infer locality if not explicitly provided
        if locality is None and callable(fn_or_expr):
            table = getattr(scope, "tbl", None)
            if table is None:
                table = getattr(scope, "_tbl", None)
            if table is not None:
                inferred_locality, inferred_unnest = _infer_locality(fn_or_expr, table)
                if locality is None:
                    locality = inferred_locality
                if not requires_unnest:
                    requires_unnest = inferred_unnest

        return (
            "base",
            Measure(
                expr=lambda t, fn=fn_or_expr: (
                    fn.resolve(ColumnScope(_tbl=t))
                    if isinstance(fn, Deferred)
                    else fn(ColumnScope(_tbl=t))
                ),
                description=description,
                locality=locality,
                requires_unnest=requires_unnest,
            ),
        )


def _build_json_definition(
    dims_dict: dict,
    meas_dict: dict,
    name: str | None = None,
) -> dict:
    return {
        "dimensions": {n: spec.to_json() for n, spec in dims_dict.items()},
        "measures": {n: spec.to_json() for n, spec in meas_dict.items()},
        "time_dimensions": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_time_dimension
        },
        "name": name,
    }


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    is_time_dimension: bool = False
    smallest_time_grain: str | None = None

    def __call__(self, table: ir.Table) -> ir.Value:
        return self.expr.resolve(table) if isinstance(self.expr, Deferred) else self.expr(table)

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        return (
            {**base, "smallest_time_grain": self.smallest_time_grain}
            if self.is_time_dimension
            else base
        )

    def __hash__(self) -> int:
        return hash(
            (self.description, self.is_time_dimension, self.smallest_time_grain),
        )


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    locality: str | None = None  # Internal: "session", "hit", "product", etc.
    requires_unnest: tuple[str, ...] = ()  # Internal: Arrays that must be unnested

    def __call__(self, table: ir.Table) -> ir.Value:
        return self.expr.resolve(table) if isinstance(self.expr, Deferred) else self.expr(table)

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        if self.locality:
            base["locality"] = self.locality
        if self.requires_unnest:
            base["requires_unnest"] = list(self.requires_unnest)
        return base

    def __hash__(self) -> int:
        return hash((self.description, self.locality, self.requires_unnest))


class SemanticTableOp(Relation):
    """Relation with semantic metadata (dimensions and measures).

    Stores ir.Table expression directly to avoid .op() → .to_expr() conversions.
    """

    table: ir.Table
    dimensions: FrozenDict[str, Dimension]
    measures: FrozenDict[str, Measure]
    calc_measures: FrozenDict[str, Any]
    name: str | None = None

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return FrozenOrderedDict(
            {
                **{col: self.table[col].op() for col in self.table.columns},
                **{name: fn(self.table).op() for name, fn in self.get_dimensions().items()},
                **{name: fn(self.table).op() for name, fn in self.get_measures().items()},
            },
        )

    @property
    def schema(self) -> Schema:
        return Schema({name: v.dtype for name, v in self.values.items()})

    @property
    def json_definition(self) -> Mapping[str, Any]:
        return _build_json_definition(
            self.get_dimensions(),
            self.get_measures(),
            self.name,
        )

    @property
    def _dims(self) -> dict[str, Dimension]:
        return dict(self.get_dimensions())

    @property
    def _base_measures(self) -> dict[str, Measure]:
        return dict(self.get_measures())

    @property
    def _calc_measures(self) -> dict[str, Any]:
        return dict(self.get_calculated_measures())

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of base measures with metadata."""
        return object.__getattribute__(self, "measures")

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions with metadata."""
        return object.__getattribute__(self, "dimensions")

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures with metadata."""
        return object.__getattribute__(self, "calc_measures")

    def __getattribute__(self, name: str):
        if name == "dimensions":
            dims = object.__getattribute__(self, "dimensions")
            return tuple(dims.keys())
        if name == "measures":
            base_meas = object.__getattribute__(self, "measures")
            calc_meas = object.__getattribute__(self, "calc_measures")
            return tuple(base_meas.keys()) + tuple(calc_meas.keys())
        return object.__getattribute__(self, name)

    def to_ibis(self):
        return self.table


class SemanticFilterOp(Relation):
    source: Relation
    predicate: Callable

    def __init__(self, source: Relation, predicate: Callable) -> None:
        super().__init__(
            source=Relation.__coerce__(source),
            predicate=_ensure_wrapped(predicate),
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_ibis(self):
        from .convert import _Resolver

        all_roots = _find_all_root_models(self.source)
        base_tbl = _to_ibis(self.source)
        dim_map = (
            {}
            if isinstance(self.source, SemanticAggregateOp)
            else _get_merged_fields(all_roots, "dimensions")
        )

        pred_fn = _unwrap(self.predicate)
        resolver = _Resolver(base_tbl, dim_map)
        pred = _resolve_expr(pred_fn, resolver)
        return base_tbl.filter(pred)


class SemanticProjectOp(Relation):
    source: Relation
    fields: tuple[str, ...]

    def __init__(self, source: Relation, fields: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), fields=tuple(fields))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        src_vals = self.source.values
        return FrozenOrderedDict(
            {k: v for k, v in src_vals.items() if k in self.fields},
        )

    @property
    def schema(self) -> Schema:
        return Schema({k: v.dtype for k, v in self.values.items()})

    def to_ibis(self):
        from .nested_access import NestedAccessMarker

        all_roots = _find_all_root_models(self.source)
        tbl = _to_ibis(self.source)

        if not all_roots:
            return tbl.select([getattr(tbl, f) for f in self.fields])

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        merged_measures = _get_merged_fields(all_roots, "measures")

        dims = [f for f in self.fields if f in merged_dimensions]
        meas = [f for f in self.fields if f in merged_measures]
        raw_fields = [
            f for f in self.fields if f not in merged_dimensions and f not in merged_measures
        ]

        dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]

        # Evaluate measures, checking for NestedAccessMarkers (experimental feature)
        meas_exprs = []
        unnested_tbl = tbl
        needs_unnesting = False

        for name in meas:
            result = merged_measures[name](tbl)
            if isinstance(result, NestedAccessMarker):
                # Apply automatic unnesting
                for array_col in result.array_path:
                    if array_col in unnested_tbl.columns:
                        unnested_tbl = unnested_tbl.unnest(array_col)
                        needs_unnesting = True

                # Build expression accessing nested fields
                if result.field_path:
                    expr = getattr(unnested_tbl, result.array_path[0])
                    for field in result.field_path:
                        expr = getattr(expr, field)
                else:
                    expr = unnested_tbl

                # Apply aggregation
                if result.operation == "count":
                    meas_exprs.append(unnested_tbl.count().name(name))
                elif result.operation in ("sum", "mean", "min", "max", "nunique"):
                    agg_fn = getattr(expr, result.operation)
                    meas_exprs.append(agg_fn().name(name))
                else:
                    raise ValueError(f"Unknown operation: {result.operation}")
            else:
                meas_exprs.append(result.name(name))

        # Use unnested table if needed
        active_tbl = unnested_tbl if needs_unnesting else tbl

        # Re-evaluate dimensions on unnested table if needed
        if needs_unnesting and dim_exprs:
            dim_exprs = [merged_dimensions[name](active_tbl).name(name) for name in dims]

        raw_exprs = [getattr(active_tbl, name) for name in raw_fields if hasattr(active_tbl, name)]

        return (
            active_tbl.group_by(dim_exprs).aggregate(meas_exprs)
            if meas_exprs and dim_exprs
            else active_tbl.aggregate(meas_exprs)
            if meas_exprs
            else active_tbl.select(dim_exprs + raw_exprs)
            if dim_exprs or raw_exprs
            else active_tbl
        )


class SemanticGroupByOp(Relation):
    source: Relation
    keys: tuple[str, ...]

    def __init__(self, source: Relation, keys: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), keys=tuple(keys))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_ibis(self):
        return _to_ibis(self.source)


class SemanticAggregateOp(Relation):
    source: Relation
    keys: tuple[str, ...]
    aggs: dict[
        str,
        Callable,
    ]  # Transformed to FrozenDict[str, _CallableWrapper] in __init__
    nested_columns: tuple[str, ...] = ()  # Track which columns are nested arrays

    def __init__(
        self,
        source: Relation,
        keys: Iterable[str],
        aggs: dict[str, Callable] | None,
        nested_columns: Iterable[str] | None = None,
    ) -> None:
        frozen_aggs = FrozenDict(
            {name: _ensure_wrapped(fn) for name, fn in (aggs or {}).items()},
        )
        super().__init__(
            source=Relation.__coerce__(source),
            keys=tuple(keys),
            aggs=frozen_aggs,
            nested_columns=tuple(nested_columns or []),
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        # Find all root models to handle joined tables properly
        all_roots = _find_all_root_models(self.source)

        # Use centralized prefixing logic
        merged_dimensions = _merge_fields_with_prefixing(
            all_roots,
            lambda root: root.get_dimensions(),
        )

        # Use the actual source table (which could be a join) as base_tbl
        base_tbl = self.source.to_expr()

        vals: dict[str, Any] = {}
        for k in self.keys:
            if k in merged_dimensions:
                vals[k] = merged_dimensions[k](base_tbl).op()
            else:
                vals[k] = base_tbl[k].op()
        for name, fn in self.aggs.items():
            vals[name] = fn(base_tbl).op()
        return FrozenOrderedDict(vals)

    @property
    def schema(self) -> Schema:
        return Schema({n: v.dtype for n, v in self.values.items()})

    @property
    def measures(self) -> tuple[str, ...]:
        return ()

    def to_ibis(self):
        from .compile_all import compile_grouped_with_all
        from .measure_scope import AllOf, BinOp, ColumnScope, MeasureRef, MeasureScope

        all_roots = _find_all_root_models(self.source)
        tbl = _to_ibis(self.source)

        # Check if we're aggregating after a prior aggregation (post-aggregation context)
        # This happens when SemanticMutateOp comes after a SemanticAggregateOp
        def has_prior_aggregate(node):
            """Recursively check if there's a SemanticAggregateOp before any mutate."""
            if isinstance(node, SemanticAggregateOp):
                return True
            if isinstance(node, SemanticMutateOp):
                return has_prior_aggregate(node.source)
            if isinstance(node, SemanticGroupByOp):
                return has_prior_aggregate(node.source)
            if hasattr(node, "source"):
                return has_prior_aggregate(node.source)
            return False

        is_post_agg = has_prior_aggregate(self.source)

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        merged_base_measures = _get_merged_fields(all_roots, "measures")
        merged_calc_measures = _get_merged_fields(all_roots, "calc_measures")

        dim_mutations = {k: merged_dimensions[k](tbl) for k in self.keys if k in merged_dimensions}
        tbl = tbl.mutate(**dim_mutations) if dim_mutations else tbl

        # Use ColumnScope for post-aggregation, MeasureScope otherwise
        if is_post_agg:
            scope = ColumnScope(_tbl=tbl)
        else:
            all_measure_names = list(merged_base_measures.keys()) + list(
                merged_calc_measures.keys(),
            )
            scope = MeasureScope(_tbl=tbl, _known=all_measure_names)

        agg_specs = {}
        calc_specs = {}

        for name, fn_wrapped in self.aggs.items():
            fn = _unwrap(fn_wrapped)
            val = _resolve_expr(fn, scope)

            # In post-aggregation context, treat all expressions as regular aggregations
            if is_post_agg:
                agg_specs[name] = lambda t, f=fn: (
                    f.resolve(ColumnScope(_tbl=t))
                    if isinstance(f, Deferred)
                    else f(ColumnScope(_tbl=t))
                )
            elif isinstance(val, MeasureRef):
                ref_name = val.name
                if ref_name in merged_calc_measures:
                    calc_specs[name] = merged_calc_measures[ref_name]
                elif ref_name in merged_base_measures:
                    measure_obj = merged_base_measures[ref_name]
                    agg_specs[name] = lambda t, m=measure_obj: m(t)
                else:
                    calc_specs[name] = val
            elif isinstance(val, AllOf | BinOp | int | float):
                calc_specs[name] = val
            else:
                agg_specs[name] = lambda t, f=fn: (
                    f.resolve(ColumnScope(_tbl=t))
                    if isinstance(f, Deferred)
                    else f(ColumnScope(_tbl=t))
                )

        referenced_measures = set()
        for calc_expr in calc_specs.values():
            _collect_measure_refs(calc_expr, referenced_measures)

        for ref_name in referenced_measures:
            if ref_name not in agg_specs and ref_name in merged_base_measures:
                measure_obj = merged_base_measures[ref_name]
                agg_specs[ref_name] = lambda t, m=measure_obj: m(t)

        requested_measure_names = list(self.aggs.keys())
        by_cols = list(self.keys)

        return (
            compile_grouped_with_all(
                tbl,
                by_cols,
                agg_specs,
                calc_specs,
                requested_measures=requested_measure_names,
            )
            if calc_specs or by_cols
            else tbl.aggregate(
                {name: agg_fn(tbl) for name, agg_fn in agg_specs.items()},
            )
        )

    def __repr__(self) -> str:
        keys_str = ", ".join(repr(k) for k in self.keys)
        aggs = list(self.aggs.keys())
        aggs_str = ", ".join(aggs[:5])
        if len(aggs) > 5:
            aggs_str += f", ... ({len(aggs)} total)"
        return f"SemanticAggregateOp(by=[{keys_str}], aggs=[{aggs_str}])"


class SemanticMutateOp(Relation):
    source: Relation
    post: dict[
        str,
        Callable,
    ]  # Transformed to FrozenDict[str, _CallableWrapper] in __init__
    nested_columns: tuple[
        str,
        ...,
    ] = ()  # Inherited from source if it has nested columns

    def __init__(
        self,
        source: Relation,
        post: dict[str, Callable] | None,
        nested_columns: tuple[str, ...] = (),
    ) -> None:
        frozen_post = FrozenDict(
            {name: _ensure_wrapped(fn) for name, fn in (post or {}).items()},
        )
        source_nested = nested_columns if nested_columns else getattr(source, "nested_columns", ())

        super().__init__(
            source=Relation.__coerce__(source),
            post=frozen_post,
            nested_columns=source_nested,
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_ibis(self):
        from .measure_scope import MeasureScope

        agg_tbl = _to_ibis(self.source)

        # Process mutations incrementally so each can reference previous ones
        # This allows: .mutate(rank=..., is_other=lambda t: t["rank"] > 5)
        current_tbl = agg_tbl
        for name, fn_wrapped in self.post.items():
            proxy = MeasureScope(_tbl=current_tbl, _known=[], _post_agg=True)
            new_col = _resolve_expr(_unwrap(fn_wrapped), proxy).name(name)
            current_tbl = current_tbl.mutate([new_col])

        return current_tbl

    def __repr__(self) -> str:
        cols = list(self.post.keys())
        cols_str = ", ".join(cols[:5])
        if len(cols) > 5:
            cols_str += f", ... ({len(cols)} total)"
        return f"SemanticMutate(cols=[{cols_str}])"


class SemanticUnnestOp(Relation):
    """Unnest an array column, expanding rows (like Malloy's nested data pattern)."""

    source: Relation
    column: str

    @property
    def schema(self) -> Schema:
        # After unnesting, the schema changes - the array column is replaced by its element schema
        # For now, delegate to source schema (ideally we'd update it)
        return self.source.schema

    @property
    def values(self) -> FrozenDict:
        return FrozenDict({})

    def to_ibis(self):
        """Convert to Ibis expression with functional struct unpacking.

        Uses pure helper functions to extract struct fields when unnesting
        produces struct columns that need to be expanded.
        """

        def build_struct_fields(col_expr, col_type):
            """Pure function: build dict of struct field selections."""
            return {name: col_expr[name] for name in col_type.names}

        def unpack_struct_if_needed(unnested_tbl, column_name):
            """Conditionally unpack struct fields into top-level columns."""
            if column_name not in unnested_tbl.columns:
                return unnested_tbl

            col_expr = unnested_tbl[column_name]
            col_type = col_expr.type()

            if hasattr(col_type, "fields") and col_type.fields:
                struct_fields = build_struct_fields(col_expr, col_type)
                return unnested_tbl.select(unnested_tbl, **struct_fields)

            return unnested_tbl

        tbl = _to_ibis(self.source)

        if self.column not in tbl.columns:
            raise ValueError(f"Column '{self.column}' not found in table")

        try:
            unnested = tbl.unnest(self.column)
        except Exception as e:
            raise ValueError(f"Failed to unnest column '{self.column}': {e}") from e

        return unpack_struct_if_needed(unnested, self.column)

    def __repr__(self) -> str:
        return f"SemanticUnnest(column={self.column!r})"


class SemanticJoinOp(Relation):
    left: Relation
    right: Relation
    how: str
    on: Callable[[Any, Any], ir.BooleanValue] | None
    lname: str | None
    rname: str | None

    def __init__(
        self,
        left: Relation,
        right: Relation,
        how: str = "inner",
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        lname: str | None = None,
        rname: str | None = None,
    ) -> None:
        super().__init__(
            left=Relation.__coerce__(left),
            right=Relation.__coerce__(right),
            how=how,
            on=on,
            lname=lname,
            rname=rname,
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        vals: dict[str, Any] = {}
        vals.update(self.left.values)
        vals.update(self.right.values)
        return FrozenOrderedDict(vals)

    @property
    def schema(self) -> Schema:
        return Schema({name: v.dtype for name, v in self.values.items()})

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "dimensions"),
        )

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of base measures with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "measures"),
        )

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "calc_measures"),
        )

    @property
    def dimensions(self) -> tuple[str, ...]:
        """Get tuple of dimension names."""
        return tuple(self.get_dimensions().keys())

    @property
    def _dims(self) -> dict[str, Dimension]:
        return dict(self.get_dimensions())

    @property
    def _base_measures(self) -> dict[str, Measure]:
        return dict(self.get_measures())

    @property
    def _calc_measures(self) -> dict[str, Any]:
        return dict(self.get_calculated_measures())

    @property
    def measures(self) -> tuple[str, ...]:
        return tuple(self.get_measures().keys()) + tuple(
            self.get_calculated_measures().keys(),
        )

    @property
    def json_definition(self) -> Mapping[str, Any]:
        return _build_json_definition(self.get_dimensions(), self.get_measures(), None)

    def with_dimensions(self, **dims) -> SemanticTable:
        return _semantic_table(
            table=self.to_ibis(),
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            name=None,
        )

    def with_measures(self, **meas) -> SemanticTable:
        from .measure_scope import MeasureScope

        joined_tbl = self.to_ibis()
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

        return _semantic_table(
            table=joined_tbl,
            dimensions=self.get_dimensions(),
            measures=new_base,
            calc_measures=new_calc,
            name=None,
        )

    def group_by(self, *keys: str) -> SemanticGroupBy:
        from .expr import SemanticGroupBy

        return SemanticGroupBy(source=self, keys=keys)

    def filter(self, predicate: Callable) -> SemanticFilter:
        from .expr import SemanticFilter

        return SemanticFilter(source=self, predicate=predicate)

    def join(
        self,
        other: SemanticTable,
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
        how: str = "inner",
        lname: str | None = None,
        rname: str | None = None,
    ) -> SemanticJoinOp:
        return SemanticJoinOp(
            left=self,
            right=_unwrap_semantic_table(other),
            on=on,
            how=how,
            lname=lname,
            rname=rname,
        )

    def join_one(
        self,
        other: SemanticTable,
        left_on: str,
        right_on: str,
        lname: str | None = None,
        rname: str | None = None,
    ) -> SemanticJoinOp:
        return SemanticJoinOp(
            left=self,
            right=_unwrap_semantic_table(other),
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="inner",
            lname=lname,
            rname=rname,
        )

    def join_many(
        self,
        other: SemanticTable,
        left_on: str,
        right_on: str,
        lname: str | None = None,
        rname: str | None = None,
    ) -> SemanticJoinOp:
        return SemanticJoinOp(
            left=self,
            right=_unwrap_semantic_table(other),
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="left",
            lname=lname,
            rname=rname,
        )

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> SemanticIndexOp:
        return SemanticIndexOp(source=self, selector=selector, by=by, sample=sample)

    def to_ibis(self):
        from .convert import _Resolver

        left_tbl_orig = _to_ibis(self.left)
        right_tbl_orig = _to_ibis(self.right)

        # Perform join first with original column names
        if self.on is not None:
            joined = left_tbl_orig.join(
                right_tbl_orig,
                self.on(_Resolver(left_tbl_orig), _Resolver(right_tbl_orig)),
                how=self.how,
            )
        else:
            joined = left_tbl_orig.join(right_tbl_orig, how=self.how)

        # Apply Ibis-level column prefixing only if explicitly requested
        # This is opt-in to avoid conflicts with semantic-level prefixing
        if self.lname is not None or self.rname is not None:
            # Determine column prefixes
            left_prefix = self.lname if self.lname is not None else ""

            if self.rname is not None:
                right_prefix = self.rname
            else:
                # Auto-derive from right table's semantic name if rname explicitly set to None
                # but lname was provided (indicating user wants prefixing)
                right_roots = _find_all_root_models(self.right)
                if right_roots and right_roots[0].name:
                    right_prefix = f"{right_roots[0].name}_{{name}}"
                else:
                    right_prefix = "{name}_right"

            # Build the select list with prefixed names
            select_exprs = []

            # Left table columns
            for col in left_tbl_orig.columns:
                new_name = left_prefix.format(name=col) if left_prefix else col
                select_exprs.append(joined[col].name(new_name))

            # Right table columns
            for col in right_tbl_orig.columns:
                new_name = right_prefix.format(name=col) if right_prefix else col
                select_exprs.append(joined[col].name(new_name))

            return joined.select(select_exprs)

        return joined

    def execute(self):
        return self.to_ibis().execute()

    def compile(self, **kwargs):
        return self.to_ibis().compile(**kwargs)

    def sql(self, **kwargs):
        import ibis

        return ibis.to_sql(self.to_ibis(), **kwargs)

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

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def as_table(self) -> SemanticTable:
        """Convert to SemanticTable, preserving merged metadata from both sides."""
        return _semantic_table(
            table=self.to_ibis(),
            dimensions=self.get_dimensions(),
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
        )

    def __repr__(self) -> str:
        left_name = getattr(self.left, "name", None) or "<expr>"
        right_name = getattr(self.right, "name", None) or "<expr>"
        on_str = "<function>" if self.on else "cross"
        return f"SemanticJoinOp(left={left_name!r}, right={right_name!r}, how={self.how!r}, on={on_str})"


class SemanticOrderByOp(Relation):
    source: Relation
    keys: tuple[
        str | ir.Value | Callable,
        ...,
    ]  # Transformed to tuple[str | _CallableWrapper, ...] in __init__

    def __init__(self, source: Relation, keys: Iterable[str | ir.Value | Callable]) -> None:
        def wrap_key(k):
            return k if isinstance(k, str | _CallableWrapper) else _ensure_wrapped(k)

        super().__init__(
            source=Relation.__coerce__(source),
            keys=tuple(wrap_key(k) for k in keys),
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_ibis(self):
        tbl = _to_ibis(self.source)

        def resolve_order_key(key):
            if isinstance(key, str):
                return (
                    tbl[key]
                    if key in tbl.columns
                    else getattr(tbl, key)
                    if hasattr(tbl, key)
                    else key
                )
            elif isinstance(key, _CallableWrapper):
                unwrapped = _unwrap(key)
                return _resolve_expr(unwrapped, tbl)
            return key

        return tbl.order_by([resolve_order_key(key) for key in self.keys])

    def __repr__(self) -> str:
        keys_list = []
        for k in list(self.keys)[:3]:
            if isinstance(k, str):
                keys_list.append(repr(k))
            else:
                keys_list.append("<expr>")
        if len(self.keys) > 3:
            keys_list.append(f"... ({len(self.keys)} total)")
        return f"SemanticOrderBy(keys=[{', '.join(keys_list)}])"


class SemanticLimitOp(Relation):
    source: Relation
    n: int
    offset: int

    def __init__(self, source: Relation, n: int, offset: int = 0) -> None:
        if n <= 0:
            raise ValueError(f"limit must be positive, got {n}")
        if offset < 0:
            raise ValueError(f"offset must be non-negative, got {offset}")
        super().__init__(source=Relation.__coerce__(source), n=n, offset=offset)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_ibis(self):
        tbl = _to_ibis(self.source)
        return tbl.limit(self.n) if self.offset == 0 else tbl.limit(self.n, offset=self.offset)


def _get_field_type_str(field_type: Any) -> str:
    return (
        "string"
        if field_type.is_string()
        else "number"
        if field_type.is_numeric()
        else "date"
        if field_type.is_temporal()
        else str(field_type)
    )


def _get_weight_expr(
    base_tbl: Any,
    by_measure: str | None,
    all_roots: list,
    is_string: bool,
) -> Any:
    import ibis

    if not by_measure:
        return ibis._.count()

    merged_measures = _get_merged_fields(all_roots, "measures")
    return (
        merged_measures[by_measure](base_tbl) if by_measure in merged_measures else ibis._.count()
    )


def _build_string_index_fragment(
    base_tbl: Any,
    field_expr: Any,
    field_name: str,
    field_path: str,
    type_str: str,
    weight_expr: Any,
) -> Any:
    import ibis

    return (
        base_tbl.group_by(field_expr.name("value"))
        .aggregate(weight=weight_expr)
        .select(
            fieldName=ibis.literal(field_name.split(".")[-1]),
            fieldPath=ibis.literal(field_path),
            fieldType=ibis.literal(type_str),
            fieldValue=ibis._["value"].cast("string"),
            weight=ibis._["weight"],
        )
    )


def _build_numeric_index_fragment(
    base_tbl: Any,
    field_expr: Any,
    field_name: str,
    field_path: str,
    type_str: str,
    weight_expr: Any,
) -> Any:
    import ibis

    return (
        base_tbl.select(field_expr.name("value"))
        .filter(ibis._["value"].notnull())
        .aggregate(
            min_val=ibis._["value"].min(),
            max_val=ibis._["value"].max(),
            weight=weight_expr,
        )
        .select(
            fieldName=ibis.literal(field_name.split(".")[-1]),
            fieldPath=ibis.literal(field_path),
            fieldType=ibis.literal(type_str),
            fieldValue=(
                ibis._["min_val"].cast("string") + " to " + ibis._["max_val"].cast("string")
            ),
            weight=ibis._["weight"],
        )
    )


def _resolve_selector(
    selector: str | list[str] | Callable | None,
    base_tbl: ir.Table,
) -> tuple[str, ...]:
    if selector is None:
        return tuple(base_tbl.columns)
    try:
        selected = base_tbl.select(selector)
        return tuple(selected.columns)
    except Exception:
        return []


def _get_fields_to_index(
    selector: str | list[str] | Callable | None,
    merged_dimensions: dict,
    base_tbl: ir.Table,
) -> tuple[str, ...]:
    import ibis.selectors as s

    # Handle None as "all fields"
    if selector is None:
        selector = s.all()

    raw_fields = _resolve_selector(selector, base_tbl)

    # If raw_fields is empty (selector failed to resolve), include all fields
    if not raw_fields:
        result = list(merged_dimensions.keys())
        result.extend(col for col in base_tbl.columns if col not in result)
    else:
        # Only include selected fields that exist in dimensions or base table
        result = [col for col in raw_fields if col in merged_dimensions or col in base_tbl.columns]

    return result


class SemanticIndexOp(Relation):
    source: Relation
    selector: str | list[str] | Callable | None
    by: str | None = None
    sample: int | None = None

    def __init__(
        self,
        source: Relation,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> None:
        # Validate sample parameter
        if sample is not None and sample <= 0:
            raise ValueError(f"sample must be positive, got {sample}")

        # Validate 'by' measure exists if provided
        if by is not None:
            all_roots = _find_all_root_models(source)
            if all_roots:
                merged_measures = _get_merged_fields(all_roots, "measures")
                if by not in merged_measures:
                    available = list(merged_measures.keys())
                    raise KeyError(
                        f"Unknown measure '{by}' for weight calculation. "
                        f"Available measures: {', '.join(available) or 'none'}",
                    )

        super().__init__(
            source=Relation.__coerce__(source),
            selector=selector,
            by=by,
            sample=sample,
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        import ibis

        return FrozenOrderedDict(
            {
                "fieldName": ibis.literal("").op(),
                "fieldPath": ibis.literal("").op(),
                "fieldType": ibis.literal("").op(),
                "fieldValue": ibis.literal("").op(),
                "weight": ibis.literal(0).op(),
            },
        )

    @property
    def schema(self) -> Schema:
        return Schema(
            {
                "fieldName": "string",
                "fieldPath": "string",
                "fieldType": "string",
                "fieldValue": "string",
                "weight": "int64",
            },
        )

    def to_ibis(self):
        from functools import reduce

        import ibis

        all_roots = _find_all_root_models(self.source)
        base_tbl = (
            _to_ibis(self.source).limit(self.sample) if self.sample else _to_ibis(self.source)
        )

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        fields_to_index = _get_fields_to_index(
            self.selector,
            merged_dimensions,
            base_tbl,
        )

        if not fields_to_index:
            return ibis.memtable(
                {
                    "fieldName": [],
                    "fieldPath": [],
                    "fieldType": [],
                    "fieldValue": [],
                    "weight": [],
                },
            )

        def build_fragment(field_name: str) -> Any:
            field_expr = (
                merged_dimensions[field_name](base_tbl)
                if field_name in merged_dimensions
                else base_tbl[field_name]
            )
            field_type = field_expr.type()
            type_str = _get_field_type_str(field_type)
            weight_expr = _get_weight_expr(
                base_tbl,
                self.by,
                all_roots,
                field_type.is_string(),
            )

            return (
                _build_string_index_fragment(
                    base_tbl,
                    field_expr,
                    field_name,
                    field_name,
                    type_str,
                    weight_expr,
                )
                if field_type.is_string() or not field_type.is_numeric()
                else _build_numeric_index_fragment(
                    base_tbl,
                    field_expr,
                    field_name,
                    field_name,
                    type_str,
                    weight_expr,
                )
            )

        fragments = [build_fragment(f) for f in fields_to_index]
        return reduce(lambda acc, frag: acc.union(frag), fragments[1:], fragments[0])

    def filter(self, predicate: Callable) -> SemanticFilter:
        from .expr import SemanticFilter

        return SemanticFilter(source=self, predicate=predicate)

    def order_by(self, *keys: str | ir.Value | Callable) -> SemanticOrderBy:
        from .expr import SemanticOrderBy

        return SemanticOrderBy(source=self, keys=keys)

    def limit(self, n: int, offset: int = 0) -> SemanticLimit:
        from .expr import SemanticLimit

        return SemanticLimit(source=self, n=n, offset=offset)

    def execute(self):
        return self.to_ibis().execute()

    def as_expr(self):
        """Return self as expression."""
        return self

    def compile(self, **kwargs):
        return self.to_ibis().compile(**kwargs)

    def sql(self, **kwargs):
        import ibis

        return ibis.to_sql(self.to_ibis(), **kwargs)

    def __getitem__(self, key):
        return self.to_ibis()[key]

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def __repr__(self) -> str:
        parts = []
        if self.selector is not None:
            # Try to show selector repr, fallback to generic
            try:
                selector_repr = repr(self.selector)
                # If it's too long, truncate
                if len(selector_repr) > 50:
                    selector_repr = selector_repr[:47] + "..."
                parts.append(f"selector={selector_repr}")
            except Exception:
                parts.append("selector=<selector>")
        else:
            parts.append("selector=all()")

        if self.by:
            parts.append(f"by={self.by!r}")
        if self.sample:
            parts.append(f"sample={self.sample}")

        return f"SemanticIndexOp({', '.join(parts)})"


def _find_root_model(node: Any) -> SemanticTableOp | None:
    """Find root SemanticTableOp in the operation tree."""
    cur = node
    while cur is not None:
        if isinstance(cur, SemanticTableOp):
            return cur
        parent = getattr(cur, "source", None)
        cur = parent
    return None


def _find_all_root_models(node: Any) -> tuple[SemanticTableOp, ...]:
    """Find all root SemanticTableOps in the operation tree (handles joins with multiple roots)."""
    if isinstance(node, SemanticTableOp):
        return [node]

    roots = []

    # Handle joins with left/right sides
    if hasattr(node, "left") and hasattr(node, "right"):
        roots.extend(_find_all_root_models(node.left))
        roots.extend(_find_all_root_models(node.right))
    # Handle single-source operations
    elif hasattr(node, "source") and node.source is not None:
        roots.extend(_find_all_root_models(node.source))

    return roots


def _update_measure_refs_in_calc(expr, prefix_map: dict[str, str]):
    """
    Recursively update MeasureRef names in a calculated measure expression.

    Args:
        expr: A MeasureExpr (MeasureRef, AllOf, BinOp, or literal)
        prefix_map: Mapping from old name to new prefixed name

    Returns:
        Updated expression with prefixed MeasureRef names
    """
    from .measure_scope import AllOf, BinOp, MeasureRef

    if isinstance(expr, MeasureRef):
        # Update the measure reference name if it's in the map
        new_name = prefix_map.get(expr.name, expr.name)
        return MeasureRef(new_name)
    elif isinstance(expr, AllOf):
        # Update the inner MeasureRef
        updated_ref = _update_measure_refs_in_calc(expr.ref, prefix_map)
        return AllOf(updated_ref)
    elif isinstance(expr, BinOp):
        # Recursively update left and right
        updated_left = _update_measure_refs_in_calc(expr.left, prefix_map)
        updated_right = _update_measure_refs_in_calc(expr.right, prefix_map)
        return BinOp(op=expr.op, left=updated_left, right=updated_right)
    else:
        # Literal number or other - return as-is
        return expr


def _merge_fields_with_prefixing(
    all_roots: Sequence[SemanticTable],
    field_accessor: callable,
) -> FrozenDict[str, Any]:
    """
    Generic function to merge any type of fields (dimensions, measures) with prefixing.

    Args:
        all_roots: List of SemanticTable root models
        field_accessor: Function that takes a root and returns the fields dict (e.g. lambda r: r.dimensions)

    Returns:
        FrozenDict mapping field names (always prefixed with table name) to field values
    """
    if not all_roots:
        return FrozenDict()

    merged_fields = {}

    # Special handling for calculated measures - need to update internal MeasureRefs
    # Determine if we're processing calc measures by checking the field type
    is_calc_measures = False
    if all_roots:
        sample_fields = field_accessor(all_roots[0])
        if sample_fields:
            from .measure_scope import AllOf, BinOp, MeasureRef

            first_val = next(iter(sample_fields.values()), None)
            is_calc_measures = isinstance(
                first_val,
                MeasureRef | AllOf | BinOp | int | float,
            )

    # Always prefix fields with table name for consistency
    for root in all_roots:
        root_name = root.name
        fields_dict = field_accessor(root)

        if is_calc_measures and root_name:
            base_map = (
                {k: f"{root_name}.{k}" for k in root.get_measures()}
                if hasattr(root, "get_measures")
                else {}
            )
            calc_map = (
                {k: f"{root_name}.{k}" for k in root.get_calculated_measures()}
                if hasattr(root, "get_calculated_measures")
                else {}
            )
            prefix_map = {**base_map, **calc_map}

        for field_name, field_value in fields_dict.items():
            if root_name:
                # Always use prefixed name with . separator
                prefixed_name = f"{root_name}.{field_name}"

                # If it's a calculated measure, update internal MeasureRefs
                if is_calc_measures:
                    field_value = _update_measure_refs_in_calc(field_value, prefix_map)

                merged_fields[prefixed_name] = field_value
            else:
                # Fallback to original name if no root name
                merged_fields[field_name] = field_value

    return FrozenDict(merged_fields)
