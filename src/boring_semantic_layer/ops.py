from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from functools import reduce
from operator import methodcaller
from typing import TYPE_CHECKING, Any

from attrs import field, frozen
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
    return source.to_ibis() if hasattr(source, "to_ibis") else source.to_expr()


def _semantic_table(*args, **kwargs) -> SemanticTable:
    from .expr import SemanticModel

    return SemanticModel(*args, **kwargs)


def _unwrap_semantic_table(other: Any) -> Any:
    return methodcaller("op")(other) if hasattr(other, "op") and callable(other.op) else other


def _unwrap(wrapped: Any) -> Any:
    return wrapped.unwrap if isinstance(wrapped, _CallableWrapper) else wrapped


def _resolve_expr(expr: Deferred | Callable | Any, scope: ir.Table) -> ir.Value:
    return (
        expr.resolve(scope)
        if isinstance(expr, Deferred)
        else expr(scope)
        if callable(expr)
        else expr
    )


def _get_field_dict(root: Any, field_type: str) -> dict:
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
        if isinstance(expr.ref, MeasureRef):
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

    Examples:
        to_semantic_table(tbl).with_measures(...) -> (None, ())  # Session level
        to_semantic_table(tbl).unnest("hits").with_measures(...) -> ("hits", ("hits",))
        unnested.unnest("product").with_measures(...) -> ("product", ("product",))
    """
    from .expr import SemanticUnnest

    if isinstance(table, SemanticUnnest):
        op = table.op()
        if hasattr(op, "column"):
            locality = op.column  # e.g., "hits"
            requires_unnest = (op.column,)
            return (locality, requires_unnest)

    return (None, ())


def _extract_measure_metadata(fn_or_expr: Any) -> tuple[Any, str | None, str | None, tuple]:
    """Extract metadata from various measure representations."""
    if isinstance(fn_or_expr, dict):
        return (
            fn_or_expr["expr"],
            fn_or_expr.get("description"),
            fn_or_expr.get("locality"),
            tuple(fn_or_expr.get("requires_unnest", [])),
        )
    elif isinstance(fn_or_expr, Measure):
        return (
            fn_or_expr.expr,
            fn_or_expr.description,
            fn_or_expr.locality,
            fn_or_expr.requires_unnest,
        )
    else:
        return (fn_or_expr, None, None, ())


def _is_calculated_measure(val: Any) -> bool:
    from .measure_scope import AllOf, BinOp, MeasureRef

    return isinstance(val, MeasureRef | AllOf | BinOp | int | float)


def _matches_aggregation_pattern(measure_expr, agg_expr, tbl):
    from toolz import curry

    from .measure_scope import AggregationExpr, ColumnScope
    from .utils import success, try_result

    if not isinstance(agg_expr, AggregationExpr):
        return success(False)

    @curry
    def evaluate_in_scope(tbl, expr):
        """Evaluate measure expression in a ColumnScope."""
        scope = ColumnScope(_tbl=tbl)
        return (
            expr.resolve(scope)
            if isinstance(expr, Deferred)
            else expr(scope)
            if callable(expr)
            else expr
        )

    @curry
    def has_matching_operation(agg_expr, result):
        """Check if the operation matches the expected aggregation."""
        if not hasattr(result, "op"):
            return False

        op_name = type(result.op()).__name__.lower()
        expected_op = "avg" if agg_expr.operation.lower() == "mean" else agg_expr.operation.lower()

        return expected_op in op_name

    @curry
    def has_matching_column(agg_expr, result):
        """Check if the column matches the expected column."""
        if not hasattr(result, "op"):
            return False

        op = result.op()
        if not hasattr(op, "args") or len(op.args) == 0:
            return False

        first_arg = op.args[0]
        if not hasattr(first_arg, "__class__"):
            return False

        arg_name = type(first_arg).__name__
        if "Field" not in arg_name and "Column" not in arg_name:
            return False

        # Try direct name attribute
        if hasattr(first_arg, "name"):
            return first_arg.name == agg_expr.column

        # Try extracting from args
        if hasattr(first_arg, "args") and len(first_arg.args) > 1:
            field_name = first_arg.args[1]
            # Skip if it's a type, not a name
            if hasattr(field_name, "__class__") and type(field_name).__name__ == "str":
                return False
            return field_name == agg_expr.column

        return False

    def matches_pattern(result):
        """Check if result matches both operation and column."""
        return has_matching_operation(agg_expr, result) and has_matching_column(agg_expr, result)

    return try_result(lambda: evaluate_in_scope(tbl, measure_expr)).map(matches_pattern)


def _find_matching_measure(agg_expr, known_measures: dict, tbl):
    """Find a measure that matches the aggregation expression pattern.

    Returns Option[str] using functional patterns.
    """
    from toolz import curry

    from .measure_scope import AggregationExpr
    from .utils import nothing, some

    if not isinstance(agg_expr, AggregationExpr):
        return nothing()

    @curry
    def is_valid_measure(measure_obj):
        """Check if object has expr attribute (is a Measure)."""
        return hasattr(measure_obj, "expr")

    @curry
    def matches_pattern(agg_expr, tbl, measure_obj):
        """Check if measure matches the aggregation pattern."""
        result = _matches_aggregation_pattern(measure_obj.expr, agg_expr, tbl)
        return result.unwrap_or(False)

    # Find first measure that matches the pattern
    for measure_name, measure_obj in known_measures.items():
        if is_valid_measure(measure_obj) and matches_pattern(agg_expr, tbl, measure_obj):
            return some(measure_name)

    return nothing()


def _make_base_measure(
    expr: Any,
    description: str | None,
    locality: str | None,
    requires_unnest: tuple,
) -> Measure:
    """Create a base measure with proper callable wrapping using functional patterns."""
    from toolz import curry

    from .measure_scope import AggregationExpr, ColumnScope
    from .utils import option_of

    # Functional mapping of aggregation operations to ibis column methods
    @curry
    def apply_aggregation(operation: str, column):
        """Apply aggregation operation to a column using functional dispatch."""
        operations = {
            "sum": lambda c: c.sum(),
            "mean": lambda c: c.mean(),
            "avg": lambda c: c.mean(),
            "count": lambda c: c.count(),
            "min": lambda c: c.min(),
            "max": lambda c: c.max(),
        }

        return (
            option_of(operations.get(operation))
            .map(lambda fn: fn(column))
            .unwrap_or_else(
                lambda: (_ for _ in ()).throw(
                    ValueError(f"Unknown aggregation operation: {operation}")
                )
            )
        )

    @curry
    def evaluate_expr(expr, scope):
        """Evaluate expression in given scope."""
        return (
            expr.resolve(scope)
            if isinstance(expr, Deferred)
            else expr(scope)
            if callable(expr)
            else expr
        )

    def convert_aggregation_expr(t, agg_expr: AggregationExpr):
        """Convert AggregationExpr to ibis expression."""
        # First, perform the aggregation
        if agg_expr.operation == "count":
            # Count operates on the table, not a specific column
            result = t.count()
        else:
            result = apply_aggregation(agg_expr.operation, t[agg_expr.column])

        # Then, apply any post-aggregation operations (e.g., .epoch_seconds())
        for method_name, args, kwargs in agg_expr.post_ops:
            result = getattr(result, method_name)(*args, **kwargs)

        return result

    # Handle AggregationExpr passed directly (e.g., from _.count())
    if isinstance(expr, AggregationExpr):

        def wrapped_expr(t):
            """Convert AggregationExpr to ibis expression."""
            return convert_aggregation_expr(t, expr)

        return Measure(
            expr=wrapped_expr,
            description=description,
            locality=locality,
            requires_unnest=requires_unnest,
        )

    if callable(expr):

        def wrapped_expr(t):
            """Wrapped expression that handles AggregationExpr conversion."""
            scope = ColumnScope(_tbl=t)
            result = evaluate_expr(expr, scope)

            # Convert AggregationExpr to actual ibis expression
            if isinstance(result, AggregationExpr):
                return convert_aggregation_expr(t, result)
            return result

        return Measure(
            expr=wrapped_expr,
            description=description,
            locality=locality,
            requires_unnest=requires_unnest,
        )
    else:
        return Measure(
            expr=lambda t, fn=expr: evaluate_expr(fn, ColumnScope(_tbl=t)),
            description=description,
            locality=locality,
            requires_unnest=requires_unnest,
        )


def _check_op_for_window(op, depth=0):
    """Recursively check an operation node for window functions."""
    if op is None or depth > 20:  # Prevent infinite recursion
        return False

    # Skip primitive types and common non-operation types
    if isinstance(op, str | int | float | bool | type(None)):
        return False

    op_name = type(op).__name__

    # Check if this is a window operation
    if "Window" in op_name or "AnalyticOp" in op_name:
        return True

    # Only recurse into ibis operation objects (they have __module__ containing 'ibis')
    if (
        hasattr(op, "__class__")
        and hasattr(op.__class__, "__module__")
        and "ibis" not in op.__class__.__module__
    ):
        return False

    # Recursively check args
    if hasattr(op, "args"):
        for arg in op.args:
            if _check_op_for_window(arg, depth + 1):
                return True

    return False


def _has_window_function(expr, depth=0):
    """Check if an ibis expression contains window functions."""
    if not hasattr(expr, "op"):
        return False
    try:
        return _check_op_for_window(expr.op(), depth)
    except Exception:
        return False


def _classify_measure(fn_or_expr: Any, scope: Any) -> tuple[str, Any]:
    """Classify measure as 'calc' or 'base' with appropriate handling."""
    from .utils import try_result

    expr, description, locality, requires_unnest = _extract_measure_metadata(fn_or_expr)

    resolved = try_result(lambda: _resolve_expr(expr, scope)).map(
        lambda val: ("calc", val) if _is_calculated_measure(val) else None
    )

    if resolved.is_success() and resolved.value is not None:
        return resolved.value

    if locality is None and callable(expr):
        table = getattr(scope, "tbl", None)
        if table is None:
            table = getattr(scope, "_tbl", None)
        if table is not None:
            inferred_locality, inferred_unnest = _infer_locality(expr, table)
            locality = locality or inferred_locality
            requires_unnest = requires_unnest or inferred_unnest

    return ("base", _make_base_measure(expr, description, locality, requires_unnest))


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
    _source_join: Any = None  # Track if this wraps a join (SemanticJoinOp) for optimization

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

    @property
    def required_columns(self) -> dict[str, set[str]]:
        """
        Column requirements for this aggregation operation.

        This property makes column requirements intrinsic to the aggregate operation,
        similar to how `schema` is intrinsic to a relation.

        Returns:
            Dict mapping table names to sets of required column names.
        """
        from . import projection_utils

        all_roots = _find_all_root_models(self.source)
        merged_dimensions = _get_merged_fields(all_roots, "dimensions")

        # Get base table for column extraction
        base_tbl = (
            self.source.to_expr() if hasattr(self.source, "to_expr") else _to_ibis(self.source)
        )

        # Get table names
        table_names = []
        for root in all_roots:
            if root.name:
                table_names.append(root.name)
            elif hasattr(root, "_source_join") and root._source_join is not None:
                join_roots = _find_all_root_models(root._source_join)
                table_names.extend([r.name for r in join_roots if r.name])

        # Extract requirements from keys using TableRequirements
        key_requirements = projection_utils.extract_requirements_from_keys(
            keys=list(self.keys),
            dimensions=merged_dimensions,
            table=base_tbl,
            table_names=table_names,
        )

        # Extract requirements from measures using TableRequirements
        measure_requirements = projection_utils.extract_requirements_from_measures(
            measures={name: _unwrap(fn) for name, fn in self.aggs.items()},
            table=base_tbl,
            table_names=table_names,
        )

        # Merge requirements (immutable operation)
        combined = key_requirements.merge(measure_requirements)

        # Merge with source requirements if available
        if hasattr(self.source, "required_columns"):
            source_reqs = projection_utils.TableRequirements.from_dict(self.source.required_columns)
            combined = combined.merge(source_reqs)

        # Convert to mutable dict for API compatibility
        return combined.to_dict()

    def to_ibis(self):
        from .compile_all import compile_grouped_with_all
        from .measure_scope import AllOf, BinOp, ColumnScope, MeasureRef, MeasureScope

        all_roots = _find_all_root_models(self.source)

        # Check if there's a join anywhere in the operation tree
        def find_join_in_tree(node):
            """Find a SemanticJoinOp in the operation tree."""
            if isinstance(node, SemanticJoinOp):
                return node
            if hasattr(node, "source") and node.source is not None:
                return find_join_in_tree(node.source)
            return None

        # Apply projection pushdown if there's a join in the tree
        join_op = find_join_in_tree(self.source)

        # Also check if a grouped table wraps a join
        if join_op is None and isinstance(self.source, SemanticGroupByOp):
            grouped_source = self.source.source
            if isinstance(grouped_source, SemanticTableOp) and hasattr(
                grouped_source, "_source_join"
            ):
                join_op = grouped_source._source_join

        if join_op is not None:
            # Use intrinsic required_columns property from this aggregate's group_by keys and measures
            # Pass to join, which will use its own required_columns property and merge with parent
            tbl = join_op.to_ibis(parent_requirements=self.required_columns)
        else:
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

        def _resolve_aggregation_exprs(expr):
            """Recursively resolve AggregationExpr to MeasureRef if a matching measure exists.

            Uses functional composition with Option monads for cleaner control flow.
            """
            from toolz import curry

            from .measure_scope import AggregationExpr, BinOp

            @curry
            def find_in_calc_measures(expr, calc_measures):
                """Find matching AggregationExpr in calc measures."""
                from .utils import nothing, some

                for calc_name, calc_expr in calc_measures.items():
                    if isinstance(calc_expr, AggregationExpr) and (
                        calc_expr.column == expr.column and calc_expr.operation == expr.operation
                    ):
                        return some(calc_name)
                return nothing()

            def resolve_aggregation(agg_expr):
                """Resolve an AggregationExpr to a MeasureRef or keep as-is."""
                # Try base measures first, then calc measures
                matched = _find_matching_measure(agg_expr, merged_base_measures, tbl)

                return matched.map(
                    MeasureRef
                ).unwrap_or_else(  # Map Option[str] -> Option[MeasureRef]
                    lambda: (
                        find_in_calc_measures(agg_expr, merged_calc_measures)
                        .map(MeasureRef)
                        .unwrap_or(agg_expr)  # Keep original if no match
                    )
                )

            if isinstance(expr, AggregationExpr):
                return resolve_aggregation(expr)
            elif isinstance(expr, BinOp):
                # Recursively resolve in binary operations
                return BinOp(
                    op=expr.op,
                    left=_resolve_aggregation_exprs(expr.left),
                    right=_resolve_aggregation_exprs(expr.right),
                )
            elif isinstance(expr, AllOf):
                # Recursively resolve the reference
                if isinstance(expr.ref, AggregationExpr):
                    resolved_ref = resolve_aggregation(expr.ref)
                    return AllOf(resolved_ref)
                return expr
            else:
                return expr

        agg_specs = {}
        calc_specs = {}

        for name, fn_wrapped in self.aggs.items():
            fn = _unwrap(fn_wrapped)
            val = _resolve_expr(fn, scope)

            # Try to resolve any AggregationExpr to existing measures
            val = _resolve_aggregation_exprs(val)

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
                    # Resolve any AggregationExpr in the calc measure
                    calc_expr = merged_calc_measures[ref_name]
                    calc_specs[name] = _resolve_aggregation_exprs(calc_expr)
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

    def get_required_columns(self) -> dict[str, set[str]]:
        """Extract column requirements from mutate operations."""
        from . import projection_utils

        all_roots = _find_all_root_models(self.source)
        table_names = [root.name for root in all_roots if root.name]

        base_tbl = (
            self.source.to_expr() if hasattr(self.source, "to_expr") else _to_ibis(self.source)
        )

        from .utils import try_result

        def process_mutation(
            reqs: projection_utils.TableRequirements, fn_wrapped: Any
        ) -> projection_utils.TableRequirements:
            """Process single mutation, extracting column requirements."""
            return (
                try_result(lambda: _unwrap(fn_wrapped))
                .flatmap(lambda fn: projection_utils.extract_columns_from_callable(fn, base_tbl))
                .map(
                    lambda cols: projection_utils._apply_requirements_to_tables(
                        reqs, table_names, cols
                    )
                    if cols
                    else reqs
                )
                .unwrap_or_else(
                    lambda: projection_utils.include_all_columns_for_table(
                        reqs, base_tbl, table_names[0]
                    )
                    if table_names
                    else reqs
                )
            )

        requirements = reduce(
            process_mutation, self.post.values(), projection_utils.TableRequirements.empty()
        )
        return requirements.to_dict()

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

    def __init__(
        self,
        left: Relation,
        right: Relation,
        how: str = "inner",
        on: Callable[[Any, Any], ir.BooleanValue] | None = None,
    ) -> None:
        super().__init__(
            left=Relation.__coerce__(left),
            right=Relation.__coerce__(right),
            how=how,
            on=on,
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
    def calc_measures(self) -> dict[str, Any]:
        """Get calculated measures as dict (for consistency with SemanticModel)."""
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
            _source_join=self,  # Pass join reference for projection pushdown
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
    ) -> SemanticJoinOp:
        return SemanticJoinOp(
            left=self,
            right=_unwrap_semantic_table(other),
            on=on,
            how=how,
        )

    def join_one(
        self,
        other: SemanticTable,
        left_on: str,
        right_on: str,
    ) -> SemanticJoinOp:
        return SemanticJoinOp(
            left=self,
            right=_unwrap_semantic_table(other),
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="inner",
        )

    def join_many(
        self,
        other: SemanticTable,
        left_on: str,
        right_on: str,
    ) -> SemanticJoinOp:
        return SemanticJoinOp(
            left=self,
            right=_unwrap_semantic_table(other),
            on=lambda left, right: getattr(left, left_on) == getattr(right, right_on),
            how="left",
        )

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> SemanticIndexOp:
        return SemanticIndexOp(source=self, selector=selector, by=by, sample=sample)

    def _collect_leaf_table_names(self) -> set[str]:
        """Collect names of all leaf (base) tables in this join tree."""
        tables = set()

        if isinstance(self.left, SemanticJoinOp):
            tables |= self.left._collect_leaf_table_names()
        else:
            left_name = self.left.name if hasattr(self.left, "name") else None
            if left_name:
                tables.add(left_name)

        if isinstance(self.right, SemanticJoinOp):
            tables |= self.right._collect_leaf_table_names()
        else:
            right_name = self.right.name if hasattr(self.right, "name") else None
            if right_name:
                tables.add(right_name)

        return tables

    @property
    def required_columns(self):
        """
        Column requirements for projection pushdown.

        This property makes projection pushdown intrinsic to the join operation,
        similar to how `schema` is intrinsic to a relation.

        Computes what columns are needed from each leaf table based on:
        1. Columns needed for measures defined on the joined tables
        2. Join key columns

        Returns:
            Dict mapping table names to sets of required column names.
        """
        return self._compute_required_columns()

    def _compute_required_columns(self, parent_requirements: dict[str, set[str]] | None = None):
        """
        Compute column requirements for projection pushdown.

        Args:
            parent_requirements: Optional dict of specific columns requested by parent operations.

        Returns:
            Dict mapping table names to sets of required column names.
        """
        from . import projection_utils

        # Start with parent requirements using immutable TableRequirements
        requirements = projection_utils.TableRequirements.from_dict(
            parent_requirements if parent_requirements else {}
        )

        # Get all root models in join tree
        all_roots = _find_all_root_models(self)

        # Collect leaf tables
        def collect_leaf_tables(node):
            if isinstance(node, SemanticJoinOp):
                return collect_leaf_tables(node.left) + collect_leaf_tables(node.right)
            table_name = node.name if hasattr(node, "name") else None
            return [(table_name, _to_ibis(node))] if table_name else []

        leaf_tables = collect_leaf_tables(self)

        # Group measures by table
        measures_by_table = {}
        for root in all_roots:
            root_measures = _get_field_dict(root, "measures")
            if root.name:
                measures_by_table[root.name] = root_measures
            else:
                # Parse prefixed measures (e.g., "marketing.spend")
                for measure_name, measure_obj in root_measures.items():
                    if "." in measure_name:
                        table_name = measure_name.split(".", 1)[0]
                        if table_name not in measures_by_table:
                            measures_by_table[table_name] = {}
                        measures_by_table[table_name][measure_name] = measure_obj

        # Extract columns needed by measures (using immutable operations)
        for table_name, table_ibis in leaf_tables:
            if table_name in measures_by_table:
                for measure_obj in measures_by_table[table_name].values():
                    measure_fn = measure_obj.expr if hasattr(measure_obj, "expr") else measure_obj
                    if callable(measure_fn):
                        cols = projection_utils.extract_columns_from_callable_safe(
                            measure_fn, table_ibis
                        )
                        if cols:
                            requirements = requirements.add_columns(table_name, cols)

        # Extract and add join key columns
        if self.on is not None:
            # Get full schema for join key extraction
            temp_left = (
                self.left.to_ibis()
                if isinstance(self.left, SemanticJoinOp)
                else _to_ibis(self.left)
            )
            temp_right = (
                self.right.to_ibis()
                if isinstance(self.right, SemanticJoinOp)
                else _to_ibis(self.right)
            )

            join_keys_result = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys_result.is_success():
                # Add join keys to leaf tables (immutable operations)
                if isinstance(self.left, SemanticJoinOp):
                    for col in join_keys_result.left_columns:
                        for leaf_name in self.left._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.left, leaf_name)
                            if leaf_table and col in _to_ibis(leaf_table).columns:
                                requirements = requirements.add_columns(leaf_name, {col})
                else:
                    left_name = self.left.name if hasattr(self.left, "name") else None
                    if left_name:
                        requirements = requirements.add_columns(
                            left_name, join_keys_result.left_columns
                        )

                if isinstance(self.right, SemanticJoinOp):
                    for col in join_keys_result.right_columns:
                        for leaf_name in self.right._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.right, leaf_name)
                            if leaf_table and col in _to_ibis(leaf_table).columns:
                                requirements = requirements.add_columns(leaf_name, {col})
                else:
                    right_name = self.right.name if hasattr(self.right, "name") else None
                    if right_name:
                        requirements = requirements.add_columns(
                            right_name, join_keys_result.right_columns
                        )

        return requirements.to_dict()

    def _get_leaf_table_by_name(self, join_op: SemanticJoinOp, target_name: str):
        """Find a leaf table by name in a join tree."""
        if isinstance(join_op.left, SemanticJoinOp):
            result = self._get_leaf_table_by_name(join_op.left, target_name)
            if result is not None:
                return result
        else:
            left_name = join_op.left.name if hasattr(join_op.left, "name") else None
            if left_name == target_name:
                return join_op.left

        if isinstance(join_op.right, SemanticJoinOp):
            result = self._get_leaf_table_by_name(join_op.right, target_name)
            if result is not None:
                return result
        else:
            right_name = join_op.right.name if hasattr(join_op.right, "name") else None
            if right_name == target_name:
                return join_op.right

        return None

    def _collect_join_keys_for_leaves(self) -> dict[str, set[str]]:
        """Collect join keys needed by each leaf table.

        For nested joins, we trace join keys back to their source leaf tables.
        Returns dict mapping leaf table names to sets of columns needed for joins.
        """
        join_columns: dict[str, set[str]] = {}

        # Recursively collect from nested joins
        if isinstance(self.left, SemanticJoinOp):
            nested_keys = self.left._collect_join_keys_for_leaves()
            for table_name, cols in nested_keys.items():
                existing = join_columns.get(table_name, set())
                join_columns[table_name] = existing | cols

        if isinstance(self.right, SemanticJoinOp):
            nested_keys = self.right._collect_join_keys_for_leaves()
            for table_name, cols in nested_keys.items():
                existing = join_columns.get(table_name, set())
                join_columns[table_name] = existing | cols

        # Add join keys for THIS level
        if self.on is not None:
            # Convert without projection to get full schema
            temp_left = (
                self.left.to_ibis(parent_requirements=None)
                if isinstance(self.left, SemanticJoinOp)
                else _to_ibis(self.left)
            )
            temp_right = (
                self.right.to_ibis(parent_requirements=None)
                if isinstance(self.right, SemanticJoinOp)
                else _to_ibis(self.right)
            )

            join_keys = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys.is_success():
                # Add join keys to the appropriate leaf tables
                if not isinstance(self.left, SemanticJoinOp):
                    # Left is a leaf table
                    left_name = self.left.name if hasattr(self.left, "name") else None
                    if left_name:
                        existing = join_columns.get(left_name, set())
                        join_columns[left_name] = existing | join_keys.left_columns
                else:
                    # Left is a nested join - need to map columns back to source tables
                    # Get all leaf tables from the nested join and their schemas
                    left_leaves = self.left._collect_leaf_table_names()
                    for col in join_keys.left_columns:
                        # Add column to each leaf table that actually has this column
                        for table_name in left_leaves:
                            if table_name:
                                # Check if this table actually has this column
                                # We do this by converting the table and checking its schema
                                leaf_table = self._get_leaf_table_by_name(self.left, table_name)
                                if leaf_table is not None:
                                    leaf_ibis = _to_ibis(leaf_table)
                                    if col in leaf_ibis.columns:
                                        existing = join_columns.get(table_name, set())
                                        join_columns[table_name] = existing | {col}

                if not isinstance(self.right, SemanticJoinOp):
                    # Right is a leaf table
                    right_name = self.right.name if hasattr(self.right, "name") else None
                    if right_name:
                        existing = join_columns.get(right_name, set())
                        join_columns[right_name] = existing | join_keys.right_columns
                else:
                    # Right is a nested join
                    right_leaves = self.right._collect_leaf_table_names()
                    for col in join_keys.right_columns:
                        for table_name in right_leaves:
                            if table_name:
                                leaf_table = self._get_leaf_table_by_name(self.right, table_name)
                                if leaf_table is not None:
                                    leaf_ibis = _to_ibis(leaf_table)
                                    if col in leaf_ibis.columns:
                                        existing = join_columns.get(table_name, set())
                                        join_columns[table_name] = existing | {col}

        return join_columns

    def to_ibis(self, parent_requirements: dict[str, set[str]] | None = None):
        """Convert join to Ibis expression with automatic projection pushdown.

        Projection pushdown is always applied using the join's intrinsic `required_columns` property.
        Uses top-down requirement propagation for n-way joins:
        1. Use required_columns property (measures + join keys)
        2. Merge with parent requirements if provided
        3. At each join level, extract join keys needed for THIS join
        4. Add join keys to requirements for respective subtrees
        5. Recursively propagate augmented requirements down
        6. Apply projections only at leaf tables

        Args:
            parent_requirements: Optional dict of column requirements from parent operations.
                If provided, these are merged with the join's intrinsic required_columns.

        Returns:
            Ibis join expression
        """
        from .convert import _Resolver

        # Use intrinsic required_columns property if parent provided requirements
        # When parent_requirements=None (e.g., for join key extraction), skip projection
        if parent_requirements is not None and self.on is not None:
            # Merge parent requirements with intrinsic requirements
            computed_requirements = self._compute_required_columns(
                parent_requirements=parent_requirements
            )
        else:
            computed_requirements = None

        # Apply projection pushdown using computed requirements
        if computed_requirements:
            # Step 1: Extract join keys for THIS level by temporarily converting without projection
            temp_left = (
                self.left.to_ibis(parent_requirements=None)
                if isinstance(self.left, SemanticJoinOp)
                else _to_ibis(self.left)
            )
            temp_right = (
                self.right.to_ibis(parent_requirements=None)
                if isinstance(self.right, SemanticJoinOp)
                else _to_ibis(self.right)
            )

            join_keys_result = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys_result.is_success():
                # Step 2: Augment requirements with join keys for subtrees
                augmented_requirements = dict(computed_requirements)

                # Add join keys to appropriate tables/subtrees
                # For nested joins, we need to trace which leaf tables these columns belong to
                if isinstance(self.left, SemanticJoinOp):
                    # Left is nested - distribute join keys to relevant leaf tables
                    for col in join_keys_result.left_columns:
                        # Find which leaf table(s) have this column
                        for leaf_name in self.left._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.left, leaf_name)
                            if leaf_table is not None:
                                leaf_ibis = _to_ibis(leaf_table)
                                if col in leaf_ibis.columns:
                                    existing = augmented_requirements.get(leaf_name, set())
                                    augmented_requirements[leaf_name] = existing | {col}
                else:
                    # Left is a leaf table
                    left_name = self.left.name if hasattr(self.left, "name") else None
                    if left_name:
                        existing = augmented_requirements.get(left_name, set())
                        augmented_requirements[left_name] = existing | join_keys_result.left_columns

                if isinstance(self.right, SemanticJoinOp):
                    # Right is nested - distribute join keys to relevant leaf tables
                    for col in join_keys_result.right_columns:
                        for leaf_name in self.right._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.right, leaf_name)
                            if leaf_table is not None:
                                leaf_ibis = _to_ibis(leaf_table)
                                if col in leaf_ibis.columns:
                                    existing = augmented_requirements.get(leaf_name, set())
                                    augmented_requirements[leaf_name] = existing | {col}
                else:
                    # Right is a leaf table
                    right_name = self.right.name if hasattr(self.right, "name") else None
                    if right_name:
                        existing = augmented_requirements.get(right_name, set())
                        augmented_requirements[right_name] = (
                            existing | join_keys_result.right_columns
                        )

                # Step 3: Recursively convert subtrees with augmented requirements
                if isinstance(self.left, SemanticJoinOp):
                    left_tbl = self.left.to_ibis(parent_requirements=augmented_requirements)
                else:
                    left_tbl = _to_ibis(self.left)
                    left_name = self.left.name if hasattr(self.left, "name") else None
                    if left_name and left_name in augmented_requirements:
                        left_cols = augmented_requirements[left_name]
                        if left_cols and left_cols != set(left_tbl.columns):
                            left_tbl = left_tbl.select(
                                [left_tbl[c] for c in left_cols if c in left_tbl.columns]
                            )

                if isinstance(self.right, SemanticJoinOp):
                    right_tbl = self.right.to_ibis(parent_requirements=augmented_requirements)
                else:
                    right_tbl = _to_ibis(self.right)
                    right_name = self.right.name if hasattr(self.right, "name") else None
                    if right_name and right_name in augmented_requirements:
                        right_cols = augmented_requirements[right_name]
                        if right_cols and right_cols != set(right_tbl.columns):
                            right_tbl = right_tbl.select(
                                [right_tbl[c] for c in right_cols if c in right_tbl.columns]
                            )
            else:
                # Couldn't extract join keys - fallback to no projection
                left_tbl = _to_ibis(self.left)
                right_tbl = _to_ibis(self.right)
        else:
            # No required_columns specified, just convert normally
            left_tbl = _to_ibis(self.left)
            right_tbl = _to_ibis(self.right)

        return (
            left_tbl.join(
                right_tbl,
                self.on(_Resolver(left_tbl), _Resolver(right_tbl)),
                how=self.how,
            )
            if self.on is not None
            else left_tbl.join(right_tbl, how=self.how)
        )

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


# ==============================================================================
# Column Tracking for Projection Pushdown
# ==============================================================================


@frozen
class ColumnTracker:
    """Immutable tracker for column references during expression evaluation.

    Uses frozenset for tracked columns. New columns are added by creating
    new tracker instances with updated sets.
    """

    columns: frozenset[str] = field(factory=frozenset, converter=frozenset)

    def with_column(self, col_name: str) -> ColumnTracker:
        """Return new tracker with additional column."""
        return ColumnTracker(columns=self.columns | {col_name})

    def merge(self, other: ColumnTracker) -> ColumnTracker:
        """Return new tracker with merged columns."""
        return ColumnTracker(columns=self.columns | other.columns)


@frozen
class ColumnExtractionResult:
    """Result of column extraction with error handling.

    Separates successful extraction from error cases.
    """

    columns: frozenset[str] = field(factory=frozenset, converter=frozenset)
    extraction_failed: bool = False
    error_type: type[Exception] | None = None

    @classmethod
    def success(cls, columns: set[str] | frozenset[str]) -> ColumnExtractionResult:
        """Create successful result."""
        return cls(columns=frozenset(columns), extraction_failed=False)

    @classmethod
    def failure(cls, error: Exception) -> ColumnExtractionResult:
        """Create failure result with error information."""
        return cls(
            columns=frozenset(),
            extraction_failed=True,
            error_type=type(error),
        )

    def is_success(self) -> bool:
        """Check if extraction succeeded."""
        return not self.extraction_failed


@frozen
class JoinColumnExtractionResult:
    """Result of join column extraction for both tables."""

    left_columns: frozenset[str] = field(factory=frozenset, converter=frozenset)
    right_columns: frozenset[str] = field(factory=frozenset, converter=frozenset)
    extraction_failed: bool = False
    error_type: type[Exception] | None = None

    @classmethod
    def success(
        cls,
        left: set[str] | frozenset[str],
        right: set[str] | frozenset[str],
    ) -> JoinColumnExtractionResult:
        """Create successful result."""
        return cls(
            left_columns=frozenset(left),
            right_columns=frozenset(right),
            extraction_failed=False,
        )

    @classmethod
    def failure(cls, error: Exception) -> JoinColumnExtractionResult:
        """Create failure result with error information."""
        return cls(
            left_columns=frozenset(),
            right_columns=frozenset(),
            extraction_failed=True,
            error_type=type(error),
        )

    def is_success(self) -> bool:
        """Check if extraction succeeded."""
        return not self.extraction_failed


def _make_tracking_proxy(
    table: ir.Table,
    on_access: Callable[[str], None],
) -> Any:
    """Create tracking proxy with custom access handler.

    Composable factory that enables different tracking strategies
    via the on_access callback.
    """

    class _TrackingProxy:
        """Proxy that tracks attribute and item access."""

        def __init__(self, inner_table: ir.Table, access_handler: Callable[[str], None]):
            object.__setattr__(self, "_table", inner_table)
            object.__setattr__(self, "_on_access", access_handler)

        def __getattr__(self, name: str):
            if name.startswith("_"):
                return getattr(self._table, name)
            self._on_access(name)
            return getattr(self._table, name)

        def __getitem__(self, name: str):
            self._on_access(name)
            return self._table[name]

    return _TrackingProxy(table, on_access)


def _extract_columns_from_callable(
    fn: Any,
    table: ir.Table,
) -> ColumnExtractionResult:
    """Extract column names referenced by a callable.

    Uses immutable tracking and returns structured result.
    """
    if not callable(fn):
        return ColumnExtractionResult.success(frozenset())

    # Use list as reference cell for immutable tracker
    tracker_ref = [ColumnTracker()]

    def on_column_access(col_name: str) -> None:
        """Callback that updates tracker reference."""
        tracker_ref[0] = tracker_ref[0].with_column(col_name)

    try:
        tracking_proxy = _make_tracking_proxy(table, on_column_access)
        fn(tracking_proxy)
        return ColumnExtractionResult.success(tracker_ref[0].columns)

    except Exception as e:
        return ColumnExtractionResult.failure(e)


def _extract_join_key_columns(
    on: Callable[[Any, Any], ir.BooleanValue],
    left_table: ir.Table,
    right_table: ir.Table,
) -> JoinColumnExtractionResult:
    """Extract column names used in join predicate.

    Uses immutable trackers for both tables.
    """
    # Use lists as reference cells for immutable trackers
    left_tracker_ref = [ColumnTracker()]
    right_tracker_ref = [ColumnTracker()]

    def on_left_access(col_name: str) -> None:
        """Callback for left table column access."""
        left_tracker_ref[0] = left_tracker_ref[0].with_column(col_name)

    def on_right_access(col_name: str) -> None:
        """Callback for right table column access."""
        right_tracker_ref[0] = right_tracker_ref[0].with_column(col_name)

    try:
        left_proxy = _make_tracking_proxy(left_table, on_left_access)
        right_proxy = _make_tracking_proxy(right_table, on_right_access)
        on(left_proxy, right_proxy)

        return JoinColumnExtractionResult.success(
            left_tracker_ref[0].columns,
            right_tracker_ref[0].columns,
        )

    except Exception as e:
        return JoinColumnExtractionResult.failure(e)


# ==============================================================================
# Table Column Requirements
# ==============================================================================


@frozen
class TableColumnRequirements:
    """Immutable representation of column requirements per table.

    Maps table names to sets of required column names.
    """

    requirements: FrozenDict[str, frozenset[str]] = field(
        factory=lambda: FrozenDict({}),
        converter=lambda d: FrozenDict(
            {k: frozenset(v) if not isinstance(v, frozenset) else v for k, v in d.items()},
        ),
    )

    def with_column(self, table_name: str, col_name: str) -> TableColumnRequirements:
        """Return new requirements with additional column for table."""
        current_cols = self.requirements.get(table_name, frozenset())
        updated_cols = current_cols | {col_name}

        return TableColumnRequirements(
            requirements=dict(self.requirements) | {table_name: updated_cols},
        )

    def with_columns(
        self,
        table_name: str,
        col_names: Iterable[str],
    ) -> TableColumnRequirements:
        """Return new requirements with multiple columns for table."""
        current_cols = self.requirements.get(table_name, frozenset())
        updated_cols = current_cols | frozenset(col_names)

        return TableColumnRequirements(
            requirements=dict(self.requirements) | {table_name: updated_cols},
        )

    def merge(self, other: TableColumnRequirements) -> TableColumnRequirements:
        """Merge requirements from another instance."""
        merged_dict = dict(self.requirements)

        for table, cols in other.requirements.items():
            if table in merged_dict:
                merged_dict[table] = merged_dict[table] | cols
            else:
                merged_dict[table] = cols

        return TableColumnRequirements(requirements=merged_dict)

    def to_dict(self) -> dict[str, set[str]]:
        """Convert to mutable dict for API compatibility."""
        return {table: set(cols) for table, cols in self.requirements.items()}


def _parse_prefixed_field(field_name: str) -> tuple[str | None, str]:
    """Parse potentially prefixed field name.

    Args:
        field_name: Field name, possibly prefixed (e.g., "table.column")

    Returns:
        Tuple of (table_name or None, column_name)
    """
    if "." in field_name:
        table, col = field_name.split(".", 1)
        return (table, col)
    return (None, field_name)


def _extract_requirements_from_keys(
    keys: Iterable[str],
    merged_dimensions: Mapping[str, Any],
    all_roots: Sequence[Any],
    table: ir.Table,
) -> TableColumnRequirements:
    """Extract column requirements from group-by keys using graph traversal."""
    from ibis.expr import operations as ibis_ops

    from .graph_utils import walk_nodes

    requirements = TableColumnRequirements()

    for key in keys:
        table_name, col_name = _parse_prefixed_field(key)

        if table_name:
            # Prefixed: we know the table
            requirements = requirements.with_column(table_name, col_name)
        else:
            # Unprefixed: resolve dimension or use conservative fallback
            if key in merged_dimensions:
                dim_fn = merged_dimensions[key]

                try:
                    # Evaluate the dimension to get an Ibis expression
                    dim_expr = dim_fn(table)

                    # Walk the expression graph to find all Field nodes (column references)
                    field_names = {node.name for node in walk_nodes(ibis_ops.Field, dim_expr)}

                    # Filter to only actual columns in the table schema
                    actual_cols = {col for col in field_names if col in table.columns}

                    if actual_cols:
                        for root in all_roots:
                            if root.name:
                                requirements = requirements.with_columns(root.name, actual_cols)
                    else:
                        # Fallback: assume key name is column name
                        for root in all_roots:
                            if root.name:
                                requirements = requirements.with_column(root.name, key)
                except Exception:
                    # Fallback: assume key name is column name
                    for root in all_roots:
                        if root.name:
                            requirements = requirements.with_column(root.name, key)
            else:
                # Raw column
                for root in all_roots:
                    if root.name:
                        requirements = requirements.with_column(root.name, key)

    return requirements


def _extract_requirements_from_measures(
    aggs: Mapping[str, Callable],
    all_roots: Sequence[Any],
    table: ir.Table,
) -> TableColumnRequirements:
    """Extract column requirements from measure aggregations using graph traversal."""
    from ibis.expr import operations as ibis_ops

    from .graph_utils import walk_nodes

    requirements = TableColumnRequirements()

    for measure_name, measure_fn in aggs.items():
        fn = _unwrap(measure_fn)

        try:
            # Evaluate the measure to get an Ibis expression
            measure_expr = fn(table)

            # Walk the expression graph to find all Field nodes (column references)
            field_names = {node.name for node in walk_nodes(ibis_ops.Field, measure_expr)}

            # Filter to only actual columns in the table schema
            actual_cols = {col for col in field_names if col in table.columns}

            if actual_cols:
                for root in all_roots:
                    if root.name:
                        requirements = requirements.with_columns(root.name, actual_cols)
        except Exception:
            # Conservative fallback: if measure name looks like a column, include it
            if measure_name.isidentifier():
                for root in all_roots:
                    if root.name:
                        requirements = requirements.with_column(root.name, measure_name)

    return requirements
