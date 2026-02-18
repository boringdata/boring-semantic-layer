from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from difflib import get_close_matches
from functools import reduce
from typing import TYPE_CHECKING, Any

import ibis
import ibis.selectors as s
from attrs import field, frozen
from ibis.common.deferred import Deferred
from ibis.expr import datatypes as dt
from ibis.expr import operations as ibis_ops
from ibis.expr import types as ir
from ibis.expr.operations.relations import Field, Relation
from ibis.expr.schema import Schema

try:
    from xorq.vendor.ibis.common.collections import FrozenDict, FrozenOrderedDict
    from xorq.vendor.ibis.expr.schema import Schema as XorqSchema

    _SchemaClass = XorqSchema
    _FrozenOrderedDict = FrozenOrderedDict
except ImportError:
    from ibis.common.collections import FrozenDict, FrozenOrderedDict

    _SchemaClass = Schema
    _FrozenOrderedDict = FrozenOrderedDict

from returns.maybe import Maybe, Nothing, Some
from returns.result import Success, safe
from toolz import curry

from . import projection_utils
from .compile_all import compile_grouped_with_all
from .graph_utils import walk_nodes
from .measure_scope import (
    AggregationExpr,
    AllOf,
    BinOp,
    ColumnScope,
    MeasureRef,
    MeasureScope,
)
from .nested_access import NestedAccessMarker


def _is_deferred(expr) -> bool:
    return isinstance(expr, Deferred)


def _normalize_to_name(arg: str | Deferred) -> str:
    """Convert a string or simple ``_.name`` Deferred to a plain string name.

    Accepts a plain string (returned as-is) or a Deferred whose resolver is a
    simple attribute access on the top-level ``_`` variable (e.g. ``_.origin``).

    Complex expressions like ``_.distance.sum()`` or ``_.a.b`` are rejected
    with a ``TypeError``.
    """
    if isinstance(arg, str):
        return arg

    # Duck-type: works for both ibis and xorq Deferred objects
    resolver = getattr(arg, "_resolver", None)
    if resolver is None:
        raise TypeError(
            f"Expected a string name or Deferred expression (_.name), "
            f"got {type(arg).__name__}"
        )

    name_wrapper = getattr(resolver, "name", None)
    obj = getattr(resolver, "obj", None)

    if name_wrapper is None or obj is None:
        raise TypeError(
            f"Only simple Deferred expressions like _.name are supported "
            f"as positional arguments, got: {arg!r}"
        )

    # Reject chained access like _.a.b (obj would itself have an .obj attr)
    if getattr(obj, "obj", None) is not None:
        raise TypeError(
            f"Only simple Deferred expressions like _.name are supported "
            f"as positional arguments, got: {arg!r}"
        )

    # Attr.name is a Just wrapper; unwrap via .value
    raw_name = getattr(name_wrapper, "value", name_wrapper)
    if not isinstance(raw_name, str):
        raise TypeError(
            f"Could not extract string name from Deferred expression: {arg!r}"
        )

    return raw_name


def _normalize_join_predicate(on):
    """Normalize a join predicate to a two-argument callable.

    Accepts:
    - ``str`` – equi-join on a column present in both sides
    - ``Deferred`` (``_.col``) – same, after extracting the name
    - ``list[str | Deferred]`` – compound equi-join on multiple columns
    - ``callable`` (non-Deferred) – returned as-is (existing lambda API)
    - ``None`` – returned as-is (for cross joins)
    """
    if on is None:
        return on

    if isinstance(on, str):
        name = on
        return lambda left, right: getattr(left, name) == getattr(right, name)

    if _is_deferred(on):
        name = _normalize_to_name(on)
        return lambda left, right: getattr(left, name) == getattr(right, name)

    if isinstance(on, (list, tuple)):
        names = [_normalize_to_name(item) for item in on]
        if len(names) == 1:
            name = names[0]
            return lambda left, right: getattr(left, name) == getattr(right, name)

        def _compound_predicate(left, right):
            from functools import reduce
            from operator import and_

            preds = [getattr(left, n) == getattr(right, n) for n in names]
            return reduce(and_, preds)

        return _compound_predicate

    if callable(on):
        return on

    raise TypeError(
        f"join `on` must be a string, Deferred (_.col), list of strings/Deferred, "
        f"or a callable, got {type(on).__name__}"
    )


if TYPE_CHECKING:
    from .expr import (
        SemanticFilter,
        SemanticGroupBy,
        SemanticLimit,
        SemanticOrderBy,
        SemanticTable,
    )


def _to_untagged(source: Any) -> ir.Table:
    return source.to_untagged() if hasattr(source, "to_untagged") else source.to_expr()


def _semantic_table(*args, **kwargs) -> SemanticTable:
    from .expr import SemanticModel

    return SemanticModel(*args, **kwargs)


def _unwrap(wrapped: Any) -> Any:
    return wrapped.unwrap if isinstance(wrapped, _CallableWrapper) else wrapped


def _semantic_repr(op: Relation) -> str:
    from ibis.expr.format import pretty

    try:
        return pretty(op)
    except Exception:
        return object.__repr__(op)


def _make_schema(fields_dict: dict[str, str]):
    """Create Schema instance from fields dict."""
    return _SchemaClass(fields_dict)


def _resolve_expr(expr: Deferred | Callable | Any, scope: ir.Table) -> ir.Value:
    result = expr.resolve(scope) if _is_deferred(expr) else expr(scope) if callable(expr) else expr

    if hasattr(result, "__class__") and hasattr(scope, "__class__"):
        result_module = result.__class__.__module__
        scope_module = scope.__class__.__module__
        result_is_regular_ibis = "ibis.expr" in result_module and "xorq" not in result_module
        scope_is_xorq = "xorq.vendor.ibis" in scope_module

        if result_is_regular_ibis and scope_is_xorq:
            from xorq.common.utils.ibis_utils import from_ibis

            result = from_ibis(result)

    return result


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
    if isinstance(expr, MeasureRef):
        refs_out.add(expr.name)
    elif isinstance(expr, AllOf):
        if isinstance(expr.ref, MeasureRef):
            refs_out.add(expr.ref.name)
    elif isinstance(expr, BinOp):
        _collect_measure_refs(expr.left, refs_out)
        _collect_measure_refs(expr.right, refs_out)


def _classify_dependencies(
    fields: list,
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    current_field: str | None = None,
) -> dict[str, str]:
    """Classify field dependencies as dimension, measure, or column."""
    return {
        f.name: (
            "dimension"
            if f.name in dimensions and f.name != current_field
            else "measure"
            if f.name in measures or f.name in calc_measures
            else "column"
        )
        for f in fields
    }


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
        # should this be dask.base.tokenize()?
        return hash(id(self._fn))

    @property
    def unwrap(self):
        return self._fn


def _ensure_wrapped(fn: Any) -> _CallableWrapper:
    """Wrap Callable or Deferred for hashability."""
    return fn if isinstance(fn, _CallableWrapper) else _CallableWrapper(fn)


def _infer_unnest(fn: Callable, table: Any) -> tuple[str, ...]:
    """Infer required unnest operations from the table.

    Examples:
        to_semantic_table(tbl).with_measures(...) -> ()  # Session level
        to_semantic_table(tbl).unnest("hits").with_measures(...) -> ("hits",)
        unnested.unnest("product").with_measures(...) -> ("product",)
    """
    from .expr import SemanticUnnest

    if isinstance(table, SemanticUnnest):
        op = table.op()
        # SemanticUnnestOp always has column attribute
        return (op.column,)

    return ()


def _extract_measure_metadata(fn_or_expr: Any) -> tuple[Any, str | None, tuple]:
    """Extract metadata from various measure representations."""
    if isinstance(fn_or_expr, dict):
        return (
            fn_or_expr["expr"],
            fn_or_expr.get("description"),
            tuple(fn_or_expr.get("requires_unnest", [])),
        )
    elif isinstance(fn_or_expr, Measure):
        return (
            fn_or_expr.expr,
            fn_or_expr.description,
            fn_or_expr.requires_unnest,
        )
    else:
        return (fn_or_expr, None, ())


def _is_calculated_measure(val: Any) -> bool:
    return isinstance(val, MeasureRef | AllOf | BinOp | int | float)


def _matches_aggregation_pattern(measure_expr, agg_expr, tbl):
    if not isinstance(agg_expr, AggregationExpr):
        return Success(False)

    @curry
    def evaluate_in_scope(tbl, expr):
        """Evaluate measure expression in a ColumnScope."""
        scope = ColumnScope(_tbl=tbl)
        return (
            expr.resolve(scope) if _is_deferred(expr) else expr(scope) if callable(expr) else expr
        )

    @curry
    def has_matching_operation(agg_expr, result):
        """Check if the operation matches the expected aggregation.

        All our supported aggregations (Sum, Mean, Count, Min, Max) are ibis operations.
        """
        op_name = type(result.op()).__name__.lower()
        expected_op = "avg" if agg_expr.operation.lower() == "mean" else agg_expr.operation.lower()

        return expected_op in op_name

    @curry
    def has_matching_column(agg_expr, result):
        """Check if result's operation references the expected column.

        All supported aggregation operations (Sum, Mean, Count, Min, Max) have:
        - args[0]: Field operation with .name attribute
        - args[1]: Optional where clause (typically None)
        """
        op = result.op()

        if not isinstance(op.args[0], Field):
            return False

        return op.args[0].name == agg_expr.column

    def matches_pattern(result):
        """Check if result matches both operation and column."""
        return has_matching_operation(agg_expr, result) and has_matching_column(agg_expr, result)

    return safe(lambda: evaluate_in_scope(tbl, measure_expr))().map(matches_pattern)


def _find_matching_measure(agg_expr, known_measures: dict, tbl):
    """Find a measure that matches the aggregation expression pattern.

    Returns Maybe[str] using functional patterns.
    """
    if not isinstance(agg_expr, AggregationExpr):
        return Nothing

    @curry
    def matches_pattern(agg_expr, tbl, measure_obj):
        """Check if measure matches the aggregation pattern.

        All measure_obj values are Measure instances with an expr attribute.
        """
        result = _matches_aggregation_pattern(measure_obj.expr, agg_expr, tbl)
        return result.value_or(False)

    for measure_name, measure_obj in known_measures.items():
        if matches_pattern(agg_expr, tbl, measure_obj):
            return Some(measure_name)

    return Nothing


def _make_base_measure(
    expr: Any,
    description: str | None,
    requires_unnest: tuple,
) -> Measure:
    """Create a base measure with proper callable wrapping using functional patterns."""

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
            Maybe.from_optional(operations.get(operation))
            .map(lambda fn: fn(column))
            .value_or(
                (_ for _ in ()).throw(ValueError(f"Unknown aggregation operation: {operation}"))
            )
        )

    @curry
    def evaluate_expr(expr, scope):
        """Evaluate expression in given scope."""
        return (
            expr.resolve(scope) if _is_deferred(expr) else expr(scope) if callable(expr) else expr
        )

    def convert_aggregation_expr(t, agg_expr: AggregationExpr):
        """Convert AggregationExpr to ibis expression."""
        if agg_expr.operation == "count":
            result = t.count()
        else:
            result = apply_aggregation(agg_expr.operation, t[agg_expr.column])

        for method_name, args, kwargs in agg_expr.post_ops:
            result = getattr(result, method_name)(*args, **kwargs)

        return result

    if isinstance(expr, AggregationExpr):

        def wrapped_expr(t):
            """Convert AggregationExpr to ibis expression."""
            return convert_aggregation_expr(t, expr)

        return Measure(
            expr=wrapped_expr,
            description=description,
            requires_unnest=requires_unnest,
        )

    if callable(expr):

        def wrapped_expr(t):
            """Wrapped expression that handles AggregationExpr conversion."""
            scope = ColumnScope(_tbl=t)
            result = evaluate_expr(expr, scope)

            if isinstance(result, AggregationExpr):
                return convert_aggregation_expr(t, result)
            return result

        return Measure(
            expr=wrapped_expr,
            description=description,
            requires_unnest=requires_unnest,
        )
    else:
        return Measure(
            expr=lambda t, fn=expr: evaluate_expr(fn, ColumnScope(_tbl=t)),
            description=description,
            requires_unnest=requires_unnest,
        )


def _classify_measure(fn_or_expr: Any, scope: Any) -> tuple[str, Any]:
    """Classify measure as 'calc' or 'base' with appropriate handling."""
    expr, description, requires_unnest = _extract_measure_metadata(fn_or_expr)

    resolved = safe(lambda: _resolve_expr(expr, scope))().map(
        lambda val: ("calc", val) if _is_calculated_measure(val) else None
    )

    if isinstance(resolved, Success) and resolved.unwrap() is not None:
        return resolved.unwrap()

    if not requires_unnest and callable(expr):
        # All scopes (MeasureScope, ColumnScope) have tbl attribute
        table = scope.tbl
        inferred_unnest = _infer_unnest(expr, table)
        requires_unnest = requires_unnest or inferred_unnest

    return ("base", _make_base_measure(expr, description, requires_unnest))


def _build_json_definition(
    dims_dict: dict,
    meas_dict: dict,
    name: str | None = None,
    description: str | None = None,
) -> dict:
    result = {
        "dimensions": {n: spec.to_json() for n, spec in dims_dict.items()},
        "measures": {n: spec.to_json() for n, spec in meas_dict.items()},
        "entity_dimensions": {n: spec.to_json() for n, spec in dims_dict.items() if spec.is_entity},
        "event_timestamp": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_event_timestamp
        },
        "time_dimensions": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_time_dimension
        },
        "name": name,
    }
    if description is not None:
        result["description"] = description
    return result


def _format_column_error(e: AttributeError, table: ir.Table) -> str:
    """Format a helpful error message for missing column errors."""
    # Extract the column name from the error
    match = re.search(r"has no attribute ['\"]([^'\"]+)['\"]", str(e))
    missing_col = match.group(1) if match else "unknown"

    # Get available columns
    available_cols = list(table.columns) if hasattr(table, "columns") else []

    # Build error message
    parts = [f"Dimension expression references non-existent column '{missing_col}'."]

    if len(available_cols) > 20:
        parts.append(f"Table has {len(available_cols)} columns. First 15: {available_cols[:15]}")
    elif available_cols:
        parts.append(f"Available columns: {available_cols}")
    else:
        parts.append(f"No columns available in {type(table).__name__} object")

    # Suggest similar column names
    suggestions = get_close_matches(missing_col, available_cols, n=3, cutoff=0.6)
    if suggestions:
        parts[-1] += f". Did you mean: {suggestions}?"

    # Add helpful tip
    example = suggestions[0] if suggestions else "column_name"
    parts.append(
        f"\n\nTip: Check that your dimension expression uses the correct column name. "
        f"For example: lambda t: t.{example}"
    )

    return " ".join(parts)


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    is_entity: bool = False
    is_time_dimension: bool = False
    is_event_timestamp: bool = False
    smallest_time_grain: str | None = None

    def __call__(self, table: ir.Table) -> ir.Value:
        try:
            return self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)
        except AttributeError as e:
            # Provide helpful error for missing columns
            if "'Table' object has no attribute" in str(
                e
            ) or "'Join' object has no attribute" in str(e):
                raise AttributeError(_format_column_error(e, table)) from e
            raise

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        if self.is_entity:
            base["is_entity"] = True
        if self.is_event_timestamp:
            base["is_event_timestamp"] = True
        if self.is_time_dimension:
            base["smallest_time_grain"] = self.smallest_time_grain
        return base

    def __hash__(self) -> int:
        return hash(
            (
                self.description,
                self.is_entity,
                self.is_event_timestamp,
                self.is_time_dimension,
                self.smallest_time_grain,
            ),
        )


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    requires_unnest: tuple[str, ...] = ()  # Internal: Arrays that must be unnested

    def __call__(self, table: ir.Table) -> ir.Value:
        return self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)

    @property
    def locality(self) -> str | None:
        """Derive locality from requires_unnest (most nested level)."""
        return self.requires_unnest[-1] if self.requires_unnest else None

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        if self.locality:
            base["locality"] = self.locality
        if self.requires_unnest:
            base["requires_unnest"] = list(self.requires_unnest)
        return base

    def __hash__(self) -> int:
        return hash((self.description, self.requires_unnest))


class SemanticTableOp(Relation):
    """Relation with semantic metadata (dimensions and measures).

    Stores ir.Table expression directly to avoid .op() → .to_expr() conversions.

    Note: Accepts both regular ibis.Table and xorq's vendored ibis.Table.
    Regular ibis tables are automatically converted to xorq in __init__.
    """

    table: Any  # Accepts both ir.Table and regular ibis.expr.types.Table
    dimensions: FrozenDict[str, Dimension]
    measures: FrozenDict[str, Measure]
    calc_measures: FrozenDict[str, Any]
    name: str | None = None
    description: str | None = None
    _source_join: Any = field(
        default=None, repr=False
    )  # Track if this wraps a join (SemanticJoinOp) for optimization

    def __init__(
        self,
        table: ir.Table,
        dimensions: dict[str, Dimension] | FrozenDict[str, Dimension],
        measures: dict[str, Measure] | FrozenDict[str, Measure],
        calc_measures: dict[str, Any] | FrozenDict[str, Any],
        name: str | None = None,
        description: str | None = None,
        _source_join: Any = None,
    ) -> None:
        # Accept both regular ibis and xorq tables without conversion
        # This allows using regular ibis by default, xorq only when provided
        super().__init__(
            table=table,
            dimensions=FrozenDict(dimensions)
            if not isinstance(dimensions, FrozenDict)
            else dimensions,
            measures=FrozenDict(measures) if not isinstance(measures, FrozenDict) else measures,
            calc_measures=FrozenDict(calc_measures)
            if not isinstance(calc_measures, FrozenDict)
            else calc_measures,
            name=name,
            description=description,
            _source_join=_source_join,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

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
    def schema(self):
        fields_dict = {name: str(v.dtype) for name, v in self.values.items()}
        return _make_schema(fields_dict)

    @property
    def json_definition(self) -> Mapping[str, Any]:
        return _build_json_definition(
            self.get_dimensions(),
            self.get_measures(),
            self.name,
            self.description,
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
        return self.calc_measures

    def get_graph(self) -> dict[str, dict[str, Any]]:
        from .graph_utils import build_dependency_graph

        return build_dependency_graph(
            self.get_dimensions(),
            self.get_measures(),
            self.get_calculated_measures(),
            self.table,
        )

    def __getattribute__(self, name: str):
        """Override attribute access to return tuples for dimensions/measures.

        This provides a cleaner API where .dimensions returns ('dim1', 'dim2')
        instead of the full FrozenDict. Use get_dimensions() to get the full dict.
        """
        # For special/internal attributes (dunder methods), use default behavior
        # This is critical for xorq's vendored ibis which uses __precomputed_hash__, etc.
        if name.startswith("__") and name.endswith("__"):
            return object.__getattribute__(self, name)

        # Custom behavior for dimensions and measures
        if name == "dimensions":
            dims = object.__getattribute__(self, "dimensions")
            return tuple(dims.keys())
        if name == "measures":
            base_meas = object.__getattribute__(self, "measures")
            calc_meas = object.__getattribute__(self, "calc_measures")
            return tuple(base_meas.keys()) + tuple(calc_meas.keys())

        # Default behavior for everything else
        return object.__getattribute__(self, name)

    def to_untagged(self):
        return self.table


class SemanticFilterOp(Relation):
    source: Relation
    predicate: Callable

    def __init__(self, source: Relation, predicate: Callable) -> None:
        super().__init__(
            source=Relation.__coerce__(source),
            predicate=_ensure_wrapped(predicate),
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        from .convert import _Resolver

        all_roots = _find_all_root_models(self.source)
        base_tbl = _to_untagged(self.source)
        dim_map = (
            {}
            if isinstance(self.source, SemanticAggregateOp)
            else _get_merged_fields(all_roots, "dimensions")
        )

        pred_fn = _unwrap(self.predicate)
        resolver = _Resolver(base_tbl, dim_map)
        pred = _resolve_expr(pred_fn, resolver)
        return base_tbl.filter(pred)

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


def _classify_fields(
    fields: tuple[str, ...],
    dimensions: dict,
    measures: dict,
) -> tuple[list[str], list[str], list[str]]:
    """Classify fields into dimensions, measures, and raw columns."""
    dims = [f for f in fields if f in dimensions]
    meas = [f for f in fields if f in measures]
    raw = [f for f in fields if f not in dimensions and f not in measures]
    return dims, meas, raw


def _process_nested_access_marker(
    marker: NestedAccessMarker,
    name: str,
    tbl: ir.Table,
) -> tuple[ir.Table, ir.Value]:
    """Process a NestedAccessMarker to unnest and build aggregation expression."""
    unnested = tbl
    for array_col in marker.array_path:
        if array_col in unnested.columns:
            unnested = unnested.unnest(array_col)

    if marker.operation == "count":
        return unnested, unnested.count().name(name)

    expr = getattr(unnested, marker.array_path[0])
    for field_name in marker.field_path:
        expr = getattr(expr, field_name)

    if marker.operation in ("sum", "mean", "min", "max", "nunique"):
        agg_fn = getattr(expr, marker.operation)
        return unnested, agg_fn().name(name)

    raise ValueError(f"Unknown operation: {marker.operation}")


def _evaluate_measures_with_unnesting(
    measure_names: list[str],
    measures: dict,
    tbl: ir.Table,
) -> dict:
    """Evaluate measures and apply automatic unnesting if needed.

    Returns dict with:
        - table: potentially unnested table
        - measure_exprs: list of evaluated measure expressions
        - needs_unnesting: whether unnesting occurred
    """
    meas_exprs = []
    current_tbl = tbl
    needs_unnesting = False

    for name in measure_names:
        result = measures[name](tbl)

        if isinstance(result, NestedAccessMarker):
            current_tbl, meas_expr = _process_nested_access_marker(result, name, current_tbl)
            meas_exprs.append(meas_expr)
            needs_unnesting = True
        else:
            meas_exprs.append(result.name(name))

    return {
        "table": current_tbl,
        "measure_exprs": meas_exprs,
        "needs_unnesting": needs_unnesting,
    }


def _build_select_or_aggregate(
    tbl: ir.Table,
    dim_exprs: list,
    meas_exprs: list,
    raw_exprs: list,
) -> ir.Table:
    """Build appropriate select/aggregate based on what expressions exist."""
    if meas_exprs and dim_exprs:
        return tbl.group_by(dim_exprs).aggregate(meas_exprs)
    if meas_exprs:
        return tbl.aggregate(meas_exprs)
    if dim_exprs or raw_exprs:
        return tbl.select(dim_exprs + raw_exprs)
    return tbl


class SemanticProjectOp(Relation):
    source: Relation
    fields: tuple[str, ...]

    def __init__(self, source: Relation, fields: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), fields=tuple(fields))

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        src_vals = self.source.values
        return FrozenOrderedDict(
            {k: v for k, v in src_vals.items() if k in self.fields},
        )

    @property
    def schema(self) -> Schema:
        return _SchemaClass(fields=_FrozenOrderedDict({k: v.dtype for k, v in self.values.items()}))

    def to_untagged(self):
        all_roots = _find_all_root_models(self.source)
        tbl = _to_untagged(self.source)

        if not all_roots:
            return tbl.select([getattr(tbl, f) for f in self.fields])

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        merged_measures = _get_merged_fields(all_roots, "measures")

        dims, meas, raw_fields = _classify_fields(self.fields, merged_dimensions, merged_measures)

        # Evaluate measures and handle automatic unnesting
        meas_result = _evaluate_measures_with_unnesting(meas, merged_measures, tbl)

        active_tbl = meas_result["table"]
        meas_exprs = meas_result["measure_exprs"]
        needs_unnesting = meas_result["needs_unnesting"]

        # Re-evaluate dimensions on unnested table if needed
        dim_exprs = (
            [merged_dimensions[name](active_tbl).name(name) for name in dims]
            if needs_unnesting
            else [merged_dimensions[name](tbl).name(name) for name in dims]
        )

        # Get raw columns that still exist after unnesting
        raw_exprs = [getattr(active_tbl, name) for name in raw_fields if name in active_tbl.columns]

        return _build_select_or_aggregate(active_tbl, dim_exprs, meas_exprs, raw_exprs)


class SemanticGroupByOp(Relation):
    source: Relation
    keys: tuple[str, ...]

    def __init__(self, source: Relation, keys: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), keys=tuple(keys))

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        return _to_untagged(self.source)


@frozen
class _MeasureSpec:
    name: str
    kind: str  # 'agg' or 'calc'
    value: Any


@frozen
class _AggregationPlan:
    agg_specs: FrozenDict[str, Callable]
    calc_specs: FrozenDict[str, Any]
    requested_measures: tuple[str, ...]
    group_by_cols: tuple[str, ...]


def _resolve_aggregation_exprs(
    expr: Any,
    merged_base_measures: dict,
    merged_calc_measures: dict,
    tbl: ir.Table,
) -> Any:
    @curry
    def find_in_calc_measures(expr, calc_measures):
        for calc_name, calc_expr in calc_measures.items():
            if isinstance(calc_expr, AggregationExpr) and (
                calc_expr.column == expr.column and calc_expr.operation == expr.operation
            ):
                return Some(calc_name)
        return Nothing

    def resolve_aggregation(agg_expr):
        matched = _find_matching_measure(agg_expr, merged_base_measures, tbl)
        return matched.map(MeasureRef).value_or(
            find_in_calc_measures(agg_expr, merged_calc_measures).map(MeasureRef).value_or(agg_expr)
        )

    if isinstance(expr, AggregationExpr):
        return resolve_aggregation(expr)
    elif isinstance(expr, BinOp):
        return BinOp(
            op=expr.op,
            left=_resolve_aggregation_exprs(
                expr.left, merged_base_measures, merged_calc_measures, tbl
            ),
            right=_resolve_aggregation_exprs(
                expr.right, merged_base_measures, merged_calc_measures, tbl
            ),
        )
    elif isinstance(expr, AllOf) and isinstance(expr.ref, AggregationExpr):
        return AllOf(resolve_aggregation(expr.ref))
    else:
        return expr


def _create_measure_spec(
    name: str,
    fn_wrapped: Any,
    scope: Any,
    is_post_agg: bool,
    merged_base_measures: dict,
    merged_calc_measures: dict,
    tbl: ir.Table,
) -> _MeasureSpec:
    fn = _unwrap(fn_wrapped)
    val = _resolve_expr(fn, scope)
    val = _resolve_aggregation_exprs(val, merged_base_measures, merged_calc_measures, tbl)

    if is_post_agg:
        return _MeasureSpec(name=name, kind="agg", value=fn)

    if isinstance(val, MeasureRef):
        ref_name = val.name
        if ref_name in merged_calc_measures:
            calc_expr = merged_calc_measures[ref_name]
            resolved = _resolve_aggregation_exprs(
                calc_expr, merged_base_measures, merged_calc_measures, tbl
            )
            return _MeasureSpec(name=name, kind="calc", value=resolved)
        elif ref_name in merged_base_measures:
            return _MeasureSpec(name=name, kind="agg", value=merged_base_measures[ref_name])
        else:
            return _MeasureSpec(name=name, kind="calc", value=val)

    if isinstance(val, AllOf | BinOp | int | float):
        return _MeasureSpec(name=name, kind="calc", value=val)

    return _MeasureSpec(name=name, kind="agg", value=fn)


def _make_agg_callable(measure: Any) -> Callable:
    if _is_deferred(measure):
        return lambda t: measure.resolve(ColumnScope(_tbl=t))
    elif callable(measure):
        return lambda t: measure(ColumnScope(_tbl=t))
    else:
        return lambda t: measure(t)


def _collect_all_measure_refs(calc_exprs) -> frozenset[str]:
    all_refs = set()
    for expr in calc_exprs:
        _collect_measure_refs(expr, all_refs)
    return frozenset(all_refs)


def _build_aggregation_plan(
    aggs: dict,
    keys: tuple,
    scope: Any,
    is_post_agg: bool,
    merged_base_measures: dict,
    merged_calc_measures: dict,
    tbl: ir.Table,
) -> _AggregationPlan:
    specs = [
        _create_measure_spec(
            name, fn, scope, is_post_agg, merged_base_measures, merged_calc_measures, tbl
        )
        for name, fn in aggs.items()
    ]

    agg_specs_list = [s for s in specs if s.kind == "agg"]
    calc_specs_list = [s for s in specs if s.kind == "calc"]

    agg_specs = FrozenDict({s.name: _make_agg_callable(s.value) for s in agg_specs_list})
    calc_specs = FrozenDict({s.name: s.value for s in calc_specs_list})

    referenced = _collect_all_measure_refs(calc_specs.values())
    additional_aggs = {
        ref: _make_agg_callable(merged_base_measures[ref])
        for ref in referenced
        if ref not in agg_specs and ref in merged_base_measures
    }

    final_agg_specs = FrozenDict({**agg_specs, **additional_aggs})

    return _AggregationPlan(
        agg_specs=final_agg_specs,
        calc_specs=calc_specs,
        requested_measures=tuple(aggs.keys()),
        group_by_cols=tuple(keys),
    )


# ---------------------------------------------------------------------------
# Pre-aggregation helpers (fan-out / chasm trap prevention)
# ---------------------------------------------------------------------------


@frozen
class _JoinTreeInfo:
    """Information collected from the join tree for pre-aggregation decisions."""
    has_join_many: bool
    table_cardinalities: dict  # table_name → "one"|"many"|"root"
    table_join_keys: dict  # table_name → {raw_col_names}
    table_ops: dict  # table_name → SemanticTableOp


def _collect_join_tree_info(join_op: SemanticJoinOp) -> _JoinTreeInfo:
    """Walk the join tree to collect cardinality and join key information."""
    table_cardinalities: dict[str, str] = {}
    table_ops: dict[str, SemanticTableOp] = {}

    def walk(node, is_right_of_many=False):
        if isinstance(node, SemanticJoinOp):
            this_is_many = node.cardinality in ("many", "cross")
            walk(node.left, is_right_of_many=False)
            walk(node.right, is_right_of_many=this_is_many)
        elif isinstance(node, SemanticTableOp):
            name = node.name
            if name:
                table_ops[name] = node
                if is_right_of_many:
                    table_cardinalities[name] = "many"
                elif name not in table_cardinalities:
                    table_cardinalities[name] = "one"

    walk(join_op)

    # The leftmost leaf of the root is the "root" table
    def find_leftmost(node):
        if isinstance(node, SemanticJoinOp):
            return find_leftmost(node.left)
        return getattr(node, "name", None)

    root_name = find_leftmost(join_op)
    if root_name:
        table_cardinalities[root_name] = "root"

    has_join_many = any(c == "many" for c in table_cardinalities.values())
    table_join_keys = join_op._collect_join_keys_for_leaves()

    return _JoinTreeInfo(
        has_join_many=has_join_many,
        table_cardinalities=table_cardinalities,
        table_join_keys=table_join_keys,
        table_ops=table_ops,
    )


def _partition_agg_specs_by_source(
    agg_specs: dict[str, Callable],
    all_roots: list[SemanticTableOp],
) -> dict[str | None, dict[str, Callable]]:
    """Partition aggregation specs by their source table.

    Prefixed measure names like ``"orders.total_amount"`` are mapped to
    ``table="orders"``.  Measures without a prefix go to ``None``.
    """
    root_names = {r.name for r in all_roots if r.name}
    partitioned: dict[str | None, dict[str, Callable]] = {}

    for measure_name, fn in agg_specs.items():
        table_name = None
        if "." in measure_name:
            prefix = measure_name.split(".", 1)[0]
            if prefix in root_names:
                table_name = prefix
        if table_name not in partitioned:
            partitioned[table_name] = {}
        partitioned[table_name][measure_name] = fn

    return partitioned


def _compute_preagg_grain(
    table_name: str,
    group_by_keys: tuple[str, ...],
    join_tree_info: _JoinTreeInfo,
    merged_dimensions: dict,
    table_op: SemanticTableOp,
) -> list[str]:
    """Compute the pre-aggregation grain for a source table.

    The grain consists of:
    1. Join keys connecting this table to the join tree
    2. Any group-by dimensions that belong to this table
    """
    grain: list[str] = []

    # Add join keys for this table
    raw_join_keys = join_tree_info.table_join_keys.get(table_name, set())
    raw_tbl = _to_untagged(table_op)
    raw_columns = set(raw_tbl.columns)
    for jk in raw_join_keys:
        if jk in raw_columns:
            grain.append(jk)

    # Add group-by dimensions that belong to this table
    table_dims = _get_field_dict(table_op, "dimensions")
    for gb_key in group_by_keys:
        if gb_key in merged_dimensions:
            # Prefixed dimension like "orders.customer_id"
            if "." in gb_key:
                prefix = gb_key.split(".", 1)[0]
                short_name = gb_key.split(".", 1)[1]
                if prefix == table_name and short_name in table_dims:
                    # Use the raw column name for grouping on the raw table
                    dim_fn = table_dims[short_name]
                    if callable(dim_fn):
                        try:
                            col_expr = dim_fn(raw_tbl)
                            col_name = col_expr.get_name()
                            if col_name in raw_columns and col_name not in grain:
                                grain.append(col_name)
                        except Exception:
                            pass

    return grain


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

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        all_roots = _find_all_root_models(self.source)

        merged_dimensions = _merge_fields_with_prefixing(
            all_roots,
            lambda root: root.get_dimensions(),
        )

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
        return _SchemaClass(fields=_FrozenOrderedDict({n: v.dtype for n, v in self.values.items()}))

    @property
    def measures(self) -> tuple[str, ...]:
        return ()

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """After aggregation, dimensions are materialized - return empty."""
        return {}

    def get_measures(self) -> Mapping[str, Measure]:
        """After aggregation, measures are materialized - return empty."""
        return {}

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """After aggregation, calculated measures are materialized - return empty."""
        return {}

    @property
    def required_columns(self) -> dict[str, set[str]]:
        """
        Column requirements for this aggregation operation.

        This property makes column requirements intrinsic to the aggregate operation,
        similar to how `schema` is intrinsic to a relation.

        Returns:
            Dict mapping table names to sets of required column names.
        """
        all_roots = _find_all_root_models(self.source)
        merged_dimensions = _get_merged_fields(all_roots, "dimensions")

        base_tbl = (
            self.source.to_expr() if hasattr(self.source, "to_expr") else _to_untagged(self.source)
        )

        table_names = []
        for root in all_roots:
            if root.name:
                table_names.append(root.name)
            elif root._source_join is not None:
                # All roots are SemanticTableOp with _source_join attribute
                join_roots = _find_all_root_models(root._source_join)
                table_names.extend([r.name for r in join_roots if r.name])

        key_requirements = projection_utils.extract_requirements_from_keys(
            keys=list(self.keys),
            dimensions=merged_dimensions,
            table=base_tbl,
            table_names=table_names,
        )

        measure_requirements = projection_utils.extract_requirements_from_measures(
            measures={name: _unwrap(fn) for name, fn in self.aggs.items()},
            table=base_tbl,
            table_names=table_names,
        )

        combined = key_requirements.merge(measure_requirements)

        if hasattr(self.source, "required_columns"):
            source_reqs = projection_utils.TableRequirements.from_dict(self.source.required_columns)
            combined = combined.merge(source_reqs)

        return combined.to_dict()

    def to_untagged(self):
        all_roots = _find_all_root_models(self.source)

        def find_join_in_tree(node):
            """Find a SemanticJoinOp in the operation tree.

            All Relation subclasses have source attribute except leaf operations.
            """
            if isinstance(node, SemanticJoinOp):
                return node
            if isinstance(node, SemanticTableOp):
                # Leaf operations - no source to traverse
                return None
            if node.source is not None:
                return find_join_in_tree(node.source)
            return None

        def has_filter_after_join(node):
            """Check if there's a filter operation between here and the join.

            This prevents us from skipping filters when we optimize by jumping
            directly to the join for projection pushdown.
            """
            if isinstance(node, SemanticFilterOp):
                return True
            if isinstance(node, SemanticJoinOp | SemanticTableOp):
                return False
            if hasattr(node, "source") and node.source is not None:
                return has_filter_after_join(node.source)
            return False

        join_op = find_join_in_tree(self.source)

        if join_op is None and isinstance(self.source, SemanticGroupByOp):
            grouped_source = self.source.source
            if isinstance(grouped_source, SemanticTableOp):
                # SemanticTableOp always has _source_join attribute
                join_op = grouped_source._source_join

        def has_prior_aggregate(node):
            """Recursively check if there's a SemanticAggregateOp before any mutate."""
            if isinstance(node, SemanticAggregateOp):
                return True
            if isinstance(node, SemanticMutateOp):
                return has_prior_aggregate(node.source)
            if isinstance(node, SemanticGroupByOp):
                return has_prior_aggregate(node.source)
            if isinstance(node, SemanticTableOp | SemanticJoinOp):
                return False
            if hasattr(node, "source"):
                return has_prior_aggregate(node.source)
            return False

        is_post_agg = has_prior_aggregate(self.source)

        # Pre-aggregation path: when join_many is present, aggregate each
        # source table's measures at its own grain before joining.
        if (
            join_op is not None
            and not has_filter_after_join(self.source)
            and not is_post_agg
        ):
            join_tree_info = _collect_join_tree_info(join_op)
            if join_tree_info.has_join_many:
                return self._to_untagged_with_preagg(
                    all_roots, join_op, join_tree_info,
                )

        # Only use the join optimization if there are no filters after the join
        # Otherwise we'd skip the filter operations
        if join_op is not None and not has_filter_after_join(self.source):
            tbl = join_op.to_untagged(parent_requirements=self.required_columns)
        else:
            tbl = _to_untagged(self.source)

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        merged_base_measures = _get_merged_fields(all_roots, "measures")
        merged_calc_measures = _get_merged_fields(all_roots, "calc_measures")

        dim_mutations = {k: merged_dimensions[k](tbl) for k in self.keys if k in merged_dimensions}
        tbl = tbl.mutate(**dim_mutations) if dim_mutations else tbl

        scope = (
            ColumnScope(_tbl=tbl)
            if is_post_agg
            else MeasureScope(
                _tbl=tbl,
                _known=list(merged_base_measures.keys()) + list(merged_calc_measures.keys()),
            )
        )

        plan = _build_aggregation_plan(
            aggs=self.aggs,
            keys=self.keys,
            scope=scope,
            is_post_agg=is_post_agg,
            merged_base_measures=merged_base_measures,
            merged_calc_measures=merged_calc_measures,
            tbl=tbl,
        )

        if plan.calc_specs or plan.group_by_cols:
            return compile_grouped_with_all(
                tbl,
                list(plan.group_by_cols),
                dict(plan.agg_specs),
                dict(plan.calc_specs),
                requested_measures=list(plan.requested_measures),
            )
        else:
            return tbl.aggregate({name: fn(tbl) for name, fn in plan.agg_specs.items()})

    def _to_untagged_with_preagg(
        self,
        all_roots: list,
        join_op: SemanticJoinOp,
        join_tree_info: _JoinTreeInfo,
    ):
        """Pre-aggregate each source table's measures at its own grain, then join.

        This prevents fan-out inflation when ``join_many`` is used.
        """
        from .compile_all import _join_tables

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        merged_base_measures = _get_merged_fields(all_roots, "measures")
        merged_calc_measures = _get_merged_fields(all_roots, "calc_measures")
        group_by_cols = list(self.keys)

        # --- 1. Try to build the full joined table (for scope / dim bridge) ---
        try:
            tbl = join_op.to_untagged(parent_requirements=self.required_columns)
            dim_mutations = {
                k: merged_dimensions[k](tbl)
                for k in self.keys
                if k in merged_dimensions
            }
            tbl = tbl.mutate(**dim_mutations) if dim_mutations else tbl
        except Exception:
            tbl = None  # chasm / column collision – work without full join

        # --- 2. Build aggregation plan ---
        if tbl is not None:
            scope = MeasureScope(
                _tbl=tbl,
                _known=list(merged_base_measures.keys())
                + list(merged_calc_measures.keys()),
            )
            plan = _build_aggregation_plan(
                aggs=self.aggs, keys=self.keys, scope=scope,
                is_post_agg=False,
                merged_base_measures=merged_base_measures,
                merged_calc_measures=merged_calc_measures, tbl=tbl,
            )
        else:
            # Derive plan directly from metadata (chasm fallback)
            agg_specs = {}
            for name, fn_wrapped in self.aggs.items():
                if name in merged_base_measures:
                    agg_specs[name] = _make_agg_callable(merged_base_measures[name])
            plan = _AggregationPlan(
                agg_specs=FrozenDict(agg_specs),
                calc_specs=FrozenDict({}),
                requested_measures=tuple(self.aggs.keys()),
                group_by_cols=tuple(self.keys),
            )

        # --- 3. Partition agg_specs by source table ---
        partitioned = _partition_agg_specs_by_source(dict(plan.agg_specs), all_roots)

        # --- 4. Pre-aggregate each source table on its raw table ---
        preagg_results: list = []

        for table_name, measures in partitioned.items():
            if table_name is None:
                # Unprefixed – aggregate on the full join if available
                if tbl is not None:
                    agg_exprs = {n: f(tbl) for n, f in measures.items()}
                    if group_by_cols:
                        r = tbl.group_by([tbl[c] for c in group_by_cols]).aggregate(**agg_exprs)
                    else:
                        r = tbl.aggregate(**agg_exprs)
                    preagg_results.append(r)
                continue

            table_op = join_tree_info.table_ops.get(table_name)
            if table_op is None:
                continue

            raw_tbl = _to_untagged(table_op)
            table_measures = _get_field_dict(table_op, "measures")
            table_dims = _get_field_dict(table_op, "dimensions")
            raw_columns = set(raw_tbl.columns)

            # Build agg expressions on the raw table
            agg_exprs: dict = {}
            for mname, _mfn in measures.items():
                short = mname.split(".", 1)[1] if "." in mname else mname
                if short in table_measures:
                    agg_exprs[mname] = table_measures[short](raw_tbl)

            if not agg_exprs:
                continue

            # --- Compute grain ---
            if not group_by_cols:
                # No group-by → scalar aggregate
                preagg_results.append(raw_tbl.aggregate(**agg_exprs))
                continue

            # a) group-by dims that live on this table
            grain: list[str] = []
            for gb_key in group_by_cols:
                if "." in gb_key:
                    prefix, short = gb_key.split(".", 1)
                    if prefix == table_name and short in table_dims:
                        dim_fn = table_dims[short]
                        if callable(dim_fn):
                            try:
                                col_name = dim_fn(raw_tbl).get_name()
                                if col_name in raw_columns and col_name not in grain:
                                    grain.append(col_name)
                            except Exception:
                                pass

            # b) if none found, use join keys (connects to ancestor tables)
            if not grain:
                join_keys = join_tree_info.table_join_keys.get(table_name, set())
                for jk in sorted(join_keys):
                    if jk in raw_columns:
                        grain.append(jk)

            if grain:
                preagg_results.append(
                    raw_tbl.group_by([raw_tbl[c] for c in grain]).aggregate(**agg_exprs)
                )
            else:
                preagg_results.append(raw_tbl.aggregate(**agg_exprs))

        if not preagg_results:
            if tbl is not None:
                return tbl.aggregate({n: f(tbl) for n, f in plan.agg_specs.items()})
            raise ValueError("No aggregation results and full join unavailable")

        # --- 5. Combine pre-agg results ---
        if not group_by_cols:
            # Cross-join all scalar results
            result = preagg_results[0]
            for pt in preagg_results[1:]:
                result = result.cross_join(pt)
        elif tbl is not None:
            result = self._join_preagg_with_dim_bridge(
                preagg_results, plan, tbl, group_by_cols,
            )
        else:
            # Chasm fallback with group-by: best-effort cross-join
            result = preagg_results[0]
            for pt in preagg_results[1:]:
                result = result.cross_join(pt)

        # --- 6. Apply calc_specs ---
        if plan.calc_specs:
            result = self._apply_calc_specs(result, plan, tbl)

        # --- 7. Select requested columns ---
        select_cols = list(dict.fromkeys(
            list(plan.group_by_cols) + list(plan.requested_measures)
            + list(plan.calc_specs.keys()),
        ))
        available = set(result.columns)
        select_cols = [c for c in select_cols if c in available]
        if select_cols:
            result = result.select([result[c] for c in select_cols])

        return result

    # -- helpers for _to_untagged_with_preagg --------------------------------

    @staticmethod
    def _join_preagg_with_dim_bridge(preagg_results, plan, tbl, group_by_cols):
        """Join pre-aggregated tables using per-table dimension bridges."""
        from .compile_all import _join_tables

        measure_names = set(plan.agg_specs.keys()) | set(plan.calc_specs.keys())

        rejoined: list = []
        for pt in preagg_results:
            pt_grain = [c for c in pt.columns if c not in measure_names]
            pt_meas = [c for c in pt.columns if c in measure_names]

            if set(group_by_cols).issubset(set(pt_grain)):
                # Already has group-by columns — re-aggregate if over-grouped
                if set(pt_grain) != set(group_by_cols):
                    re_aggs = {m: pt[m].sum() for m in pt_meas}
                    pt = pt.group_by([pt[c] for c in group_by_cols]).aggregate(**re_aggs)
                rejoined.append(pt)
            elif pt_grain:
                # Build a per-table dim bridge with ONLY this table's grain cols
                bridge_cols = list(group_by_cols) + [
                    c for c in pt_grain if c not in group_by_cols
                ]
                bridge_cols = [c for c in bridge_cols if c in tbl.columns]
                if bridge_cols:
                    dim_bridge = tbl.select([tbl[c] for c in bridge_cols]).distinct()
                    common = [c for c in pt_grain if c in dim_bridge.columns]
                    if common:
                        preds = [pt[c] == dim_bridge[c] for c in common]
                        bridge_only = [c for c in dim_bridge.columns if c not in common]
                        joined_pt = pt.left_join(dim_bridge, preds).select(
                            [pt] + [dim_bridge[c] for c in bridge_only]
                        )
                        gb_avail = [c for c in group_by_cols if c in joined_pt.columns]
                        if gb_avail:
                            re_aggs = {
                                m: joined_pt[m].sum()
                                for m in pt_meas
                                if m in joined_pt.columns
                            }
                            if re_aggs:
                                joined_pt = joined_pt.group_by(
                                    [joined_pt[c] for c in gb_avail]
                                ).aggregate(**re_aggs)
                        rejoined.append(joined_pt)
                    else:
                        rejoined.append(pt)
                else:
                    rejoined.append(pt)
            else:
                rejoined.append(pt)

        return _join_tables(group_by_cols, rejoined)

    @staticmethod
    def _apply_calc_specs(result, plan, tbl):
        """Apply calculated measure specs (ratios, percent-of-total, etc.)."""
        from .compile_all import _compile_formula, _collect_all_refs

        needed_totals: set[str] = set()
        for ast in plan.calc_specs.values():
            _collect_all_refs(ast, needed_totals)

        if needed_totals:
            totals_aggs = {
                ref: result[ref].sum()
                for ref in needed_totals
                if ref in result.columns
            }
            all_tbl = result.aggregate(**totals_aggs) if totals_aggs else None
        else:
            all_tbl = None

        out = result.cross_join(all_tbl) if all_tbl is not None else result
        calc_cols = {
            name: _compile_formula(ast, out, all_tbl, tbl if tbl is not None else out)
            for name, ast in plan.calc_specs.items()
        }
        return out.mutate(**calc_cols)


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

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        agg_tbl = _to_untagged(self.source)

        # Process mutations incrementally so each can reference previous ones
        # This allows: .mutate(rank=..., is_other=lambda t: t["rank"] > 5)
        current_tbl = agg_tbl
        for name, fn_wrapped in self.post.items():
            proxy = MeasureScope(_tbl=current_tbl, _known=[], _post_agg=True)
            resolved = _resolve_expr(_unwrap(fn_wrapped), proxy)

            # If resolved expression is from regular ibis but table is xorq, convert it
            if hasattr(resolved, "__class__") and hasattr(current_tbl, "__class__"):
                resolved_module = resolved.__class__.__module__
                table_module = current_tbl.__class__.__module__
                if (
                    "ibis.expr" in resolved_module
                    and "xorq" not in resolved_module
                    and "xorq.vendor.ibis" in table_module
                ):
                    from xorq.common.utils.ibis_utils import from_ibis

                    resolved = from_ibis(resolved)

            new_col = resolved.name(name)
            current_tbl = current_tbl.mutate([new_col])

        return current_tbl

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


class SemanticUnnestOp(Relation):
    """Unnest an array column, expanding rows (like Malloy's nested data pattern)."""

    source: Relation
    column: str

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def schema(self) -> Schema:
        # After unnesting, the schema changes - the array column is replaced by its element schema
        # For now, delegate to source schema (ideally we'd update it)
        return self.source.schema

    @property
    def values(self) -> FrozenDict:
        return FrozenDict({})

    def to_untagged(self):
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

            # Only Struct types have fields to unpack
            if isinstance(col_type, dt.Struct) and col_type.fields:
                struct_fields = build_struct_fields(col_expr, col_type)
                return unnested_tbl.select(unnested_tbl, **struct_fields)

            return unnested_tbl

        tbl = _to_untagged(self.source)

        if self.column not in tbl.columns:
            raise ValueError(f"Column '{self.column}' not found in table")

        try:
            unnested = tbl.unnest(self.column)
        except Exception as e:
            raise ValueError(f"Failed to unnest column '{self.column}': {e}") from e

        return unpack_struct_if_needed(unnested, self.column)

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


class SemanticJoinOp(Relation):
    left: Relation
    right: Relation
    how: str
    on: (
        Callable[[Any, Any], Any] | None
    )  # Returns BooleanValue from either ibis or xorq.vendor.ibis
    cardinality: str  # "one", "many", or "cross"

    def __init__(
        self,
        left: Relation,
        right: Relation,
        how: str = "inner",
        on: Callable[[Any, Any], Any] | None = None,
        cardinality: str = "one",
    ) -> None:
        super().__init__(
            left=Relation.__coerce__(left),
            right=Relation.__coerce__(right),
            how=how,
            on=on,
            cardinality=cardinality,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        vals: dict[str, Any] = {}
        vals.update(self.left.values)
        vals.update(self.right.values)
        return FrozenOrderedDict(vals)

    @property
    def schema(self):
        """Get schema of semantic table.

        Uses runtime imports to handle both regular ibis and xorq (which vendors ibis).
        Converts dtypes to strings to allow Schema to parse them into the correct dtype objects.
        """
        fields_dict = {name: str(v.dtype) for name, v in self.values.items()}
        return _make_schema(fields_dict)

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "dimensions"),
            source=self,  # Pass self to extract join keys
        )

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of base measures with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "measures"),
            source=self,
        )

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "calc_measures"),
            source=self,
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

    @property
    def name(self) -> str | None:
        return None

    @property
    def description(self) -> str | None:
        """Get description for joined model by combining root model descriptions."""
        roots = _find_all_root_models(self)
        base_descriptions = []
        for root in roots:
            root_name = getattr(root, "name", None) or "unnamed"
            root_desc = getattr(root, "description", None)
            if root_desc:
                base_descriptions.append(f"{root_name} ({root_desc})")
            else:
                base_descriptions.append(root_name)
        if base_descriptions:
            return "Joined model combining: " + ", ".join(base_descriptions)
        return None

    @property
    def table(self):
        return self.to_untagged()

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

    def with_dimensions(self, **dims) -> SemanticTable:
        return _semantic_table(
            table=self.to_untagged(),
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            name=None,
        )

    def with_measures(self, **meas) -> SemanticTable:
        joined_tbl = self.to_untagged()
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

    def join_one(
        self,
        other: SemanticTable,
        on: Callable[[Any, Any], ir.BooleanValue],
        how: str = "inner",
    ):
        """Join with one-to-one relationship semantics."""
        from .expr import SemanticJoin

        return SemanticJoin(
            left=self,
            right=other.op(),
            on=on,
            how=how,
            cardinality="one",
        )

    def join_many(
        self,
        other: SemanticTable,
        on: Callable[[Any, Any], ir.BooleanValue],
        how: str = "left",
    ):
        """Join with one-to-many relationship semantics."""
        from .expr import SemanticJoin

        return SemanticJoin(
            left=self,
            right=other.op(),
            on=on,
            how=how,
            cardinality="many",
        )

    def join_cross(self, other: SemanticTable):
        """Cross join (Cartesian product) with another semantic model."""
        from .expr import SemanticJoin

        return SemanticJoin(
            left=self,
            right=other.op(),
            on=None,
            how="cross",
            cardinality="cross",
        )

    def join(self, *args, **kwargs):
        """Deprecated: Use join_one(), join_many(), or join_cross() instead."""
        raise TypeError(
            "The join() method has been removed. Use join_one(), join_many(), or join_cross() instead.\n\n"
            "For one-to-one relationships:\n"
            "  table.join_one(other, lambda l, r: l.id == r.id, how='inner')\n\n"
            "For one-to-many relationships:\n"
            "  table.join_many(other, lambda l, r: l.id == r.id, how='left')\n\n"
            "For Cartesian product:\n"
            "  table.join_cross(other)"
        )

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> SemanticIndexOp:
        """Create an index for search/discovery.

        Supports ibis selectors (s.all(), s.cols(), etc.).
        """

        # Handle ibis selectors
        processed_selector = selector
        if selector is not None and "ibis.selectors" in str(type(selector).__module__):
            # Handle s.all() - select all columns
            if type(selector).__name__ == "AllColumns":
                processed_selector = None
            # Handle s.cols() - select specific columns
            elif type(selector).__name__ == "Cols":
                # Extract column names from the Cols selector
                processed_selector = sorted(selector.names)
            # For other selectors, keep as-is
            else:
                processed_selector = selector

        return SemanticIndexOp(source=self, selector=processed_selector, by=by, sample=sample)

    def _collect_leaf_table_names(self) -> set[str]:
        """Collect names of all leaf (base) tables in this join tree."""
        tables = set()

        if isinstance(self.left, SemanticJoinOp):
            tables |= self.left._collect_leaf_table_names()
        else:
            left_name = getattr(self.left, "name", None)
            if left_name:
                tables.add(left_name)

        if isinstance(self.right, SemanticJoinOp):
            tables |= self.right._collect_leaf_table_names()
        else:
            right_name = getattr(self.right, "name", None)
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
            table_name = getattr(node, "name", None)
            return [(table_name, _to_untagged(node))] if table_name else []

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
                    # All measure_obj values are Measure instances with expr attribute
                    measure_fn = measure_obj.expr
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
                self.left.to_untagged()
                if isinstance(self.left, SemanticJoinOp)
                else _to_untagged(self.left)
            )
            temp_right = (
                self.right.to_untagged()
                if isinstance(self.right, SemanticJoinOp)
                else _to_untagged(self.right)
            )

            join_keys_result = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys_result.is_success():
                # Add join keys to leaf tables (immutable operations)
                if isinstance(self.left, SemanticJoinOp):
                    for col in join_keys_result.left_columns:
                        for leaf_name in self.left._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.left, leaf_name)
                            if leaf_table and col in _to_untagged(leaf_table).columns:
                                requirements = requirements.add_columns(leaf_name, {col})
                else:
                    left_name = getattr(self.left, "name", None)
                    if left_name:
                        requirements = requirements.add_columns(
                            left_name, join_keys_result.left_columns
                        )

                if isinstance(self.right, SemanticJoinOp):
                    for col in join_keys_result.right_columns:
                        for leaf_name in self.right._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.right, leaf_name)
                            if leaf_table and col in _to_untagged(leaf_table).columns:
                                requirements = requirements.add_columns(leaf_name, {col})
                else:
                    right_name = getattr(self.right, "name", None)
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
            left_name = getattr(join_op.left, "name", None)
            if left_name == target_name:
                return join_op.left

        if isinstance(join_op.right, SemanticJoinOp):
            result = self._get_leaf_table_by_name(join_op.right, target_name)
            if result is not None:
                return result
        else:
            right_name = getattr(join_op.right, "name", None)
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
                self.left.to_untagged(parent_requirements=None)
                if isinstance(self.left, SemanticJoinOp)
                else _to_untagged(self.left)
            )
            temp_right = (
                self.right.to_untagged(parent_requirements=None)
                if isinstance(self.right, SemanticJoinOp)
                else _to_untagged(self.right)
            )

            join_keys = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys.is_success():
                # Add join keys to the appropriate leaf tables
                if not isinstance(self.left, SemanticJoinOp):
                    # Left is a leaf table
                    left_name = getattr(self.left, "name", None)
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
                                    leaf_ibis = _to_untagged(leaf_table)
                                    if col in leaf_ibis.columns:
                                        existing = join_columns.get(table_name, set())
                                        join_columns[table_name] = existing | {col}

                if not isinstance(self.right, SemanticJoinOp):
                    # Right is a leaf table
                    right_name = getattr(self.right, "name", None)
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
                                    leaf_ibis = _to_untagged(leaf_table)
                                    if col in leaf_ibis.columns:
                                        existing = join_columns.get(table_name, set())
                                        join_columns[table_name] = existing | {col}

        return join_columns

    def to_untagged(self, parent_requirements: dict[str, set[str]] | None = None):
        """Convert join to Ibis expression.

        Note: Projection pushdown has been disabled for compatibility with xorq's
        vendored ibis, which has stricter column access after joins. Without projection
        pushdown, all columns from both tables are available after the join.

        Args:
            parent_requirements: Ignored. Kept for API compatibility.

        Returns:
            Ibis join expression with all columns from both tables
        """
        from .convert import _Resolver

        # Simply convert both sides without any projection pushdown
        left_tbl = (
            _to_untagged(self.left)
            if not isinstance(self.left, SemanticJoinOp)
            else self.left.to_untagged()
        )
        right_tbl = (
            _to_untagged(self.right)
            if not isinstance(self.right, SemanticJoinOp)
            else self.right.to_untagged()
        )

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
        return self.to_untagged().execute()

    def compile(self, **kwargs):
        return self.to_untagged().compile(**kwargs)

    def sql(self, **kwargs):
        return ibis.to_sql(self.to_untagged(), **kwargs)

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
            table=self.to_untagged(),
            dimensions=self.get_dimensions(),
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
        )


class _OrderByProxy:
    """Proxy that resolves short column names to dot-prefixed columns.

    After aggregating a joined model the ibis table has columns like
    ``flights.flight_count``.  This proxy lets ``t.flight_count`` resolve
    to ``tbl["flights.flight_count"]`` so that ``order_by`` lambdas and
    Deferred expressions work without requiring the full prefix.

    Raises ``AttributeError`` / ``KeyError`` when a short name is ambiguous
    (matches multiple prefixed columns), following Malloy's convention that
    ambiguous names must be fully qualified.
    """

    __slots__ = ("_tbl", "_short_to_full", "_ambiguous")

    def __init__(self, tbl: ir.Table) -> None:
        object.__setattr__(self, "_tbl", tbl)
        mapping: dict[str, str] = {}
        ambiguous: set[str] = set()
        for col in tbl.columns:
            if "." in col:
                short = col.rsplit(".", 1)[1]
                if short in ambiguous:
                    continue
                if short in mapping:
                    # Second table has the same short name — mark ambiguous
                    del mapping[short]
                    ambiguous.add(short)
                else:
                    mapping[short] = col
        object.__setattr__(self, "_short_to_full", mapping)
        object.__setattr__(self, "_ambiguous", frozenset(ambiguous))

    def _check_ambiguous(self, name: str) -> None:
        ambiguous = object.__getattribute__(self, "_ambiguous")
        if name in ambiguous:
            tbl = object.__getattribute__(self, "_tbl")
            matches = [c for c in tbl.columns if c.endswith(f".{name}")]
            raise AttributeError(
                f"Ambiguous column '{name}' matches multiple prefixed columns: "
                f"{matches}. Use the full prefixed name to disambiguate."
            )

    def __getattr__(self, name: str) -> ir.Value:
        tbl = object.__getattribute__(self, "_tbl")
        # Direct column match
        if name in tbl.columns:
            return tbl[name]
        # Check ambiguity before short-name resolution
        self._check_ambiguous(name)
        # Short-name resolution
        short_to_full = object.__getattribute__(self, "_short_to_full")
        if name in short_to_full:
            return tbl[short_to_full[name]]
        # Fallback to raw table attribute (e.g. ibis methods)
        return getattr(tbl, name)

    def __getitem__(self, name: str) -> ir.Value:
        tbl = object.__getattribute__(self, "_tbl")
        if name in tbl.columns:
            return tbl[name]
        self._check_ambiguous(name)
        short_to_full = object.__getattribute__(self, "_short_to_full")
        if name in short_to_full:
            return tbl[short_to_full[name]]
        return tbl[name]


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

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        tbl = _to_untagged(self.source)
        proxy = _OrderByProxy(tbl)

        def resolve_order_key(key):
            if isinstance(key, str):
                # First try column access, then fallback to attribute (for computed columns)
                return tbl[key] if key in tbl.columns else getattr(proxy, key, key)
            elif isinstance(key, _CallableWrapper):
                unwrapped = _unwrap(key)
                return _resolve_expr(unwrapped, proxy)
            return key

        return tbl.order_by([resolve_order_key(key) for key in self.keys])

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


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

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        tbl = _to_untagged(self.source)
        return tbl.limit(self.n) if self.offset == 0 else tbl.limit(self.n, offset=self.offset)

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


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
    if selector is None:
        selector = s.all()

    raw_fields = _resolve_selector(selector, base_tbl)

    if not raw_fields:
        result = list(merged_dimensions.keys())
        result.extend(col for col in base_tbl.columns if col not in result)
    else:
        result = [col for col in raw_fields if col in merged_dimensions or col in base_tbl.columns]

    return result


class SemanticIndexOp(Relation):
    source: Relation
    selector: str | list[str] | tuple[str, ...] | Callable | None
    by: str | None = None
    sample: int | None = None

    def __init__(
        self,
        source: Relation,
        selector: str | list[str] | tuple[str, ...] | Callable | None = None,
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

        # Convert selector to tuple if it's a list (Ibis requires hashable types)
        hashable_selector = tuple(selector) if isinstance(selector, list) else selector

        super().__init__(
            source=Relation.__coerce__(source),
            selector=hashable_selector,
            by=by,
            sample=sample,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
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

    @property
    def keys(self) -> tuple[str, ...]:
        return ("fieldValue", "fieldName", "fieldPath", "fieldType")

    @property
    def aggs(self) -> dict[str, Any]:
        return {"weight": lambda t: t.weight}

    def to_untagged(self):
        all_roots = _find_all_root_models(self.source)
        base_tbl = (
            _to_untagged(self.source).limit(self.sample)
            if self.sample
            else _to_untagged(self.source)
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
        return self.to_untagged().execute()

    def as_expr(self):
        """Return self as expression."""
        return self

    def compile(self, **kwargs):
        return self.to_untagged().compile(**kwargs)

    def sql(self, **kwargs):
        return ibis.to_sql(self.to_untagged(), **kwargs)

    def __getitem__(self, key):
        return self.to_untagged()[key]

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)


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

    if hasattr(node, "left") and hasattr(node, "right"):
        roots.extend(_find_all_root_models(node.left))
        roots.extend(_find_all_root_models(node.right))
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


def _extract_join_key_column_names(source: Relation) -> set[str]:
    """
    Extract column names used as join keys from the operation tree.

    When columns are used as join keys, Ibis merges them instead of creating
    _right suffixes, so we need to identify them to avoid incorrect renaming.

    Args:
        source: The relation to search for join operations

    Returns:
        Set of column names used as join keys
    """
    from ibis.expr.operations.relations import Field

    join_keys = set()

    def find_joins(node):
        """Recursively find join operations and extract key columns."""
        if isinstance(node, SemanticJoinOp) and node.on:
            # Evaluate the join predicate with expressions, not operations
            try:
                left_expr = node.left.to_expr() if hasattr(node.left, "to_expr") else node.left
                right_expr = node.right.to_expr() if hasattr(node.right, "to_expr") else node.right
                pred_expr = node.on(left_expr, right_expr)

                # Walk the predicate to find Field nodes
                from .graph_utils import walk_nodes

                fields = list(walk_nodes(Field, pred_expr))
                for field in fields:
                    if hasattr(field, "name"):
                        join_keys.add(field.name)
            except Exception:
                # If we can't extract keys, continue - better to skip than fail
                pass

        # Recursively search in left and right
        if hasattr(node, "left") and isinstance(node.left, Relation):
            find_joins(node.left)
        if hasattr(node, "right") and isinstance(node.right, Relation):
            find_joins(node.right)
        if hasattr(node, "source") and isinstance(node.source, Relation):
            find_joins(node.source)

    find_joins(source)
    return join_keys


def _build_column_rename_map(
    all_roots: Sequence[SemanticTable],
    field_accessor: callable,
    source: Relation | None = None,
) -> dict[str, str]:
    """
    Build a mapping of dimension names to their renamed column names in joined tables.

    When Ibis joins tables with duplicate column names, it renames columns from later
    tables with '_right' suffix. However, columns used as join keys are merged and
    NOT renamed, so we exclude them from the rename map.

    Uses graph_utils for generic traversal and the returns library for safe handling.

    Args:
        all_roots: List of root semantic tables in join order
        field_accessor: Function to get fields (dimensions) from a root
        source: Optional source relation to extract join keys from

    Returns:
        Dict mapping dimension names like 'airports.city' to renamed columns like 'city_right'
    """
    # Build column index using graph_utils (returns Result)
    from returns.result import Failure

    from .graph_utils import build_column_index_from_roots, extract_column_from_dimension

    column_index_result = build_column_index_from_roots(all_roots)
    if isinstance(column_index_result, Failure):
        # If we can't build the index, return empty map (dimensions will use fallback behavior)
        return {}

    column_index = column_index_result.value_or({})

    # Extract join key columns to exclude from renaming
    join_keys = _extract_join_key_column_names(source) if source else set()

    # Process dimensions and determine which need renamed columns
    rename_map = {}

    for idx, root in enumerate(all_roots):
        if not root.name:
            continue

        fields_dict = field_accessor(root)
        if not fields_dict:
            continue

        root_tbl = root.to_untagged()

        for field_name, field_value in fields_dict.items():
            # Extract column name using graph_utils (returns Maybe)
            column_maybe = extract_column_from_dimension(field_value, root_tbl)

            # Use Maybe pattern from returns library
            column_maybe.bind_optional(
                lambda base_column: _check_and_add_rename(  # noqa: B023
                    rename_map=rename_map,
                    base_column=base_column,
                    prefixed_name=f"{root.name}.{field_name}",  # noqa: B023
                    table_idx=idx,  # noqa: B023
                    column_index=column_index,
                    join_keys=join_keys,
                )
            )

    return rename_map


def _check_and_add_rename(
    rename_map: dict[str, str],
    base_column: str,
    prefixed_name: str,
    table_idx: int,
    column_index: dict[str, list[int]],
    join_keys: set[str],
) -> None:
    """
    Check if a column needs renaming and add to rename map if so.

    Helper function for _build_column_rename_map that encapsulates the
    rename decision logic.

    Args:
        rename_map: Map to update with renames
        base_column: The base column name
        prefixed_name: The prefixed dimension name (e.g., 'airports.city')
        table_idx: Index of the current table
        column_index: Index of column occurrences
        join_keys: Set of column names used as join keys (these don't get renamed)
    """
    # Skip columns that are join keys - they get merged, not renamed
    if base_column in join_keys:
        return

    if base_column in column_index:
        tables_with_column = column_index[base_column]
        # Check if any table before this one has the same column
        earlier_tables = [t for t in tables_with_column if t < table_idx]
        if earlier_tables:
            # This column will be renamed with _right suffix
            rename_map[prefixed_name] = f"{base_column}_right"


def _wrap_dimension_for_renamed_column(dimension: Dimension, renamed_column: str) -> Dimension:
    """
    Wrap a dimension to access a renamed column in a joined table.

    Args:
        dimension: The original dimension
        renamed_column: The renamed column name (e.g., 'city_right')

    Returns:
        A new Dimension that accesses the renamed column
    """

    # Create a new callable that accesses the renamed column
    def renamed_accessor(table: ir.Table) -> ir.Value:
        return table[renamed_column]

    # Return a new Dimension with the wrapped callable but same metadata
    return Dimension(
        expr=renamed_accessor,
        description=dimension.description,
        is_entity=dimension.is_entity,
        is_time_dimension=dimension.is_time_dimension,
        is_event_timestamp=dimension.is_event_timestamp,
        smallest_time_grain=dimension.smallest_time_grain,
    )


def _merge_fields_with_prefixing(
    all_roots: Sequence[SemanticTable],
    field_accessor: callable,
    source: Relation | None = None,
) -> FrozenDict[str, Any]:
    """
    Generic function to merge any type of fields (dimensions, measures) with prefixing.

    Args:
        all_roots: List of SemanticTable root models
        field_accessor: Function that takes a root and returns the fields dict (e.g. lambda r: r.dimensions)
        source: Optional source relation to extract join keys from for proper column renaming

    Returns:
        FrozenDict mapping field names (always prefixed with table name) to field values
    """
    if not all_roots:
        return FrozenDict()

    merged_fields = {}

    is_calc_measures = False
    is_dimensions = False
    if all_roots:
        sample_fields = field_accessor(all_roots[0])
        if sample_fields:
            from .measure_scope import AllOf, BinOp, MeasureRef

            first_val = next(iter(sample_fields.values()), None)
            is_calc_measures = isinstance(
                first_val,
                MeasureRef | AllOf | BinOp | int | float,
            )
            is_dimensions = isinstance(first_val, Dimension)

    # For dimensions, build a column rename map to handle Ibis join conflicts
    column_rename_map = {}
    if is_dimensions:
        column_rename_map = _build_column_rename_map(all_roots, field_accessor, source)

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
                # If it's a dimension that needs column renaming, wrap the callable
                elif is_dimensions and prefixed_name in column_rename_map:
                    field_value = _wrap_dimension_for_renamed_column(
                        field_value, column_rename_map[prefixed_name]
                    )

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

    tracker_ref = [ColumnTracker()]

    def on_column_access(col_name: str) -> None:
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
    left_tracker_ref = [ColumnTracker()]
    right_tracker_ref = [ColumnTracker()]

    def on_left_access(col_name: str) -> None:
        left_tracker_ref[0] = left_tracker_ref[0].with_column(col_name)

    def on_right_access(col_name: str) -> None:
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
