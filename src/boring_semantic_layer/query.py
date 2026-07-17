"""
Query interface for semantic API with filter and time dimension support.

Provides parameter-based querying as an alternative to method chaining.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, ClassVar, Literal

import ibis
from attrs import frozen
from ibis.common.collections import FrozenDict
from toolz import curry

from .utils import safe_eval


def _get_ibis_api():
    """Return xorq's vendored ibis API if available, else plain ibis.

    Filter expressions built with ``ibis._`` / ``ibis.literal()`` must use the
    same ibis implementation as the table they will be resolved against.  Since
    ``_ensure_xorq_table()`` converts tables to xorq when possible, we should
    build filter expressions with xorq's ibis to match.  For backends that
    xorq does not support, plain ibis is used as the fallback.
    """
    try:
        from ._xorq import api as xo

        return xo
    except Exception:
        return ibis

# Time grain type alias
TimeGrain = Literal[
    "TIME_GRAIN_YEAR",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_SECOND",
]

# Mapping of time grain identifiers to ibis truncate units (immutable)
TIME_GRAIN_TRANSFORMATIONS: FrozenDict = {
    "TIME_GRAIN_YEAR": "Y",
    "TIME_GRAIN_QUARTER": "Q",
    "TIME_GRAIN_MONTH": "M",
    "TIME_GRAIN_WEEK": "W",
    "TIME_GRAIN_DAY": "D",
    "TIME_GRAIN_HOUR": "h",
    "TIME_GRAIN_MINUTE": "m",
    "TIME_GRAIN_SECOND": "s",
}

# Order of grains from finest to coarsest (immutable)
TIME_GRAIN_ORDER: tuple[str, ...] = (
    "TIME_GRAIN_SECOND",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_YEAR",
)


# Filter parsing and compilation lives in ``boring_semantic_layer.predicate``.


@curry
def _is_time_dimension(dims_dict: dict[str, Any], dim_name: str) -> bool:
    """Check if a dimension is a time dimension (curried for partial application)."""
    return dim_name in dims_dict and dims_dict[dim_name].is_time_dimension


def _find_time_dimension(semantic_table: Any, dimensions: list[str]) -> str | None:
    """
    Find the first time dimension in the query dimensions list.

    Uses functional composition to find matching dimension.
    """
    dims_dict = semantic_table.get_dimensions()
    is_time_dim = _is_time_dimension(dims_dict)
    return next((dim for dim in dimensions if is_time_dim(dim)), None)


def _find_any_time_dimension(semantic_table: Any) -> str | None:
    """Find the first declared time dimension on a semantic table."""
    dims_dict = semantic_table.get_dimensions()
    return next((name for name, dim in dims_dict.items() if dim.is_time_dimension), None)


@curry
def _make_grain_id(grain: str) -> str:
    """Convert grain name to TIME_GRAIN_ identifier (curried)."""
    return f"TIME_GRAIN_{grain.upper()}"


def _normalize_grain(grain: str) -> str:
    """Accept both short ("month") and long ("TIME_GRAIN_MONTH") grain names."""
    if grain in TIME_GRAIN_TRANSFORMATIONS:
        return grain
    canonical = _make_grain_id(grain)
    if canonical in TIME_GRAIN_TRANSFORMATIONS:
        return canonical
    raise ValueError(
        f"Invalid time grain: '{grain}'. "
        f"Valid values: {list(TIME_GRAIN_TRANSFORMATIONS.keys())} "
        f"or short forms like 'month', 'quarter', 'year', etc.",
    )


def _validate_time_grain(
    time_grain: TimeGrain,
    smallest_allowed_grain: str | None,
    dimension_name: str,
) -> None:
    """
    Validate that requested time grain is not finer than smallest allowed grain.

    Raises:
        ValueError: If requested grain is finer than allowed grain.
    """
    if not smallest_allowed_grain:
        return

    smallest_grain = _make_grain_id(smallest_allowed_grain)
    if smallest_grain not in TIME_GRAIN_ORDER:
        return

    requested_idx = TIME_GRAIN_ORDER.index(time_grain)
    smallest_idx = TIME_GRAIN_ORDER.index(smallest_grain)

    if requested_idx < smallest_idx:
        raise ValueError(
            f"Requested time grain '{time_grain}' is finer than the smallest "
            f"allowed grain '{smallest_allowed_grain}' for dimension '{dimension_name}'",
        )


@frozen(kw_only=True, slots=True)
class Filter:
    """
    Unified filter class supporting JSON, string, and callable formats.

    Examples:
        # JSON simple filter
        Filter(filter={"field": "country", "operator": "=", "value": "US"})

        # JSON compound filter
        Filter(filter={
            "operator": "AND",
            "conditions": [
                {"field": "country", "operator": "=", "value": "US"},
                {"field": "tier", "operator": "in", "values": ["gold", "platinum"]}
            ]
        })

        # String expression (evaluated with ibis._)
        Filter(filter="_.carrier == 'AA'")

        # Callable function
        Filter(filter=lambda t: t.amount > 1000)
    """

    filter: FrozenDict | str | Callable

    COMPOUND_OPERATORS: ClassVar[set] = {"AND", "OR"}

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.filter, dict | str) and not callable(self.filter):
            raise ValueError("Filter must be a dict, string, or callable")

    def _convert_filter_value(self, value: Any) -> Any:
        """
        Convert string date/timestamp values to ibis literals for proper SQL generation.

        This fixes TYPE_MISMATCH errors on backends like Athena that require typed
        date literals. Uses a simple loop to avoid nested try/except blocks.
        """
        if not isinstance(value, str):
            return value

        # Try parsing as timestamp first (more general), then date
        _ibis = _get_ibis_api()
        for dtype in ("timestamp", "date"):
            try:
                return _ibis.literal(value, type=dtype)
            except (ValueError, TypeError):
                pass

        # Not a date/timestamp, return original value
        return value

    def to_callable(self) -> Callable:
        """Convert filter to callable that can be used with SemanticTable.filter()."""
        from . import predicate as pred_mod
        from .ops import _ensure_xorq_table

        if isinstance(self.filter, dict):
            pred = pred_mod.from_dict(self.filter)
            ibis_module = _get_ibis_api()

            def _dict_filter(t):
                return pred_mod.compile(
                    pred,
                    ibis_module._,
                    ibis_module=ibis_module,
                ).resolve(_ensure_xorq_table(t))

            # Deferred resolution: columns can't be statically introspected
            # (see ops._dimension_only_source_table). Marked so callers can opt
            # out of static-column optimizations consistently regardless of
            # whether xorq is installed.
            _dict_filter.__bsl_deferred_resolution__ = True
            return _dict_filter
        elif isinstance(self.filter, str):
            _ibis = _get_ibis_api()
            expr = safe_eval(
                self.filter,
                context={"_": _ibis._, "ibis": _ibis},
            ).unwrap()

            def _str_filter(t):
                return expr.resolve(_ensure_xorq_table(t))

            _str_filter.__bsl_deferred_resolution__ = True
            return _str_filter
        elif callable(self.filter):
            return self.filter
        raise ValueError("Filter must be a dict, string, or callable")


@curry
def _normalize_filter(
    filter_spec: dict[str, Any] | str | Callable | Filter,
) -> Callable:
    """
    Normalize filter specification to callable (curried for composition).

    Accepts dict, string, callable, or Filter and returns unified callable.
    """
    if isinstance(filter_spec, Filter):
        return filter_spec.to_callable()
    elif isinstance(filter_spec, dict | str):
        return Filter(filter=filter_spec).to_callable()
    elif callable(filter_spec):
        return filter_spec
    else:
        raise ValueError(f"Unsupported filter type: {type(filter_spec)}")


@curry
def _make_order_key(field: str, direction: str):
    """Create order key for sorting (curried)."""
    return ibis.desc(field) if direction.lower() == "desc" else field


def _normalize_field_name(
    field_name: str,
    known_fields: set[str],
    expected_prefix: str | None = None,
) -> str:
    """Resolve model-prefixed field names for standalone models."""
    if field_name in known_fields or "." not in field_name:
        return field_name

    prefix, unprefixed = field_name.split(".", 1)
    if expected_prefix is None or prefix != expected_prefix:
        return field_name

    return unprefixed if unprefixed in known_fields else field_name


def _normalize_fields(
    fields: Sequence[str] | None,
    known_fields: set[str],
    expected_prefix: str | None = None,
) -> list[str]:
    """Normalize a list of field names against known semantic fields."""
    if not fields:
        return []
    return [_normalize_field_name(field, known_fields, expected_prefix) for field in fields]


def _normalize_order_by(
    order_by: Sequence[tuple[str, str]] | None,
    known_fields: set[str],
    expected_prefix: str | None = None,
) -> list[tuple[str, str]] | None:
    """Normalize order_by fields using the same fallback as dimensions/measures."""
    if not order_by:
        return order_by

    return [
        (_normalize_field_name(field, known_fields, expected_prefix), direction)
        for field, direction in order_by
    ]


def _normalize_time_grains(
    time_grains: Mapping[str, TimeGrain] | None,
    known_dimensions: set[str],
    expected_prefix: str | None = None,
) -> dict[str, TimeGrain]:
    """Normalize per-dimension time grain keys against known semantic dimensions."""
    if not time_grains:
        return {}
    return {
        _normalize_field_name(dim, known_dimensions, expected_prefix): grain
        for dim, grain in time_grains.items()
    }


def _raise_unknown_semantic_fields(kind: str, fields: set[str], allowed: set[str]) -> None:
    unknown = sorted(fields - allowed)
    if unknown:
        raise ValueError(
            f"Unknown semantic {kind}: {', '.join(unknown)}. "
            f"Allowed fields: {', '.join(sorted(allowed)) or 'none'}",
        )


def _filter_semantic_fields(filter_spec: Any) -> set[str]:
    """Return dict-filter field references that can be boundary-checked."""
    raw = filter_spec.filter if isinstance(filter_spec, Filter) else filter_spec
    return _extract_filter_fields(raw) if isinstance(raw, dict) else set()


def _validate_semantic_boundaries(
    *,
    dimensions: Sequence[str],
    measures: Sequence[str] | None,
    filters: Sequence[Any],
    having: Sequence[Any],
    order_by: Sequence[tuple[str, str]] | None,
    known_dimensions: set[str],
    known_measures: set[str],
    model_name: str | None = None,
) -> None:
    """Ensure structured query fields do not escape the declared semantic model."""
    semantic_fields = known_dimensions | known_measures
    _raise_unknown_semantic_fields("dimensions", set(dimensions), known_dimensions)
    if measures is not None:
        _raise_unknown_semantic_fields("measures", set(measures), known_measures)
    if order_by:
        order_fields = {field for field, _ in order_by}
        _raise_unknown_semantic_fields("order_by fields", order_fields, semantic_fields)

    filter_fields: set[str] = set()
    for filter_spec in [*filters, *having]:
        filter_fields.update(
            _normalize_field_name(field, semantic_fields, model_name)
            for field in _filter_semantic_fields(filter_spec)
        )
    _raise_unknown_semantic_fields("filter fields", filter_fields, semantic_fields)


def _extract_filter_fields(filter_spec: dict) -> set[str]:
    """Extract all field names referenced by a dict filter (including compound)."""
    from . import predicate as pred_mod

    if not isinstance(filter_spec, dict):
        return set()
    return pred_mod.fields(pred_mod.from_dict(filter_spec))


def _normalize_filter_fields(
    filter_obj: dict,
    known_fields: set[str],
    model_name: str | None = None,
) -> dict:
    """Recursively normalize field names inside a filter dict."""
    if filter_obj.get("operator") in ("AND", "OR"):
        return {
            **filter_obj,
            "conditions": [
                _normalize_filter_fields(c, known_fields, model_name)
                for c in filter_obj.get("conditions", [])
            ],
        }
    field = filter_obj.get("field")
    if field:
        normalized = _normalize_field_name(field, known_fields, model_name)
        if normalized != field:
            return {**filter_obj, "field": normalized}
    return filter_obj


def _build_post_agg_predicate(filter_obj: dict) -> Any:
    """Build an ibis predicate (``Deferred``) for post-aggregation filters.

    Delegates to the ``Predicate`` AST in ``predicate``. Bracket-access
    field resolution preserves dotted names from joined models (e.g.
    ``orders.total_amount``) on the aggregated table.
    """
    from . import predicate as pred_mod

    pred = pred_mod.from_dict(filter_obj)
    return pred_mod.compile(pred, ibis._, post_agg=True, ibis_module=ibis)


def _normalize_post_agg_filter(
    filter_spec: Any,
    known_measures: set[str],
    model_name: str | None = None,
) -> Callable:
    """Normalize a measure filter for post-aggregation (HAVING) application.

    Handles dict, Filter objects, and callables.  For dict/Filter filters the
    field names are accessed via bracket notation so dotted names from joined
    models work correctly after aggregation.  Field names are normalised
    against *known_measures* so that ``"model.total_sales"`` resolves to
    ``"total_sales"`` on standalone models but stays prefixed on joins.
    """
    raw = filter_spec.filter if isinstance(filter_spec, Filter) else filter_spec
    if isinstance(raw, dict):
        raw = _normalize_filter_fields(raw, known_measures, model_name)
        expr = _build_post_agg_predicate(raw)
        return lambda t: expr.resolve(t)
    if callable(raw):
        return raw
    return _normalize_filter(filter_spec)


def _is_measure_filter(
    filter_spec: Any,
    known_measures: set[str],
    model_name: str | None = None,
) -> bool:
    """Return True if *any* field in a dict/Filter filter references a known measure."""
    # Unwrap Filter objects to inspect their inner dict
    raw = filter_spec
    if isinstance(raw, Filter):
        raw = raw.filter
    if not isinstance(raw, dict):
        return False
    for field in _extract_filter_fields(raw):
        if field in known_measures:
            return True
        # Handle model-prefixed names like "lineitems.metric_ventas"
        if "." in field:
            _prefix, name = field.split(".", 1)
            if name in known_measures:
                return True
            if model_name and _prefix == model_name and name in known_measures:
                return True
    return False


def _split_filter(
    filter_spec: Any,
    known_measures: set[str],
    model_name: str | None,
    pre_agg: list,
    post_agg: list,
) -> None:
    """Route *filter_spec* to *pre_agg* or *post_agg* lists.

    For AND compound filters mixing dimension and measure conditions the
    compound is split so that each condition lands in the right bucket.
    OR compounds with any measure field are kept whole in *post_agg*.
    """
    raw = filter_spec.filter if isinstance(filter_spec, Filter) else filter_spec

    # Compound AND: split individual conditions
    if isinstance(raw, dict) and raw.get("operator") == "AND":
        conditions = raw.get("conditions", [])
        if not conditions:
            raise ValueError("Compound filter must have non-empty conditions list")
        for cond in conditions:
            _split_filter(cond, known_measures, model_name, pre_agg, post_agg)
        return

    if _is_measure_filter(filter_spec, known_measures, model_name):
        post_agg.append(filter_spec)
    else:
        pre_agg.append(filter_spec)


def _build_time_range_filters(semantic_table: Any, time_dimension: str, time_range: Mapping[str, str]) -> list[Callable]:
    """Build reusable filters for a specific time dimension and range."""
    if not isinstance(time_range, dict) or "start" not in time_range or "end" not in time_range:
        raise ValueError("time_range must be a dict with 'start' and 'end' keys")

    from datetime import datetime

    dim_obj = semantic_table.get_dimensions().get(time_dimension)
    if dim_obj is None:
        raise ValueError(
            f"Dimension '{time_dimension}' not found. "
            f"Available dimensions: {list(semantic_table.get_dimensions().keys())}"
        )
    if not dim_obj.is_time_dimension:
        raise ValueError(
            f"Dimension '{time_dimension}' is not a time dimension. "
            "compare_periods and time ranges require a time dimension."
        )

    start_dt = datetime.fromisoformat(time_range["start"])
    end_dt = datetime.fromisoformat(time_range["end"])
    if end_dt < start_dt:
        raise ValueError("time_range end must be greater than or equal to start")
    return [
        lambda t, dim=dim_obj, start=start_dt: dim(t) >= start,
        lambda t, dim=dim_obj, end=end_dt: dim(t) <= end,
    ]


def compare_periods(
    semantic_table: Any,
    dimensions: Sequence[str] | None = None,
    measures: Sequence[str] | None = None,
    current_time_range: Mapping[str, str] | None = None,
    previous_time_range: Mapping[str, str] | None = None,
    filters: Sequence[dict[str, Any] | str | Callable | Filter] | None = None,
    time_dimension: str | None = None,
    time_grain: TimeGrain | None = None,
    time_grains: Mapping[str, TimeGrain] | None = None,
    order_by: Sequence[tuple[str, str]] | None = None,
    limit: int | None = None,
    strict_semantic_boundaries: bool = False,
) -> Any:
    """Compare two time ranges and return current/previous/delta columns."""
    from .api import to_semantic_table

    dimensions = list(dimensions or [])
    measures = list(measures or [])
    filters = list(filters or [])

    if not measures:
        raise ValueError("compare_periods requires at least one measure")
    if current_time_range is None or previous_time_range is None:
        raise ValueError(
            "compare_periods requires both 'current_time_range' and 'previous_time_range'"
        )

    dims_dict = semantic_table.get_dimensions()
    known_dimensions = set(dims_dict)
    known_measures = set(semantic_table.get_measures()) | set(semantic_table.get_calculated_measures())
    model_name = getattr(semantic_table, "name", None)

    dimensions = _normalize_fields(dimensions, known_dimensions, expected_prefix=model_name)
    measures = _normalize_fields(measures, known_measures, expected_prefix=model_name)
    time_grains = _normalize_time_grains(time_grains, known_dimensions, model_name)

    resolved_time_dimension = time_dimension
    if resolved_time_dimension is not None:
        resolved_time_dimension = _normalize_fields(
            [resolved_time_dimension], known_dimensions, expected_prefix=model_name
        )[0]
    else:
        resolved_time_dimension = _find_time_dimension(semantic_table, dimensions) or _find_any_time_dimension(
            semantic_table
        )

    if resolved_time_dimension is None:
        raise ValueError(
            "compare_periods requires a time dimension. Mark one with "
            ".with_dimensions(dim_name={'expr': ..., 'is_time_dimension': True}) "
            "or pass time_dimension explicitly."
        )

    if strict_semantic_boundaries:
        comparison_order_fields = set(dimensions)
        for measure in measures:
            comparison_order_fields.update(
                {
                    f"{measure}_current",
                    f"{measure}_previous",
                    f"{measure}_delta",
                    f"{measure}_pct_change",
                }
            )
        _validate_semantic_boundaries(
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            having=[],
            order_by=None,
            known_dimensions=known_dimensions,
            known_measures=known_measures,
            model_name=model_name,
        )
        if time_dimension is not None:
            _raise_unknown_semantic_fields(
                "time_dimension",
                {resolved_time_dimension},
                known_dimensions,
            )
        if order_by:
            normalized_order_by = _normalize_order_by(
                order_by,
                comparison_order_fields,
                expected_prefix=model_name,
            )
            _raise_unknown_semantic_fields(
                "order_by fields",
                {field for field, _ in normalized_order_by},
                comparison_order_fields,
            )

    current_result = query(
        semantic_table=semantic_table,
        dimensions=dimensions,
        measures=measures,
        filters=[
            *filters,
            *_build_time_range_filters(semantic_table, resolved_time_dimension, current_time_range),
        ],
        time_grain=time_grain,
        time_grains=time_grains,
        strict_semantic_boundaries=strict_semantic_boundaries,
    )
    previous_result = query(
        semantic_table=semantic_table,
        dimensions=dimensions,
        measures=measures,
        filters=[
            *filters,
            *_build_time_range_filters(semantic_table, resolved_time_dimension, previous_time_range),
        ],
        time_grain=time_grain,
        time_grains=time_grains,
        strict_semantic_boundaries=strict_semantic_boundaries,
    )

    current_tbl = current_result.as_table().table.rename(
        {f"{measure}_current": measure for measure in measures}
    )
    previous_tbl = previous_result.as_table().table.rename(
        {
            **{f"{measure}_previous": measure for measure in measures},
            **{f"__previous_{dim}": dim for dim in dimensions},
        }
    )

    if dimensions:
        join_predicates = [current_tbl[dim] == previous_tbl[f"__previous_{dim}"] for dim in dimensions]
        joined = current_tbl.join(previous_tbl, join_predicates, how="outer")
        result_tbl = joined.select(
            *[
                joined[dim].coalesce(joined[f"__previous_{dim}"]).name(dim)
                for dim in dimensions
            ],
            *[joined[f"{measure}_current"] for measure in measures],
            *[joined[f"{measure}_previous"] for measure in measures],
        )
    else:
        joined = current_tbl.join(previous_tbl, how="cross")
        result_tbl = joined.select(
            *[joined[f"{measure}_current"] for measure in measures],
            *[joined[f"{measure}_previous"] for measure in measures],
        )

    pct_mutations = {}
    delta_mutations = {}
    for measure in measures:
        current_col = result_tbl[f"{measure}_current"]
        previous_col = result_tbl[f"{measure}_previous"]
        delta_expr = current_col.fill_null(0) - previous_col.fill_null(0)
        delta_mutations[f"{measure}_delta"] = delta_expr
        pct_mutations[f"{measure}_pct_change"] = delta_expr / previous_col.nullif(0)

    result_tbl = result_tbl.mutate(**delta_mutations, **pct_mutations)

    if order_by:
        order_by = _normalize_order_by(order_by, set(result_tbl.columns), expected_prefix=model_name)
        result_tbl = result_tbl.order_by(
            [
                result_tbl[field].desc() if direction.lower() == "desc" else result_tbl[field]
                for field, direction in order_by
            ]
        )
    if limit is not None:
        result_tbl = result_tbl.limit(limit)

    return to_semantic_table(result_tbl, name=f"{model_name or 'model'}_period_comparison")


def query(
    semantic_table: Any,  # SemanticModel, but avoiding circular import
    dimensions: Sequence[str] | None = None,
    measures: Sequence[str] | None = None,
    filters: Sequence[dict[str, Any] | str | Callable | Filter] | None = None,
    order_by: Sequence[tuple[str, str]] | None = None,
    limit: int | None = None,
    time_grain: TimeGrain | None = None,
    time_grains: Mapping[str, TimeGrain] | None = None,
    time_range: Mapping[str, str] | None = None,
    having: Sequence[dict[str, Any] | str | Callable | Filter] | None = None,
    strict_semantic_boundaries: bool = False,
) -> Any:  # Returns SemanticModel or SemanticAggregate
    """
    Query semantic table using parameter-based interface with time dimension support.

    Args:
        semantic_table: The SemanticTable to query
        dimensions: List of dimension names to group by
        measures: List of measure names to aggregate
        filters: List of filters (dict, str, callable, or Filter objects).
            Dict filters referencing measure fields are automatically applied
            after aggregation (HAVING semantics).  Callable/string filters are
            always applied before aggregation.
        order_by: List of (field, direction) tuples
        limit: Maximum number of rows to return
        time_grain: Optional time grain to apply to ALL time dimensions (e.g., "TIME_GRAIN_MONTH").
            Cannot be used together with time_grains.
        time_grains: Optional per-dimension time grains as a dict mapping dimension names
            to grain values (e.g., {"order_date": "TIME_GRAIN_MONTH", "ship_date": "TIME_GRAIN_QUARTER"}).
            Cannot be used together with time_grain.
        time_range: Optional time range filter with 'start' and 'end' keys
        having: Optional list of post-aggregation filters.  These are always
            applied after group-by/aggregate regardless of field type.  Use
            this for callable/lambda filters that reference measures.
        strict_semantic_boundaries: When True, structured dimensions, measures,
            order_by fields, and dict/Filter filters must reference declared
            dimensions, measures, or calculated measures only.

    Returns:
        SemanticAggregate or SemanticTable ready for execution

    Examples:
        # Basic query
        result = st.query(
            dimensions=["carrier"],
            measures=["flight_count"]
        ).execute()

        # With JSON filter on a measure (auto-detected as HAVING)
        result = st.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[{"field": "flight_count", "operator": ">", "value": 100}]
        ).execute()

        # With explicit having for callable filters on measures
        result = st.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            having=[lambda t: t.flight_count > 100]
        ).execute()

        # With time grain (applies to all time dimensions)
        result = st.query(
            dimensions=["order_date"],
            measures=["total_sales"],
            time_grain="TIME_GRAIN_MONTH"
        ).execute()

        # With per-dimension time grains
        result = st.query(
            dimensions=["order_date", "ship_date"],
            measures=["total_sales"],
            time_grains={"order_date": "TIME_GRAIN_MONTH", "ship_date": "TIME_GRAIN_QUARTER"}
        ).execute()

        # With time range
        result = st.query(
            dimensions=["order_date"],
            measures=["total_sales"],
            time_range={"start": "2024-01-01", "end": "2024-12-31"}
        ).execute()
    """
    from .ops import Dimension

    result = semantic_table
    model_name = getattr(result, "name", None)
    known_dimensions = set(result.get_dimensions())
    known_measures = set(result.get_measures()) | set(result.get_calculated_measures())
    known_order_fields = known_dimensions | known_measures

    dimensions = _normalize_fields(dimensions, known_dimensions, expected_prefix=model_name)
    measures = (
        _normalize_fields(measures, known_measures, expected_prefix=model_name)
        if measures is not None
        else None
    )
    order_by = _normalize_order_by(order_by, known_order_fields, expected_prefix=model_name)
    filters = list(filters or [])  # Copy to avoid mutating input
    having = list(having or [])

    if strict_semantic_boundaries:
        _validate_semantic_boundaries(
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            having=having,
            order_by=order_by,
            known_dimensions=known_dimensions,
            known_measures=known_measures,
            model_name=model_name,
        )

    # Step 0: Add time_range as a filter if specified
    if time_range:
        time_dim_name = _find_time_dimension(result, dimensions)
        if not time_dim_name:
            raise ValueError(
                "time_range filter requires a time dimension in the query dimensions. "
                f"Available dimensions: {list(dimensions)}. "
                "Mark a dimension as a time dimension using: "
                ".with_dimensions(dim_name={'expr': lambda t: t.column, 'is_time_dimension': True})"
            )

        filters.extend(_build_time_range_filters(result, time_dim_name, time_range))

    # Step 1: Handle time grain transformations
    if time_grain and time_grains:
        raise ValueError(
            "Cannot specify both 'time_grain' and 'time_grains'. "
            "Use 'time_grain' to apply a single grain to all time dimensions, "
            "or 'time_grains' to specify per-dimension grains."
        )

    # Build per-dimension grain mapping: either from time_grains directly,
    # or by expanding time_grain to all time dimensions in the query.
    grain_map: dict[str, str] = {}
    time_grains = _normalize_time_grains(time_grains, known_dimensions, model_name)
    if time_grains:
        grain_map = {dim: _normalize_grain(g) for dim, g in time_grains.items()}
    elif time_grain:
        normalized = _normalize_grain(time_grain)
        dims_dict = result.get_dimensions()
        for dim_name in dimensions:
            if dim_name in dims_dict and dims_dict[dim_name].is_time_dimension:
                grain_map[dim_name] = normalized

    if grain_map:
        time_dims_to_transform = {}
        dims_dict = result.get_dimensions()
        for dim_name, grain in grain_map.items():
            if dim_name not in dims_dict:
                raise ValueError(
                    f"Dimension '{dim_name}' not found. "
                    f"Available dimensions: {list(dims_dict.keys())}",
                )
            dim_obj = dims_dict[dim_name]
            if not dim_obj.is_time_dimension:
                raise ValueError(
                    f"Dimension '{dim_name}' is not a time dimension. "
                    "time_grains can only be applied to time dimensions.",
                )
            _validate_time_grain(grain, dim_obj.smallest_time_grain, dim_name)

            # NOTE: We capture dim_obj (not dim_obj.expr) and call dim(t) because
            # Dimension.__call__ properly resolves Deferred expressions via:
            #   self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)
            # Calling orig_expr(t) directly on a Deferred would cause infinite recursion.
            truncate_unit = TIME_GRAIN_TRANSFORMATIONS[grain]
            time_dims_to_transform[dim_name] = Dimension(
                expr=lambda t, dim=dim_obj, unit=truncate_unit: dim(t).truncate(unit),
                description=dim_obj.description,
                is_time_dimension=dim_obj.is_time_dimension,
                smallest_time_grain=dim_obj.smallest_time_grain,
            )

        if time_dims_to_transform:
            result = result.with_dimensions(**time_dims_to_transform)

    # Step 2: Apply filters — separate pre-agg (dimension) from post-agg (measure)
    pre_agg_filters = []
    post_agg_filters = list(having)
    for filter_spec in filters:
        _split_filter(filter_spec, known_measures, model_name, pre_agg_filters, post_agg_filters)

    for filter_spec in pre_agg_filters:
        filter_fn = _normalize_filter(filter_spec)
        result = result.filter(filter_fn)

    # Step 3: Group by and aggregate
    if dimensions:
        result = result.group_by(*dimensions)
        # Materialize grouped dimensions even when no measures are requested.
        # This avoids returning a bare group-by object that compiles to SELECT *.
        result = result.aggregate(*measures) if measures else result.aggregate()
    elif measures:
        # No dimensions = grand total aggregation
        result = result.group_by().aggregate(*measures)

    # Step 3.5: Apply measure filters after aggregation (HAVING semantics)
    for filter_spec in post_agg_filters:
        filter_fn = _normalize_post_agg_filter(filter_spec, known_measures, model_name)
        result = result.filter(filter_fn)

    # Step 4: Apply ordering using functional composition
    if order_by:
        order_keys = [_make_order_key(field, direction) for field, direction in order_by]
        result = result.order_by(*order_keys)

    # Step 5: Apply limit
    if limit:
        result = result.limit(limit)

    return result
