"""Foundation Op classes: Table, Filter, Project, GroupBy.

These four are the simpler, mostly-pass-through ops. SemanticTableOp is
the leaf that holds dimension/measure metadata; the others wrap it
(or a downstream op) and contribute one operation each. The aggregate,
join, and order/limit ops live in their own modules.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from attrs import field
from ibis.expr import types as ir
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema

from .._xorq import FrozenDict, FrozenOrderedDict, Schema as XorqSchema
from ..nested_access import NestedAccessMarker
from ._callable import _ensure_wrapped
from ._format import _semantic_repr
from ._measure_helpers import _build_json_definition
from ._root_models import _find_all_root_models
from ._values import Dimension, Measure


_SchemaClass = XorqSchema
_FrozenOrderedDict = FrozenOrderedDict


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
        from ._core import _make_schema, _mutate_dimensions_with_dependencies, logger

        dims = self.get_dimensions()
        measures = self.get_measures()
        calc_measures = self.get_calculated_measures()
        # Build enriched table with all dimensions resolved (handles derived deps)
        enriched = _mutate_dimensions_with_dependencies(self.table, dims.keys(), dims)
        base_values = {
            **{col: self.table[col].op() for col in self.table.columns},
            **{name: enriched[name].op() for name in dims},
            **{name: fn(enriched).op() for name, fn in measures.items()},
        }
        # Resolve calculated measure types via a dummy table with base measure dtypes.
        # ``infer_calc_dtype`` mirrors the AggregationExpr rewrite from
        # ``compile_grouped_with_all`` so calc measures with inline aggregations
        # (e.g. ``AllOf(AggregationExpr)``) round-trip through type inference.
        if calc_measures:
            from ..compile_all import _get_ibis_module, infer_calc_dtype

            measure_schema = {
                name: base_values[name].dtype for name in measures if name in base_values
            }
            ibis_module = _get_ibis_module(enriched)
            for name, expr in calc_measures.items():
                try:
                    compiled = infer_calc_dtype(
                        expr, measure_schema, enriched, ibis_module
                    )
                    base_values[name] = compiled.op()
                except Exception as e:
                    # Joined models with dotted column names, calc measures
                    # whose inline aggregations don't apply to the dummy schema,
                    # etc. Type info is best-effort; surface for debugging.
                    logger.debug(
                        "calc-measure type inference failed for %r: %s", name, e
                    )
        return FrozenOrderedDict(base_values)

    @property
    def schema(self):
        from ._core import _make_schema

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
        from ..graph_utils import build_dependency_graph

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
        # Conversion happens at SemanticModel construction; self.table is
        # already xorq when supported, plain ibis when not.
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
        from ..convert import _Resolver
        from ._core import (
            _get_merged_fields,
            _mutate_dimensions_with_dependencies,
            _resolve_expr,
            _to_untagged,
            _unwrap,
        )

        # Avoid an isinstance check against SemanticAggregateOp by duck-typing:
        # post-aggregation sources expose neither dimensions nor an enrichable
        # base table, so an empty dim_map is the right behavior.
        from ._core import SemanticAggregateOp

        all_roots = _find_all_root_models(self.source)
        base_tbl = _to_untagged(self.source)
        dim_map = (
            {}
            if isinstance(self.source, SemanticAggregateOp)
            else _get_merged_fields(all_roots, "dimensions")
        )

        # Enrich table with derived dimensions so multi-level deps
        # (e.g. d_two -> d_one -> distance) resolve correctly in filters.
        # Best-effort: skip dimensions whose columns aren't available yet
        # (e.g. join-based dims); those resolve through the Resolver fallback.
        enriched = base_tbl
        for dim_name in dim_map:
            try:
                enriched = _mutate_dimensions_with_dependencies(
                    enriched, [dim_name], dim_map
                )
            except (TypeError, KeyError, AttributeError):
                pass

        pred_fn = _unwrap(self.predicate)
        resolver = _Resolver(enriched, dim_map)
        pred = _resolve_expr(pred_fn, resolver)
        return enriched.filter(pred)

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
        from ._core import _get_merged_fields, _to_untagged

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
        from ._core import _to_untagged

        return _to_untagged(self.source)
