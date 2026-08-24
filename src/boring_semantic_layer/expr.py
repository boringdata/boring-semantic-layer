from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import ibis
from ibis.common.collections import FrozenDict
from ibis.common.deferred import Deferred
from ibis.expr import types as ir
from ibis.expr.types.groupby import GroupedTable as IbisGroupedTable
from ibis.expr.types.relations import Table as IbisTable
from returns.result import Success, safe

from ._xorq import (
    GroupedTable,
    Table,
)
from .errors import QueryError
from .measure_scope import MeasureScope
from .ops import (
    Dimension,
    Measure,
    NestAggSpec,
    SemanticAggregateOp,
    SemanticFilterOp,
    SemanticGroupByOp,
    SemanticIndexOp,
    SemanticJoinOp,
    SemanticLimitOp,
    SemanticOrderByOp,
    SemanticProjectOp,
    SemanticTableOp,
    SemanticUnnestOp,
    _classify_measure,
    _collect_struct,
    _exact_filter_fields,
    _extract_columns_from_callable,
    _extract_join_key_columns,
    _find_all_root_models,
    _get_merged_fields,
    _has_prior_aggregate,
    _is_deferred,
    _normalize_join_predicate,
    _normalize_to_name,
    _unwrap,
    make_bare_ref_lambda,
)

logger = logging.getLogger(__name__)

_JOIN_REMOVED_MESSAGE = (
    "The join() method has been removed. Use join_one(), join_many(), or join_cross() instead.\n\n"
    "For one-to-one relationships:\n"
    "  table.join_one(other, lambda l, r: l.id == r.id)\n\n"
    "For one-to-many relationships:\n"
    "  table.join_many(other, lambda l, r: l.id == r.id)\n\n"
    "For Cartesian product:\n"
    "  table.join_cross(other)"
)

_NON_LEFT_JOIN_MESSAGE = (
    "Semantic joins only support how='left'; got how={how!r}. "
    "Non-left joins can silently change which left-side rows contribute to measures. "
    "For inner-join semantics, use a left semantic join followed by an explicit "
    "filter on a non-nullable field from the right table. Use join_cross() for a "
    "Cartesian product."
)

_BLOCKED_IBIS_METHODS = [
    "alias",
    "anti_join",
    "any_inner_join",
    "any_left_join",
    "as_scalar",
    "asof_join",
    "bind",
    "cache",
    "cast",
    "count",
    "cross_join",
    "describe",
    "difference",
    "distinct",
    "drop",
    "drop_null",
    "dropna",
    "equals",
    "fill_null",
    "fillna",
    "get_backend",
    "head",
    "info",
    "inner_join",
    "intersect",
    "left_join",
    "nunique",
    "outer_join",
    "pivot_longer",
    "pivot_wider",
    "preview",
    "projection",
    "relocate",
    "rename",
    "right_join",
    "rowid",
    "sample",
    "semi_join",
    "to_array",
    "to_delta",
    "to_torch",
    "topk",
    "try_cast",
    "unbind",
    "union",
    "unpack",
    "value_counts",
    "view",
    "visualize",
    "window_by",
]


def to_untagged(expr):
    from .ops import _rebind_to_canonical_backend

    if isinstance(expr, SemanticTable):
        return _rebind_to_canonical_backend(expr.op().to_untagged())

    result = safe(lambda: expr.to_untagged())()
    if isinstance(result, Success):
        return _rebind_to_canonical_backend(result.unwrap())

    raise TypeError(f"Cannot convert {type(expr)} to Ibis expression")


def _ensure_executable(expr: SemanticTable) -> None:
    """Refuse to sink a definition-side semantic expression to output.

    A semantic model — and any pre-aggregation chain over it (filters,
    joins, order_by/limit) — is a definition, not a query. Sinking it to
    output would return the raw underlying table: every physical column at
    row grain, with computed dimensions missing, bypassing the semantic
    layer entirely. Output requires either an aggregation stage in the
    chain or a model explicitly materialized from a query result
    (``_materialized_result``); everything else raises ``QueryError``
    pointing at the intended spellings.
    """
    node = expr.op()
    while node is not None:
        if isinstance(node, SemanticAggregateOp | SemanticIndexOp):
            # An aggregate completes a query; an index IS a query result
            # (fieldName/fieldValue/weight summary rows).
            return
        if isinstance(node, SemanticGroupByOp):
            keys = ", ".join(repr(k) for k in node.keys)
            raise QueryError(
                f".group_by({keys}) has no aggregation yet — complete the query "
                "with .aggregate(...) (an empty .aggregate() returns the "
                "distinct grouped values)."
            )
        if isinstance(node, SemanticTableOp):
            if node._materialized_result:
                return
            break
        if isinstance(node, SemanticJoinOp):
            break
        node = getattr(node, "source", None)

    name = getattr(expr, "name", None)
    label = f"Semantic model {name!r}" if name else "This semantic expression"
    dims = sorted(expr.get_dimensions()) if hasattr(expr, "get_dimensions") else []
    meas = (
        sorted({*expr.get_measures(), *expr.get_calculated_measures()})
        if hasattr(expr, "get_measures")
        else []
    )
    dim_hint = repr(dims[0]) if dims else "..."
    meas_hint = repr(meas[0]) if meas else "..."
    raise QueryError(
        f"{label} is a definition, not a query — executing it would return the "
        "raw underlying table (every physical column at row grain), bypassing "
        "its dimensions and measures. Build a query first, e.g. "
        f".group_by({dim_hint}).aggregate({meas_hint}) or "
        ".query(dimensions=[...], measures=[...]), or call .to_untagged() to "
        "work with the raw ibis table explicitly."
        + (f" Declared dimensions: {dims}; measures: {meas}." if dims or meas else "")
    )


def _flatten_group_keys(keys: tuple) -> tuple:
    """Flatten list/tuple arguments so ``group_by(["a", "b"])`` works like ibis."""
    flat: list = []
    for k in keys:
        if isinstance(k, (list, tuple)):
            flat.extend(k)
        else:
            flat.append(k)
    return tuple(flat)


def _query_module():
    """Call-time accessor for the query layer (which sits above expr).

    The .query()/.compare_periods() convenience methods dispatch upward the
    same way .chart() does — resolved at call time so the expression layer
    holds no import-time dependency on the layers built on top of it.
    """
    import importlib

    return importlib.import_module("boring_semantic_layer.query")


def to_tagged(expr, aggregate_cache_storage=None):
    # Serialization sits above the expression layer; resolve at call time
    # (see _query_module).
    import importlib

    ser = importlib.import_module("boring_semantic_layer.serialization")
    return ser.to_tagged(expr, aggregate_cache_storage=aggregate_cache_storage)


class SemanticTable(ir.Table):
    @property
    def name(self) -> str | None:
        """The semantic model's name, or None where no single model applies.

        Defined on the base so every semantic expression answers `.name`
        (aggregates, limits, and other derived shapes return None instead
        of raising an AttributeError that blames ibis's Table).
        """
        return getattr(self.op(), "name", None)

    @property
    def description(self) -> str | None:
        """The semantic model's description, or None."""
        return getattr(self.op(), "description", None)

    def get_graph(self):
        """Get the dependency graph for this semantic table.

        Returns the dependency graph showing how dimensions and measures
        relate to each other. This works on all semantic table types
        (SemanticModel, joins, filters, group_by, etc.).

        For joins, this merges graphs from both left and right sides.
        For filters/limits/ordering, this returns the graph from the source.

        Returns:
            dict: Dependency graph mapping field names to metadata with "deps" and "type" keys.
                  Use graph utility functions for traversal:
                  - graph_predecessors(graph, field): direct dependencies
                  - graph_successors(graph, field): direct dependents
                  - graph_bfs(graph, field): breadth-first traversal
                  - graph_invert(graph): reverse dependencies
                  - graph_to_dict(graph): export to JSON format
        """
        op = self.op()

        # For SemanticModel, get cached graph from the op
        if hasattr(op, "get_graph"):
            return op.get_graph()

        # For joins, merge graphs from left and right ops with prefixing
        if hasattr(op, "left") and hasattr(op, "right"):
            merged = {}

            # Add left graph with prefixes (both field names and their dependencies)
            if hasattr(op.left, "get_graph") and hasattr(op.left, "name"):
                left_name = op.left.name
                for field_name, field_data in op.left.get_graph().items():
                    prefixed_name = f"{left_name}.{field_name}" if left_name else field_name
                    # Also prefix the dependencies
                    prefixed_deps = {
                        f"{left_name}.{dep_name}" if left_name else dep_name: dep_type
                        for dep_name, dep_type in field_data["deps"].items()
                    }
                    merged[prefixed_name] = {"deps": prefixed_deps, "type": field_data["type"]}

            # Add right graph with prefixes (both field names and their dependencies)
            if hasattr(op.right, "get_graph") and hasattr(op.right, "name"):
                right_name = op.right.name
                for field_name, field_data in op.right.get_graph().items():
                    prefixed_name = f"{right_name}.{field_name}" if right_name else field_name
                    # Also prefix the dependencies
                    prefixed_deps = {
                        f"{right_name}.{dep_name}" if right_name else dep_name: dep_type
                        for dep_name, dep_type in field_data["deps"].items()
                    }
                    merged[prefixed_name] = {"deps": prefixed_deps, "type": field_data["type"]}

            return merged

        # For pass-through nodes (filter, limit, order_by, group_by, aggregate), get graph from source
        # Walk the node tree to find any node with a graph attribute
        from .graph_utils import walk_nodes
        from .ops import SemanticTableOp

        for node in walk_nodes((SemanticTableOp,), self):
            if hasattr(node, "get_graph"):
                return node.get_graph()

        # Fallback to empty graph
        return {}

    def chart(
        self,
        spec: dict[str, Any] | None = None,
        backend: str = "echarts",
        format: str = "static",
    ):
        """Create a chart from this semantic result."""
        # The chart package is a presentation extra layered ABOVE the core
        # expression API; resolve it at call time so core never depends on
        # it at import time (mirrors pandas-style optional .plot accessors).
        import importlib

        create_chart = importlib.import_module("boring_semantic_layer.chart").chart
        return create_chart(self, spec=spec, backend=backend, format=format)

    def query(
        self,
        dimensions: Sequence[str] | None = None,
        measures: Sequence[str] | None = None,
        filters: Sequence[dict | str | Callable] | None = None,
        order_by: Sequence[tuple[str, str] | str] | None = None,
        limit: int | None = None,
        time_grain: str | None = None,
        time_grains: dict[str, str] | None = None,
        time_range: dict[str, str] | None = None,
        having: Sequence[dict] | None = None,
    ):
        """Run a declarative (JSON-style) query against this semantic table.

        Available on every semantic expression (models, joins, filters,
        grouped/aggregated results). See ``boring_semantic_layer.query.query``
        for parameter semantics and worked examples.
        """
        return _query_module().query(
            semantic_table=self,
            dimensions=dimensions,
            measures=measures,
            filters=filters,
            order_by=order_by,
            limit=limit,
            time_grain=time_grain,
            time_grains=time_grains,
            time_range=time_range,
            having=having,
        )

    def compare_periods(
        self,
        dimensions: Sequence[str] | None = None,
        measures: Sequence[str] | None = None,
        current_time_range: dict[str, str] | None = None,
        previous_time_range: dict[str, str] | None = None,
        filters: Sequence[dict | str | Callable] | None = None,
        time_dimension: str | None = None,
        time_grain: str | None = None,
        time_grains: dict[str, str] | None = None,
        order_by: Sequence[tuple[str, str] | str] | None = None,
        limit: int | None = None,
    ):
        """Compare measures across two time ranges (current/previous/delta)."""
        return _query_module().compare_periods(
            semantic_table=self,
            dimensions=dimensions,
            measures=measures,
            current_time_range=current_time_range,
            previous_time_range=previous_time_range,
            filters=filters,
            time_dimension=time_dimension,
            time_grain=time_grain,
            time_grains=time_grains,
            order_by=order_by,
            limit=limit,
        )

    def filter(self, predicate: Callable) -> SemanticFilter:
        return SemanticFilter(source=self.op(), predicate=predicate)

    def group_by(self, *keys: str | Deferred):
        normalized = tuple(_normalize_to_name(k) for k in _flatten_group_keys(keys))
        return SemanticGroupBy(source=self.op(), keys=normalized)

    def aggregate(self, *measure_names, nest: dict[str, Callable] | None = None, **aliased):
        """Aggregate measures without grouping (produces a single row result).

        This is a convenience method that delegates to group_by().aggregate().

        Args:
            *measure_names: Measure names to aggregate
            nest: Optional nested aggregations
            **aliased: Optional aliased aggregations

        Returns:
            SemanticAggregate with no grouping keys
        """
        return self.group_by().aggregate(*measure_names, nest=nest, **aliased)

    agg = aggregate

    def mutate(self, **post):
        """Add derived row-grain columns (ADR 0001: desugars to dimensions).

        The derivations are row-grain expressions — they register as
        dimensions on the model (usable as group-by keys and in downstream
        queries). Post-aggregation derivations are not part of the semantic
        layer: declare a calculated measure for cross-measure math, or drop
        to ibis with ``.to_untagged().mutate(...)`` for row math over a
        query result.
        """
        from .ops import (
            SemanticJoinOp,
            SemanticTableOp,
            _resolve_expr,
        )

        if _has_prior_aggregate(self.op()):
            raise QueryError(
                ".mutate() on a filtered/ordered/limited query result is not "
                "supported: the result is a plain table, not a semantic model. "
                "Call .mutate() on the aggregate itself (before "
                "filter/order_by/limit — it desugars to the measure path), "
                "declare a calculated measure on the model, or drop to ibis "
                "with .to_untagged().mutate(...)."
            )

        def contains_join(node) -> bool:
            if isinstance(node, SemanticJoinOp):
                return True
            if isinstance(node, SemanticTableOp):
                return getattr(node, "_source_join", None) is not None
            source = getattr(node, "source", None)
            return contains_join(source) if source is not None else False

        if contains_join(self.op()):
            # Register lazily so the join structure (and the pre-agg
            # fan-out machinery behind it) stays intact; the dimension
            # materializes per-table or via the dimension bridge at
            # aggregation time.
            with_dims = getattr(self, "with_dimensions", None)
            if with_dims is None:
                raise TypeError(
                    f".mutate() is not supported on {type(self).__name__}; "
                    "use with_dimensions()/with_measures() on the model instead."
                )
            return with_dims(**post)

        # Flat model: materialize the columns in chain order (so they are
        # visible on the untagged table) and register them as dimensions.
        tbl = self.op().to_untagged()
        for name, fn in post.items():
            resolved = _resolve_expr(fn, tbl)
            tbl = tbl.mutate(resolved.name(name))
        all_roots = _find_all_root_models(self.op())
        model = _build_semantic_model_from_roots(tbl, all_roots)
        return model.with_dimensions(**{n: make_bare_ref_lambda(n) for n in post})

    def order_by(self, *keys: str | ir.Value | Callable):
        return SemanticOrderBy(source=self.op(), keys=keys)

    def limit(self, n: int, offset: int = 0):
        return SemanticLimit(source=self.op(), n=n, offset=offset)

    def unnest(self, column: str) -> SemanticUnnest:
        return SemanticUnnest(source=self.op(), column=column)

    def select(self, *args, **kwargs):
        """Prevent select() on semantic tables.

        The semantic layer works with dimensions and measures, not arbitrary column selection.
        Use .to_untagged().select() if you need to perform Ibis operations.
        """
        raise NotImplementedError(
            "select() is not supported on semantic tables. "
            "Use group_by() and aggregate() to work with dimensions and measures, "
            "or call .to_untagged().select() to convert to an Ibis table first."
        )

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def __repr__(self) -> str:
        """Return the graph repr of the underlying operation."""
        return repr(self.op())

    def to_untagged(self):
        return self.op().to_untagged()

    def to_tagged(self, aggregate_cache_storage=None):
        return to_tagged(self, aggregate_cache_storage=aggregate_cache_storage)

    def execute(self, **kwargs):
        # Accept kwargs for ibis compatibility (params, limit, etc)
        from .ops import _rebind_to_canonical_backend

        _ensure_executable(self)
        return _rebind_to_canonical_backend(to_untagged(self)).execute(**kwargs)

    def compile(self, **kwargs):
        from .ops import _rebind_to_canonical_backend

        _ensure_executable(self)
        return _rebind_to_canonical_backend(to_untagged(self)).compile(**kwargs)

    def sql(self, **kwargs):
        from .ops import _rebind_to_canonical_backend

        _ensure_executable(self)
        return ibis.to_sql(_rebind_to_canonical_backend(to_untagged(self)), **kwargs)

    def to_pandas(self, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_pandas(**kwargs)

    def to_pyarrow(self, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_pyarrow(**kwargs)

    def to_pyarrow_batches(self, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_pyarrow_batches(**kwargs)

    def to_polars(self, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_polars(**kwargs)

    def to_csv(self, path, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_csv(path, **kwargs)

    def to_parquet(self, path, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_parquet(path, **kwargs)

    def to_parquet_dir(self, path, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_parquet_dir(path, **kwargs)

    def to_json(self, path, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_json(path, **kwargs)

    def to_xlsx(self, path, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_xlsx(path, **kwargs)

    def to_pandas_batches(self, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_pandas_batches(**kwargs)

    def to_sql(self, **kwargs):
        _ensure_executable(self)
        return self.to_untagged().to_sql(**kwargs)


def _make_blocked_method(name):
    def method(self, *args, **kwargs):
        raise AttributeError(
            f"'{type(self).__name__}' does not support '{name}()'. "
            f"Call .to_untagged().{name}() to use ibis operations directly."
        )

    method.__name__ = name
    method.__qualname__ = f"SemanticTable.{name}"
    return method


for _name in _BLOCKED_IBIS_METHODS:
    setattr(SemanticTable, _name, _make_blocked_method(_name))


def _create_dimension(expr: Dimension | Callable | dict) -> Dimension:
    if isinstance(expr, Dimension):
        return expr
    if isinstance(expr, dict):
        return Dimension(
            expr=expr["expr"],
            description=expr.get("description"),
            is_entity=expr.get("is_entity", False),
            is_event_timestamp=expr.get("is_event_timestamp", False),
            is_time_dimension=expr.get("is_time_dimension", False),
            smallest_time_grain=expr.get("smallest_time_grain"),
            derived_dimensions=tuple(expr.get("derived_dimensions") or ()),
            metadata=dict(expr.get("metadata") or {}),
        )
    return Dimension(expr=expr, description=None)


_SUPPORTED_DERIVED_TIME_DIMENSIONS = frozenset({"year", "month", "day"})


def _extract_derived_time_part(value: ir.Value, part: str) -> ir.Value:
    if part == "year":
        return value.year()
    if part == "month":
        return value.month()
    if part == "day":
        return value.day()
    raise ValueError(
        f"Unsupported derived dimension part '{part}'. "
        f"Supported values: {sorted(_SUPPORTED_DERIVED_TIME_DIMENSIONS)}",
    )


def _expand_derived_dimensions(
    dimensions: Mapping[str, Dimension | Callable | dict] | None,
) -> FrozenDict[str, Dimension]:
    base_dimensions = {
        dim_name: _create_dimension(dim) for dim_name, dim in (dimensions or {}).items()
    }
    expanded_dimensions = dict(base_dimensions)

    for dim_name, dim in base_dimensions.items():
        for part in dim.derived_dimensions:
            normalized_part = part.strip().lower()
            if normalized_part not in _SUPPORTED_DERIVED_TIME_DIMENSIONS:
                raise ValueError(
                    f"Invalid derived dimension '{part}' for '{dim_name}'. "
                    f"Supported values: {sorted(_SUPPORTED_DERIVED_TIME_DIMENSIONS)}",
                )

            derived_name = f"{dim_name}_{normalized_part}"
            if derived_name in expanded_dimensions:
                # Keep explicitly defined dimensions and avoid duplicate regeneration.
                continue

            base_expr = dim.expr

            def derived_expr(
                table: ir.Table,
                _base_expr=base_expr,
                _part=normalized_part,
            ) -> ir.Value:
                value = _base_expr.resolve(table) if _is_deferred(_base_expr) else _base_expr(table)
                return _extract_derived_time_part(value, _part)

            expanded_dimensions[derived_name] = Dimension(expr=derived_expr, description=None)

    return FrozenDict(expanded_dimensions)


def _derive_name(table: Any) -> str | None:
    expr = safe(lambda: table.to_expr())().value_or(table)
    return safe(lambda: expr.get_name())().value_or(None)


def _build_semantic_model_from_roots(
    ibis_table: ir.Table,
    all_roots: tuple,
    field_filter: set | None = None,
    materialized_result: bool = False,
) -> SemanticModel:
    # A model derived from a materialized query result stays a result model
    # (executable); a model derived from raw sources stays a definition.
    materialized = materialized_result or any(
        getattr(root, "_materialized_result", False) for root in all_roots
    )
    if not all_roots:
        return SemanticModel(
            table=ibis_table,
            dimensions={},
            measures={},
            calc_measures={},
            _materialized_result=materialized,
        )

    all_dims = _get_merged_fields(all_roots, "dimensions")
    all_measures = _get_merged_fields(all_roots, "measures")
    all_calc = _get_merged_fields(all_roots, "calc_measures")

    if field_filter is not None:
        all_dims = {k: v for k, v in all_dims.items() if k in field_filter}
        all_measures = {k: v for k, v in all_measures.items() if k in field_filter}
        all_calc = {k: v for k, v in all_calc.items() if k in field_filter}

    return SemanticModel(
        table=ibis_table,
        dimensions=all_dims,
        measures=all_measures,
        calc_measures=all_calc,
        _materialized_result=materialized,
    )


def _get_entity_dims(op) -> frozenset[str]:
    """Return the logical entity grain carried by an operation.

    Pass-through operations (most importantly filters) retain their source
    grain.  A ``join_one`` also retains the left grain, while a ``join_many``
    can add entity dimensions from its many side.  Deriving the grain from the
    operation tree avoids treating the prefixed copies exposed by a joined
    model as independent entities merely because metadata has been merged.
    """
    if isinstance(op, SemanticFilterOp):
        return _get_entity_dims(op.source)

    # Aggregation establishes a new result grain. SemanticAggregateOp does
    # not currently expose entity metadata for that output, so inheriting the
    # source entities would falsely claim (for example) that a month-level
    # result still has its source's day-level grain.
    if isinstance(op, SemanticAggregateOp):
        return frozenset()

    if isinstance(op, SemanticJoinOp):
        left_entities = _get_entity_dims(op.left)
        if op.cardinality == "one":
            return left_entities or _get_entity_dims(op.right)
        return left_entities | _get_entity_dims(op.right)

    if isinstance(op, SemanticTableOp):
        source_join = getattr(op, "_source_join", None)
        if source_join is not None:
            # A materialized join wrapper carries all inherited dimensions as
            # prefixed copies plus any dimensions declared directly on the
            # wrapper. Preserve only the unprefixed local entity declarations;
            # inherited prefixed copies are already represented by the source
            # join's logical grain.
            local_entities = {
                name
                for name, dim in op.get_dimensions().items()
                if "." not in name and getattr(dim, "is_entity", False)
            }
            return _get_entity_dims(source_join) | frozenset(local_entities)
        return frozenset(
            # Joined field maps use ``model.field`` names.  Leaf model maps do
            # not, so normalizing the prefix lets same-grain fact models still
            # compare by their declared semantic entity names.
            name.rsplit(".", 1)[-1]
            for name, dim in op.get_dimensions().items()
            if getattr(dim, "is_entity", False)
        )

    source = getattr(op, "source", None)
    return _get_entity_dims(source) if source is not None else frozenset()


def _has_measure_fields(op) -> bool:
    """Return whether an op defines base or calculated measures."""
    get_measures = getattr(op, "get_measures", None)
    get_calculated = getattr(op, "get_calculated_measures", None)
    return bool(get_measures and get_measures()) or bool(get_calculated and get_calculated())


def _get_entity_source_columns(op) -> frozenset[str]:
    """Return physical columns that define an operation's logical entity grain."""
    if isinstance(op, SemanticFilterOp):
        return _get_entity_source_columns(op.source)
    if isinstance(op, SemanticAggregateOp):
        return frozenset()
    if isinstance(op, SemanticJoinOp):
        left_columns = _get_entity_source_columns(op.left)
        if op.cardinality == "one":
            return left_columns or _get_entity_source_columns(op.right)
        return left_columns | _get_entity_source_columns(op.right)
    if isinstance(op, SemanticTableOp):
        source_join = getattr(op, "_source_join", None)
        if source_join is not None:
            inherited = set(_get_entity_source_columns(source_join))
            table = op.to_untagged()
            for name, dim in op.get_dimensions().items():
                if "." in name or not getattr(dim, "is_entity", False):
                    continue
                extraction = _extract_columns_from_callable(
                    lambda t, entity_dim=dim: entity_dim(t), table
                )
                if extraction.is_success() and extraction.columns:
                    inherited.update(extraction.columns)
                else:
                    inherited.add(name)
            return frozenset(inherited)
        table = op.to_untagged()
        columns: set[str] = set()
        for name, dim in op.get_dimensions().items():
            if not getattr(dim, "is_entity", False):
                continue
            extraction = _extract_columns_from_callable(
                lambda t, entity_dim=dim: entity_dim(t), table
            )
            if extraction.is_success() and extraction.columns:
                columns.update(extraction.columns)
            else:
                columns.add(name.rsplit(".", 1)[-1])
        return frozenset(columns)
    source = getattr(op, "source", None)
    return _get_entity_source_columns(source) if source is not None else frozenset()


def _detect_grain_cardinality(left_op, right_op, on=None) -> str:
    """Compare entity dimensions to detect grain mismatch.

    If both sides declare ``is_entity`` dimensions and the sets differ,
    returns ``"many"`` only for multi-fact joins where both sides define
    measures. Pure dimension lookups should stay ``join_one`` so they can
    use deferred dimension joins.
    """
    import warnings

    left_entities = _get_entity_dims(left_op)
    right_entities = _get_entity_dims(right_op)

    join_misses_entity_grain = False
    if on is not None and left_entities and left_entities == right_entities:
        try:
            normalized_on = _normalize_join_predicate(on)
            left_table = left_op.to_untagged()
            right_table = right_op.to_untagged()
            join_columns = _extract_join_key_columns(normalized_on, left_table, right_table)
            if join_columns.is_success():
                left_entity_columns = _get_entity_source_columns(left_op)
                right_entity_columns = _get_entity_source_columns(right_op)
                join_misses_entity_grain = not (
                    left_entity_columns <= join_columns.left_columns
                    and right_entity_columns <= join_columns.right_columns
                )
        except Exception as exc:
            # Inconclusive predicate analysis keeps the explicit join_one
            # contract; aggregation-time validation still fails closed where
            # source-aware preaggregation depends on predicate shape.
            logger.debug("Grain-cardinality predicate analysis inconclusive: %s", exc)
            join_misses_entity_grain = False

    if (
        left_entities
        and right_entities
        and (left_entities != right_entities or join_misses_entity_grain)
        and _has_measure_fields(left_op)
        and _has_measure_fields(right_op)
    ):
        left_name = getattr(left_op, "name", None) or "left"
        right_name = getattr(right_op, "name", None) or "right"
        warnings.warn(
            f"Grain mismatch detected: {left_name} entity dims {sorted(left_entities)} "
            f"and {right_name} entity dims {sorted(right_entities)} are not both "
            "covered by the join keys. "
            f"Upgrading join_one to join_many for automatic pre-aggregation.",
            stacklevel=2,
        )
        return "many"
    return "one"


def _join_one_with_detected_grain(
    left_op,
    other,
    on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
) -> SemanticJoin:
    """Construct ``join_one`` consistently for every semantic wrapper."""
    other_op = other.op() if isinstance(other, SemanticTable) else other
    cardinality = _detect_grain_cardinality(left_op, other_op, on)
    return SemanticJoin(
        left=left_op,
        right=other_op,
        on=on,
        how="left",
        cardinality=cardinality,
    )


def _find_join_provenance(op):
    """Find the join represented by an op or a materialized model wrapper."""
    if isinstance(op, SemanticJoinOp):
        return op
    if isinstance(op, SemanticTableOp):
        return getattr(op, "_source_join", None)
    source = getattr(op, "source", None)
    return _find_join_provenance(source) if source is not None else None


def _replace_metadata_preserving_filters(
    op,
    *,
    dimensions: Mapping[str, Dimension | Callable | dict],
    measures: Mapping[str, Measure | Callable],
    calc_measures: Mapping[str, Any],
) -> SemanticTable:
    """Replace metadata without collapsing filters into a flat table.

    Fanout-safe aggregation discovers both joins and the predicates between a
    join and an aggregate from the semantic operation tree.  Materializing a
    ``SemanticFilterOp`` into a new leaf model erases those predicates.  Peel
    the contiguous filter chain, update the underlying model metadata, and
    rebuild the same chain so the planner can still route each predicate to
    its owning source before pre-aggregation.
    """
    predicates: list[
        tuple[
            Callable,
            Mapping[str, Dimension],
            frozenset[str],
            bool,
            Callable | None,
        ]
    ] = []
    source = op
    while isinstance(source, SemanticFilterOp):
        predicate_source = source.source
        get_dimensions = getattr(predicate_source, "get_dimensions", None)
        predicate_dims = dict(get_dimensions()) if get_dimensions else {}
        predicate = _unwrap(source.predicate)
        try:
            deferred_resolution = bool(
                object.__getattribute__(predicate, "__bsl_deferred_resolution__")
            )
        except (AttributeError, TypeError):
            deferred_resolution = False
        try:
            serialization_predicate = object.__getattribute__(
                predicate, "__bsl_serialization_predicate__"
            )
        except (AttributeError, TypeError):
            serialization_predicate = None
        predicates.append(
            (
                predicate,
                predicate_dims,
                _exact_filter_fields(predicate),
                deferred_resolution,
                serialization_predicate,
            )
        )
        source = source.source

    source_join = _find_join_provenance(source)
    if isinstance(source, SemanticTableOp):
        table = source.table
        name = source.name
        description = source.description
    else:
        table = source.to_untagged()
        name = getattr(source, "name", None)
        description = getattr(source, "description", None)

    rebuilt: SemanticTable = SemanticModel(
        table=table,
        dimensions=dimensions,
        measures=measures,
        calc_measures=calc_measures,
        name=name,
        description=description,
        _source_join=source_join,
        _materialized_result=getattr(source, "_materialized_result", False),
    )
    for (
        predicate,
        predicate_dims,
        exact_filter_fields,
        deferred_resolution,
        prior_serialization_predicate,
    ) in reversed(predicates):
        # Bind each predicate to the dimension definitions visible when the
        # filter was authored. A later metadata overlay may intentionally
        # replace a same-named dimension (for example raw timestamp -> month
        # bucket); letting the earlier row filter see that replacement changes
        # filter-before-transform semantics and drops partially covered buckets.
        def bound_predicate(
            t,
            pred=predicate,
            dims=predicate_dims,
            fields=exact_filter_fields,
        ):
            from .convert import _Resolver

            # Filter lowering normally passes a resolver carrying the *new*
            # metadata. Unwrap it to the physical relation before applying the
            # dimensions captured above, otherwise an old identity dimension
            # still delegates through the new same-named month bucket.
            try:
                physical_table = object.__getattribute__(t, "_t")
            except (AttributeError, TypeError):
                physical_table = t
            scope_dims = dict(dims)
            try:
                runtime_dims = object.__getattribute__(t, "_dims")
            except (AttributeError, TypeError):
                runtime_dims = {}
            # Exact JSON fields that were undeclared when the filter was
            # authored are synthesized for the current execution scope.  A
            # joined-table resolver maps them to collision-safe physical
            # aliases, while a source-local resolver maps them to raw columns.
            # Only fill missing entries: declared dimensions captured at the
            # filter boundary must retain their original transformation.
            for field in fields:
                if field not in scope_dims and field in runtime_dims:
                    scope_dims[field] = runtime_dims[field]
            scope = _Resolver(physical_table, scope_dims)
            return pred.resolve(scope) if _is_deferred(pred) else pred(scope)

        def serialization_predicate(
            t,
            pred=predicate,
            dims=predicate_dims,
            fields=exact_filter_fields,
        ):
            """Inline the filter against its author-time dimension scope."""
            from .convert import _Resolver

            symbolic_physical = t._t

            class _SymbolicPhysicalTable:
                """Expose raw fields while reserving exact semantic lookups."""

                def __getattr__(self, name):
                    if name in fields:
                        raise AttributeError(name)
                    return getattr(symbolic_physical, name)

                def __getitem__(self, name):
                    return symbolic_physical[name]

                @property
                def columns(self):
                    return symbolic_physical.columns

            physical_scope = _SymbolicPhysicalTable()
            serialization_dims = {
                name: (
                    lambda _table, _dim=dimension, _physical=symbolic_physical: (
                        _dim.resolve(_physical) if _is_deferred(_dim) else _dim(_physical)
                    )
                )
                for name, dimension in dims.items()
            }
            for field in fields:
                if field not in serialization_dims:
                    # Keep undeclared exact fields in semantic resolver scope;
                    # their sidecar metadata synthesizes the appropriate
                    # joined/raw mapping after reconstruction.
                    serialization_dims[field] = lambda _table, _field=field, _scope=t: _scope[
                        _field
                    ]
            scope = _Resolver(physical_scope, serialization_dims)
            return pred.resolve(scope) if _is_deferred(pred) else pred(scope)

        # JSON filters need their exact AST field spellings after this
        # metadata-preserving rebind.  Without them, a valid undeclared
        # qualified raw field (for example ``items.status``) cannot be
        # synthesized in the joined namespace and the filter becomes
        # unresolvable.  Preserve the separate deferred marker for ordinary
        # string filters as well; it controls dimension-table shortcuts.
        if exact_filter_fields:
            bound_predicate.__bsl_filter_fields__ = exact_filter_fields
        if deferred_resolution:
            bound_predicate.__bsl_deferred_resolution__ = True
        # The generic serializer cannot safely infer the semantics of this
        # closure from its captured resolver state.  Give it an explicit
        # symbolic source that inlines the dimensions visible when the filter
        # was authored.  This retains filter-before-overlay behavior without
        # serializing Dimension objects themselves.
        bound_predicate.__bsl_serialization_predicate__ = (
            prior_serialization_predicate or serialization_predicate
        )

        rebuilt = SemanticFilter(source=rebuilt.op(), predicate=bound_predicate)
    return rebuilt


class SemanticModel(SemanticTable):
    def __init__(
        self,
        table: Any,
        dimensions: Mapping[str, Dimension | Callable | dict] | None = None,
        measures: Mapping[str, Measure | Callable] | None = None,
        calc_measures: Mapping[str, Any] | None = None,
        name: str | None = None,
        description: str | None = None,
        _source_join: Any | None = None,
        _materialized_result: bool = False,
    ) -> None:
        # Convert ibis → xorq once at the boundary; internal code paths can
        # then assume xorq-vendored tables when the backend is supported.
        # Falls back to the plain ibis table on backends xorq can't wrap.
        from .ops import _ensure_xorq_table

        table = _ensure_xorq_table(table)

        dims = _expand_derived_dimensions(dimensions)

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

        op = SemanticTableOp(
            table=table,
            dimensions=dims,
            measures=meas,
            calc_measures=calc_meas,
            name=derived_name,
            description=description,
            _source_join=_source_join,
            _materialized_result=_materialized_result,
        )

        super().__init__(op)

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
    def description(self):
        return self.op().description

    @property
    def dimensions(self):
        return self.op().dimensions

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    @property
    def _dims(self):
        return self.op()._dims

    @property
    def _base_measures(self):
        return self.op()._base_measures

    @property
    def _calc_measures(self):
        return self.op()._calc_measures

    @property
    def table(self):
        return self.op().table

    def with_dimensions(self, **dims) -> SemanticModel:
        return SemanticModel(
            table=self.op().table,
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            name=self.name,
            description=self.description,
            _source_join=self.op()._source_join,
            _materialized_result=self.op()._materialized_result,
        )

    def with_measures(self, **meas) -> SemanticModel:
        new_base_meas = dict(self.get_measures())
        new_calc_meas = dict(self.get_calculated_measures())

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        base_tbl = self.op().table
        scope = MeasureScope(_tbl=base_tbl, _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope, name)
            _store_classified_measure(name, kind, value, new_base_meas, new_calc_meas)

        return SemanticModel(
            table=self.op().table,
            dimensions=self.get_dimensions(),
            measures=new_base_meas,
            calc_measures=new_calc_meas,
            name=self.name,
            description=self.description,
            _source_join=self.op()._source_join,
            _materialized_result=self.op()._materialized_result,
        )

    def join_one(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-one relationship semantics.

        When both models have ``is_entity`` dimensions declared and their entity
        sets differ, the join is automatically upgraded to ``join_many`` semantics
        so that BSL's pre-aggregation logic can align the grains before joining.
        This prevents fan-out / double-counting in multi-fact star schemas.

        Args:
            other: The semantic model to join with
            on: Join predicate. Accepts a lambda ``(left, right) -> bool``, a column
                name string, a Deferred ``_.col``, or a list of strings/Deferred for
                compound equi-joins.
        Returns:
            SemanticJoin: The joined semantic model

        Examples:
            >>> orders.join_one(customers, on="customer_id")
            >>> orders.join_one(customers, on=_.customer_id)
            >>> orders.join_one(customers, on=lambda o, c: o.customer_id == c.customer_id)
        """
        return _join_one_with_detected_grain(self.op(), other, on)

    def join_many(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-many relationship semantics.

        Args:
            other: The semantic model to join with
            on: Join predicate. Accepts a lambda ``(left, right) -> bool``, a column
                name string, a Deferred ``_.col``, or a list of strings/Deferred for
                compound equi-joins.
        Returns:
            SemanticJoin: The joined semantic model

        Examples:
            >>> customer.join_many(orders, on="customer_id")
            >>> customer.join_many(orders, on=_.customer_id)
            >>> customer.join_many(orders, on=lambda c, o: c.customer_id == o.customer_id)
        """
        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoin(left=self.op(), right=other_op, on=on, how="left", cardinality="many")

    def join_cross(self, other: SemanticModel) -> SemanticJoin:
        """Cross join (Cartesian product) with another semantic model.

        Args:
            other: The semantic model to cross join with

        Returns:
            SemanticJoin: The joined semantic model

        Examples:
            >>> table_a.join_cross(table_b)  # Cartesian product of all rows
        """
        other_op = other.op() if isinstance(other, SemanticModel) else other
        return SemanticJoin(
            left=self.op(), right=other_op, on=None, how="cross", cardinality="cross"
        )

    def join(self, *args, **kwargs):
        """Deprecated: Use join_one() or join_many() instead.

        The generic join() method has been removed. Please use:
        - join_one(other, lambda l, r: condition) for one-to-one relationships
        - join_many(other, lambda l, r: condition) for one-to-many relationships
        - join_cross(other) for Cartesian product

        Examples:
            Old: table.join(other, lambda l, r: l.id == r.id, how="left")
            New: table.join_many(other, lambda l, r: l.id == r.id)

            Old: table.join(other, lambda l, r: l.id == r.id)
            New: table.join_one(other, lambda l, r: l.id == r.id)
        """
        raise TypeError(_JOIN_REMOVED_MESSAGE)

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ):
        processed_selector = selector
        if selector is not None and "ibis.selectors" in str(type(selector).__module__):
            if type(selector).__name__ == "AllColumns":
                processed_selector = None
            elif type(selector).__name__ == "Cols":
                processed_selector = sorted(selector.names)
            else:
                processed_selector = selector

        return SemanticIndexOp(
            source=self.op(),
            selector=processed_selector,
            by=by,
            sample=sample,
        )

    def to_untagged(self):
        return self.op().to_untagged()

    def as_expr(self):
        return self

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


class SemanticJoin(SemanticTable):
    def __init__(
        self,
        left: SemanticTableOp,
        right: SemanticTableOp,
        on: Callable[[Any, Any], ir.BooleanValue]
        | str
        | Deferred
        | Sequence[str | Deferred]
        | None = None,
        how: str = "left",
        cardinality: str = "one",
    ) -> None:
        is_cross_join = how == "cross" and cardinality == "cross"
        if how != "left" and not is_cross_join:
            raise ValueError(_NON_LEFT_JOIN_MESSAGE.format(how=how))
        on = _normalize_join_predicate(on)
        op = SemanticJoinOp(left=left, right=right, on=on, how=how, cardinality=cardinality)
        super().__init__(op)

    @property
    def left(self):
        return self.op().left

    @property
    def right(self):
        return self.op().right

    @property
    def on(self):
        return self.op().on

    @property
    def how(self):
        return self.op().how

    @property
    def values(self):
        return self.op().values

    @property
    def schema(self):
        return self.op().schema

    @property
    def name(self):
        return getattr(self.op(), "name", None)

    @property
    def description(self):
        return self.op().description

    @property
    def table(self):
        return self.op().to_untagged()

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ):
        processed_selector = selector
        if selector is not None and "ibis.selectors" in str(type(selector).__module__):
            if type(selector).__name__ == "AllColumns":
                processed_selector = None
            elif type(selector).__name__ == "Cols":
                processed_selector = sorted(selector.names)
            else:
                processed_selector = selector

        return SemanticIndexOp(
            source=self.op(),
            selector=processed_selector,
            by=by,
            sample=sample,
        )

    def to_untagged(self):
        return self.op().to_untagged()

    def as_expr(self):
        return self

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

    @property
    def dimensions(self):
        return self.op().dimensions

    @property
    def measures(self):
        return self.op().measures

    @property
    def _dims(self):
        return self.op()._dims

    @property
    def _base_measures(self):
        return self.op()._base_measures

    @property
    def _calc_measures(self):
        return self.op()._calc_measures

    @property
    def calc_measures(self):
        return self.op().calc_measures

    @property
    def json_definition(self):
        return self.op().json_definition

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.op())
        return _build_semantic_model_from_roots(
            self.op().to_untagged(),
            all_roots,
            materialized_result=_has_prior_aggregate(self.op()),
        )

    def with_dimensions(self, **dims) -> SemanticModel:
        """Add or update dimensions."""
        return SemanticModel(
            table=self.op().to_untagged(),
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            _source_join=self.op(),  # Pass join reference for projection pushdown
        )

    def with_measures(self, **meas) -> SemanticModel:
        from .measure_scope import MeasureScope
        from .ops import _classify_measure

        joined_tbl = self.op().to_untagged()
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
            kind, value = _classify_measure(fn_or_expr, scope, name)
            _store_classified_measure(name, kind, value, new_base, new_calc)

        return SemanticModel(
            table=joined_tbl,
            dimensions=self.get_dimensions(),
            measures=new_base,
            calc_measures=new_calc,
            _source_join=self.op(),  # Pass join reference for projection pushdown
        )

    def join_one(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-one relationship semantics."""
        return _join_one_with_detected_grain(self.op(), other, on)

    def join_many(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-many relationship semantics."""
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=on,
            how="left",
            cardinality="many",
        )

    def join_cross(self, other: SemanticModel) -> SemanticJoin:
        """Cross join (Cartesian product) with another semantic model."""
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=None,
            how="cross",
            cardinality="cross",
        )

    def join(self, *args, **kwargs):
        """Deprecated: Use join_one(), join_many(), or join_cross() instead."""
        raise TypeError(_JOIN_REMOVED_MESSAGE)

    def group_by(self, *keys: str | Deferred):
        normalized = tuple(_normalize_to_name(k) for k in _flatten_group_keys(keys))
        return SemanticGroupBy(source=self.op(), keys=normalized)

    def filter(self, predicate: Callable):
        return SemanticFilter(source=self.op(), predicate=predicate)


class SemanticFilter(SemanticTable):
    def __init__(self, source: SemanticTableOp, predicate: Callable) -> None:
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

    def _metadata_source(self):
        source = self.op().source
        while isinstance(source, SemanticFilterOp):
            source = source.source
        return source

    @property
    def name(self):
        return getattr(self._metadata_source(), "name", None)

    @property
    def description(self):
        return getattr(self._metadata_source(), "description", None)

    @property
    def table(self):
        return self.op().to_untagged()

    @property
    def json_definition(self):
        return getattr(self._metadata_source(), "json_definition", {})

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    @property
    def dimensions(self):
        """Return dimension names as a tuple."""
        return tuple(self.get_dimensions().keys())

    @property
    def measures(self):
        """Return measure names as a tuple."""
        return tuple(self.get_measures().keys()) + tuple(self.get_calculated_measures().keys())

    @property
    def calc_measures(self):
        return dict(self.get_calculated_measures())

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.op().source)
        return _build_semantic_model_from_roots(
            self.op().to_untagged(),
            all_roots,
            materialized_result=_has_prior_aggregate(self.op()),
        )

    def with_dimensions(self, **dims) -> SemanticTable:
        """Add or update dimensions while retaining filter/join lineage."""
        all_roots = _find_all_root_models(self.op().source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        return _replace_metadata_preserving_filters(
            self.op(),
            dimensions={**existing_dims, **dims},
            measures=existing_meas,
            calc_measures=existing_calc,
        )

    def with_measures(self, **meas) -> SemanticTable:
        """Add or update measures while retaining filter/join lineage."""
        all_roots = _find_all_root_models(self.op().source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        new_base_meas = dict(existing_meas)
        new_calc_meas = dict(existing_calc)

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        scope = MeasureScope(_tbl=self.op().to_untagged(), _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope, name)
            _store_classified_measure(name, kind, value, new_base_meas, new_calc_meas)

        return _replace_metadata_preserving_filters(
            self.op(),
            dimensions=existing_dims,
            measures=new_base_meas,
            calc_measures=new_calc_meas,
        )

    def join_one(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-one relationship semantics."""
        return _join_one_with_detected_grain(self.op(), other, on)

    def join_many(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-many relationship semantics."""
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=on,
            how="left",
            cardinality="many",
        )

    def join_cross(self, other: SemanticModel) -> SemanticJoin:
        """Cross join (Cartesian product) with another semantic model."""
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=None,
            how="cross",
            cardinality="cross",
        )

    def join(self, *args, **kwargs):
        """Deprecated: Use join_one(), join_many(), or join_cross() instead."""
        raise TypeError(_JOIN_REMOVED_MESSAGE)


def _make_row_struct_collector(columns: tuple[str, ...]) -> Callable:
    """Per-row struct collection for the bare ``group_by`` nest form.

    Collects one struct per source row within each outer group, duplicates
    included — the historical ``nest={"x": lambda t: t.group_by([...])}``
    semantics that re-grouping via nested access relies on.
    """

    def collect_rows(tbl):
        return _collect_struct({col: tbl[col] for col in columns})

    return collect_rows


def _split_nest_pipeline(name: str, probe_op):
    """Separate an inner aggregate from pipeline steps chained after it.

    ``order_by``/``limit``/``filter`` applied after the inner ``aggregate``
    are per-group modifiers: ordering and truncation of the collected
    array, and HAVING predicates evaluated at the inner grain.
    """
    order_keys: tuple[Any, ...] = ()
    limit_spec: tuple[int, int] | None = None
    having: list[Any] = []
    current = probe_op
    while not isinstance(current, SemanticAggregateOp):
        if isinstance(current, SemanticLimitOp):
            if limit_spec is not None:
                raise NotImplementedError(
                    f"nest entry {name!r}: multiple limit() steps are not supported."
                )
            limit_spec = (current.n, current.offset)
        elif isinstance(current, SemanticOrderByOp):
            if order_keys:
                raise NotImplementedError(
                    f"nest entry {name!r}: multiple order_by() steps are not supported."
                )
            order_keys = tuple(current.keys)
        elif isinstance(current, SemanticFilterOp):
            having.append(current.predicate)
        else:
            raise NotImplementedError(
                f"nest entry {name!r}: unsupported nested query shape "
                f"{type(current).__name__}. Supported forms are t.group_by(...) "
                "and t.group_by(...).aggregate(...) optionally followed by "
                "filter/order_by/limit.",
            )
        current = current.source
    return current, order_keys, limit_spec, tuple(having)


def _regrain_nested_specs(aggs: dict, new_outer_keys: tuple[str, ...]) -> dict:
    """Widen child ``nest=`` plans to a re-grained parent's keys.

    A nest entry's inner aggregate is re-grouped at (outer + inner) keys;
    any ``nest=`` entries *it* carries were compiled against the lambda's
    original grain and must widen to the new keys too, or the group-by
    that joins them back onto their parent has no outer key columns.
    """
    out = {}
    for agg_name, agg_fn in aggs.items():
        spec = _unwrap(agg_fn)
        if not isinstance(spec, NestAggSpec):
            out[agg_name] = agg_fn
            continue
        inner = spec.inner_op
        widened = tuple(new_outer_keys) + tuple(k for k in inner.keys if k not in new_outer_keys)
        if widened == tuple(inner.keys):
            out[agg_name] = agg_fn
            continue
        inner_source = inner.source
        if isinstance(inner_source, SemanticGroupByOp):
            inner_source = inner_source.source
        out[agg_name] = NestAggSpec(
            inner_op=SemanticAggregateOp(
                source=SemanticGroupByOp(source=inner_source, keys=widened),
                keys=widened,
                aggs=_regrain_nested_specs(dict(inner.aggs), widened),
                nested_columns=inner.nested_columns,
            ),
            struct_fields=spec.struct_fields,
            order_keys=spec.order_keys,
            limit_spec=spec.limit_spec,
            having=spec.having,
        )
    return out


def _build_nest_agg(name: str, fn: Callable, source_op, outer_keys: tuple[str, ...]):
    """Classify a ``nest=`` lambda against the semantic source.

    The lambda receives the aggregation's semantic source table, so measure
    and dimension names resolve exactly like a top-level query:

    - ``t.group_by(...).aggregate(...)`` compiles as its own semantic
      aggregation at (outer keys + inner keys) grain and is attached to the
      outer aggregate as an array-of-structs column (:class:`NestAggSpec`).
      ``filter``/``order_by``/``limit`` chained after the aggregate become
      HAVING predicates, array ordering, and array truncation per outer
      group.
    - Bare ``t.group_by(...)`` keeps the historical per-row struct
      collection over the raw source rows.

    Anything else raises ``NotImplementedError`` — silently collecting raw
    rows in place of the requested query is never acceptable.
    """
    probe = fn(SemanticTable(source_op))

    if isinstance(probe, SemanticAggregate) or (
        isinstance(probe, SemanticFilter | SemanticOrderBy | SemanticLimit)
        and not isinstance(probe, SemanticGroupBy)
    ):
        inner_op, order_keys, limit_spec, having = _split_nest_pipeline(name, probe.op())
        inner_keys = tuple(inner_op.keys)
        combined = tuple(outer_keys) + tuple(k for k in inner_keys if k not in outer_keys)
        inner_source = inner_op.source
        if isinstance(inner_source, SemanticGroupByOp):
            # Re-group the inner query at (outer + inner) grain over its own
            # source chain, keeping any filters the lambda applied.
            inner_source = inner_source.source
        combined_op = SemanticAggregateOp(
            source=SemanticGroupByOp(source=inner_source, keys=combined),
            keys=combined,
            aggs=_regrain_nested_specs(dict(inner_op.aggs), combined),
            nested_columns=inner_op.nested_columns,
        )
        struct_fields = inner_keys + tuple(n for n in inner_op.aggs if n not in inner_keys)
        return NestAggSpec(
            inner_op=combined_op,
            struct_fields=struct_fields,
            order_keys=order_keys,
            limit_spec=limit_spec,
            having=having,
        )

    if isinstance(probe, SemanticGroupBy):
        if probe.op().source != source_op:
            raise NotImplementedError(
                f"nest entry {name!r}: transformations before a bare group_by "
                "(e.g. filter without a following .aggregate(...)) are not "
                "supported inside nest=. Add .aggregate(...) to the nested query.",
            )
        return _make_row_struct_collector(tuple(probe.op().keys))

    if isinstance(probe, SemanticTable):
        raise NotImplementedError(
            f"nest entry {name!r}: unsupported nested query shape "
            f"{type(probe).__name__}. Supported forms are t.group_by(...) and "
            "t.group_by(...).aggregate(...), optionally followed by "
            "filter/order_by/limit.",
        )

    if isinstance(probe, GroupedTable | IbisGroupedTable | Table | IbisTable):
        raise NotImplementedError(
            f"nest entry {name!r}: the lambda returned a raw ibis expression "
            f"({type(probe).__module__}.{type(probe).__name__}). Build the nested "
            "query from the semantic table argument instead, e.g. "
            'nest={"x": lambda t: t.group_by("dim").aggregate("measure")}.',
        )

    raise NotImplementedError(
        f"nest entry {name!r}: nest lambdas must return t.group_by(...) or "
        f"t.group_by(...).aggregate(...), got "
        f"{type(probe).__module__}.{type(probe).__name__}.",
    )


class SemanticGroupBy(SemanticTable):
    def __init__(self, source: SemanticTableOp, keys: tuple[str, ...]) -> None:
        op = SemanticGroupByOp(source=source, keys=keys)
        super().__init__(op)

    def filter(self, predicate: Callable) -> SemanticGroupBy:
        """Filter rows before aggregation, keeping the grouping keys.

        A filter between group_by and aggregate is pre-aggregation row
        filtering, so it commutes with the grouping. The inherited filter
        wrapped the group-by op itself, and aggregate() — which only
        recovers keys from its direct source — silently dropped the
        requested grouping.
        """
        filtered = SemanticFilter(source=self.op().source, predicate=predicate)
        return SemanticGroupBy(source=filtered.op(), keys=self.op().keys)

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
        *measure_names: str | Callable | Deferred,
        nest: dict[str, Callable] | None = None,
        **aliased,
    ):
        aggs = {}
        for item in measure_names:
            if _is_deferred(item):
                try:
                    name = _normalize_to_name(item)
                    aggs[name] = make_bare_ref_lambda(name)
                except TypeError:
                    # Complex Deferred (e.g. _.distance.sum()) — treat as callable
                    aggs[f"_measure_{id(item)}"] = item
            elif isinstance(item, str):
                aggs[item] = make_bare_ref_lambda(item)
            elif callable(item):
                aggs[f"_measure_{id(item)}"] = item
            else:
                raise TypeError(
                    f"measure_names must be strings, callables, or Deferred expressions, "
                    f"got {type(item)}",
                )

        aggs.update(aliased)

        if nest:
            source_op = self.op().source
            nest_aggs = {
                name: _build_nest_agg(name, fn, source_op, self.keys) for name, fn in nest.items()
            }
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

    agg = aggregate


class SemanticAggregate(SemanticTable):
    def __init__(
        self,
        source: SemanticTableOp,
        keys: tuple[str, ...],
        aggs: dict[str, Any],
        nested_columns: list[str] | None = None,
    ) -> None:
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
    def dimensions(self):
        """After aggregation, dimensions are materialized - return empty tuple."""
        return ()

    @property
    def measures(self):
        return self.op().measures

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    @property
    def nested_columns(self):
        return self.op().nested_columns

    def mutate(self, **post) -> SemanticAggregate:
        """Add post-aggregation derived columns (ADR 0001 desugaring).

        Folds the derivations into this aggregation's measure set: each
        lambda is classified by the calc analyzer (measure references,
        windows, and ``t.all(...)`` become calc measures; expressions
        over group keys are applied to the aggregated result). The chain
        stays a single ``SemanticAggregateOp`` — no separate operator.
        """
        op = self.op()
        return SemanticAggregate(
            source=op.source,
            keys=op.keys,
            aggs={**dict(op.aggs), **post},
            nested_columns=op.nested_columns,
        )

    def join_one(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-one relationship semantics."""
        return _join_one_with_detected_grain(self.op(), other, on)

    def join_many(
        self,
        other: SemanticModel,
        on: Callable[[Any, Any], ir.BooleanValue] | str | Deferred | Sequence[str | Deferred],
    ) -> SemanticJoin:
        """Join with one-to-many relationship semantics."""
        return SemanticJoin(
            left=self.op(),
            right=other.op(),
            on=on,
            how="left",
            cardinality="many",
        )

    def join_cross(self, other: SemanticModel) -> SemanticJoin:
        """Cross join (Cartesian product) with another semantic model."""
        return SemanticJoin(
            left=self.op(),
            right=other.op() if isinstance(other, SemanticModel) else other,
            on=None,
            how="cross",
            cardinality="cross",
        )

    def join(self, *args, **kwargs):
        """Deprecated: Use join_one(), join_many(), or join_cross() instead."""
        raise TypeError(_JOIN_REMOVED_MESSAGE)

    def as_table(self) -> SemanticModel:
        return SemanticModel(
            table=self.op().to_untagged(),
            dimensions={},
            measures={},
            calc_measures={},
            _materialized_result=True,
        )


class SemanticOrderBy(SemanticTable):
    def __init__(
        self, source: SemanticTableOp, keys: tuple[str | ir.Value | Callable, ...]
    ) -> None:
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

    @property
    def dimensions(self):
        return tuple(self.op().get_dimensions().keys())

    @property
    def measures(self):
        return tuple(self.op().get_measures().keys()) + tuple(
            self.op().get_calculated_measures().keys()
        )

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(
            self.op().to_untagged(),
            all_roots,
            materialized_result=_has_prior_aggregate(self.op()),
        )


class SemanticLimit(SemanticTable):
    def __init__(self, source: SemanticTableOp, n: int, offset: int = 0) -> None:
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

    @property
    def dimensions(self):
        return tuple(self.op().get_dimensions().keys())

    @property
    def measures(self):
        return tuple(self.op().get_measures().keys()) + tuple(
            self.op().get_calculated_measures().keys()
        )

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(
            self.op().to_untagged(),
            all_roots,
            materialized_result=_has_prior_aggregate(self.op()),
        )


class SemanticUnnest(SemanticTable):
    def __init__(self, source: SemanticTableOp, column: str) -> None:
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

    @property
    def dimensions(self):
        return tuple(self.op().get_dimensions().keys())

    @property
    def measures(self):
        return tuple(self.op().get_measures().keys()) + tuple(
            self.op().get_calculated_measures().keys()
        )

    def get_dimensions(self):
        return self.op().get_dimensions()

    def get_measures(self):
        return self.op().get_measures()

    def get_calculated_measures(self):
        return self.op().get_calculated_measures()

    def as_table(self) -> SemanticModel:
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(
            self.op().to_untagged(),
            all_roots,
            materialized_result=_has_prior_aggregate(self.op()),
        )

    def with_dimensions(self, **dims) -> SemanticModel:
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
        all_roots = _find_all_root_models(self.source)
        existing_dims = _get_merged_fields(all_roots, "dimensions") if all_roots else {}
        existing_meas = _get_merged_fields(all_roots, "measures") if all_roots else {}
        existing_calc = _get_merged_fields(all_roots, "calc_measures") if all_roots else {}

        new_base_meas = dict(existing_meas)
        new_calc_meas = dict(existing_calc)

        all_measure_names = (
            tuple(new_base_meas.keys()) + tuple(new_calc_meas.keys()) + tuple(meas.keys())
        )
        scope = MeasureScope(_tbl=self, _known=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope, name)
            _store_classified_measure(name, kind, value, new_base_meas, new_calc_meas)

        return SemanticModel(
            table=self,
            dimensions=existing_dims,
            measures=new_base_meas,
            calc_measures=new_calc_meas,
        )


def _store_classified_measure(name, kind, value, base_meas, calc_meas):
    """Store a classified measure, evicting a same-named entry of the other
    kind. Measure lookup is base-first, so a base measure left behind when a
    redefinition lands in the calc map would silently keep serving the old
    definition."""
    if kind == "calc":
        if name in getattr(value, "depends_on", ()):
            raise ValueError(
                f"Measure {name!r} cannot be defined in terms of itself. "
                "Reference other measures or columns, or use a new name."
            )
        base_meas.pop(name, None)
        calc_meas[name] = value
    else:
        calc_meas.pop(name, None)
        base_meas[name] = value


class SemanticProject(SemanticTable):
    def __init__(self, source: SemanticTableOp, fields: tuple[str, ...]) -> None:
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
        all_roots = _find_all_root_models(self.source)
        return _build_semantic_model_from_roots(
            self.op().to_untagged(),
            all_roots,
            field_filter=set(self.fields),
            materialized_result=_has_prior_aggregate(self.op()),
        )
