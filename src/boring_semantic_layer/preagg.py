"""Staged source-aware pre-aggregation planning.

The public semantic operation lives in :mod:`boring_semantic_layer.ops`, but
the fan-out-safe planner is deliberately kept here as a pipeline of small,
named stages.  Each stage accepts and returns attrs records so its inputs and
outputs are explicit and independently inspectable.

``ops`` imports this module lazily from ``SemanticAggregateOp``.  The lazy
boundary lets the stage implementation reuse the low-level expression helpers
that still live in ``ops`` without creating an import-time cycle.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, ClassVar

from attrs import Factory, define, field, frozen

# This module is imported lazily by ops after ops has finished loading.
from . import ops as _ops
from ._xorq import FrozenDict
from .measure_scope import MeasureScope


def _as_tuple(value):
    return tuple(value or ())


def _as_frozen_dict(value):
    return MappingProxyType(dict(value or {}))


def _as_nested_frozen_dict(value):
    return MappingProxyType(
        {key: MappingProxyType(dict(nested)) for key, nested in (value or {}).items()}
    )


@frozen(slots=True)
class PreAggregationRequest:
    """Stable inputs supplied by ``SemanticAggregateOp``."""

    aggregate_op: Any
    all_roots: tuple[Any, ...] = field(converter=tuple)
    join_op: Any
    join_tree_info: Any
    filters: tuple[Any, ...] = field(factory=tuple, converter=_as_tuple)


@frozen(slots=True)
class PreAggregationMetadata:
    """Semantic metadata shared by every later planning stage."""

    request: PreAggregationRequest
    root_names: frozenset[str] = field(converter=frozenset)
    filter_fns: tuple[Callable, ...] = field(converter=tuple)
    exact_filter_fields: frozenset[str] = field(converter=frozenset)
    dimensions: Mapping[str, Any] = field(converter=_as_frozen_dict)
    base_measures: Mapping[str, Any] = field(converter=_as_frozen_dict)
    calc_measures: Mapping[str, Any] = field(converter=_as_frozen_dict)
    group_by_cols: tuple[str, ...] = field(converter=tuple)
    join_column_lineage: Mapping[str, Mapping[str, str]] = field(converter=_as_nested_frozen_dict)
    wrapper_local_dimensions: Mapping[str, Any] = field(converter=_as_frozen_dict)


@frozen(slots=True)
class PreparedJoin:
    """Flattened join and dimension materialization, when available."""

    metadata: PreAggregationMetadata
    joined_base_table: Any | None
    table: Any | None
    wrapper_dimension_owners: Mapping[str, str | None] = field(
        factory=FrozenDict, converter=_as_frozen_dict
    )


@frozen(slots=True)
class FilterPlan:
    """Resolved filter ownership and row-precise conjunction legs."""

    prepared_join: PreparedJoin
    table: Any | None
    raw_tables: Mapping[str, Any] = field(converter=_as_frozen_dict)
    owners: tuple[frozenset[str], ...] = field(converter=tuple)
    legs: Mapping[int, tuple[tuple[Any, frozenset[str]], ...]] = field(converter=_as_frozen_dict)


@frozen(slots=True)
class PartitionedAggregation:
    """Logical aggregate plan partitioned by owning semantic source."""

    filter_plan: FilterPlan
    plan: Any
    measures_by_source: Mapping[str | None, Mapping[str, Callable]] = field(
        converter=_as_frozen_dict
    )


@frozen(slots=True)
class MetadataStage:
    """Resolve semantic fields and validate predicate-sensitive joins."""

    request: PreAggregationRequest

    def run(self) -> PreAggregationMetadata:
        aggregate_op = self.request.aggregate_op
        join_op = self.request.join_op
        join_tree_info = self.request.join_tree_info
        all_roots = self.request.all_roots

        root_names = {
            name
            for name, cardinality in join_tree_info.table_cardinalities.items()
            if cardinality == "root"
        }
        predicate_sensitive = (
            bool(self.request.filters)
            or any(
                "." in name and name.split(".", 1)[0] not in root_names
                for name in (*aggregate_op.keys, *aggregate_op.aggs.keys())
            )
            or any("." not in name for name in aggregate_op.aggs)
        )
        if predicate_sensitive:
            _ops._validate_preaggregation_join_predicates(join_op)

        filter_fns = tuple(_ops._unwrap(pred) for pred in self.request.filters)
        exact_filter_fields = frozenset().union(
            *(_ops._exact_filter_fields(fn) for fn in filter_fns)
        )
        dimensions = _ops._get_merged_fields(all_roots, "dimensions", source=join_op)
        dimensions = _ops._augment_dimensions_with_raw_columns(
            dimensions,
            (*aggregate_op.keys, *exact_filter_fields),
            all_roots,
            join_op,
        )
        if exact_filter_fields:
            _ops._validate_qualified_filter_fields(exact_filter_fields, dimensions, all_roots)

        base_measures = _ops._get_merged_fields(all_roots, "measures")
        calc_measures = _ops._get_merged_fields(all_roots, "calc_measures")
        join_column_lineage, _joined_column_names = _ops._build_join_column_lineage(join_op)
        wrapper_local_dimensions = {
            name: dimension
            for name, dimension in _ops._join_wrapper_local_dimensions(all_roots).items()
            if name in aggregate_op.keys
        }
        return PreAggregationMetadata(
            request=self.request,
            root_names=root_names,
            filter_fns=filter_fns,
            exact_filter_fields=exact_filter_fields,
            dimensions=dimensions,
            base_measures=base_measures,
            calc_measures=calc_measures,
            group_by_cols=aggregate_op.keys,
            join_column_lineage=join_column_lineage,
            wrapper_local_dimensions=wrapper_local_dimensions,
        )


@frozen(slots=True)
class JoinPreparationStage:
    """Build the flattened dimension bridge and bind wrapper dimensions."""

    metadata: PreAggregationMetadata

    def run(self) -> PreparedJoin:
        aggregate_op = self.metadata.request.aggregate_op
        join_op = self.metadata.request.join_op
        wrapper_dimension_owners: dict[str, str | None] = {}

        try:
            joined_base_table = join_op.to_untagged(parent_requirements=None)
        except Exception:
            joined_base_table = None
            table = None

        if joined_base_table is not None:
            if self.metadata.wrapper_local_dimensions:
                wrapper_dimension_owners, wrapper_dimension_errors = (
                    _ops._infer_join_wrapper_dimension_owners(
                        self.metadata.wrapper_local_dimensions,
                        joined_base_table,
                        self.metadata.dimensions,
                        self.metadata.join_column_lineage,
                    )
                )
                if wrapper_dimension_errors:
                    details = "; ".join(
                        f"{name!r}: {reason}"
                        for name, reason in sorted(wrapper_dimension_errors.items())
                    )
                    raise ValueError(
                        "Cannot source-preaggregate by join-wrapper dimension(s) "
                        f"because their row-level lineage is not reproducible: {details}. "
                        "Define each dimension on one owning semantic model before "
                        "joining, or aggregate the joined rows explicitly."
                    )
            try:
                table = _ops._mutate_dimensions_with_dependencies(
                    joined_base_table,
                    [key for key in aggregate_op.keys if key in self.metadata.dimensions],
                    self.metadata.dimensions,
                )
            except Exception:
                table = None

        if joined_base_table is None and self.metadata.wrapper_local_dimensions:
            raise ValueError(
                "Cannot source-preaggregate by a join-wrapper dimension because "
                "the flattened join is unavailable for source-lineage analysis."
            )

        if table is not None:
            _ops._reject_unresolvable_group_keys(
                aggregate_op.keys,
                self.metadata.dimensions,
                table,
                self.metadata.request.all_roots,
            )

        return PreparedJoin(
            metadata=self.metadata,
            joined_base_table=joined_base_table,
            table=table,
            wrapper_dimension_owners=wrapper_dimension_owners,
        )


@frozen(slots=True)
class FilterPlanningStage:
    """Assign filters to sources and split row-precise conjunction legs."""

    prepared_join: PreparedJoin

    def run(self) -> FilterPlan:
        metadata = self.prepared_join.metadata
        table = self.prepared_join.table
        filter_fns = metadata.filter_fns
        filters_on_table: set[int] = set()
        table_filter_exprs: dict[int, Any] = {}

        if table is not None and filter_fns:
            from .convert import _Resolver

            dimensions_for_table = dict(metadata.dimensions)
            table_columns = set(table.columns)
            by_suffix: dict[str, list[str]] = {}
            for dimension_name in metadata.dimensions:
                if "." in dimension_name:
                    by_suffix.setdefault(dimension_name.split(".", 1)[1], []).append(dimension_name)
            for short_name, full_names in by_suffix.items():
                if (
                    short_name not in dimensions_for_table
                    and short_name not in table_columns
                    and len(full_names) == 1
                ):
                    dimensions_for_table[short_name] = metadata.dimensions[full_names[0]]

            for index, predicate_fn in enumerate(filter_fns):
                try:
                    resolver = _Resolver(table, dimensions_for_table)
                    predicate_expr = _ops._resolve_expr(predicate_fn, resolver)
                    filtered = table.filter(predicate_expr)
                except Exception:
                    continue
                table = filtered
                filters_on_table.add(index)
                table_filter_exprs[index] = predicate_expr

        raw_tables: dict[str, Any] = {}
        owners_by_filter: list[frozenset[str]] = []
        if filter_fns:
            for table_name, table_op in metadata.request.join_tree_info.table_ops.items():
                try:
                    raw_tables[table_name] = _ops._to_untagged(table_op)
                except Exception:
                    continue

            for predicate_fn in filter_fns:
                owners: set[str] = set()
                for table_name, table_op in metadata.request.join_tree_info.table_ops.items():
                    raw_table = raw_tables.get(table_name)
                    if raw_table is None:
                        continue
                    try:
                        _ops._resolve_expr(
                            predicate_fn,
                            _ops._table_filter_resolver(
                                raw_table,
                                table_op,
                                table_name,
                                _ops._exact_filter_fields(predicate_fn),
                            ),
                        )
                        owners.add(table_name)
                    except Exception:
                        pass
                owners_by_filter.append(frozenset(owners))

            for index, owners in enumerate(owners_by_filter):
                if index in filters_on_table or len(owners) == 1:
                    continue
                if len(owners) > 1:
                    raise ValueError(
                        f"Filter #{index} is ambiguous: it resolves against multiple "
                        f"joined tables ({', '.join(sorted(owners))}) and could "
                        "not be applied to the full joined table. Qualify the "
                        'field with a table prefix (e.g. t["orders.status"]).'
                    )
                raise ValueError(
                    f"Filter #{index} does not resolve against the joined table or "
                    "any single source table; it would be silently ignored. "
                    "Check the dimension/column name, or qualify it with a "
                    'table prefix (e.g. t["orders.status"]).'
                )

        filter_legs: dict[int, tuple[tuple[Any, frozenset[str]], ...]] = {}
        if table_filter_exprs:
            leaf_types = _ops._leaf_rel_types()
            base_relation_to_table: dict[Any, str] = {}
            for table_name, raw_table in raw_tables.items():
                for leaf in _ops.walk_nodes(leaf_types, raw_table):
                    key = _ops._base_rel_key(leaf)
                    if base_relation_to_table.get(key, table_name) != table_name:
                        base_relation_to_table[key] = "__ambiguous__"
                    else:
                        base_relation_to_table[key] = table_name
            for index, expression in table_filter_exprs.items():
                if owners_by_filter[index] and len(owners_by_filter[index]) == 1:
                    continue
                filter_legs[index] = tuple(
                    (
                        leg,
                        frozenset(_ops._leg_source_tables(leg, base_relation_to_table, leaf_types)),
                    )
                    for leg in _ops._flatten_and_legs(expression)
                )

        return FilterPlan(
            prepared_join=self.prepared_join,
            table=table,
            raw_tables=raw_tables,
            owners=tuple(owners_by_filter),
            legs=filter_legs,
        )


@frozen(slots=True)
class AggregationPlanningStage:
    """Classify aggregates and partition them by their owning source."""

    filter_plan: FilterPlan

    def run(self) -> PartitionedAggregation:
        metadata = self.filter_plan.prepared_join.metadata
        request = metadata.request
        aggregate_op = request.aggregate_op
        table = self.filter_plan.table

        if table is not None:
            scope = MeasureScope(
                _tbl=table,
                _known=[
                    *metadata.base_measures.keys(),
                    *metadata.calc_measures.keys(),
                ],
            )
            plan = _ops._build_aggregation_plan(
                aggs=aggregate_op.aggs,
                keys=aggregate_op.keys,
                scope=scope,
                is_post_agg=False,
                merged_base_measures=metadata.base_measures,
                merged_calc_measures=metadata.calc_measures,
                tbl=table,
            )
        else:
            aggregate_specs = {
                name: _ops._make_agg_callable(metadata.base_measures[name])
                for name in aggregate_op.aggs
                if name in metadata.base_measures
            }
            plan = _ops._AggregationPlan(
                agg_specs=FrozenDict(aggregate_specs),
                calc_specs=FrozenDict({}),
                requested_measures=tuple(aggregate_op.aggs.keys()),
                group_by_cols=tuple(aggregate_op.keys),
            )

        partition_roots = _ops._find_all_root_models(request.join_op)
        partitioned = _ops._partition_agg_specs_by_source(dict(plan.agg_specs), partition_roots)
        wrapper_specs = partitioned.get(None, {})
        for measure_name, measure_fn in tuple(wrapper_specs.items()):
            measure_obj = metadata.base_measures.get(measure_name)
            if not isinstance(measure_obj, _ops.Measure):
                continue
            owner = _ops._infer_join_wrapper_measure_owner(measure_obj, request.join_tree_info)
            if owner is None:
                continue
            partitioned.setdefault(owner, {})[measure_name] = measure_fn
            del wrapper_specs[measure_name]
        if not wrapper_specs:
            partitioned.pop(None, None)

        frozen_partitioned = {
            source: FrozenDict(measures) for source, measures in partitioned.items()
        }
        return PartitionedAggregation(
            filter_plan=self.filter_plan,
            plan=plan,
            measures_by_source=frozen_partitioned,
        )


@frozen(slots=True)
class PreAggregationProducts:
    """Frozen products emitted by all source compilation stages."""

    results: tuple[Any, ...] = field(converter=tuple)
    decomposed_means: Mapping[str, tuple[str, str]] = field(converter=_as_frozen_dict)
    reaggregation_ops: Mapping[str, str] = field(converter=_as_frozen_dict)
    empty_count_measures: frozenset[str] = field(converter=frozenset)
    deferred_count_distincts: Mapping[str, tuple] = field(converter=_as_frozen_dict)
    totals_sources: Mapping[str | None, tuple[Any, Mapping[str, Any]]] = field(
        converter=_as_frozen_dict
    )


@define(slots=True)
class PreAggregationProductsBuilder:
    """Stage-local mutable builder that freezes before result combination."""

    results: list[Any] = Factory(list)
    decomposed_means: dict[str, tuple[str, str]] = Factory(dict)
    reaggregation_ops: dict[str, str] = Factory(dict)
    empty_count_measures: set[str] = Factory(set)
    deferred_count_distincts: dict[str, tuple] = Factory(dict)
    totals_sources: dict[str | None, tuple[Any, Mapping[str, Any]]] = Factory(dict)

    def freeze(self) -> PreAggregationProducts:
        totals_sources = {
            source: (table, FrozenDict(expressions))
            for source, (table, expressions) in self.totals_sources.items()
        }
        return PreAggregationProducts(
            results=self.results,
            decomposed_means=self.decomposed_means,
            reaggregation_ops=self.reaggregation_ops,
            empty_count_measures=self.empty_count_measures,
            deferred_count_distincts=self.deferred_count_distincts,
            totals_sources=totals_sources,
        )


@frozen(slots=True)
class PreparedSource:
    """A source relation after filters and participation restrictions."""

    partition: PartitionedAggregation
    table_name: str
    measures: Mapping[str, Callable] = field(converter=_as_frozen_dict)
    table_op: Any
    raw_table: Any
    source_key_names: Mapping[str, str] = field(converter=_as_frozen_dict)


@frozen(slots=True)
class SourceMeasureExpressions:
    """Source-bound measure expressions grouped by compilation strategy."""

    source: PreparedSource
    measure_binding_op: Any
    aggregate_expressions: Mapping[str, Any] = field(converter=_as_frozen_dict)
    exact_measures: Mapping[str, Any] = field(converter=_as_frozen_dict)
    nested_measures: Mapping[str, Any] = field(converter=_as_frozen_dict)


@frozen(slots=True)
class SourceGrainPlan:
    """Materialized source grain and public/private group-key mapping."""

    expressions: SourceMeasureExpressions
    raw_table: Any
    aggregate_expressions: Mapping[str, Any] = field(converter=_as_frozen_dict)
    grain: tuple[str, ...] = field(converter=tuple)
    available_join_keys: tuple[str, ...] = field(converter=tuple)
    local_group_keys: Mapping[str, str] = field(converter=_as_frozen_dict)
    local_group_outputs: Mapping[str, str] = field(converter=_as_frozen_dict)
    has_cross_table_group_by: bool


@frozen(slots=True)
class SourcePreparationStage:
    """Apply source filters and restrict measures to participating rows."""

    partition: PartitionedAggregation
    table_name: str
    measures: Mapping[str, Callable] = field(converter=_as_frozen_dict)

    def run(self) -> PreparedSource:
        filter_plan = self.partition.filter_plan
        metadata = filter_plan.prepared_join.metadata
        join_tree_info = metadata.request.join_tree_info
        table_op = join_tree_info.table_ops.get(self.table_name)
        if table_op is None:
            raise KeyError(f"Unknown semantic source {self.table_name!r}")

        raw_table = _ops._to_untagged(table_op)
        source_key_names = metadata.join_column_lineage.get(self.table_name, {})
        needs_bridge = False
        if metadata.filter_fns:
            residual_cross_legs = False
            for index, predicate_fn in enumerate(metadata.filter_fns):
                if filter_plan.owners[index] == frozenset({self.table_name}):
                    predicate_expr = _ops._resolve_expr(
                        predicate_fn,
                        _ops._table_filter_resolver(
                            raw_table,
                            table_op,
                            self.table_name,
                            _ops._exact_filter_fields(predicate_fn),
                        ),
                    )
                    raw_table = raw_table.filter(predicate_expr)
                    continue

                needs_bridge = True
                for leg_expression, leg_sources in filter_plan.legs.get(index, ()):
                    if leg_sources == {self.table_name}:
                        try:
                            leg_op = _ops._inline_to_base_op(
                                leg_expression.op(),
                                _ops._leaf_rel_types(),
                                target_tbl=raw_table,
                            )
                            raw_table = raw_table.filter(leg_op.to_expr())
                        except Exception:
                            residual_cross_legs = True
                    elif self.table_name in leg_sources and len(leg_sources) > 1:
                        residual_cross_legs = True

            if residual_cross_legs and self.measures:
                raise ValueError(
                    f"A filter mixes columns of {self.table_name!r} with other "
                    "tables in a way that cannot be applied row-precisely "
                    f"to {self.table_name!r} (e.g. OR across tables); its "
                    "measures would be silently inflated to join-key grain. "
                    "Split the condition into separate .filter() calls, or "
                    "restate it against a single table."
                )

        needs_participation = join_tree_info.table_cardinalities.get(self.table_name) not in (
            "root",
            "cross",
        ) and bool(self.measures)
        table = filter_plan.table
        if needs_bridge or needs_participation:
            join_keys = join_tree_info.table_join_keys.get(self.table_name, set())
            if table is not None:
                shared = _ops._source_join_key_pairs(
                    self.table_name,
                    join_keys,
                    raw_table.columns,
                    table.columns,
                    metadata.join_column_lineage,
                )
                if shared:
                    key_bridge = table.select(
                        [table[joined_name].name(raw_name) for raw_name, joined_name in shared]
                    ).distinct()
                    predicates = [
                        raw_table[raw_name] == key_bridge[raw_name]
                        for raw_name, _joined_name in shared
                    ]
                    raw_table = raw_table.inner_join(key_bridge, predicates).select(raw_table)
                elif needs_participation:
                    raise ValueError(
                        f"Measures on {self.table_name!r} cannot be restricted "
                        "to rows that participate in its join: no join-key "
                        "column is available on both the raw table and the "
                        "joined table. Computing them on the raw table would "
                        "silently count rows the join can never produce."
                    )
            else:
                participation_bridged = not needs_participation
                if needs_participation:
                    for root_name, cardinality in join_tree_info.table_cardinalities.items():
                        if cardinality != "root":
                            continue
                        root_op = join_tree_info.table_ops.get(root_name)
                        if root_op is None:
                            continue
                        try:
                            root_raw = _ops._to_untagged(root_op)
                        except Exception:
                            continue
                        root_join_keys = join_tree_info.table_join_keys.get(root_name, set())
                        shared = sorted(
                            join_keys
                            & root_join_keys
                            & set(raw_table.columns)
                            & set(root_raw.columns)
                        )
                        if shared:
                            key_bridge = root_raw.select(
                                [root_raw[column] for column in shared]
                            ).distinct()
                            predicates = [
                                raw_table[column] == key_bridge[column] for column in shared
                            ]
                            raw_table = raw_table.inner_join(key_bridge, predicates).select(
                                raw_table
                            )
                            participation_bridged = True
                if not participation_bridged:
                    raise ValueError(
                        f"Measures on {self.table_name!r} cannot be restricted "
                        "to rows that participate in its join: the full "
                        "joined table is unavailable (chasm fallback) and no "
                        "join-key column is shared with the root table. "
                        "Computing them on the raw table would silently count "
                        "rows the join can never produce."
                    )

                for index, predicate_fn in enumerate(metadata.filter_fns):
                    owners = filter_plan.owners[index]
                    if self.table_name in owners or len(owners) != 1:
                        continue
                    (owner_name,) = owners
                    owner_op = join_tree_info.table_ops.get(owner_name)
                    owner_raw = filter_plan.raw_tables.get(owner_name)
                    if owner_op is None or owner_raw is None:
                        continue
                    owner_raw = owner_raw.filter(
                        _ops._resolve_expr(
                            predicate_fn,
                            _ops._table_filter_resolver(
                                owner_raw,
                                owner_op,
                                owner_name,
                                _ops._exact_filter_fields(predicate_fn),
                            ),
                        )
                    )
                    owner_join_keys = join_tree_info.table_join_keys.get(owner_name, set())
                    shared = sorted(
                        join_keys
                        & owner_join_keys
                        & set(raw_table.columns)
                        & set(owner_raw.columns)
                    )
                    if shared:
                        key_bridge = owner_raw.select(
                            [owner_raw[column] for column in shared]
                        ).distinct()
                        predicates = [raw_table[column] == key_bridge[column] for column in shared]
                        raw_table = raw_table.inner_join(key_bridge, predicates).select(raw_table)

        return PreparedSource(
            partition=self.partition,
            table_name=self.table_name,
            measures=self.measures,
            table_op=table_op,
            raw_table=raw_table,
            source_key_names=source_key_names,
        )


@frozen(slots=True)
class SourceMeasureStage:
    """Bind measures and select decomposed, exact, nested, or deferred paths."""

    source: PreparedSource
    products: PreAggregationProductsBuilder

    def run(self) -> SourceMeasureExpressions:
        metadata = self.source.partition.filter_plan.prepared_join.metadata
        table_measures = _ops._get_field_dict(self.source.table_op, "measures")
        raw_table = self.source.raw_table
        measure_binding_op = _ops._to_op(raw_table)
        aggregate_expressions: dict[str, Any] = {}
        totals_expressions: dict[str, Any] = {}
        exact_measures: dict[str, Any] = {}
        nested_measures: dict[str, Any] = {}

        for measure_name in self.source.measures:
            short_name = measure_name.split(".", 1)[1] if "." in measure_name else measure_name
            source_measure = (
                table_measures.get(short_name)
                if "." in measure_name
                else metadata.base_measures.get(measure_name)
            )
            if not isinstance(source_measure, _ops.Measure):
                continue

            expression = source_measure(raw_table)
            if isinstance(expression, _ops.NestedAccessMarker):
                nested_measures[measure_name] = source_measure
                totals_expressions[measure_name] = expression
                if expression.operation in {"count", "nunique"}:
                    self.products.empty_count_measures.add(measure_name)
                continue

            if _ops._is_count_expr(expression):
                self.products.empty_count_measures.add(measure_name)
            totals_expressions[measure_name] = expression

            if _ops._is_mean_expr(expression):
                mean_op = expression.op()
                base_column = mean_op.arg.to_expr()
                mean_where = mean_op.where.to_expr() if mean_op.where is not None else None
                sum_column = f"_sum__{measure_name}"
                count_column = f"_count__{measure_name}"
                aggregate_expressions[sum_column] = base_column.sum(where=mean_where)
                aggregate_expressions[count_column] = base_column.count(where=mean_where)
                self.products.decomposed_means[measure_name] = (
                    sum_column,
                    count_column,
                )
            elif _ops._is_count_distinct_expr(expression):
                self.products.deferred_count_distincts[measure_name] = (
                    self.source.table_name,
                    short_name,
                    raw_table,
                    source_measure,
                    {},
                )
            else:
                reaggregation_op = _ops._reagg_op_for_expr(expression)
                if reaggregation_op is None and metadata.group_by_cols:
                    exact_measures[measure_name] = source_measure
                else:
                    if reaggregation_op is not None:
                        self.products.reaggregation_ops[measure_name] = reaggregation_op
                    aggregate_expressions[measure_name] = expression

        if totals_expressions:
            self.products.totals_sources[self.source.table_name] = (
                raw_table,
                totals_expressions,
            )

        return SourceMeasureExpressions(
            source=self.source,
            measure_binding_op=measure_binding_op,
            aggregate_expressions=aggregate_expressions,
            exact_measures=exact_measures,
            nested_measures=nested_measures,
        )


@frozen(slots=True)
class SourceGrainPlanningStage:
    """Materialize source-local group keys and choose the safe grain."""

    expressions: SourceMeasureExpressions
    products: PreAggregationProductsBuilder

    def run(self) -> SourceGrainPlan:
        source = self.expressions.source
        partition = source.partition
        filter_plan = partition.filter_plan
        prepared_join = filter_plan.prepared_join
        metadata = prepared_join.metadata
        raw_table = source.raw_table
        raw_columns = set(raw_table.columns)
        table_dimensions = _ops._get_field_dict(source.table_op, "dimensions")
        local_dimensions: list[str] = []
        local_group_keys: dict[str, str] = {}
        local_group_outputs: dict[str, str] = {}
        has_cross_table_group_by = False

        for group_key in metadata.group_by_cols:
            if "." in group_key:
                prefix, short_name = group_key.split(".", 1)
                if prefix == source.table_name and short_name in table_dimensions:
                    dimension = table_dimensions[short_name]
                    if callable(dimension):
                        resolved_via_dependencies = False
                        try:
                            dimension_expr = dimension(raw_table)
                        except Exception:
                            raw_table = _ops._mutate_dimensions_with_dependencies(
                                raw_table, [short_name], table_dimensions
                            )
                            raw_columns = set(raw_table.columns)
                            dimension_expr = raw_table[short_name]
                            resolved_via_dependencies = True

                        if not resolved_via_dependencies and _ops._is_direct_physical_field(
                            dimension_expr, raw_table, short_name
                        ):
                            if short_name not in local_dimensions:
                                local_dimensions.append(short_name)
                            local_group_keys[group_key] = short_name
                        else:
                            raw_group_name = _ops._allocate_local_group_alias(
                                group_key, raw_columns
                            )
                            raw_table = raw_table.mutate(**{raw_group_name: dimension_expr})
                            raw_columns = set(raw_table.columns)
                            if raw_group_name not in local_dimensions:
                                local_dimensions.append(raw_group_name)
                            local_group_keys[group_key] = raw_group_name
                            local_group_outputs[raw_group_name] = group_key
                elif prefix == source.table_name and short_name in raw_columns:
                    if short_name not in local_dimensions:
                        local_dimensions.append(short_name)
                    local_group_keys[group_key] = short_name
                elif prefix != source.table_name:
                    has_cross_table_group_by = True
                continue

            if group_key not in metadata.dimensions:
                continue

            if group_key in metadata.wrapper_local_dimensions:
                wrapper_owner = prepared_join.wrapper_dimension_owners.get(group_key)
                if wrapper_owner != source.table_name:
                    has_cross_table_group_by = True
                    continue

                dimension = metadata.wrapper_local_dimensions[group_key]
                joined_to_raw = {
                    joined_name: raw_name
                    for raw_name, joined_name in metadata.join_column_lineage.get(
                        source.table_name, {}
                    ).items()
                }
                owner_scope = _ops._RenamedResolver(raw_table, joined_to_raw)
                dimension_scope = _ops._JoinWrapperDimensionResolver(
                    owner_scope,
                    dict(metadata.dimensions),
                    resolving=(group_key,),
                )
                try:
                    dimension_expr = dimension(
                        dimension_scope,
                        _dims=dict(metadata.dimensions),
                    )
                except Exception as exc:
                    raise ValueError(
                        f"Join-wrapper dimension {group_key!r} was bound to "
                        f"semantic model {source.table_name!r} but could not be "
                        "evaluated in that model's raw namespace."
                    ) from exc

                direct_raw_name = next(
                    (
                        column
                        for column in raw_columns
                        if _ops._is_direct_physical_field(dimension_expr, raw_table, column)
                    ),
                    None,
                )
                if direct_raw_name is not None:
                    if direct_raw_name not in local_dimensions:
                        local_dimensions.append(direct_raw_name)
                    local_group_keys[group_key] = direct_raw_name
                else:
                    raw_group_name = _ops._allocate_local_group_alias(group_key, raw_columns)
                    raw_table = raw_table.mutate(**{raw_group_name: dimension_expr})
                    raw_columns = set(raw_table.columns)
                    local_dimensions.append(raw_group_name)
                    local_group_keys[group_key] = raw_group_name
                    local_group_outputs[raw_group_name] = group_key
            elif group_key in raw_columns:
                if group_key not in local_dimensions:
                    local_dimensions.append(group_key)
                local_group_keys[group_key] = group_key
            else:
                dimension = metadata.dimensions[group_key]
                try:
                    dimension_expr = (
                        dimension(raw_table)
                        if callable(dimension)
                        else _ops._resolve_expr(dimension, raw_table)
                    )
                    raw_table = raw_table.mutate(**{group_key: dimension_expr})
                    raw_columns = set(raw_table.columns)
                    if group_key not in local_dimensions:
                        local_dimensions.append(group_key)
                    local_group_keys[group_key] = group_key
                except Exception:
                    has_cross_table_group_by = True

        for measure_name in source.measures:
            deferred = self.products.deferred_count_distincts.get(measure_name)
            if deferred is None:
                continue
            (
                source_name,
                short_name,
                _old_raw_table,
                source_measure,
                _old_local_group_keys,
            ) = deferred
            self.products.deferred_count_distincts[measure_name] = (
                source_name,
                short_name,
                raw_table,
                source_measure,
                dict(local_group_keys),
            )

        join_keys = metadata.request.join_tree_info.table_join_keys.get(source.table_name, set())
        available_join_keys = tuple(
            join_key for join_key in sorted(join_keys) if join_key in raw_columns
        )
        match (local_dimensions, has_cross_table_group_by):
            case ([], _):
                grain = available_join_keys
            case (_, True):
                grain = tuple(
                    dict.fromkeys(
                        local_dimensions
                        + [
                            join_key
                            for join_key in available_join_keys
                            if join_key not in local_dimensions
                        ]
                    )
                )
            case _:
                grain = tuple(local_dimensions)

        aggregate_expressions = dict(self.expressions.aggregate_expressions)
        final_raw_op = _ops._to_op(raw_table)
        if final_raw_op is not self.expressions.measure_binding_op:
            aggregate_expressions = {
                name: _ops._to_op(expression)
                .replace({self.expressions.measure_binding_op: final_raw_op})
                .to_expr()
                for name, expression in aggregate_expressions.items()
            }

        return SourceGrainPlan(
            expressions=self.expressions,
            raw_table=raw_table,
            aggregate_expressions=aggregate_expressions,
            grain=grain,
            available_join_keys=available_join_keys,
            local_group_keys=local_group_keys,
            local_group_outputs=local_group_outputs,
            has_cross_table_group_by=has_cross_table_group_by,
        )


def _restore_local_group_names(
    table,
    local_group_outputs: Mapping[str, str],
    *,
    nested: bool,
):
    local_renames = {
        public_name: raw_name
        for raw_name, public_name in local_group_outputs.items()
        if raw_name in table.columns
    }
    collisions = sorted(set(local_renames) & (set(table.columns) - set(local_renames.values())))
    if collisions:
        label = "nested source preaggregation" if nested else "source preaggregation"
        raise ValueError(
            "Cannot restore semantic group-key names after "
            f"{label} because they collide with aggregate columns: "
            f"{collisions}. Rename the aggregate field."
        )
    return table.rename(local_renames) if local_renames else table


@frozen(slots=True)
class SourceCompilationStage:
    """Compile one prepared source into one or more pre-aggregate tables."""

    grain_plan: SourceGrainPlan
    products: PreAggregationProductsBuilder

    def run(self) -> None:
        expressions = self.grain_plan.expressions
        source = expressions.source
        partition = source.partition
        metadata = partition.filter_plan.prepared_join.metadata
        raw_table = self.grain_plan.raw_table
        aggregate_expressions = dict(self.grain_plan.aggregate_expressions)
        exact_measures = dict(expressions.exact_measures)
        nested_measures = dict(expressions.nested_measures)

        if not metadata.group_by_cols:
            if not aggregate_expressions and not exact_measures and not nested_measures:
                return
            if aggregate_expressions:
                preaggregate = raw_table.aggregate(**aggregate_expressions)
                for measure_name, (
                    sum_column,
                    count_column,
                ) in self.products.decomposed_means.items():
                    if sum_column in preaggregate.columns and count_column in preaggregate.columns:
                        preaggregate = preaggregate.mutate(
                            **{measure_name: preaggregate[sum_column] / preaggregate[count_column]}
                        ).drop(sum_column, count_column)
                self.products.results.append(preaggregate)
            if nested_measures:
                self.products.results.append(
                    _ops._compile_exact_measure_table(raw_table, (), nested_measures)
                )
            return

        if not aggregate_expressions and not exact_measures and not nested_measures:
            return

        join_tree_info = metadata.request.join_tree_info
        table_name = source.table_name
        table = partition.filter_plan.table
        grain = self.grain_plan.grain

        if exact_measures:
            exact_needs_source_spine = join_tree_info.table_cardinalities.get(table_name) not in (
                "root",
                "cross",
            )
            if not self.grain_plan.has_cross_table_group_by and not exact_needs_source_spine:
                for measure_name, measure in exact_measures.items():
                    aggregate_expressions[measure_name] = measure(raw_table)
            else:
                self.products.results.append(
                    _ops._exact_grain_preagg(
                        raw_table,
                        table,
                        metadata.group_by_cols,
                        self.grain_plan.available_join_keys,
                        exact_measures,
                        joined_key_names=source.source_key_names,
                        local_group_keys=self.grain_plan.local_group_keys,
                    )
                )

        if nested_measures:
            nested_needs_source_spine = join_tree_info.table_cardinalities.get(table_name) not in (
                "root",
                "cross",
            )
            if (
                not self.grain_plan.has_cross_table_group_by
                and not nested_needs_source_spine
                and grain
            ):
                nested_preaggregate = _ops._compile_exact_measure_table(
                    raw_table, grain, nested_measures
                )
                nested_preaggregate = _restore_local_group_names(
                    nested_preaggregate,
                    self.grain_plan.local_group_outputs,
                    nested=True,
                )
                joined_grain = tuple(
                    self.grain_plan.local_group_outputs.get(name, name) for name in grain
                )
                self.products.results.append(
                    _ops._rename_preagg_grain_to_joined_aliases(
                        nested_preaggregate,
                        joined_grain,
                        source.source_key_names,
                    )
                )
            else:
                self.products.results.append(
                    _ops._exact_grain_preagg(
                        raw_table,
                        table,
                        metadata.group_by_cols,
                        self.grain_plan.available_join_keys,
                        nested_measures,
                        joined_key_names=source.source_key_names,
                        local_group_keys=self.grain_plan.local_group_keys,
                    )
                )

        if not aggregate_expressions:
            return
        if grain:
            preaggregate = raw_table.group_by([raw_table[column] for column in grain]).aggregate(
                **aggregate_expressions
            )
            preaggregate = _restore_local_group_names(
                preaggregate,
                self.grain_plan.local_group_outputs,
                nested=False,
            )
            joined_grain = tuple(
                self.grain_plan.local_group_outputs.get(name, name) for name in grain
            )
            self.products.results.append(
                _ops._rename_preagg_grain_to_joined_aliases(
                    preaggregate,
                    joined_grain,
                    source.source_key_names,
                )
            )
        else:
            self.products.results.append(raw_table.aggregate(**aggregate_expressions))


@frozen(slots=True)
class SourcePreAggregationStage:
    """Run the source sub-pipeline and freeze its accumulated products."""

    partition: PartitionedAggregation

    def run(self) -> PreAggregationProducts:
        products = PreAggregationProductsBuilder()
        metadata = self.partition.filter_plan.prepared_join.metadata
        table = self.partition.filter_plan.table

        for table_name, measures in self.partition.measures_by_source.items():
            if table_name is None:
                if table is None:
                    continue
                aggregate_expressions = {
                    name: measure_fn(table) for name, measure_fn in measures.items()
                }
                products.totals_sources[None] = (
                    table,
                    dict(aggregate_expressions),
                )
                if metadata.group_by_cols:
                    preaggregate = table.group_by(
                        [table[column] for column in metadata.group_by_cols]
                    ).aggregate(**aggregate_expressions)
                else:
                    preaggregate = table.aggregate(**aggregate_expressions)
                products.results.append(preaggregate)
                continue

            prepared_source = SourcePreparationStage(self.partition, table_name, measures).run()
            expressions = SourceMeasureStage(prepared_source, products).run()
            grain_plan = SourceGrainPlanningStage(expressions, products).run()
            SourceCompilationStage(grain_plan, products).run()

        return products.freeze()


@frozen(slots=True)
class CombinedResult:
    partition: PartitionedAggregation
    products: PreAggregationProducts
    table: Any


@frozen(slots=True)
class ResultCombinationStage:
    """Join source products at the requested grain and attach distinct counts."""

    partition: PartitionedAggregation
    products: PreAggregationProducts

    def _fallback_result(self):
        plan = self.partition.plan
        filter_plan = self.partition.filter_plan
        table = filter_plan.table
        aggregate_op = filter_plan.prepared_join.metadata.request.aggregate_op
        if table is None:
            raise ValueError("No aggregation results and full join unavailable")
        if plan.calc_specs:
            raise ValueError(
                "Pre-aggregation cannot compute calculated measure(s) "
                f"{sorted(plan.calc_specs)} on this joined model: none of "
                "the requested measures aggregate at a single source's "
                "grain, so there is no fan-out-safe base to calculate "
                "from. This happens when a calc measure builds its "
                "reduction inline — e.g. "
                "t.distance.sum() / t.all(t.distance.sum()) — rather than "
                "referencing a declared measure. Declare the reduction as "
                "a measure and reference it by name:\n"
                "    .with_measures(total=lambda t: t.distance.sum())\n"
                "    .with_measures(share=lambda t: t.total / t.all(t.total))"
            )
        if not plan.agg_specs:
            raise ValueError(
                "Pre-aggregation produced no measures for "
                f"{sorted(aggregate_op.aggs)} with group keys "
                f"{list(plan.group_by_cols)}; aggregating the joined table "
                "here would ignore the request entirely."
            )
        specs = {name: measure_fn(table) for name, measure_fn in plan.agg_specs.items()}
        group_columns = [column for column in plan.group_by_cols if column in table.columns]
        if group_columns:
            return table.group_by(group_columns).aggregate(**specs)
        return table.aggregate(specs)

    def run(self) -> CombinedResult:
        filter_plan = self.partition.filter_plan
        metadata = filter_plan.prepared_join.metadata
        aggregate_op = metadata.request.aggregate_op
        group_by_cols = metadata.group_by_cols
        preaggregate_results = self.products.results

        if not preaggregate_results and not self.products.deferred_count_distincts:
            return CombinedResult(
                partition=self.partition,
                products=self.products,
                table=self._fallback_result(),
            )

        result = None
        if preaggregate_results:
            if not group_by_cols:
                result = preaggregate_results[0]
                for preaggregate in preaggregate_results[1:]:
                    result = result.cross_join(preaggregate)
            elif filter_plan.table is not None:
                result = aggregate_op._join_preagg_with_dim_bridge(
                    preaggregate_results,
                    self.partition.plan,
                    filter_plan.table,
                    list(group_by_cols),
                    decomposed_means=tuple(self.products.decomposed_means.items()),
                    reagg_ops=tuple(self.products.reaggregation_ops.items()),
                    empty_count_measures=tuple(self.products.empty_count_measures),
                )
            else:
                result = aggregate_op._build_minimal_dim_bridge(
                    preaggregate_results,
                    self.partition.plan,
                    list(group_by_cols),
                    metadata.request.join_tree_info,
                    metadata.dimensions,
                    decomposed_means=tuple(self.products.decomposed_means.items()),
                    reagg_ops=tuple(self.products.reaggregation_ops.items()),
                    empty_count_measures=tuple(self.products.empty_count_measures),
                )

        if self.products.deferred_count_distincts:
            count_distinct_parts: list[Any] = []
            join_tree_info = metadata.request.join_tree_info
            for measure_name, (
                source_table_name,
                _short_name,
                source_raw,
                source_measure,
                local_group_keys,
            ) in self.products.deferred_count_distincts.items():
                if not group_by_cols:
                    count_distinct_parts.append(
                        source_raw.aggregate(**{measure_name: source_measure(source_raw)})
                    )
                    continue

                if filter_plan.table is None:
                    raise ValueError(
                        "COUNT DISTINCT measures require the full joined table "
                        "for grouped source-aware aggregation but it is unavailable "
                        "(chasm fallback)."
                    )
                join_keys = join_tree_info.table_join_keys.get(source_table_name, set())
                source_key_names = metadata.join_column_lineage.get(source_table_name, {})
                available_join_keys = tuple(
                    key
                    for key in sorted(join_keys)
                    if key in source_raw.columns
                    and source_key_names.get(key, key) in filter_plan.table.columns
                )
                if not available_join_keys:
                    raise ValueError(
                        f"COUNT DISTINCT measure {measure_name!r} cannot be "
                        "attached to the requested group grain without a "
                        "shared join key."
                    )

                exact = _ops._exact_grain_preagg(
                    source_raw,
                    filter_plan.table,
                    group_by_cols,
                    available_join_keys,
                    {measure_name: source_measure},
                    joined_key_names=source_key_names,
                    local_group_keys=local_group_keys,
                )
                group_spine = filter_plan.table.select(
                    [filter_plan.table[column] for column in group_by_cols]
                ).distinct()
                predicates = [
                    _ops.null_safe_equal(group_spine[column], exact[column])
                    for column in group_by_cols
                ]
                count_distinct_table = group_spine.left_join(exact, predicates).select(
                    [group_spine] + [exact[measure_name]]
                )
                count_distinct_parts.append(
                    _ops._fill_missing_count_identities(count_distinct_table, (measure_name,))
                )

            for count_distinct_table in count_distinct_parts:
                measure_columns = [
                    column
                    for column in count_distinct_table.columns
                    if column in self.products.deferred_count_distincts
                ]
                grain_columns = [
                    column
                    for column in count_distinct_table.columns
                    if column not in self.products.deferred_count_distincts
                ]
                if result is None:
                    result = count_distinct_table
                elif grain_columns:
                    common = [column for column in grain_columns if column in result.columns]
                    if common:
                        predicates = [
                            _ops.null_safe_equal(result[column], count_distinct_table[column])
                            for column in common
                        ]
                        result = result.left_join(count_distinct_table, predicates).select(
                            [result] + [count_distinct_table[column] for column in measure_columns]
                        )
                    else:
                        result = result.cross_join(count_distinct_table)
                else:
                    result = result.cross_join(count_distinct_table)

            result = _ops._fill_missing_count_identities(
                result, self.products.deferred_count_distincts
            )

        return CombinedResult(
            partition=self.partition,
            products=self.products,
            table=result,
        )


@frozen(slots=True)
class CalculatedMeasureStage:
    """Apply calculated measures using fan-out-safe zero-grain totals."""

    combined: CombinedResult

    def run(self) -> CombinedResult:
        plan = self.combined.partition.plan
        if not plan.calc_specs:
            return self.combined

        products = self.combined.products

        def fanout_safe_totals():
            parts = [
                _ops._compile_evaluated_measure_table(source_table, (), total_expressions)
                for source_table, total_expressions in (products.totals_sources.values())
                if total_expressions
            ]
            if not parts:
                return None
            total = parts[0]
            for part in parts[1:]:
                total = total.cross_join(part)
            return total

        filter_plan = self.combined.partition.filter_plan
        aggregate_op = filter_plan.prepared_join.metadata.request.aggregate_op
        result = aggregate_op._apply_calc_specs(
            self.combined.table,
            plan,
            filter_plan.table,
            totals_builder=fanout_safe_totals,
        )
        return CombinedResult(
            partition=self.combined.partition,
            products=products,
            table=result,
        )


@frozen(slots=True)
class FinalProjectionStage:
    """Validate and project the exact requested result columns."""

    combined: CombinedResult

    def run(self):
        result = self.combined.table
        plan = self.combined.partition.plan
        available = frozenset(result.columns)
        requested = tuple(
            dict.fromkeys(
                (
                    *plan.group_by_cols,
                    *plan.requested_measures,
                    *plan.calc_specs.keys(),
                )
            )
        )
        missing = [column for column in requested if column not in available]
        if missing:
            raise ValueError(
                f"Pre-aggregation could not attach requested column(s) {missing} "
                f"to the result; available columns: {sorted(available)}. "
                "Grouping a cross-joined model by one side's dimension while "
                "aggregating the other side's measures is not supported — "
                "restructure the query (e.g. join on an explicit key, or "
                "aggregate each side separately and combine)."
            )
        if requested:
            result = result.select([result[column] for column in requested])
        return result


@frozen(slots=True)
class PreAggregationPlanner:
    """Orchestrate the explicit source-aware planning stages."""

    request: PreAggregationRequest

    stage_types: ClassVar[tuple[type, ...]] = (
        MetadataStage,
        JoinPreparationStage,
        FilterPlanningStage,
        AggregationPlanningStage,
        SourcePreAggregationStage,
        ResultCombinationStage,
        CalculatedMeasureStage,
        FinalProjectionStage,
    )

    def run(self):
        metadata = MetadataStage(self.request).run()
        prepared_join = JoinPreparationStage(metadata).run()
        filter_plan = FilterPlanningStage(prepared_join).run()
        partition = AggregationPlanningStage(filter_plan).run()
        products = SourcePreAggregationStage(partition).run()
        combined = ResultCombinationStage(partition, products).run()
        calculated = CalculatedMeasureStage(combined).run()
        return FinalProjectionStage(calculated).run()


def execute_preaggregation(
    aggregate_op,
    all_roots,
    join_op,
    join_tree_info,
    filters=None,
):
    """Build and execute a staged pre-aggregation plan."""

    request = PreAggregationRequest(
        aggregate_op=aggregate_op,
        all_roots=all_roots,
        join_op=join_op,
        join_tree_info=join_tree_info,
        filters=filters,
    )
    return PreAggregationPlanner(request).run()


__all__ = [
    "AggregationPlanningStage",
    "CalculatedMeasureStage",
    "CombinedResult",
    "FilterPlan",
    "FilterPlanningStage",
    "FinalProjectionStage",
    "JoinPreparationStage",
    "MetadataStage",
    "PartitionedAggregation",
    "PreAggregationMetadata",
    "PreAggregationPlanner",
    "PreAggregationProducts",
    "PreAggregationProductsBuilder",
    "PreAggregationRequest",
    "PreparedJoin",
    "PreparedSource",
    "ResultCombinationStage",
    "SourceCompilationStage",
    "SourceGrainPlan",
    "SourceGrainPlanningStage",
    "SourceMeasureExpressions",
    "SourceMeasureStage",
    "SourcePreAggregationStage",
    "SourcePreparationStage",
    "execute_preaggregation",
]
