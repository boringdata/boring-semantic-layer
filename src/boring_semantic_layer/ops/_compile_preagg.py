"""Compile strategy: fan-out-safe source-grain pre-aggregation.

Extracted verbatim from ``SemanticAggregateOp._to_untagged_with_preagg``
and its dimension-bridge helpers; ``op`` is the aggregate op being
compiled. Decomposing the pipeline body along its ``# --- N.`` phase
markers is the follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .._xorq import FrozenDict, null_safe_equal
from ..calc_compiler import _to_op, apply_calc_measures
from ..graph_utils import walk_nodes
from ..measure_scope import MeasureScope
from ..nested_access import NestedAccessMarker
from ._core import (
    Measure,
    SemanticJoinOp,
    _AggregationPlan,
    _allocate_local_group_alias,
    _attach_dim_column,
    _augment_dimensions_with_raw_columns,
    _base_rel_key,
    _build_aggregation_plan,
    _build_join_column_lineage,
    _compile_evaluated_measure_table,
    _compile_exact_measure_table,
    _exact_filter_fields,
    _exact_grain_preagg,
    _find_all_root_models,
    _flatten_and_legs,
    _get_field_dict,
    _get_merged_fields,
    _infer_join_wrapper_dimension_owners,
    _infer_join_wrapper_measure_owner,
    _inline_to_base_op,
    _is_direct_physical_field,
    _join_wrapper_local_dimensions,
    _JoinTreeInfo,
    _JoinWrapperDimensionResolver,
    _leaf_rel_types,
    _leg_source_tables,
    _make_agg_callable,
    _mutate_dimensions_with_dependencies,
    _partition_agg_specs_by_source,
    _reject_unresolvable_group_keys,
    _rename_preagg_grain_to_joined_aliases,
    _resolve_expr,
    _source_join_key_pairs,
    _table_filter_resolver,
    _to_untagged,
    _unwrap,
    _validate_preaggregation_join_predicates,
    _validate_qualified_filter_fields,
)
from ._normalize import _RenamedResolver
from ._reductions import (
    _build_reagg,
    _fill_missing_count_identities,
    _is_count_distinct_expr,
    _is_count_expr,
    _is_mean_expr,
    _reagg_op_for_expr,
)


@dataclass
class _PreaggScope:
    """Read-only inputs shared by every pre-aggregation phase.

    Built by :func:`_build_scope` (prologue + phase 1) and not mutated
    afterwards; phase functions unpack the fields they use.
    """

    op: Any
    join_op: Any
    join_tree_info: Any
    all_roots: list
    filter_fns: list
    exact_filter_fields: frozenset
    merged_dimensions: dict
    merged_base_measures: dict
    merged_calc_measures: dict
    group_by_cols: list
    join_column_lineage: dict
    wrapper_local_dimensions: dict
    wrapper_dimension_owners: dict
    tbl: Any
    filters_on_tbl: set
    tbl_filter_exprs: dict


@dataclass
class _PreaggAccumulators:
    """What phase 4 (per-source pre-aggregation) produces."""

    preagg_results: list
    decomposed_means: dict
    reagg_ops: dict
    empty_count_measures: set
    deferred_count_distincts: dict
    totals_sources: dict


def _build_scope(
    op,
    all_roots: list,
    join_op,
    join_tree_info,
    filters: list | None,
) -> _PreaggScope:
    """Prologue + phase 1: metadata merge, joined-table build, filter application."""
    root_names = {
        name
        for name, cardinality in join_tree_info.table_cardinalities.items()
        if cardinality == "root"
    }
    predicate_sensitive = (
        bool(filters)
        or any(
            "." in name and name.split(".", 1)[0] not in root_names
            for name in (*op.keys, *op.aggs.keys())
        )
        or any("." not in name for name in op.aggs)
    )
    if predicate_sensitive:
        _validate_preaggregation_join_predicates(join_op)
    filters = filters or []
    filter_fns = [_unwrap(pred) for pred in filters]
    exact_filter_fields = frozenset().union(*(_exact_filter_fields(fn) for fn in filter_fns))
    merged_dimensions = _get_merged_fields(all_roots, "dimensions", source=join_op)
    merged_dimensions = _augment_dimensions_with_raw_columns(
        merged_dimensions,
        (*op.keys, *exact_filter_fields),
        all_roots,
        join_op,
    )
    if exact_filter_fields:
        _validate_qualified_filter_fields(exact_filter_fields, merged_dimensions, all_roots)
    merged_base_measures = _get_merged_fields(all_roots, "measures")
    merged_calc_measures = _get_merged_fields(all_roots, "calc_measures")
    group_by_cols = list(op.keys)
    join_column_lineage, _joined_column_names = _build_join_column_lineage(join_op)
    wrapper_local_dimensions = {
        name: dimension
        for name, dimension in _join_wrapper_local_dimensions(all_roots).items()
        if name in group_by_cols
    }
    wrapper_dimension_owners: dict[str, str | None] = {}

    # --- 1. Try to build the full joined table (for scope / dim bridge) ---
    # Pre-agg needs all tables for dimension bridges — no pruning here.
    try:
        joined_base_tbl = join_op.to_untagged(parent_requirements=None)
    except Exception:
        joined_base_tbl = None
        tbl = None  # chasm / column collision – work without full join

    if joined_base_tbl is not None:
        if wrapper_local_dimensions:
            wrapper_dimension_owners, wrapper_dimension_errors = (
                _infer_join_wrapper_dimension_owners(
                    wrapper_local_dimensions,
                    joined_base_tbl,
                    merged_dimensions,
                    join_column_lineage,
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
            tbl = _mutate_dimensions_with_dependencies(
                joined_base_tbl,
                [k for k in op.keys if k in merged_dimensions],
                merged_dimensions,
            )
        except Exception:
            tbl = None  # dimension materialization fallback

    if joined_base_tbl is None and wrapper_local_dimensions:
        raise ValueError(
            "Cannot source-preaggregate by a join-wrapper dimension because "
            "the flattened join is unavailable for source-lineage analysis."
        )

    if tbl is not None:
        _reject_unresolvable_group_keys(op.keys, merged_dimensions, tbl, all_roots)

    # Apply collected filters to the full joined table so that
    # dimension bridges only include rows surviving the filter. A
    # filter that fails to resolve here may still be pushed to its
    # owning source table below; anything handled by neither path
    # raises instead of silently dropping the filter.
    filters_on_tbl: set[int] = set()
    tbl_filter_exprs: dict[int, Any] = {}
    if tbl is not None and filter_fns:
        from ..convert import _Resolver

        # Bare aliases for prefixed dims: a filter written `t.size`
        # against a join where exactly one table declares `size` must
        # resolve on the joined table too — otherwise the dim bridge is
        # built from the UNFILTERED join and sibling tables' measures
        # silently ignore the filter. Physical columns keep priority
        # (aliases are only added for names that are not columns of the
        # joined table), and ambiguous suffixes get no alias so they
        # still hit the loud ownership check below.
        dims_for_tbl = dict(merged_dimensions)
        tbl_cols = set(tbl.columns)
        _by_suffix: dict[str, list[str]] = {}
        for dname in merged_dimensions:
            if "." in dname:
                _by_suffix.setdefault(dname.split(".", 1)[1], []).append(dname)
        for short, fulls in _by_suffix.items():
            if short not in dims_for_tbl and short not in tbl_cols and len(fulls) == 1:
                dims_for_tbl[short] = merged_dimensions[fulls[0]]

        for i, pred_fn in enumerate(filter_fns):
            try:
                resolver = _Resolver(tbl, dims_for_tbl)
                pred_expr = _resolve_expr(pred_fn, resolver)
                filtered = tbl.filter(pred_expr)
            except Exception:
                continue
            tbl = filtered
            filters_on_tbl.add(i)
            tbl_filter_exprs[i] = pred_expr

    return _PreaggScope(
        op=op,
        join_op=join_op,
        join_tree_info=join_tree_info,
        all_roots=all_roots,
        filter_fns=filter_fns,
        exact_filter_fields=exact_filter_fields,
        merged_dimensions=merged_dimensions,
        merged_base_measures=merged_base_measures,
        merged_calc_measures=merged_calc_measures,
        group_by_cols=group_by_cols,
        join_column_lineage=join_column_lineage,
        wrapper_local_dimensions=wrapper_local_dimensions,
        wrapper_dimension_owners=wrapper_dimension_owners,
        tbl=tbl,
        filters_on_tbl=filters_on_tbl,
        tbl_filter_exprs=tbl_filter_exprs,
    )


def _resolve_filter_ownership(scope: _PreaggScope) -> tuple[dict, list]:
    """Phase 1b: attribute each filter to the source table(s) it resolves on."""
    filter_fns = scope.filter_fns
    join_tree_info = scope.join_tree_info
    filters_on_tbl = scope.filters_on_tbl
    # --- 1b. Determine which source table(s) each filter belongs to ---
    # Ownership resolution uses each table's own dimensions (bare and
    # table-prefixed) so ``t["orders.status"]`` resolves only against
    # ``orders``. A filter owned by exactly one table is pushed to that
    # table's raw table; a filter resolving against several tables is
    # ambiguous and is only applied through the filtered joined table.
    raw_tables: dict = {}
    filter_owners: list[frozenset] = []
    if filter_fns:
        for tname, top in join_tree_info.table_ops.items():
            try:
                raw_tables[tname] = _to_untagged(top)
            except Exception:
                continue
        for pred_fn in filter_fns:
            owners = set()
            for tname, top in join_tree_info.table_ops.items():
                raw = raw_tables.get(tname)
                if raw is None:
                    continue
                try:
                    _resolve_expr(
                        pred_fn,
                        _table_filter_resolver(
                            raw,
                            top,
                            tname,
                            _exact_filter_fields(pred_fn),
                        ),
                    )
                    owners.add(tname)
                except Exception:
                    pass
            filter_owners.append(frozenset(owners))
        for i, owners in enumerate(filter_owners):
            if i in filters_on_tbl or len(owners) == 1:
                continue
            if len(owners) > 1:
                raise ValueError(
                    f"Filter #{i} is ambiguous: it resolves against multiple "
                    f"joined tables ({', '.join(sorted(owners))}) and could "
                    "not be applied to the full joined table. Qualify the "
                    'field with a table prefix (e.g. t["orders.status"]).'
                )
            raise ValueError(
                f"Filter #{i} does not resolve against the joined table or "
                "any single source table; it would be silently ignored. "
                "Check the dimension/column name, or qualify it with a "
                'table prefix (e.g. t["orders.status"]).'
            )
    return raw_tables, filter_owners


def _split_cross_table_legs(scope: _PreaggScope, raw_tables: dict, filter_owners: list) -> dict:
    """Phase 1c: split cross-table AND conjunctions into per-table legs."""
    tbl_filter_exprs = scope.tbl_filter_exprs
    # --- 1c. Split cross-table conjunctions into per-table legs ---
    # A compound like (t["orders.status"]=="open") & (t.qty >= 2) has no
    # single owner, so it used to reach the many side only through a
    # join-KEY bridge, keeping every item of any qualifying order — the
    # item-level leg was silently dropped. Split top-level ANDs and track
    # each leg's source tables via field provenance so legs can be pushed
    # row-precisely to the table they constrain.
    filter_legs: dict[int, list] = {}
    if tbl_filter_exprs:
        leaf_types = _leaf_rel_types()
        base_rel_to_table: dict = {}
        for tname, raw in raw_tables.items():
            for leaf in walk_nodes(leaf_types, raw):
                key = _base_rel_key(leaf)
                # Same physical table on both sides (op-join): a leg
                # can't be attributed to one alias — never match.
                if base_rel_to_table.get(key, tname) != tname:
                    base_rel_to_table[key] = "__ambiguous__"
                else:
                    base_rel_to_table[key] = tname
        for i, expr in tbl_filter_exprs.items():
            if filter_owners[i] and len(filter_owners[i]) == 1:
                continue  # whole filter pushes to its single owner
            filter_legs[i] = [
                (leg, _leg_source_tables(leg, base_rel_to_table, leaf_types))
                for leg in _flatten_and_legs(expr)
            ]
    return filter_legs


def _build_plan(scope: _PreaggScope):
    """Phase 2: build the aggregation plan (or its chasm-fallback shape)."""
    op = scope.op
    tbl = scope.tbl
    merged_base_measures = scope.merged_base_measures
    merged_calc_measures = scope.merged_calc_measures
    # --- 2. Build aggregation plan ---
    if tbl is not None:
        scope = MeasureScope(
            _tbl=tbl,
            _known=list(merged_base_measures.keys()) + list(merged_calc_measures.keys()),
        )
        plan = _build_aggregation_plan(
            aggs=op.aggs,
            keys=op.keys,
            scope=scope,
            is_post_agg=False,
            merged_base_measures=merged_base_measures,
            merged_calc_measures=merged_calc_measures,
            tbl=tbl,
        )
    else:
        # Derive plan directly from metadata (chasm fallback)
        agg_specs = {}
        for name in op.aggs:
            if name in merged_base_measures:
                agg_specs[name] = _make_agg_callable(merged_base_measures[name])
        plan = _AggregationPlan(
            agg_specs=FrozenDict(agg_specs),
            calc_specs=FrozenDict({}),
            requested_measures=tuple(op.aggs.keys()),
            group_by_cols=tuple(op.keys),
        )
    return plan


def _partition_by_source(scope: _PreaggScope, plan) -> dict:
    """Phase 3: partition agg specs by owning source table."""
    join_op = scope.join_op
    join_tree_info = scope.join_tree_info
    merged_base_measures = scope.merged_base_measures
    # --- 3. Partition agg_specs by source table ---
    # Partition by the join's per-table roots, not ``all_roots``. When a
    # wrapper SemanticTableOp from ``SemanticJoin.with_measures()`` /
    # ``with_dimensions()`` is in the tree, ``all_roots`` collapses to a
    # single name=None root, so prefixed aggregates would all fall into the
    # unprefixed bucket and bypass per-grain pre-aggregation.
    partition_roots = _find_all_root_models(join_op)
    partitioned = _partition_agg_specs_by_source(dict(plan.agg_specs), partition_roots)
    # Measures declared on a joined wrapper have unprefixed result names.
    # Route field-bearing reductions back to their unique owning leaf so
    # they do not aggregate the fanned-out join. Relation-only reductions
    # (notably t.count()) remain explicit joined-row-grain measures.
    wrapper_specs = partitioned.get(None, {})
    for measure_name, measure_fn in tuple(wrapper_specs.items()):
        measure_obj = merged_base_measures.get(measure_name)
        if not isinstance(measure_obj, Measure):
            continue
        owner = _infer_join_wrapper_measure_owner(measure_obj, join_tree_info)
        if owner is None:
            continue
        partitioned.setdefault(owner, {})[measure_name] = measure_fn
        del wrapper_specs[measure_name]
    if not wrapper_specs:
        partitioned.pop(None, None)
    return partitioned


def to_untagged_with_preagg(
    op,
    all_roots: list,
    join_op: SemanticJoinOp,
    join_tree_info: _JoinTreeInfo,
    filters: list | None = None,
):
    """Pre-aggregate each source table's measures at its own grain, then join.

    This prevents fan-out inflation when ``join_many`` is used.
    """
    scope = _build_scope(op, all_roots, join_op, join_tree_info, filters)
    filter_fns = scope.filter_fns
    merged_dimensions = scope.merged_dimensions
    merged_base_measures = scope.merged_base_measures
    group_by_cols = scope.group_by_cols
    join_column_lineage = scope.join_column_lineage
    wrapper_local_dimensions = scope.wrapper_local_dimensions
    wrapper_dimension_owners = scope.wrapper_dimension_owners
    tbl = scope.tbl

    raw_tables, filter_owners = _resolve_filter_ownership(scope)
    filter_legs = _split_cross_table_legs(scope, raw_tables, filter_owners)
    plan = _build_plan(scope)
    partitioned = _partition_by_source(scope, plan)

    # --- 4. Pre-aggregate each source table on its raw table ---
    _preagg_results: list = []
    # Track MEAN measures decomposed into SUM + COUNT for correct re-agg
    _decomposed_means: dict[str, tuple[str, str]] = {}
    # Track correct re-aggregation op per measure (default "sum")
    _reagg_ops: dict[str, str] = {}
    # COUNT reductions produce zero for an empty group. A source-level
    # pre-aggregate has no row for an unmatched outer-join group, so its
    # later dimension-bridge join must restore that identity explicitly.
    _empty_count_measures: set[str] = set()
    # Track COUNT DISTINCT measures deferred past pre-aggregation.
    # Value: (table_name, short_name, raw_tbl, measure_fn,
    #         {joined_group_name: materialized_raw_column})
    _deferred_count_distincts: dict[str, tuple] = {}
    # Fan-out-safe totals sources for t.all(...): per table, the
    # filtered raw table plus the original (undecomposed) measure
    # expressions, aggregated at zero grain on first need.
    _totals_sources: dict = {}

    for table_name, measures in partitioned.items():
        if table_name is None:
            # Unprefixed – aggregate on the full join if available
            if tbl is not None:
                agg_exprs = {n: f(tbl) for n, f in measures.items()}
                _totals_sources[None] = (tbl, dict(agg_exprs))
                if group_by_cols:
                    r = tbl.group_by([tbl[c] for c in group_by_cols]).aggregate(**agg_exprs)
                else:
                    r = tbl.aggregate(**agg_exprs)
                _preagg_results.append(r)
            continue

        table_op = join_tree_info.table_ops.get(table_name)
        if table_op is None:
            continue

        raw_tbl = _to_untagged(table_op)
        source_key_names = join_column_lineage.get(table_name, {})

        # Push filters owned by this table onto its raw table. Filters
        # handled elsewhere (applied to the full joined table, or owned
        # by another table) reach this table via a join-key bridge.
        needs_bridge = False
        if filter_fns:
            residual_cross_legs = False
            for i, pred_fn in enumerate(filter_fns):
                if filter_owners[i] == frozenset({table_name}):
                    pred_expr = _resolve_expr(
                        pred_fn,
                        _table_filter_resolver(
                            raw_tbl,
                            table_op,
                            table_name,
                            _exact_filter_fields(pred_fn),
                        ),
                    )
                    raw_tbl = raw_tbl.filter(pred_expr)
                    continue
                needs_bridge = True
                # Push this table's legs of a cross-table conjunction at
                # row grain; legs spanning tables (cross-table OR) keep
                # row-level information the key bridge cannot recover.
                for leg_expr, leg_srcs in filter_legs.get(i, ()):
                    if leg_srcs == {table_name}:
                        try:
                            leg_op = _inline_to_base_op(
                                leg_expr.op(), _leaf_rel_types(), target_tbl=raw_tbl
                            )
                            raw_tbl = raw_tbl.filter(leg_op.to_expr())
                        except Exception:
                            residual_cross_legs = True
                    elif table_name in leg_srcs and len(leg_srcs) > 1:
                        residual_cross_legs = True

            if residual_cross_legs and measures:
                raise ValueError(
                    f"A filter mixes columns of {table_name!r} with other "
                    "tables in a way that cannot be applied row-precisely "
                    f"to {table_name!r} (e.g. OR across tables); its "
                    "measures would be silently inflated to join-key "
                    "grain. Split the condition into separate .filter() "
                    "calls, or restate it against a single table."
                )

        # Rows of every non-root/right table whose join keys are NULL or
        # match no left-side row never appear in a LEFT JOIN. Restrict
        # both join_many and join_one measure legs to actual participants;
        # otherwise scalar right measures include orphan source rows and
        # flattened CountStar reductions count unmatched left rows.
        needs_participation = join_tree_info.table_cardinalities.get(table_name) not in (
            "root",
            "cross",
        ) and bool(measures)

        # Filters not pushed here (cross-table, ambiguous, or owned
        # by another table) restrict via join keys from the filtered
        # full joined table, or from the owning table's raw table.
        if needs_bridge or needs_participation:
            jk = join_tree_info.table_join_keys.get(table_name, set())
            if tbl is not None:
                shared = _source_join_key_pairs(
                    table_name,
                    jk,
                    raw_tbl.columns,
                    tbl.columns,
                    join_column_lineage,
                )
                if shared:
                    key_bridge = tbl.select(
                        [tbl[joined].name(raw) for raw, joined in shared]
                    ).distinct()
                    preds = [raw_tbl[raw] == key_bridge[raw] for raw, _joined in shared]
                    raw_tbl = raw_tbl.inner_join(key_bridge, preds).select(raw_tbl)
                elif needs_participation:
                    raise ValueError(
                        f"Measures on {table_name!r} cannot be restricted "
                        "to rows that participate in its join: no "
                        "join-key column is available on both the raw "
                        "table and the joined table. Computing them on "
                        "the raw table would silently count rows the "
                        "join can never produce."
                    )
            else:
                # Chasm fallback: restrict participation via the raw
                # keys of root-side tables that share join-key columns.
                participation_bridged = not needs_participation
                if needs_participation:
                    for root_name, card in join_tree_info.table_cardinalities.items():
                        if card != "root":
                            continue
                        root_op = join_tree_info.table_ops.get(root_name)
                        if root_op is None:
                            continue
                        try:
                            root_raw = _to_untagged(root_op)
                        except Exception:
                            continue
                        root_jk = join_tree_info.table_join_keys.get(root_name, set())
                        shared = sorted(jk & root_jk & set(raw_tbl.columns) & set(root_raw.columns))
                        if shared:
                            key_bridge = root_raw.select([root_raw[c] for c in shared]).distinct()
                            preds = [raw_tbl[c] == key_bridge[c] for c in shared]
                            raw_tbl = raw_tbl.inner_join(key_bridge, preds).select(raw_tbl)
                            participation_bridged = True
                if not participation_bridged:
                    raise ValueError(
                        f"Measures on {table_name!r} cannot be restricted "
                        "to rows that participate in its join: the "
                        "full joined table is unavailable (chasm fallback) "
                        "and no join-key column is shared with the root "
                        "table. Computing them on the raw table would "
                        "silently count rows the join can never produce."
                    )
                # Chasm fallback: restrict via each owning table's keys
                for i, pred_fn in enumerate(filter_fns):
                    owners = filter_owners[i]
                    if table_name in owners or len(owners) != 1:
                        continue
                    (owner_name,) = owners
                    owner_op = join_tree_info.table_ops.get(owner_name)
                    owner_raw = raw_tables.get(owner_name)
                    if owner_op is None or owner_raw is None:
                        continue
                    owner_raw = owner_raw.filter(
                        _resolve_expr(
                            pred_fn,
                            _table_filter_resolver(
                                owner_raw,
                                owner_op,
                                owner_name,
                                _exact_filter_fields(pred_fn),
                            ),
                        )
                    )
                    owner_jk = join_tree_info.table_join_keys.get(owner_name, set())
                    shared = sorted(jk & owner_jk & set(raw_tbl.columns) & set(owner_raw.columns))
                    if shared:
                        key_bridge = owner_raw.select([owner_raw[c] for c in shared]).distinct()
                        preds = [raw_tbl[c] == key_bridge[c] for c in shared]
                        raw_tbl = raw_tbl.inner_join(key_bridge, preds).select(raw_tbl)

        table_measures = _get_field_dict(table_op, "measures")
        table_dims = _get_field_dict(table_op, "dimensions")
        raw_columns = set(raw_tbl.columns)

        # Build agg expressions on the raw table
        measure_binding_op = _to_op(raw_tbl)
        agg_exprs: dict = {}
        _tot_exprs: dict = {}
        _exact_measures_t: dict = {}
        _nested_measures_t: dict = {}
        for mname, _mfn in measures.items():
            short = mname.split(".", 1)[1] if "." in mname else mname
            source_measure = (
                table_measures.get(short) if "." in mname else merged_base_measures.get(mname)
            )
            if isinstance(source_measure, Measure):
                expr = source_measure(raw_tbl)
                if isinstance(expr, NestedAccessMarker):
                    # Nested arrays are a separate aggregation grain, not
                    # ibis reductions that can be inspected via `.op()`.
                    # Keep the original source-bound measure for exact
                    # target-grain compilation after group-key routing.
                    _nested_measures_t[mname] = source_measure
                    _tot_exprs[mname] = expr
                    if expr.operation in {"count", "nunique"}:
                        _empty_count_measures.add(mname)
                    continue
                if _is_count_expr(expr):
                    _empty_count_measures.add(mname)
                # Original expression on the filtered raw table: at zero
                # grain this is fan-out-safe, so it powers t.all(...) totals
                _tot_exprs[mname] = expr
                # Decompose MEAN into SUM + COUNT for correct re-aggregation
                if _is_mean_expr(expr):
                    mean_op = expr.op()
                    base_col = mean_op.arg.to_expr()
                    # mean(where=...) must filter both legs, or the
                    # decomposed mean silently ignores its condition
                    mean_where = mean_op.where.to_expr() if mean_op.where is not None else None
                    sum_col = f"_sum__{mname}"
                    count_col = f"_count__{mname}"
                    agg_exprs[sum_col] = base_col.sum(where=mean_where)
                    agg_exprs[count_col] = base_col.count(where=mean_where)
                    _decomposed_means[mname] = (sum_col, count_col)
                elif _is_count_distinct_expr(expr):
                    # COUNT DISTINCT is immune to fan-out — defer past pre-agg
                    _deferred_count_distincts[mname] = (
                        table_name,
                        short,
                        raw_tbl,
                        source_measure,
                        {},
                    )
                else:
                    reagg = _reagg_op_for_expr(expr)
                    if reagg is None and group_by_cols:
                        # Non-decomposable (median, stddev, compound
                        # ratio): computed at the exact target grain
                        # after the grain decision below
                        _exact_measures_t[mname] = source_measure
                    else:
                        if reagg is not None:
                            _reagg_ops[mname] = reagg
                        agg_exprs[mname] = expr

        if _tot_exprs:
            _totals_sources[table_name] = (raw_tbl, _tot_exprs)

        # --- Compute grain ---
        if not group_by_cols:
            if not agg_exprs and not _exact_measures_t and not _nested_measures_t:
                continue
            # No group-by → scalar aggregate
            if agg_exprs:
                pt = raw_tbl.aggregate(**agg_exprs)
                # Recompute MEAN from SUM/COUNT for scalar results
                for mname, (sc, cc) in _decomposed_means.items():
                    if sc in pt.columns and cc in pt.columns:
                        pt = pt.mutate(**{mname: pt[sc] / pt[cc]})
                        pt = pt.drop(sc, cc)
                _preagg_results.append(pt)
            if _nested_measures_t:
                _preagg_results.append(
                    _compile_exact_measure_table(raw_tbl, (), _nested_measures_t)
                )
            continue

        # a) group-by dims that live on this table
        _local_dims = []
        _local_group_keys: dict[str, str] = {}
        # Derived dimensions are materialized under private, collision-safe
        # raw aliases. Generic preaggregates rename those aliases back to
        # the public semantic group keys before joining the dimension
        # bridge; exact reductions use `_local_group_keys` directly.
        _local_group_outputs: dict[str, str] = {}
        has_cross_table_gb = False
        for gb_key in group_by_cols:
            if "." in gb_key:
                prefix, short = gb_key.split(".", 1)
                if prefix == table_name and short in table_dims:
                    dim_fn = table_dims[short]
                    if callable(dim_fn):
                        resolved_via_deps = False
                        try:
                            dim_expr = dim_fn(raw_tbl)
                        except Exception:
                            # Derived dim referencing other derived dims
                            raw_tbl = _mutate_dimensions_with_dependencies(
                                raw_tbl, [short], table_dims
                            )
                            raw_columns = set(raw_tbl.columns)
                            dim_expr = raw_tbl[short]
                            resolved_via_deps = True
                        if not resolved_via_deps and _is_direct_physical_field(
                            dim_expr, raw_tbl, short
                        ):
                            # Simple column reference — use directly
                            if short not in _local_dims:
                                _local_dims.append(short)
                            _local_group_keys[gb_key] = short
                        else:
                            # `get_name()` is deliberately insufficient
                            # here: `t.kind.upper()` can still be named
                            # "kind". Materialize every non-Field expression
                            # under an allocator-owned alias so neither the
                            # source schema nor user internal-prefix columns
                            # can be overwritten.
                            raw_group_name = _allocate_local_group_alias(gb_key, raw_columns)
                            raw_tbl = raw_tbl.mutate(**{raw_group_name: dim_expr})
                            raw_columns = set(raw_tbl.columns)
                            if raw_group_name not in _local_dims:
                                _local_dims.append(raw_group_name)
                            _local_group_keys[gb_key] = raw_group_name
                            _local_group_outputs[raw_group_name] = gb_key
                elif prefix == table_name and short in raw_columns:
                    # A qualified raw column is a valid source-local
                    # dimension even when it was not declared explicitly.
                    # Keep it at its owning table's grain; falling through
                    # to join-key grain would repeat a coarser aggregate
                    # once per dimension value during bridge re-joining.
                    if short not in _local_dims:
                        _local_dims.append(short)
                    _local_group_keys[gb_key] = short
                elif prefix != table_name:
                    has_cross_table_gb = True
            elif gb_key in merged_dimensions:
                # Unprefixed derived dimension (e.g. registered on the
                # join wrapper via with_dimensions, or lowered from a
                # pre-aggregation .mutate()). Materialize it on this
                # raw table when its expression resolves here so the
                # per-table grain matches the requested grouping;
                # tables that lack the source columns fall back to
                # join keys + the dimension bridge.
                if gb_key in wrapper_local_dimensions:
                    wrapper_owner = wrapper_dimension_owners.get(gb_key)
                    if wrapper_owner != table_name:
                        # Constants and dimensions owned by another leaf
                        # are attached through the full joined dimension
                        # bridge. Never re-evaluate the wrapper definition
                        # against a same-named raw column on this source.
                        has_cross_table_gb = True
                        continue

                    dim_fn = wrapper_local_dimensions[gb_key]
                    joined_to_raw = {
                        joined_name: raw_name
                        for raw_name, joined_name in join_column_lineage.get(table_name, {}).items()
                    }
                    owner_scope = _RenamedResolver(raw_tbl, joined_to_raw)
                    dimension_scope = _JoinWrapperDimensionResolver(
                        owner_scope,
                        dict(merged_dimensions),
                        resolving=(gb_key,),
                    )
                    try:
                        dim_expr = dim_fn(
                            dimension_scope,
                            _dims=dict(merged_dimensions),
                        )
                    except Exception as exc:
                        raise ValueError(
                            f"Join-wrapper dimension {gb_key!r} was bound to "
                            f"semantic model {table_name!r} but could not be "
                            "evaluated in that model's raw namespace."
                        ) from exc

                    direct_raw_name = next(
                        (
                            column
                            for column in raw_columns
                            if _is_direct_physical_field(dim_expr, raw_tbl, column)
                        ),
                        None,
                    )
                    if direct_raw_name is not None:
                        if direct_raw_name not in _local_dims:
                            _local_dims.append(direct_raw_name)
                        _local_group_keys[gb_key] = direct_raw_name
                    else:
                        raw_group_name = _allocate_local_group_alias(gb_key, raw_columns)
                        raw_tbl = raw_tbl.mutate(**{raw_group_name: dim_expr})
                        raw_columns = set(raw_tbl.columns)
                        _local_dims.append(raw_group_name)
                        _local_group_keys[gb_key] = raw_group_name
                        _local_group_outputs[raw_group_name] = gb_key
                elif gb_key in raw_columns:
                    if gb_key not in _local_dims:
                        _local_dims.append(gb_key)
                    _local_group_keys[gb_key] = gb_key
                else:
                    dim_fn = merged_dimensions[gb_key]
                    try:
                        dim_expr = (
                            dim_fn(raw_tbl) if callable(dim_fn) else _resolve_expr(dim_fn, raw_tbl)
                        )
                        raw_tbl = raw_tbl.mutate(**{gb_key: dim_expr})
                        raw_columns = set(raw_tbl.columns)
                        if gb_key not in _local_dims:
                            _local_dims.append(gb_key)
                        _local_group_keys[gb_key] = gb_key
                    except Exception:
                        has_cross_table_gb = True

        # COUNT DISTINCT is evaluated after all source pre-aggregates are
        # combined. Carry the final (possibly dimension-materialized) raw
        # relation and its local group mapping into that exact-grain path.
        for mname in measures:
            deferred = _deferred_count_distincts.get(mname)
            if deferred is None:
                continue
            src_name, short, _old_raw, source_measure, _old_local = deferred
            _deferred_count_distincts[mname] = (
                src_name,
                short,
                raw_tbl,
                source_measure,
                dict(_local_group_keys),
            )

        if not agg_exprs and not _exact_measures_t and not _nested_measures_t:
            continue

        join_keys = join_tree_info.table_join_keys.get(table_name, set())
        available_jk = tuple(jk for jk in sorted(join_keys) if jk in raw_columns)

        # b) if none found, use join keys; if cross-table gb, augment with them
        match (_local_dims, has_cross_table_gb):
            case ([], _):
                grain = available_jk
            case (_, True):
                grain = tuple(
                    dict.fromkeys(
                        _local_dims + [jk for jk in available_jk if jk not in _local_dims]
                    )
                )
            case _:
                grain = tuple(_local_dims)

        # Materializing a derived group dimension above replaces raw_tbl
        # with a Project relation.  Reductions were built before that
        # projection; relation-argument reductions such as CountStar keep
        # pointing at the old table and fail ibis's aggregate integrity
        # check.  Rebind every reduction to the final relation used by the
        # group-by.  Field-based reductions need the same treatment for
        # consistency, even though ibis can sometimes dereference them
        # through a projection automatically.
        final_raw_op = _to_op(raw_tbl)
        if final_raw_op is not measure_binding_op:
            agg_exprs = {
                name: _to_op(expr).replace({measure_binding_op: final_raw_op}).to_expr()
                for name, expr in agg_exprs.items()
            }

        if _exact_measures_t:
            exact_needs_source_spine = join_tree_info.table_cardinalities.get(table_name) not in (
                "root",
                "cross",
            )
            if not has_cross_table_gb and not exact_needs_source_spine:
                # Local grain IS the target grain — aggregate the
                # original root/cross expressions directly, no re-agg
                # happens. Non-root sources still use the exact bridge so
                # unmatched LEFT JOIN groups receive the expression's
                # actual empty-set value rather than blanket NULL.
                for m, fn in _exact_measures_t.items():
                    agg_exprs[m] = fn(raw_tbl)
            else:
                _preagg_results.append(
                    _exact_grain_preagg(
                        raw_tbl,
                        tbl,
                        group_by_cols,
                        available_jk,
                        _exact_measures_t,
                        joined_key_names=source_key_names,
                        local_group_keys=_local_group_keys,
                    )
                )

        if _nested_measures_t:
            nested_needs_source_spine = join_tree_info.table_cardinalities.get(table_name) not in (
                "root",
                "cross",
            )
            if not has_cross_table_gb and not nested_needs_source_spine and grain:
                # A root/cross source grouped only by its own dimensions
                # is already at the requested grain. Compile nested
                # measures directly there and keep them separate from the
                # regular preaggregate so sibling joins cannot fan out the
                # unnested rows.
                nested_pt = _compile_exact_measure_table(raw_tbl, grain, _nested_measures_t)
                local_renames = {
                    public_name: raw_name
                    for raw_name, public_name in _local_group_outputs.items()
                    if raw_name in nested_pt.columns
                }
                collisions = sorted(
                    set(local_renames) & (set(nested_pt.columns) - set(local_renames.values()))
                )
                if collisions:
                    raise ValueError(
                        "Cannot restore semantic group-key names after nested "
                        "source preaggregation because they collide with "
                        f"aggregate columns: {collisions}. Rename the aggregate field."
                    )
                if local_renames:
                    nested_pt = nested_pt.rename(local_renames)
                joined_grain = tuple(_local_group_outputs.get(name, name) for name in grain)
                _preagg_results.append(
                    _rename_preagg_grain_to_joined_aliases(
                        nested_pt, joined_grain, source_key_names
                    )
                )
            else:
                # Cross-source dimensions and non-root sources need the
                # joined group-domain spine. This also restores the true
                # empty-set value for unmatched LEFT JOIN groups.
                _preagg_results.append(
                    _exact_grain_preagg(
                        raw_tbl,
                        tbl,
                        group_by_cols,
                        available_jk,
                        _nested_measures_t,
                        joined_key_names=source_key_names,
                        local_group_keys=_local_group_keys,
                    )
                )

        if agg_exprs:
            if grain:
                pt = raw_tbl.group_by([raw_tbl[c] for c in grain]).aggregate(**agg_exprs)
                local_renames = {
                    public_name: raw_name
                    for raw_name, public_name in _local_group_outputs.items()
                    if raw_name in pt.columns
                }
                collisions = sorted(
                    set(local_renames) & (set(pt.columns) - set(local_renames.values()))
                )
                if collisions:
                    raise ValueError(
                        "Cannot restore semantic group-key names after source "
                        "preaggregation because they collide with aggregate "
                        f"columns: {collisions}. Rename the aggregate field."
                    )
                if local_renames:
                    pt = pt.rename(local_renames)
                joined_grain = tuple(_local_group_outputs.get(name, name) for name in grain)
                _preagg_results.append(
                    _rename_preagg_grain_to_joined_aliases(pt, joined_grain, source_key_names)
                )
            else:
                _preagg_results.append(raw_tbl.aggregate(**agg_exprs))

    # Freeze mutable accumulators
    preagg_results = tuple(_preagg_results)
    decomposed_means = tuple(_decomposed_means.items())
    reagg_ops = tuple(_reagg_ops.items())
    empty_count_measures = tuple(_empty_count_measures)

    if not preagg_results and not _deferred_count_distincts:
        if tbl is None:
            raise ValueError("No aggregation results and full join unavailable")
        # Nothing could be pre-aggregated at a source grain. This fallback
        # used to aggregate the flattened join while ignoring both the
        # group keys and every calc spec: with only calc measures
        # requested it returned ``tbl.aggregate({})`` — an Aggregate with
        # no columns at all, which surfaces much later as an unrelated
        # arrow/schema error instead of naming the problem.
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
                f"Pre-aggregation produced no measures for {sorted(op.aggs)} "
                f"with group keys {list(plan.group_by_cols)}; aggregating the "
                "joined table here would ignore the request entirely."
            )
        specs = {n: f(tbl) for n, f in plan.agg_specs.items()}
        group_cols = [c for c in plan.group_by_cols if c in tbl.columns]
        if group_cols:
            return tbl.group_by(group_cols).aggregate(**specs)
        return tbl.aggregate(specs)

    # --- 5. Combine pre-agg results ---
    result = None
    if preagg_results:
        if not group_by_cols:
            # Cross-join all scalar results
            result = preagg_results[0]
            for pt in preagg_results[1:]:
                result = result.cross_join(pt)
        elif tbl is not None:
            result = _join_preagg_with_dim_bridge(
                preagg_results,
                plan,
                tbl,
                group_by_cols,
                decomposed_means=decomposed_means,
                reagg_ops=reagg_ops,
                empty_count_measures=empty_count_measures,
            )
        else:
            # Chasm fallback with group-by: build minimal dim bridge from raw tables
            result = _build_minimal_dim_bridge(
                preagg_results,
                plan,
                group_by_cols,
                join_tree_info,
                merged_dimensions,
                decomposed_means=decomposed_means,
                reagg_ops=reagg_ops,
                empty_count_measures=empty_count_measures,
            )

    # --- 5b. Compute deferred COUNT DISTINCT measures ---
    # COUNT DISTINCT cannot be re-aggregated from per-key partial counts.
    # For grouped queries, bridge the requested group domain to the
    # measure's owning raw source and evaluate the original expression
    # there. Re-evaluating it directly on the flattened join loses source
    # provenance when physical column names collide (e.g. both sides have
    # ``id``), and can count an unmatched left key as a right-side value.
    if _deferred_count_distincts:
        cd_parts: list = []
        join_column_lineage, _joined_columns = _build_join_column_lineage(join_op)
        for mname, (
            src_tbl_name,
            _short,
            src_raw,
            src_fn,
            local_group_keys,
        ) in _deferred_count_distincts.items():
            if not group_by_cols:
                cd_parts.append(src_raw.aggregate(**{mname: src_fn(src_raw)}))
                continue

            if tbl is None:
                raise ValueError(
                    "COUNT DISTINCT measures require the full joined table "
                    "for grouped source-aware aggregation but it is unavailable "
                    "(chasm fallback)."
                )
            join_keys = join_tree_info.table_join_keys.get(src_tbl_name, set())
            source_key_names = join_column_lineage.get(src_tbl_name, {})
            available_jk = tuple(
                key
                for key in sorted(join_keys)
                if key in src_raw.columns and source_key_names.get(key, key) in tbl.columns
            )
            if not available_jk:
                raise ValueError(
                    f"COUNT DISTINCT measure {mname!r} cannot be attached to "
                    "the requested group grain without a shared join key."
                )

            cd_exact = _exact_grain_preagg(
                src_raw,
                tbl,
                group_by_cols,
                available_jk,
                {mname: src_fn},
                joined_key_names=source_key_names,
                local_group_keys=local_group_keys,
            )
            # The exact source aggregate has no row for an unmatched
            # outer-join group. Attach it to the joined group domain and
            # restore COUNT DISTINCT's empty-set identity before calc
            # measures are compiled.
            group_spine = tbl.select([tbl[c] for c in group_by_cols]).distinct()
            predicates = [null_safe_equal(group_spine[c], cd_exact[c]) for c in group_by_cols]
            cd_pt = group_spine.left_join(cd_exact, predicates).select(
                [group_spine] + [cd_exact[mname]]
            )
            cd_parts.append(_fill_missing_count_identities(cd_pt, (mname,)))

        # Merge count-distinct parts into result
        for cd_pt in cd_parts:
            cd_meas = [c for c in cd_pt.columns if c in _deferred_count_distincts]
            cd_grain = [c for c in cd_pt.columns if c not in _deferred_count_distincts]
            if result is None:
                result = cd_pt
            elif cd_grain:
                common = [c for c in cd_grain if c in result.columns]
                if common:
                    preds = [null_safe_equal(result[c], cd_pt[c]) for c in common]
                    result = result.left_join(cd_pt, preds).select(
                        [result] + [cd_pt[m] for m in cd_meas]
                    )
                else:
                    result = result.cross_join(cd_pt)
            else:
                result = result.cross_join(cd_pt)

        result = _fill_missing_count_identities(result, _deferred_count_distincts)

    # --- 6. Apply calc_specs ---
    if plan.calc_specs:

        def _fanout_safe_totals():
            """Zero-grain totals from per-table raw aggregates.

            Aggregating each (filtered) source table without grouping
            cannot fan out, so ``t.all(...)`` denominators stay correct
            under join_many.
            """
            parts = [
                _compile_evaluated_measure_table(src, (), texprs)
                for src, texprs in _totals_sources.values()
                if texprs
            ]
            if not parts:
                return None
            total = parts[0]
            for p in parts[1:]:
                total = total.cross_join(p)
            return total

        result = _apply_calc_specs(result, plan, tbl, totals_builder=_fanout_safe_totals)

    # --- 7. Select requested columns ---
    available = frozenset(result.columns)
    requested = tuple(
        dict.fromkeys((*plan.group_by_cols, *plan.requested_measures, *plan.calc_specs.keys()))
    )
    missing = [c for c in requested if c not in available]
    if missing:
        # Dropping the missing columns would return a result that silently
        # ignores part of the query (e.g. cross-joined models have no
        # dimension bridge, so group keys from the other side never get
        # attached to a pre-aggregated measure leg).
        raise ValueError(
            f"Pre-aggregation could not attach requested column(s) {missing} "
            f"to the result; available columns: {sorted(available)}. "
            "Grouping a cross-joined model by one side's dimension while "
            "aggregating the other side's measures is not supported — "
            "restructure the query (e.g. join on an explicit key, or "
            "aggregate each side separately and combine)."
        )
    if requested:
        result = result.select([result[c] for c in requested])

    return result


def _join_preagg_with_dim_bridge(
    preagg_results,
    plan,
    tbl,
    group_by_cols,
    decomposed_means=(),
    reagg_ops=(),
    empty_count_measures=(),
):
    """Join pre-aggregated tables using per-table dimension bridges.

    ``decomposed_means`` and ``reagg_ops`` are tuples of (key, value) pairs.
    """
    from ..nested_compile import join_tables as _join_tables

    reagg_map = dict(reagg_ops)
    # Include decomposed auxiliary columns in measure names
    aux_cols = frozenset(c for _, (sc, cc) in decomposed_means for c in (sc, cc))
    measure_names = frozenset(plan.agg_specs.keys()) | frozenset(plan.calc_specs.keys()) | aux_cols
    gb_set = frozenset(group_by_cols)

    def _rejoin_one(pt):
        pt_grain = tuple(c for c in pt.columns if c not in measure_names)
        pt_meas = tuple(c for c in pt.columns if c in measure_names)

        if gb_set <= frozenset(pt_grain):
            # Already has group-by columns — re-aggregate if over-grouped
            if frozenset(pt_grain) != gb_set:
                re_aggs = {m: _build_reagg(pt[m], reagg_map.get(m, "sum")) for m in pt_meas}
                pt = pt.group_by([pt[c] for c in group_by_cols]).aggregate(**re_aggs)

            # A right/source-local preaggregate has no row for an
            # unmatched LEFT JOIN group even when it is already at the
            # requested semantic grain. Reattach it to the full joined
            # group domain so SUM/MEDIAN remain NULL and COUNT can restore
            # its zero identity later.
            group_spine = tbl.select([tbl[c] for c in group_by_cols]).distinct()
            predicates = [null_safe_equal(group_spine[c], pt[c]) for c in group_by_cols]
            return group_spine.left_join(pt, predicates).select(
                [group_spine] + [pt[c] for c in pt_meas]
            )

        if not pt_grain:
            return pt

        # Build a per-table dim bridge with ONLY this table's grain cols
        bridge_cols = tuple(
            dict.fromkeys(c for c in (*group_by_cols, *pt_grain) if c in tbl.columns)
        )
        if not bridge_cols:
            return pt

        dim_bridge = tbl.select([tbl[c] for c in bridge_cols]).distinct()
        common = tuple(c for c in pt_grain if c in dim_bridge.columns)
        if not common:
            return pt

        # Null-safe equality: a NULL group key (real NULL dim value, or
        # minted by the outer join for parents with no children) must
        # still match its pre-agg row
        preds = [null_safe_equal(dim_bridge[c], pt[c]) for c in common]
        joined_pt = dim_bridge.left_join(pt, preds).select([dim_bridge] + [pt[c] for c in pt_meas])
        gb_avail = tuple(c for c in group_by_cols if c in joined_pt.columns)
        if gb_avail:
            re_aggs = {
                m: _build_reagg(joined_pt[m], reagg_map.get(m, "sum"))
                for m in pt_meas
                if m in joined_pt.columns
            }
            if re_aggs:
                joined_pt = joined_pt.group_by([joined_pt[c] for c in gb_avail]).aggregate(
                    **re_aggs
                )
        return joined_pt

    rejoined = tuple(_rejoin_one(pt) for pt in preagg_results)
    result = _join_tables(group_by_cols, list(rejoined))

    # Recompute MEAN from decomposed SUM/COUNT after combining
    for mname, (sc, cc) in decomposed_means:
        if sc in result.columns and cc in result.columns:
            result = result.mutate(**{mname: result[sc] / result[cc]})
            result = result.drop(sc, cc)

    return _fill_missing_count_identities(result, empty_count_measures)


def _build_minimal_dim_bridge(
    preagg_results,
    plan,
    group_by_cols,
    join_tree_info,
    merged_dimensions,
    decomposed_means=(),
    reagg_ops=(),
    empty_count_measures=(),
):
    """Build dim bridges from raw tables when full join is unavailable.

    When the full ibis join fails (e.g. column collisions with 3+
    ``join_many`` arms sharing the same key), we build per-table dimension
    bridges using only the raw single-table data already captured in
    *join_tree_info*.  This avoids the ibis collision entirely because we
    never join more than two tables at once.

    ``decomposed_means`` and ``reagg_ops`` are tuples of (key, value) pairs.
    """
    from ..nested_compile import join_tables as _join_tables

    reagg_map = dict(reagg_ops)
    aux_cols = frozenset(c for _, (sc, cc) in decomposed_means for c in (sc, cc))
    measure_names = frozenset(plan.agg_specs.keys()) | frozenset(plan.calc_specs.keys()) | aux_cols
    gb_set = frozenset(group_by_cols)

    def _bridge_one_preagg(pt):
        pt_grain = tuple(c for c in pt.columns if c not in measure_names)
        pt_meas = tuple(c for c in pt.columns if c in measure_names)

        # (a) Pre-agg already carries all group-by cols — re-aggregate.
        if gb_set <= frozenset(pt_grain):
            if frozenset(pt_grain) != gb_set:
                re_aggs = {m: _build_reagg(pt[m], reagg_map.get(m, "sum")) for m in pt_meas}
                return pt.group_by([pt[c] for c in group_by_cols]).aggregate(**re_aggs)
            return pt

        # (b) Scalar pre-agg — nothing to bridge.
        if not pt_grain:
            return pt

        # (c) Bridge missing group-by dims from raw tables.
        bridged = pt
        for gb_col in group_by_cols:
            if gb_col in bridged.columns:
                continue
            bridged = _attach_dim_column(
                bridged,
                gb_col,
                measure_names,
                join_tree_info,
                merged_dimensions,
            )

        # Re-aggregate onto the requested group-by granularity.
        gb_avail = tuple(c for c in group_by_cols if c in bridged.columns)
        re_aggs = {
            m: _build_reagg(bridged[m], reagg_map.get(m, "sum"))
            for m in pt_meas
            if m in bridged.columns
        }
        match (gb_avail, bool(re_aggs)):
            case ((), _) | (_, False):
                return bridged
            case _:
                return bridged.group_by([bridged[c] for c in gb_avail]).aggregate(**re_aggs)

    rejoined = tuple(_bridge_one_preagg(pt) for pt in preagg_results)
    result = _join_tables(group_by_cols, list(rejoined))

    # Recompute MEAN from decomposed SUM/COUNT after combining
    for mname, (sc, cc) in decomposed_means:
        if sc in result.columns and cc in result.columns:
            result = result.mutate(**{mname: result[sc] / result[cc]})
            result = result.drop(sc, cc)

    return _fill_missing_count_identities(result, empty_count_measures)


def _apply_calc_specs(result, plan, tbl, totals_builder=None):
    """Apply calculated measure specs to the pre-aggregated result.

    Each calc spec is a :class:`CalcMeasure` whose lambda is
    re-evaluated against the post-aggregation result via the
    ibis-native compiler. ``t.all(measure_ref)`` patterns trigger a
    no-group-by totals aggregation that gets cross-joined into the
    result so non-sum measures (mean/quantile/…) get correct overall
    values; ``apply_calc_measures`` builds the totals lazily on
    first use.

    ``totals_builder`` overrides how the base totals table is built.
    The pre-agg path passes a fan-out-safe builder that aggregates
    each source table at zero grain — re-running agg specs on the
    fanned-out join would inflate ``t.all(...)`` denominators.
    """
    if not plan.calc_specs:
        return result
    base_for_calc = tbl if tbl is not None else result
    known = frozenset(plan.agg_specs.keys()) | frozenset(plan.calc_specs.keys())
    return apply_calc_measures(
        result,
        base_for_calc,
        dict(plan.calc_specs),
        known,
        agg_specs=dict(plan.agg_specs),
        totals_base_builder=totals_builder,
    )
