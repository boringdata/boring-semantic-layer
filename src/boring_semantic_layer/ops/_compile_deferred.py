"""Compile strategy: aggregate first, LEFT JOIN deferred dimension tables.

Extracted verbatim from
``SemanticAggregateOp._to_untagged_with_deferred_joins``.
"""

from __future__ import annotations

from ..calc_compiler import apply_calc_measures
from ..measure_scope import MeasureScope
from ._core import (
    SemanticJoinOp,
    SemanticTableOp,
    _augment_dimensions_with_raw_columns,
    _build_aggregation_plan,
    _DeferrableJoin,
    _get_merged_fields,
    _JoinTreeInfo,
    _mutate_dimensions_with_dependencies,
    _resolve_expr,
    _to_untagged,
    _unwrap,
)


def to_untagged_with_deferred_joins(
    op,
    all_roots: list,
    join_op: SemanticJoinOp,
    join_tree_info: _JoinTreeInfo,
    deferrable: list[_DeferrableJoin],
    filters: list | None = None,
):
    """Aggregate first, then LEFT JOIN deferred dimension tables.

    For ``join_one`` dimension lookups where the right table's PK matches
    the join key and no measures from the right table are used, we can:
    1. Strip deferred tables from the join tree
    2. Add join keys to GROUP BY so they survive aggregation
    3. Aggregate on the core (non-deferred) tables
    4. LEFT JOIN deferred dimension tables onto the aggregated result
    """
    filters = filters or []
    deferred_names = {d.table_name for d in deferrable}

    merged_dimensions = _get_merged_fields(all_roots, "dimensions", source=join_op)
    merged_dimensions = _augment_dimensions_with_raw_columns(
        merged_dimensions, op.keys, all_roots, join_op
    )
    merged_base_measures = _get_merged_fields(all_roots, "measures")
    merged_calc_measures = _get_merged_fields(all_roots, "calc_measures")

    # --- 1. Rebuild join tree without deferred tables ---
    def strip_deferred(node):
        """Remove deferred right-side tables from the join tree."""
        if not isinstance(node, SemanticJoinOp):
            return node
        # Recurse left
        new_left = strip_deferred(node.left)
        # If right side is a deferred table, return just the left
        if isinstance(node.right, SemanticTableOp) and node.right.name in deferred_names:
            return new_left
        return SemanticJoinOp(
            left=new_left,
            right=node.right,
            how=node.how,
            on=node.on,
            cardinality=node.cardinality,
        )

    core_join = strip_deferred(join_op)

    # --- 2. Build core table and aggregate ---
    if isinstance(core_join, SemanticJoinOp):
        core_tbl = core_join.to_untagged(parent_requirements=None)
    else:
        core_tbl = _to_untagged(core_join)

    # Resolve group-by dimensions on the core table
    core_group_keys = []
    for k in op.keys:
        if k in deferred_names or any(k.startswith(f"{dn}.") for dn in deferred_names):
            # Skip deferred table's own dimensions from group-by
            # (they'll be added via post-agg join)
            continue
        core_group_keys.append(k)

    # Add left-side join keys to group-by so they survive aggregation
    for d in deferrable:
        for jk in d.join_keys_left:
            if jk not in core_group_keys and jk in core_tbl.columns:
                core_group_keys.append(jk)

    # Resolve dimensions for group-by on the core table
    core_tbl = _mutate_dimensions_with_dependencies(
        core_tbl,
        [k for k in core_group_keys if k in merged_dimensions],
        merged_dimensions,
    )

    # Apply filters — loudly. Filters referencing deferred tables were
    # already excluded from deferral by _find_deferrable_joins, so a
    # filter that fails to resolve here is a genuine error, not a
    # cross-table predicate awaiting another mechanism.
    if filters:
        from ..convert import _Resolver

        for i, pred in enumerate(filters):
            pred_fn = _unwrap(pred)
            try:
                resolver = _Resolver(core_tbl, merged_dimensions)
                pred_expr = _resolve_expr(pred_fn, resolver)
            except Exception as exc:
                raise ValueError(
                    f"Filter #{i} does not resolve against the aggregation "
                    "source table; it would be silently ignored. Check the "
                    "dimension/column name, or qualify it with a table "
                    'prefix (e.g. t["orders.status"]).'
                ) from exc
            core_tbl = core_tbl.filter(pred_expr)

    # Build aggregation expressions
    scope = MeasureScope(
        _tbl=core_tbl,
        _known=list(merged_base_measures.keys()) + list(merged_calc_measures.keys()),
    )
    plan = _build_aggregation_plan(
        aggs=op.aggs,
        keys=tuple(core_group_keys),
        scope=scope,
        is_post_agg=False,
        merged_base_measures=merged_base_measures,
        merged_calc_measures=merged_calc_measures,
        tbl=core_tbl,
    )

    # Execute aggregation on core table
    if plan.group_by_cols:
        gb_exprs = []
        for col in plan.group_by_cols:
            if col in core_tbl.columns:
                gb_exprs.append(core_tbl[col])
            elif col in merged_dimensions:
                dim_fn = merged_dimensions[col]
                gb_exprs.append(dim_fn(core_tbl).name(col))

        agg_exprs = {name: fn(core_tbl) for name, fn in plan.agg_specs.items()}
        result = core_tbl.group_by(gb_exprs).aggregate(**agg_exprs)
    else:
        agg_exprs = {name: fn(core_tbl) for name, fn in plan.agg_specs.items()}
        result = core_tbl.aggregate(**agg_exprs)

    # Handle calculated measures
    if plan.calc_specs:
        known = frozenset(merged_base_measures) | frozenset(merged_calc_measures)
        result = apply_calc_measures(
            result,
            core_tbl,
            dict(plan.calc_specs),
            known,
            agg_specs=dict(plan.agg_specs),
        )

    # --- 3. LEFT JOIN deferred dimension tables ---
    for d in deferrable:
        dim_tbl = _to_untagged(d.table_op)

        # Reuse the original join predicate to preserve key pairing
        # (avoids mismatch from independently sorting left/right keys)
        join_preds = d.on_predicate(result, dim_tbl)

        # Compute deferred dimension columns on the dimension table.
        # For direct columns (e.g., cc_name) this is a no-op rename.
        # For derived expressions (e.g., cc_name.upper()) this materializes
        # the computed column so it's available after the join.
        dim_cols_to_add = []
        right_dims = d.table_op.get_dimensions()
        for dim_name in d.deferred_dims:
            short = dim_name.split(".", 1)[1] if "." in dim_name else dim_name
            if short in right_dims:
                dim_fn = right_dims[short]
                if callable(dim_fn):
                    try:
                        expr = dim_fn(dim_tbl)
                    except Exception:
                        # Derived dims may reference other derived dims;
                        # materialize dependencies first. Failures beyond
                        # that raise — silently dropping the requested
                        # dimension returned unlabeled rows at a hidden
                        # grain.
                        dim_tbl = _mutate_dimensions_with_dependencies(dim_tbl, [short], right_dims)
                        dim_cols_to_add.append((dim_name, short))
                        continue
                    col_name = expr.get_name()
                    if col_name in dim_tbl.columns:
                        # Direct column — use as-is
                        dim_cols_to_add.append((dim_name, col_name))
                    else:
                        # Derived expression — mutate onto dim table
                        # Use a temp name to avoid collisions
                        temp_name = f"__deferred_{short}"
                        dim_tbl = dim_tbl.mutate(**{temp_name: expr})
                        dim_cols_to_add.append((dim_name, temp_name))

        if dim_cols_to_add:
            # Perform the LEFT JOIN
            joined = result.left_join(dim_tbl, join_preds)
            # Select: all from result + deferred dim columns renamed to prefixed names
            select_exprs = [result]
            for prefixed_name, raw_col in dim_cols_to_add:
                select_exprs.append(dim_tbl[raw_col].name(prefixed_name))
            result = joined.select(select_exprs)

    # --- 4. Select only originally requested columns ---
    original_cols = list(op.keys) + list(op.aggs.keys())
    # Add deferred dim names (they were requested)
    for d in deferrable:
        for dd in d.deferred_dims:
            if dd not in original_cols:
                original_cols.append(dd)
    # Also keep calc measure names
    if plan.calc_specs:
        for cm in plan.calc_specs:
            if cm not in original_cols:
                original_cols.append(cm)

    available = frozenset(result.columns)
    select_cols = [c for c in original_cols if c in available]
    if select_cols and set(select_cols) != available:
        result = result.select([result[c] for c in select_cols])

    return result


# -- helpers for _to_untagged_with_preagg --------------------------------
