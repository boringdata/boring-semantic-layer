"""Formatting functions for pretty-printing semantic layer operations."""

from __future__ import annotations

# Import from regular ibis (works with both regular ibis and xorq backends)
try:
    from ibis.expr.format import fmt, render_fields, render_schema
except ImportError:
    # Fallback to xorq's vendored ibis if regular ibis is not available
    from xorq.vendor.ibis.expr.format import fmt, render_fields, render_schema

from boring_semantic_layer.ops import (
    SemanticAggregateOp,
    SemanticFilterOp,
    SemanticGroupByOp,
    SemanticIndexOp,
    SemanticJoinOp,
    SemanticLimitOp,
    SemanticMutateOp,
    SemanticOrderByOp,
    SemanticProjectOp,
    SemanticTableOp,
    SemanticUnnestOp,
)


@fmt.register(SemanticTableOp)
def _format_semantic_table(op: SemanticTableOp, **kwargs):
    """Format SemanticTableOp for pretty printing."""
    # Access the actual operation fields using object.__getattribute__ to avoid any __getattr__ issues
    dims = object.__getattribute__(op, 'dimensions')
    measures = object.__getattribute__(op, 'measures')
    calc_measures = object.__getattribute__(op, 'calc_measures')
    name = object.__getattribute__(op, 'name')

    name_part = f": {name}" if name else ""
    lines = [f"SemanticTable{name_part}"]

    # ANSI color codes for terminal output
    DIM_COLOR = "\033[36m"      # Cyan for dimensions
    MEASURE_COLOR = "\033[35m"  # Magenta for measures
    CALC_COLOR = "\033[33m"     # Yellow for calculated measures
    RESET = "\033[0m"

    # Show dimensions with color coding
    if dims:
        for dim_name in dims.keys():
            lines.append(f"  {DIM_COLOR}{dim_name} [dim]{RESET}")

    # Show measures with color coding
    all_measures = {**measures, **calc_measures}
    if all_measures:
        for meas_name in all_measures.keys():
            # Distinguish between base measures and calculated measures
            if meas_name in calc_measures:
                lines.append(f"  {CALC_COLOR}{meas_name} [calc]{RESET}")
            else:
                lines.append(f"  {MEASURE_COLOR}{meas_name} [measure]{RESET}")

    return "\n".join(lines)


@fmt.register(SemanticFilterOp)
def _format_semantic_filter(op: SemanticFilterOp, source=None, **kwargs):
    """Format SemanticFilterOp for pretty printing."""
    if source is None:
        top = "Filter\n"
    else:
        top = f"Filter[{source}]\n"
    return top + render_fields({"predicate": "<predicate>"}, 1)


@fmt.register(SemanticAggregateOp)
def _format_semantic_aggregate(op: SemanticAggregateOp, source=None, **kwargs):
    """Format SemanticAggregateOp for pretty printing."""
    if source is None:
        top = "Aggregate\n"
    else:
        top = f"Aggregate[{source}]\n"

    fields = {}
    if op.keys:
        # Only show first few keys
        keys_to_show = list(op.keys[:3])
        if len(op.keys) > 3:
            keys_to_show.append(f"... and {len(op.keys) - 3} more")
        fields["groups"] = keys_to_show

    if op.aggs:
        # Only show first few aggregations
        agg_names = list(op.aggs.keys())[:3]
        if len(op.aggs) > 3:
            agg_names.append(f"... and {len(op.aggs) - 3} more")
        fields["metrics"] = agg_names

    return top + render_fields(fields, 1)


@fmt.register(SemanticJoinOp)
def _format_semantic_join(op: SemanticJoinOp, left=None, right=None, **kwargs):
    """Format SemanticJoinOp for pretty printing."""
    fields = {
        "how": op.how,
    }
    if left:
        fields["left"] = left
    if right:
        fields["right"] = right

    return "Join\n" + render_fields(fields, 1)


@fmt.register(SemanticGroupByOp)
def _format_semantic_groupby(op: SemanticGroupByOp, source=None, **kwargs):
    """Format SemanticGroupByOp for pretty printing."""
    if source is None:
        top = "GroupBy\n"
    else:
        top = f"GroupBy[{source}]\n"

    keys_to_show = list(op.keys[:3])
    if len(op.keys) > 3:
        keys_to_show.append(f"... and {len(op.keys) - 3} more")

    return top + render_fields({"keys": keys_to_show}, 1)


@fmt.register(SemanticProjectOp)
def _format_semantic_project(op: SemanticProjectOp, source=None, **kwargs):
    """Format SemanticProjectOp for pretty printing."""
    if source is None:
        top = "Project\n"
    else:
        top = f"Project[{source}]\n"

    fields_to_show = list(op.fields[:3])
    if len(op.fields) > 3:
        fields_to_show.append(f"... and {len(op.fields) - 3} more")

    return top + render_fields({"fields": fields_to_show}, 1)


@fmt.register(SemanticOrderByOp)
def _format_semantic_orderby(op: SemanticOrderByOp, source=None, **kwargs):
    """Format SemanticOrderByOp for pretty printing."""
    if source is None:
        top = "OrderBy\n"
    else:
        top = f"OrderBy[{source}]\n"

    return top + render_fields({"sort_keys": list(op.keys)}, 1)


@fmt.register(SemanticLimitOp)
def _format_semantic_limit(op: SemanticLimitOp, source=None, **kwargs):
    """Format SemanticLimitOp for pretty printing."""
    if source is None:
        top = "Limit\n"
    else:
        top = f"Limit[{source}]\n"

    return top + render_fields({"n": op.n}, 1)


@fmt.register(SemanticMutateOp)
def _format_semantic_mutate(op: SemanticMutateOp, source=None, **kwargs):
    """Format SemanticMutateOp for pretty printing."""
    if source is None:
        top = "Mutate\n"
    else:
        top = f"Mutate[{source}]\n"

    exprs_to_show = list(op.exprs.keys())[:3]
    if len(op.exprs) > 3:
        exprs_to_show.append(f"... and {len(op.exprs) - 3} more")

    return top + render_fields({"new_columns": exprs_to_show}, 1)


@fmt.register(SemanticUnnestOp)
def _format_semantic_unnest(op: SemanticUnnestOp, source=None, **kwargs):
    """Format SemanticUnnestOp for pretty printing."""
    if source is None:
        top = "Unnest\n"
    else:
        top = f"Unnest[{source}]\n"

    return top


@fmt.register(SemanticIndexOp)
def _format_semantic_index(op: SemanticIndexOp, source=None, **kwargs):
    """Format SemanticIndexOp for pretty printing."""
    if source is None:
        top = "Index\n"
    else:
        top = f"Index[{source}]\n"

    return top + render_fields({"index": op.index}, 1)
