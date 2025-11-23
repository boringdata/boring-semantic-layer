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
    dims = object.__getattribute__(op, 'dimensions')
    measures = object.__getattribute__(op, 'measures')
    calc_measures = object.__getattribute__(op, 'calc_measures')
    name = object.__getattribute__(op, 'name')

    DIM_COLOR = "\033[36m"      # Cyan for dimensions
    MEASURE_COLOR = "\033[35m"  # Magenta for measures
    CALC_COLOR = "\033[33m"     # Yellow for calculated measures
    HEADER_COLOR = "\033[1;34m" # Bold Blue for headers
    RESET = "\033[0m"

    name_part = f": {HEADER_COLOR}{name}{RESET}" if name else ""
    lines = [f"{HEADER_COLOR}SemanticTable{RESET}{name_part}"]

    if dims:
        for dim_name, dim_obj in dims.items():
            marker = ""
            if dim_obj.is_entity:
                marker = "🔑 "
            elif dim_obj.is_event_timestamp:
                marker = "⏱️ "

            lines.append(f"  {marker}{DIM_COLOR}{dim_name} [dim]{RESET}")

    all_measures = {**measures, **calc_measures}
    if all_measures:
        for meas_name in all_measures.keys():
            if meas_name in calc_measures:
                lines.append(f"  {CALC_COLOR}{meas_name} [calc]{RESET}")
            else:
                lines.append(f"  {MEASURE_COLOR}{meas_name} [measure]{RESET}")

    return "\n".join(lines)


@fmt.register(SemanticFilterOp)
def _format_semantic_filter(op: SemanticFilterOp, source=None, **kwargs):
    """Format SemanticFilterOp for pretty printing."""
    OP_COLOR = "\033[1;32m"  # Bold Green for operation names
    REF_COLOR = "\033[93m"   # Bright Yellow for references
    RESET = "\033[0m"

    # Access the predicate
    predicate = object.__getattribute__(op, 'predicate')

    # Try to get a readable representation of the predicate
    pred_repr = "<predicate>"
    if hasattr(predicate, '__name__'):
        pred_repr = f"λ {predicate.__name__}"
    elif hasattr(predicate, 'unwrap'):
        # It's a wrapped callable
        unwrapped = predicate.unwrap
        if hasattr(unwrapped, '__name__'):
            pred_repr = f"λ {unwrapped.__name__}"

    if source is None:
        top = f"{OP_COLOR}Filter{RESET}\n"
    else:
        top = f"{OP_COLOR}Filter{RESET}[{REF_COLOR}{source}{RESET}]\n"
    return top + render_fields({"predicate": pred_repr}, 1)


@fmt.register(SemanticAggregateOp)
def _format_semantic_aggregate(op: SemanticAggregateOp, source=None, **kwargs):
    """Format SemanticAggregateOp for pretty printing."""
    OP_COLOR = "\033[1;32m"   # Bold Green for operation names
    REF_COLOR = "\033[93m"    # Bright Yellow for references
    DIM_COLOR = "\033[36m"    # Cyan for dimensions (groups)
    MEASURE_COLOR = "\033[35m" # Magenta for measures (metrics)
    RESET = "\033[0m"

    # Access the actual operation fields
    aggs = object.__getattribute__(op, 'aggs')
    keys = object.__getattribute__(op, 'keys')

    if source is None:
        top = f"{OP_COLOR}Aggregate{RESET}\n"
    else:
        top = f"{OP_COLOR}Aggregate{RESET}[{REF_COLOR}{source}{RESET}]\n"

    # Build colored output manually instead of using render_fields
    lines = [top.rstrip()]

    if keys:
        lines.append("  groups:")
        keys_to_show = list(keys[:3])
        for key in keys_to_show:
            lines.append(f"    {DIM_COLOR}{key}{RESET}")
        if len(keys) > 3:
            lines.append(f"    ... and {len(keys) - 3} more")

    if aggs:
        lines.append("  metrics:")
        agg_names = list(aggs.keys())[:3]
        for metric in agg_names:
            lines.append(f"    {MEASURE_COLOR}{metric}{RESET}")
        if len(aggs) > 3:
            lines.append(f"    ... and {len(aggs) - 3} more")

    return "\n".join(lines)


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
    OP_COLOR = "\033[1;32m"  # Bold Green for operation names
    REF_COLOR = "\033[93m"   # Bright Yellow for references
    DIM_COLOR = "\033[36m"   # Cyan for dimensions (keys)
    RESET = "\033[0m"

    keys = object.__getattribute__(op, 'keys')

    if source is None:
        top = f"{OP_COLOR}GroupBy{RESET}\n"
    else:
        top = f"{OP_COLOR}GroupBy{RESET}[{REF_COLOR}{source}{RESET}]\n"

    # Build colored output manually
    lines = [top.rstrip()]
    lines.append("  keys:")

    keys_to_show = list(keys[:3])
    for key in keys_to_show:
        lines.append(f"    {DIM_COLOR}{key}{RESET}")
    if len(keys) > 3:
        lines.append(f"    ... and {len(keys) - 3} more")

    return "\n".join(lines)


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
    OP_COLOR = "\033[1;32m"  # Bold Green for operation names
    REF_COLOR = "\033[93m"   # Bright Yellow for references
    RESET = "\033[0m"

    # Access the actual operation field
    post = object.__getattribute__(op, 'post')

    if source is None:
        top = f"{OP_COLOR}Mutate{RESET}\n"
    else:
        top = f"{OP_COLOR}Mutate{RESET}[{REF_COLOR}{source}{RESET}]\n"

    exprs_to_show = list(post.keys())[:3]
    if len(post) > 3:
        exprs_to_show.append(f"... and {len(post) - 3} more")

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
