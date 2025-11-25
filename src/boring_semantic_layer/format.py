"""Formatting functions for pretty-printing semantic layer operations."""

from __future__ import annotations

try:
    from ibis.expr.format import fmt, render_fields, render_schema
except ImportError:
    from xorq.vendor.ibis.expr.format import fmt, render_fields

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
    dims = object.__getattribute__(op, "dimensions")
    measures = object.__getattribute__(op, "measures")
    calc_measures = object.__getattribute__(op, "calc_measures")
    name = object.__getattribute__(op, "name")

    DIM_COLOR = "\033[36m"
    MEASURE_COLOR = "\033[35m"
    CALC_COLOR = "\033[33m"
    HEADER_COLOR = "\033[1;34m"
    RESET = "\033[0m"

    name_part = f": {HEADER_COLOR}{name}{RESET}" if name else ""
    lines = [f"{HEADER_COLOR}SemanticTable{RESET}{name_part}"]

    if dims:
        for dim_name in dims:
            lines.append(f"  {DIM_COLOR}{dim_name} [dim]{RESET}")

    all_measures = {**measures, **calc_measures}
    if all_measures:
        for meas_name in all_measures:
            if meas_name in calc_measures:
                lines.append(f"  {CALC_COLOR}{meas_name} [calc]{RESET}")
            else:
                lines.append(f"  {MEASURE_COLOR}{meas_name} [measure]{RESET}")

    return "\n".join(lines)


@fmt.register(SemanticFilterOp)
def _format_semantic_filter(op: SemanticFilterOp, source=None, **kwargs):
    OP_COLOR = "\033[1;32m"
    REF_COLOR = "\033[93m"
    RESET = "\033[0m"

    predicate = object.__getattribute__(op, "predicate")

    pred_repr = "<predicate>"
    if hasattr(predicate, "__name__"):
        pred_repr = f"λ {predicate.__name__}"
    elif hasattr(predicate, "unwrap"):
        unwrapped = predicate.unwrap
        if hasattr(unwrapped, "__name__"):
            pred_repr = f"λ {unwrapped.__name__}"

    if source is None:
        top = f"{OP_COLOR}Filter{RESET}\n"
    else:
        top = f"{OP_COLOR}Filter{RESET}[{REF_COLOR}{source}{RESET}]\n"
    return top + render_fields({"predicate": pred_repr}, 1)


@fmt.register(SemanticAggregateOp)
def _format_semantic_aggregate(op: SemanticAggregateOp, source=None, **kwargs):
    OP_COLOR = "\033[1;32m"
    REF_COLOR = "\033[93m"
    DIM_COLOR = "\033[36m"
    MEASURE_COLOR = "\033[35m"
    RESET = "\033[0m"

    aggs = object.__getattribute__(op, "aggs")
    keys = object.__getattribute__(op, "keys")

    if source is None:
        top = f"{OP_COLOR}Aggregate{RESET}\n"
    else:
        top = f"{OP_COLOR}Aggregate{RESET}[{REF_COLOR}{source}{RESET}]\n"

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
    fields = {"how": op.how}
    if left:
        fields["left"] = left
    if right:
        fields["right"] = right
    return "Join\n" + render_fields(fields, 1)


@fmt.register(SemanticGroupByOp)
def _format_semantic_groupby(op: SemanticGroupByOp, source=None, **kwargs):
    OP_COLOR = "\033[1;32m"
    REF_COLOR = "\033[93m"
    DIM_COLOR = "\033[36m"
    RESET = "\033[0m"

    keys = object.__getattribute__(op, "keys")

    if source is None:
        top = f"{OP_COLOR}GroupBy{RESET}\n"
    else:
        top = f"{OP_COLOR}GroupBy{RESET}[{REF_COLOR}{source}{RESET}]\n"

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
    top = "Project\n" if source is None else f"Project[{source}]\n"
    fields_to_show = list(op.fields[:3])
    if len(op.fields) > 3:
        fields_to_show.append(f"... and {len(op.fields) - 3} more")
    return top + render_fields({"fields": fields_to_show}, 1)


@fmt.register(SemanticOrderByOp)
def _format_semantic_orderby(op: SemanticOrderByOp, source=None, **kwargs):
    top = "OrderBy\n" if source is None else f"OrderBy[{source}]\n"
    return top + render_fields({"sort_keys": list(op.keys)}, 1)


@fmt.register(SemanticLimitOp)
def _format_semantic_limit(op: SemanticLimitOp, source=None, **kwargs):
    top = "Limit\n" if source is None else f"Limit[{source}]\n"
    return top + render_fields({"n": op.n}, 1)


@fmt.register(SemanticMutateOp)
def _format_semantic_mutate(op: SemanticMutateOp, source=None, **kwargs):
    OP_COLOR = "\033[1;32m"
    REF_COLOR = "\033[93m"
    RESET = "\033[0m"

    post = object.__getattribute__(op, "post")
    top = (
        f"{OP_COLOR}Mutate{RESET}\n"
        if source is None
        else f"{OP_COLOR}Mutate{RESET}[{REF_COLOR}{source}{RESET}]\n"
    )

    exprs_to_show = list(post.keys())[:3]
    if len(post) > 3:
        exprs_to_show.append(f"... and {len(post) - 3} more")
    return top + render_fields({"new_columns": exprs_to_show}, 1)


@fmt.register(SemanticUnnestOp)
def _format_semantic_unnest(op: SemanticUnnestOp, source=None, **kwargs):
    return "Unnest\n" if source is None else f"Unnest[{source}]\n"


@fmt.register(SemanticIndexOp)
def _format_semantic_index(op: SemanticIndexOp, source=None, **kwargs):
    top = "Index\n" if source is None else f"Index[{source}]\n"
    return top + render_fields({"index": op.index}, 1)
