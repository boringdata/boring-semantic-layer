"""Formatting functions for pretty-printing semantic layer operations."""

from __future__ import annotations

try:
    from ibis.expr.format import fmt, render_fields
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

    name_part = f": {name}" if name else ""
    lines = [f"SemanticTable{name_part}"]

    if dims:
        for dim_name, dim_obj in dims.items():
            marker = ""
            if dim_obj.is_entity:
                marker = "🔑 "
            elif dim_obj.is_event_timestamp:
                marker = "⏱️ "
            lines.append(f"  {marker}{dim_name} [dim]")

    all_measures = {**measures, **calc_measures}
    if all_measures:
        for meas_name in all_measures:
            if meas_name in calc_measures:
                lines.append(f"  {meas_name} [calc]")
            else:
                lines.append(f"  {meas_name} [measure]")

    return "\n".join(lines)


@fmt.register(SemanticFilterOp)
def _format_semantic_filter(op: SemanticFilterOp, source=None, **kwargs):
    predicate = object.__getattribute__(op, "predicate")

    pred_repr = "<predicate>"
    if hasattr(predicate, "__name__"):
        pred_repr = f"λ {predicate.__name__}"
    elif hasattr(predicate, "unwrap"):
        unwrapped = predicate.unwrap
        if hasattr(unwrapped, "__name__"):
            pred_repr = f"λ {unwrapped.__name__}"

    if source is None:
        top = "Filter\n"
    else:
        top = f"Filter[{source}]\n"
    return top + render_fields({"predicate": pred_repr}, 1)


@fmt.register(SemanticAggregateOp)
def _format_semantic_aggregate(op: SemanticAggregateOp, source=None, **kwargs):
    aggs = object.__getattribute__(op, "aggs")
    keys = object.__getattribute__(op, "keys")

    top = "Aggregate" if source is None else f"Aggregate[{source}]"
    lines = [top]

    if keys:
        lines.append("  groups:")
        keys_to_show = list(keys[:3])
        for key in keys_to_show:
            lines.append(f"    {key}")
        if len(keys) > 3:
            lines.append(f"    ... and {len(keys) - 3} more")

    if aggs:
        lines.append("  metrics:")
        agg_names = list(aggs.keys())[:3]
        for metric in agg_names:
            lines.append(f"    {metric}")
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
    keys = object.__getattribute__(op, "keys")

    top = "GroupBy" if source is None else f"GroupBy[{source}]"
    lines = [top]
    lines.append("  keys:")

    keys_to_show = list(keys[:3])
    for key in keys_to_show:
        lines.append(f"    {key}")
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
    post = object.__getattribute__(op, "post")
    top = "Mutate\n" if source is None else f"Mutate[{source}]\n"

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
