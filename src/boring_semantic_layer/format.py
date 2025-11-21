"""Formatting functions for pretty-printing semantic layer operations."""

from __future__ import annotations

from xorq.vendor.ibis.expr import format as fmt

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


@fmt.fmt.register(SemanticTableOp)
def _format_semantic_table(op: SemanticTableOp, **kwargs):
    """Format SemanticTableOp with concise metadata summary."""
    dims_dict = object.__getattribute__(op, "dimensions")
    base_measures = object.__getattribute__(op, "measures")
    calc_measures = object.__getattribute__(op, "calc_measures")
    all_measures = {**base_measures, **calc_measures}

    num_dims = len(dims_dict)
    num_measures = len(all_measures)

    lines = [f"SemanticTableOp[{op.name}]"]

    if dims_dict:
        dim_names = list(dims_dict.keys())
        shown_dims = dim_names[:3]
        dims_preview = ", ".join(shown_dims)
        if num_dims > 3:
            lines.append(f"  {num_dims} dimensions: {dims_preview}, ...")
        else:
            lines.append(
                f"  {num_dims} dimension{'s' if num_dims != 1 else ''}: {dims_preview}",
            )

    if all_measures:
        meas_names = list(all_measures.keys())
        shown_meas = meas_names[:3]
        meas_preview = ", ".join(shown_meas)
        if num_measures > 3:
            lines.append(f"  {num_measures} measures: {meas_preview}, ...")
        else:
            lines.append(
                f"  {num_measures} measure{'s' if num_measures != 1 else ''}: {meas_preview}",
            )

    return "\n".join(lines)


@fmt.fmt.register(SemanticFilterOp)
def _format_semantic_filter(op: SemanticFilterOp, **kwargs):
    """Format SemanticFilterOp showing source and predicate info."""
    source_type = type(op.source).__name__

    lines = ["SemanticFilterOp"]
    lines.append(f"  source: {source_type}")
    lines.append("  predicate: <function>")

    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, "measures") and hasattr(op.source, "calc_measures"):
        meas_dict = object.__getattribute__(op.source, "measures")
        calc_dict = object.__getattribute__(op.source, "calc_measures")
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticGroupByOp)
def _format_semantic_groupby(op: SemanticGroupByOp, **kwargs):
    """Format SemanticGroupByOp showing source and keys."""
    source_type = type(op.source).__name__
    keys_str = ", ".join(repr(k) for k in op.keys)

    lines = ["SemanticGroupByOp"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  keys: [{keys_str}]")

    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, "measures") and hasattr(op.source, "calc_measures"):
        meas_dict = object.__getattribute__(op.source, "measures")
        calc_dict = object.__getattribute__(op.source, "calc_measures")
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticOrderByOp)
def _format_semantic_orderby(op: SemanticOrderByOp, **kwargs):
    """Format SemanticOrderByOp showing source and keys."""
    source_type = type(op.source).__name__
    keys_str = ", ".join(repr(k) if isinstance(k, str) else "<expr>" for k in op.keys)

    lines = ["SemanticOrderByOp"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  keys: [{keys_str}]")

    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, "measures") and hasattr(op.source, "calc_measures"):
        meas_dict = object.__getattribute__(op.source, "measures")
        calc_dict = object.__getattribute__(op.source, "calc_measures")
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticLimitOp)
def _format_semantic_limit(op: SemanticLimitOp, **kwargs):
    """Format SemanticLimitOp showing source, limit, and offset."""
    source_type = type(op.source).__name__

    lines = ["SemanticLimitOp"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  n: {op.n}")
    if op.offset:
        lines.append(f"  offset: {op.offset}")

    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, "measures") and hasattr(op.source, "calc_measures"):
        meas_dict = object.__getattribute__(op.source, "measures")
        calc_dict = object.__getattribute__(op.source, "calc_measures")
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticMutateOp)
def _format_semantic_mutate(op: SemanticMutateOp, **kwargs):
    """Format SemanticMutateOp showing source and columns."""
    source_type = type(op.source).__name__
    cols = list(op.post.keys())
    cols_str = ", ".join(cols[:5])
    if len(cols) > 5:
        cols_str += f", ... ({len(cols)} total)"

    lines = ["SemanticMutateOp"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  columns: [{cols_str}]")

    return "\n".join(lines)


@fmt.fmt.register(SemanticProjectOp)
def _format_semantic_project(op: SemanticProjectOp, **kwargs):
    """Format SemanticProjectOp showing source and fields."""
    source_type = type(op.source).__name__
    fields = list(op.fields)
    fields_str = ", ".join(repr(f) for f in fields[:5])
    if len(fields) > 5:
        fields_str += f", ... ({len(fields)} total)"

    lines = ["SemanticProjectOp"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  fields: [{fields_str}]")

    return "\n".join(lines)


@fmt.fmt.register(SemanticAggregateOp)
def _format_semantic_aggregate(op: SemanticAggregateOp, **kwargs):
    """Format SemanticAggregateOp showing source, keys, and aggs."""
    source_type = type(op.source).__name__
    keys_str = ", ".join(repr(k) for k in op.keys)
    aggs = list(op.aggs.keys())
    aggs_str = ", ".join(aggs[:5])
    if len(aggs) > 5:
        aggs_str += f", ... ({len(aggs)} total)"

    lines = ["SemanticAggregateOp"]
    lines.append(f"  source: {source_type}")
    if op.keys:
        lines.append(f"  keys: [{keys_str}]")
    lines.append(f"  aggs: [{aggs_str}]")

    # If source has dimensions/measures, show count
    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, "measures") and hasattr(op.source, "calc_measures"):
        meas_dict = object.__getattribute__(op.source, "measures")
        calc_dict = object.__getattribute__(op.source, "calc_measures")
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticUnnestOp)
def _format_semantic_unnest(op: SemanticUnnestOp, **kwargs):
    """Format SemanticUnnestOp showing source and column."""
    source_type = type(op.source).__name__

    lines = ["SemanticUnnestOp"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  column: {op.column!r}")

    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, "measures") and hasattr(op.source, "calc_measures"):
        meas_dict = object.__getattribute__(op.source, "measures")
        calc_dict = object.__getattribute__(op.source, "calc_measures")
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticJoinOp)
def _format_semantic_join(op: SemanticJoinOp, **kwargs):
    """Format SemanticJoinOp showing left/right tables, how, and on."""
    left_name = getattr(op.left, "name", None) or type(op.left).__name__
    right_name = getattr(op.right, "name", None) or type(op.right).__name__
    on_str = "<function>" if op.on else "cross"

    lines = ["SemanticJoinOp"]
    lines.append(f"  left: {left_name}")
    lines.append(f"  right: {right_name}")
    lines.append(f"  how: {op.how!r}")
    lines.append(f"  on: {on_str}")

    return "\n".join(lines)


@fmt.fmt.register(SemanticIndexOp)
def _format_semantic_index(op: SemanticIndexOp, **kwargs):
    """Format SemanticIndexOp showing selector, by, and sample."""
    lines = ["SemanticIndexOp"]

    # Format selector
    if op.selector is not None:
        try:
            selector_repr = repr(op.selector)
            if len(selector_repr) > 50:
                selector_repr = selector_repr[:47] + "..."
            lines.append(f"  selector: {selector_repr}")
        except Exception:
            lines.append("  selector: <selector>")
    else:
        lines.append("  selector: all()")

    if op.by:
        lines.append(f"  by: {op.by!r}")
    if op.sample:
        lines.append(f"  sample: {op.sample}")

    # Show source information
    source_type = type(op.source).__name__
    lines.append(f"  source: {source_type}")

    if hasattr(op.source, "dimensions"):
        dims_dict = object.__getattribute__(op.source, "dimensions")
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    return "\n".join(lines)
