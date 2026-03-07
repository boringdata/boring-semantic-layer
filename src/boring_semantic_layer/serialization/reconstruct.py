"""Handler registry for reconstructing BSL ops from tag metadata.

Dispatches on ``metadata["bsl_op_type"]`` strings — matching xorq's
``FROM_YAML_HANDLERS`` pattern. Each handler receives ``(metadata, xorq_expr,
source, context)`` and returns a BSL expression.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from returns.result import safe

from .context import BSLSerializationContext
from .extract import deserialize_calc_measures
from .helpers import deserialize_structured


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

BSL_RECONSTRUCTORS: dict[str, Callable] = {}


def register_reconstructor(*op_names: str):
    """Decorator to register a reconstructor for one or more op type names."""

    def decorator(func):
        for name in op_names:
            BSL_RECONSTRUCTORS[name] = func
        return func

    return decorator


# ---------------------------------------------------------------------------
# Per-op reconstructors
# ---------------------------------------------------------------------------


@register_reconstructor("SemanticTableOp")
def _reconstruct_semantic_table(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    from .. import expr as bsl_expr
    from .. import ops

    def _create_dimension(name: str, dim_data: dict) -> ops.Dimension:
        expr_col = dim_data.get("expr")
        expr_struct = dim_data.get("expr_struct")
        if isinstance(expr_col, str):
            expr = lambda t, c=expr_col: t[c]  # noqa: E731
        elif expr_struct is not None:
            expr = context.deserialize_expr(expr_struct, f"Dimension '{name}'")
        else:
            expr = lambda t, n=name: t[n]  # noqa: E731
        return ops.Dimension(
            expr=expr,
            description=dim_data.get("description"),
            is_entity=dim_data.get("is_entity", False),
            is_event_timestamp=dim_data.get("is_event_timestamp", False),
            is_time_dimension=dim_data.get("is_time_dimension", False),
            smallest_time_grain=dim_data.get("smallest_time_grain"),
        )

    def _create_measure(name: str, meas_data: dict) -> ops.Measure:
        expr = context.deserialize_expr(
            meas_data.get("expr_struct"),
            f"Measure '{name}'",
        )
        return ops.Measure(
            expr=expr,
            description=meas_data.get("description"),
            requires_unnest=tuple(meas_data.get("requires_unnest", [])),
        )

    def _unwrap_cached_nodes(expr):
        """Unwrap Tag and CachedNode wrappers to get to the underlying expression."""
        return _unwrap_xorq_wrappers(expr, strip_remote=False)

    def _reconstruct_table():
        from xorq.common.utils.graph_utils import walk_nodes
        from xorq.common.utils.ibis_utils import from_ibis
        from xorq.expr.relations import Read
        from xorq.vendor import ibis
        from xorq.vendor.ibis.expr.operations import relations as xorq_rel

        unwrapped_expr = _unwrap_cached_nodes(xorq_expr)

        is_self_ref = isinstance(unwrapped_expr.op(), xorq_rel.SelfReference)

        read_ops = list(walk_nodes((Read,), unwrapped_expr))
        in_memory_tables = list(walk_nodes((xorq_rel.InMemoryTable,), unwrapped_expr))
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), unwrapped_expr))

        total_leaf_tables = (
            len(read_ops) + len(in_memory_tables) + (len(db_tables) if not read_ops else 0)
        )
        if total_leaf_tables > 1:
            expr = (
                unwrapped_expr.to_expr() if hasattr(unwrapped_expr, "to_expr") else unwrapped_expr
            )
            return from_ibis(expr) if not hasattr(expr.op(), "source") else expr

        if read_ops:
            base = read_ops[0].to_expr()
            return base.view() if is_self_ref else base

        if in_memory_tables:
            proxy = in_memory_tables[0].args[2]
            return from_ibis(ibis.memtable(proxy.to_frame()))

        if db_tables:
            base = db_tables[0].to_expr()
            return base.view() if is_self_ref else base

        return xorq_expr.to_expr()

    dim_meta = context.parse_field(metadata, "dimensions")
    meas_meta = context.parse_field(metadata, "measures")
    calc_meta = context.parse_field(metadata, "calc_measures")

    dimensions = {name: _create_dimension(name, data) for name, data in dim_meta.items()}
    measures = {name: _create_measure(name, data) for name, data in meas_meta.items()}
    calc_measures = deserialize_calc_measures(calc_meta) if calc_meta else {}

    return bsl_expr.SemanticModel(
        table=_reconstruct_table(),
        dimensions=dimensions,
        measures=measures,
        calc_measures=calc_measures,
        name=metadata.get("name"),
    )


@register_reconstructor("SemanticFilterOp")
def _reconstruct_filter(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticFilterOp requires source")
    predicate = context.deserialize_expr(
        metadata.get("predicate_struct"),
        "SemanticFilterOp",
    )
    return source.filter(predicate)


@register_reconstructor("SemanticGroupByOp")
def _reconstruct_group_by(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticGroupByOp requires source")
    keys = tuple(context.parse_field(metadata, "keys")) or ()
    return source.group_by(*keys) if keys else source


@register_reconstructor("SemanticAggregateOp")
def _reconstruct_aggregate(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticAggregateOp requires source")
    aggs_struct = context.parse_structured_dict(metadata.get("aggs_struct", ()))
    if not aggs_struct:
        raise ValueError("SemanticAggregateOp has no aggs_struct")
    return source.aggregate(*aggs_struct.keys())


@register_reconstructor("SemanticMutateOp")
def _reconstruct_mutate(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticMutateOp requires source")

    post_struct = context.parse_structured_dict(metadata.get("post_struct", ()))

    if post_struct:
        exprs = {
            name: context.deserialize_expr(data, f"Mutate({name})")
            for name, data in post_struct.items()
        }
        return source.mutate(**exprs)
    return source


@register_reconstructor("SemanticProjectOp")
def _reconstruct_project(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticProjectOp requires source")
    fields = tuple(context.parse_field(metadata, "fields")) or ()
    return source.select(*fields) if fields else source


@register_reconstructor("SemanticOrderByOp")
def _reconstruct_order_by(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticOrderByOp requires source")

    def _deserialize_key(key_meta: dict):
        match key_meta.get("type"):
            case "string":
                return key_meta["value"]
            case "callable":
                return context.deserialize_expr(
                    key_meta.get("value_struct"),
                    "Order-by callable key",
                )
            case _:
                raise ValueError(f"Unknown order-by key type: {key_meta.get('type')}")

    order_keys_meta = context.parse_field(metadata, "order_keys")
    if not order_keys_meta:
        return source
    keys = [_deserialize_key(key_meta) for key_meta in order_keys_meta]
    return source.order_by(*keys) if keys else source


@register_reconstructor("SemanticLimitOp")
def _reconstruct_limit(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    if source is None:
        raise ValueError("SemanticLimitOp requires source")
    return source.limit(n=int(metadata.get("n", 0)), offset=int(metadata.get("offset", 0)))


@register_reconstructor("SemanticJoinOp")
def _reconstruct_join(
    metadata: dict, xorq_expr, source, context: BSLSerializationContext
):
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.vendor.ibis.expr.operations import relations as xorq_rel

    from .. import expr as bsl_expr

    left_metadata = context.parse_field(metadata, "left")
    right_metadata = context.parse_field(metadata, "right")

    if not left_metadata or not right_metadata:
        raise ValueError("SemanticJoinOp requires both 'left' and 'right' metadata")

    left_xorq_expr, right_xorq_expr = _split_join_expr(xorq_expr)

    db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), xorq_expr))
    if db_tables:
        canonical_backend = db_tables[0].source
        left_xorq_expr = _rebind_to_backend(left_xorq_expr, canonical_backend)
        right_xorq_expr = _rebind_to_backend(right_xorq_expr, canonical_backend)

    left_model = reconstruct_bsl_operation(left_metadata, left_xorq_expr, context)
    right_model = reconstruct_bsl_operation(right_metadata, right_xorq_expr, context)

    how = metadata.get("how", "inner")
    on_struct = metadata.get("on_struct")

    if on_struct is None:
        return bsl_expr.SemanticJoin(
            left=left_model.op() if hasattr(left_model, "op") else left_model,
            right=right_model.op() if hasattr(right_model, "op") else right_model,
            on=None,
            how=how,
        )

    predicate = context.deserialize_join_predicate(on_struct)
    return left_model.join_many(right_model, on=predicate, how=how)


# ---------------------------------------------------------------------------
# xorq wrapper helpers
# ---------------------------------------------------------------------------


def _unwrap_xorq_wrappers(expr, *, strip_remote: bool = False):
    """Walk past Tag, CachedNode, and optionally RemoteTable wrappers."""
    from xorq.expr.relations import CachedNode, RemoteTable, Tag

    op = expr.op()
    if isinstance(op, Tag):
        expr = op.parent.to_expr() if hasattr(op.parent, "to_expr") else op.parent
        op = expr.op()
    if isinstance(op, CachedNode):
        expr = op.parent
        op = expr.op()
    if strip_remote and isinstance(op, RemoteTable):
        expr = op.args[3]
    return expr


def _unwrap_join_ref(expr):
    """If expr is a JoinReference, return the underlying table."""
    from xorq.vendor.ibis.expr.operations.relations import JoinReference

    if isinstance(expr.op(), JoinReference):
        return expr.op().parent.to_expr()
    return expr


def _rebind_to_backend(expr, target_backend):
    """Rebind all DatabaseTable ops in *expr* to use *target_backend*."""
    from xorq.common.utils.graph_utils import replace_nodes
    from xorq.vendor.ibis.expr.operations import relations as xorq_rel

    def replacer(op, _kwargs):
        if isinstance(op, xorq_rel.DatabaseTable) and op.source is not target_backend:
            kwargs = dict(zip(op.__argnames__, op.__args__, strict=False))
            kwargs["source"] = target_backend
            return op.__recreate__(kwargs)
        return op

    return replace_nodes(replacer, expr).to_expr()


def _split_join_expr(xorq_expr):
    """Extract left and right table expressions from a joined xorq expression."""
    from xorq.vendor.ibis.expr.operations.relations import JoinChain

    expr = _unwrap_xorq_wrappers(xorq_expr, strip_remote=True)
    op = expr.op()

    while not isinstance(op, JoinChain) and hasattr(op, "parent"):
        expr = op.parent.to_expr() if hasattr(op.parent, "to_expr") else op.parent
        op = expr.op()

    if not isinstance(op, JoinChain) or not op.rest:
        return xorq_expr, xorq_expr

    right_expr = _unwrap_join_ref(op.rest[-1].table.to_expr())
    match len(op.rest):
        case 1:
            left_expr = _unwrap_join_ref(op.first.to_expr())
        case _:
            left_expr = _unwrap_join_ref(op.first.to_expr())
            for link in op.rest[:-1]:
                preds = tuple(p.to_expr() for p in link.predicates)
                left_expr = left_expr.join(
                    _unwrap_join_ref(link.table.to_expr()), preds, how=link.how
                )

    return left_expr, right_expr


# ---------------------------------------------------------------------------
# Metadata extraction from xorq expressions
# ---------------------------------------------------------------------------


def extract_xorq_metadata(xorq_expr) -> dict[str, Any] | None:
    """Walk a xorq expression tree to find BSL tag metadata."""
    from xorq.expr.relations import Tag

    @safe
    def get_op(expr):
        return expr.op()

    @safe
    def get_parent_expr(op):
        return op.parent.to_expr()

    def is_bsl_tag(op) -> bool:
        return isinstance(op, Tag) and "bsl_op_type" in op.metadata

    maybe_op = get_op(xorq_expr).map(lambda op: op if is_bsl_tag(op) else None)

    if bsl_op := maybe_op.value_or(None):
        return dict(bsl_op.metadata)

    parent_expr = get_op(xorq_expr).bind(get_parent_expr).value_or(None)
    if parent_expr is None:
        return None

    return extract_xorq_metadata(parent_expr)


# ---------------------------------------------------------------------------
# Dispatch entry point
# ---------------------------------------------------------------------------


def reconstruct_bsl_operation(
    metadata: dict[str, Any],
    xorq_expr,
    context: BSLSerializationContext,
):
    """Reconstruct a BSL operation from metadata and a xorq expression.

    Walks the metadata tree recursively, dispatching to registered
    reconstructors by ``bsl_op_type``.
    """
    op_type = metadata.get("bsl_op_type")
    source = None
    source_metadata = context.parse_field(metadata, "source")
    if source_metadata:
        source = reconstruct_bsl_operation(source_metadata, xorq_expr, context)
    reconstructor = BSL_RECONSTRUCTORS.get(op_type)
    if not reconstructor:
        raise ValueError(f"Unknown BSL operation type: {op_type}")
    return reconstructor(metadata, xorq_expr, source, context)
