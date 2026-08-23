"""Handler registry for reconstructing BSL ops from tag metadata.

Dispatches on ``metadata["bsl_op_type"]`` strings — matching xorq's
``FROM_YAML_HANDLERS`` pattern. Each handler receives ``(metadata, xorq_expr,
source, context)`` and returns a BSL expression.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from typing import Any

from returns.result import safe

from .context import SUPPORTED_PAYLOAD_MAJORS, BSLSerializationContext
from .extract import deserialize_calc_measures
from .freeze import thaw

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
            # Every serialized dimension carries either a column name or a
            # resolver tree. Falling back to ``t[name]`` here turned an
            # unreadable payload (a v1.0 pickle field, a future encoding)
            # into a raw column silently: ``amount = _.amount * 1.1`` came
            # back as ``amount``, with no error and plausible numbers.
            raise ValueError(
                f"Dimension {name!r} has no readable expression in this payload "
                f"(keys: {sorted(dim_data)}). It was written by an incompatible "
                "version of boring-semantic-layer — re-serialize the model."
            )
        return ops.Dimension(
            expr=expr,
            description=dim_data.get("description"),
            is_entity=dim_data.get("is_entity", False),
            is_event_timestamp=dim_data.get("is_event_timestamp", False),
            is_time_dimension=dim_data.get("is_time_dimension", False),
            smallest_time_grain=dim_data.get("smallest_time_grain"),
            derived_dimensions=tuple(dim_data.get("derived_dimensions") or ()),
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
        from .._xorq import (
            Read,
            from_ibis,
            ibis,
            walk_nodes,
        )
        from .._xorq import (
            relations as xorq_rel,
        )

        unwrapped_expr = _unwrap_cached_nodes(xorq_expr)

        is_self_ref = isinstance(unwrapped_expr.op(), xorq_rel.SelfReference)

        read_ops = list(walk_nodes((Read,), unwrapped_expr))
        in_memory_tables = list(walk_nodes((xorq_rel.InMemoryTable,), unwrapped_expr))
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), unwrapped_expr))
        unbound_tables = list(walk_nodes((xorq_rel.UnboundTable,), unwrapped_expr))

        total_leaf_tables = (
            len(read_ops)
            + len(in_memory_tables)
            + (len(db_tables) if not read_ops else 0)
            + len(unbound_tables)
        )
        if total_leaf_tables > 1:
            expr = (
                unwrapped_expr.to_expr() if hasattr(unwrapped_expr, "to_expr") else unwrapped_expr
            )
            return from_ibis(expr) if not hasattr(expr.op(), "source") else expr

        # Preserve authored deferred shaping: when the leaf is a pure per-row
        # chain (mutate/select/filter — no aggregation, no join) over exactly
        # one relation, the chain IS the model's table. Walking to the bare
        # base relation here discarded the shaping, so a model built on a
        # deferred star-schema view (e.g. columns like `is_open` derived via
        # .mutate) recovered against the RAW source and every field
        # referencing a derived column failed to resolve. Query entries
        # (lowered aggregations) still fall through to the base walk below —
        # digging under the aggregate is what recovery is FOR there. Reserved
        # __bsl_jk_ join-key temporaries a preserved chain may carry are
        # inverted by _strip_internal_join_temps at the call site.
        from .._xorq import JoinChain

        leaf_op = unwrapped_expr.op()
        is_bare_leaf = isinstance(
            leaf_op,
            (
                Read,
                xorq_rel.InMemoryTable,
                xorq_rel.DatabaseTable,
                xorq_rel.UnboundTable,
                xorq_rel.SelfReference,
            ),
        )
        if (
            total_leaf_tables == 1
            and not is_bare_leaf
            # memtable leaves keep the from_ibis conversion below
            and not in_memory_tables
            and not walk_nodes((xorq_rel.Aggregate, JoinChain), unwrapped_expr)
        ):
            return unwrapped_expr

        if read_ops:
            base = read_ops[0].to_expr()
            return base.view() if is_self_ref else base

        if in_memory_tables:
            proxy = in_memory_tables[0].args[2]
            return from_ibis(ibis.memtable(proxy.to_frame()))

        if db_tables:
            base = db_tables[0].to_expr()
            return base.view() if is_self_ref else base

        if unbound_tables:
            base = unbound_tables[0].to_expr()
            return base.view() if is_self_ref else base

        return xorq_expr.to_expr()

    dim_meta = context.parse_field(metadata, "dimensions")
    meas_meta = context.parse_field(metadata, "measures")
    calc_meta = context.parse_field(metadata, "calc_measures")

    dimensions = {name: _create_dimension(name, data) for name, data in dim_meta.items()}
    measures = {name: _create_measure(name, data) for name, data in meas_meta.items()}
    calc_measures = deserialize_calc_measures(calc_meta) if calc_meta else {}

    # Wrapper tables (join.with_measures()/with_dimensions()) must be
    # rebuilt AROUND the reconstructed join: without _source_join the
    # model executes on the lowered fanned-out join and the pre-agg
    # machinery (fan-out-safe sums, t.all() totals, filter pushdown)
    # never runs.
    source_join_meta = context.parse_field(metadata, "source_join")
    if source_join_meta:
        join_model = reconstruct_bsl_operation(source_join_meta, xorq_expr, context)
        join_op = join_model.op() if hasattr(join_model, "op") else join_model
        return bsl_expr.SemanticModel(
            table=join_op.to_untagged(),
            dimensions=dimensions,
            measures=measures,
            calc_measures=calc_measures,
            name=metadata.get("name"),
            description=metadata.get("description"),
            _source_join=join_op,
        )

    return bsl_expr.SemanticModel(
        table=_strip_internal_join_temps(_reconstruct_table()),
        dimensions=dimensions,
        measures=measures,
        calc_measures=calc_measures,
        name=metadata.get("name"),
        description=metadata.get("description"),
    )


@register_reconstructor("SemanticFilterOp")
def _reconstruct_filter(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
    if source is None:
        raise ValueError("SemanticFilterOp requires source")
    predicate = context.deserialize_expr(
        metadata.get("predicate_struct"),
        "SemanticFilterOp",
    )
    filter_fields = context.parse_field(metadata, "filter_fields")
    if filter_fields and (
        not isinstance(filter_fields, list | tuple)
        or not all(isinstance(field, str) and field for field in filter_fields)
    ):
        raise ValueError("SemanticFilterOp has invalid filter_fields metadata")
    if filter_fields:
        # Structured deserialization returns a Deferred and necessarily loses
        # attributes from the original JSON-filter callable.  Restore those
        # attributes on a small callable adapter instead of inferring field
        # ownership from the Deferred expression: string/callable filters can
        # contain identical syntax without carrying JSON's strict semantics.
        deferred_predicate = predicate

        def predicate(t):
            return deferred_predicate.resolve(t)

        predicate.__bsl_filter_fields__ = frozenset(filter_fields)
        predicate.__bsl_deferred_resolution__ = True
    return source.filter(predicate)


@register_reconstructor("SemanticGroupByOp")
def _reconstruct_group_by(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
    if source is None:
        raise ValueError("SemanticGroupByOp requires source")
    keys = tuple(context.parse_field(metadata, "keys")) or ()
    return source.group_by(*keys) if keys else source


@register_reconstructor("SemanticAggregateOp")
def _reconstruct_aggregate(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
    if source is None:
        raise ValueError("SemanticAggregateOp requires source")
    aggs_struct = context.parse_structured_dict(metadata.get("aggs_struct", ()))
    if not aggs_struct:
        raise ValueError("SemanticAggregateOp has no aggs_struct")

    # Entries the query referenced by bare measure name replay by name, so
    # they route back through measure resolution (fan-out-safe pre-aggregation
    # on joined models). Everything else is a query-local expression and is
    # rebuilt from its serialized resolver tree — replaying *those* by name
    # silently substitutes a same-named model measure for the user's
    # expression, e.g. ``aggregate(n=lambda t: t.a.max())`` returning the
    # model's ``n = a.sum()``.
    bare_refs = _bare_ref_names(metadata, aggs_struct, source)

    names: list[str] = []
    aliased: dict = {}
    for name, data in aggs_struct.items():
        if name in bare_refs or data is None:
            names.append(name)
        else:
            aliased[name] = context.deserialize_expr(data, f"Aggregate({name})")
    return source.aggregate(*names, **aliased)


def _bare_ref_names(metadata: dict, aggs_struct: dict, source) -> set[str]:
    """Names in ``aggs_struct`` that were written as bare measure references.

    Payloads written by current BSL carry ``agg_bare_refs`` explicitly. For
    older payloads, recover the distinction structurally: a bare reference
    serializes to exactly the tree of ``make_bare_ref_lambda(name)``, so
    comparing against a freshly built one separates the two cases without
    consulting the model's measure names.
    """
    if "agg_bare_refs" in metadata:
        declared = thaw(metadata["agg_bare_refs"])
        return {n for n in declared if isinstance(n, str)}

    from ..ops import make_bare_ref_lambda
    from .codec import expr_to_structured
    from .freeze import list_to_tuple

    known: set[str] = set()

    def _looks_like_model_measure(name: str) -> bool:
        # Historical heuristic, kept only for entries this function cannot
        # classify structurally (see below).
        nonlocal known
        if not known:
            source_op = source.op()
            for getter in ("get_measures", "get_calculated_measures"):
                with suppress(Exception):
                    known |= set(getattr(source_op, getter)().keys())
        return name in known or sum(1 for k in known if k.endswith(f".{name}")) == 1

    recovered: set[str] = set()
    for name, data in aggs_struct.items():
        if data is None:
            # Nothing to rebuild from — name replay is the only option.
            recovered.add(name)
            continue
        canonical = expr_to_structured(make_bare_ref_lambda(name)).value_or(None)
        if canonical is not None:
            if canonical == list_to_tuple(data):
                recovered.add(name)
        elif _looks_like_model_measure(name):
            recovered.add(name)
    return recovered


@register_reconstructor("SemanticProjectOp")
def _reconstruct_project(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
    if source is None:
        raise ValueError("SemanticProjectOp requires source")
    fields = tuple(context.parse_field(metadata, "fields")) or ()
    return source.select(*fields) if fields else source


@register_reconstructor("SemanticOrderByOp")
def _reconstruct_order_by(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
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
def _reconstruct_limit(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
    if source is None:
        raise ValueError("SemanticLimitOp requires source")
    return source.limit(n=int(metadata.get("n", 0)), offset=int(metadata.get("offset", 0)))


def _strip_internal_join_temps(expr):
    """Invert BSL's temporary join-key renames on a recovered leaf table.

    ``SemanticJoinOp.to_untagged`` renames left-side predicate columns that
    collide across the join to ``__bsl_jk_<name>`` (see ``_RenamedResolver``)
    to sidestep ibis ambiguous-deref errors. Those temporaries live in the
    lowered leaf projections. When leaf recovery cannot walk to the base
    relation and keeps a lowered projection as the model's table — e.g. an
    ``into_backend`` seam makes the leaf multi-relation — the declared
    dimensions/measures reference the ORIGINAL names and no longer resolve.
    Rename the reserved temporaries back so the recovered leaf carries the
    schema the model was authored against.

    Only exact-prefix temporaries whose original name is free are inverted;
    the ``__bsl_jk_<name>_N`` overflow spelling (a user column literally
    named ``__bsl_jk_<name>`` existed) is left alone — inverting it could
    corrupt that user column.
    """
    from ..ops._normalize import _BSL_JOIN_KEY_TMP_PREFIX

    try:
        columns = list(expr.columns)
    except Exception:
        return expr
    renames = {}
    for col in columns:
        if not col.startswith(_BSL_JOIN_KEY_TMP_PREFIX):
            continue
        original = col[len(_BSL_JOIN_KEY_TMP_PREFIX) :]
        if original and original not in columns and original not in renames:
            renames[original] = col
    if not renames:
        return expr
    try:
        return expr.rename(**renames)
    except Exception:
        return expr


def _validate_join_leaf(model, metadata, side: str) -> None:
    """Check a reconstructed join leaf against its declared fields.

    Only missing-column failures (AttributeError/KeyError) are treated as
    misassignment — other resolution errors (e.g. measures that need an
    unnest context) are not evidence the table is wrong.
    """
    from .. import ops as bsl_ops

    op = model.op() if hasattr(model, "op") else model
    if not isinstance(op, bsl_ops.SemanticTableOp):
        return
    try:
        tbl = op.table.to_expr() if hasattr(op.table, "to_expr") else op.table
    except Exception:
        return
    name = metadata.get("name") or side
    for kind, fields in (("dimension", op.get_dimensions()), ("measure", op.get_measures())):
        for fname, fn in fields.items():
            try:
                fn(tbl)
            except (AttributeError, KeyError) as exc:
                raise ValueError(
                    f"Round-trip could not recover the {side} join table "
                    f"{name!r}: its {kind} {fname!r} does not resolve against "
                    f"the recovered table ({type(exc).__name__}: {exc}). "
                    "If the underlying error names a missing METHOD, the "
                    "field's expression uses an API this ibis runtime does "
                    "not have (e.g. Column.filter — use .sum(where=...) "
                    "forms instead). If it names a missing COLUMN, the "
                    "expression was lowered through the pre-aggregation "
                    "path and cannot be reconstructed — serialize the model "
                    "(or the un-aggregated join) instead."
                ) from exc
            except Exception:
                continue


@register_reconstructor("SemanticJoinOp")
def _reconstruct_join(metadata: dict, xorq_expr, source, context: BSLSerializationContext):
    from .. import expr as bsl_expr
    from .._xorq import relations as xorq_rel
    from .._xorq import walk_nodes

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

    # Guard against leaf misassignment: expressions lowered through the
    # pre-agg path put partial-aggregate/key-bridge joins where the raw
    # join used to be, so _split_join_expr can hand back the wrong table
    # for a side. When shapes happen to align this silently returns wrong
    # numbers — validate that each leaf's declared fields resolve against
    # its recovered table and raise otherwise.
    _validate_join_leaf(left_model, left_metadata, "left")
    _validate_join_leaf(right_model, right_metadata, "right")

    # ``how`` in stored payloads is informational: semantic joins are always
    # LEFT joins (join_cross carries how="cross" on the op directly).
    # Default to "many" for payloads serialized before cardinality was
    # emitted — join_many is a safe superset of join_one behaviour, while
    # the reverse silently skips pre-aggregation.  (Fixes #223.)
    cardinality = metadata.get("cardinality", "many")
    on_struct = metadata.get("on_struct")

    if on_struct is None:
        return bsl_expr.SemanticJoin(
            left=left_model.op() if hasattr(left_model, "op") else left_model,
            right=right_model.op() if hasattr(right_model, "op") else right_model,
            on=None,
            how="cross" if cardinality == "cross" else "left",
            cardinality=cardinality,
        )

    predicate = context.deserialize_join_predicate(on_struct)
    join_method = {
        "one": "join_one",
        "many": "join_many",
        "cross": "join_cross",
    }.get(cardinality, "join_many")
    if join_method == "join_cross":
        return left_model.join_cross(right_model)
    return getattr(left_model, join_method)(right_model, on=predicate)


# ---------------------------------------------------------------------------
# xorq wrapper helpers
# ---------------------------------------------------------------------------


def _unwrap_xorq_wrappers(expr, *, strip_remote: bool = False):
    """Walk past Tag, CachedNode, and optionally RemoteTable wrappers."""
    from .._xorq import CachedNode, RemoteTable, Tag

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
    from .._xorq import JoinReference

    if isinstance(expr.op(), JoinReference):
        return expr.op().parent.to_expr()
    return expr


def _rebind_to_backend(expr, target_backend):
    """Rebind every ``DatabaseTable`` op in *expr* to *target_backend*.

    Thin re-export of the primitive defined in ``ops`` so callers in this
    module don't have to reach across the package layer.
    """
    from ..ops import _rebind_to_backend as _impl

    return _impl(expr, target_backend)


def _split_join_expr(xorq_expr):
    """Extract left and right table expressions from a joined xorq expression."""
    from .._xorq import JoinChain

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
    from .._xorq import Tag

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


#: Payload format versions this build knows how to read. ``bsl_version`` was
#: written from the start but never checked, so a v1.0 tag (whose expressions
#: were pickled — a format no longer read at all) used to load as a model with
#: silently degraded fields instead of failing.
def _check_payload_version(metadata: dict[str, Any]) -> None:
    """Refuse a payload written by an incompatible serializer version."""
    version = metadata.get("bsl_version")
    if version is None:
        # Metadata assembled in-process (tests, direct reconstructor calls)
        # carries no version; only tagged payloads are gated.
        return
    major = str(version).split(".", 1)[0]
    if not major.isdigit() or int(major) not in SUPPORTED_PAYLOAD_MAJORS:
        raise ValueError(
            f"Cannot read BSL payload version {version!r}: this build reads "
            f"major version(s) {sorted(SUPPORTED_PAYLOAD_MAJORS)}. Re-serialize "
            "the model with a matching boring-semantic-layer version."
        )


def reconstruct_bsl_operation(
    metadata: dict[str, Any],
    xorq_expr,
    context: BSLSerializationContext,
):
    """Reconstruct a BSL operation from metadata and a xorq expression.

    Walks the metadata tree recursively, dispatching to registered
    reconstructors by ``bsl_op_type``.
    """
    _check_payload_version(metadata)
    op_type = metadata.get("bsl_op_type")
    source = None
    source_metadata = context.parse_field(metadata, "source")
    if source_metadata:
        source = reconstruct_bsl_operation(source_metadata, xorq_expr, context)
    reconstructor = BSL_RECONSTRUCTORS.get(op_type)
    if not reconstructor:
        raise ValueError(f"Unknown BSL operation type: {op_type}")
    return reconstructor(metadata, xorq_expr, source, context)
