"""xorq/ibis compatibility shims.

These functions bridge plain ibis and xorq's vendored ibis: convert one
to the other (``_ensure_xorq_table``), patch SortKey shape differences
(``_patch_xorq_sortkey_compat``), and rebind ``DatabaseTable`` nodes so
expressions composed from separately-converted tables share a single
backend (``_rebind_to_backend`` / ``_rebind_to_canonical_backend``).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _patch_xorq_sortkey_compat():
    """Register a map_ibis handler so ibis SortKey → xorq SortKey.

    ibis 11 uses ``SortKey.expr``, ibis 12 renamed it to ``SortKey.arg``,
    while xorq's vendored ibis keeps ``SortKey.expr``.  Handle both.
    """
    from ibis.expr.operations.sortkeys import SortKey as IbisSortKey

    from .._xorq import SortKey as XorqSortKey, map_ibis

    if IbisSortKey in map_ibis.registry:
        return  # already patched

    @map_ibis.register(IbisSortKey)
    def _map_sort_key(val, kwargs=None):
        # ibis 12 uses .arg, ibis 11 uses .expr
        sort_expr = getattr(val, "arg", None) or getattr(val, "expr")
        return XorqSortKey(
            expr=map_ibis(sort_expr, None),
            ascending=val.ascending,
            nulls_first=val.nulls_first,
        )


def _ensure_xorq_table(table):
    """Convert plain ibis Table to xorq-vendored ibis if possible.

    This is the single boundary between user-supplied ibis tables and
    BSL's internal xorq representation. ``SemanticModel`` calls it once
    at construction so internal code paths can assume xorq tables when
    the backend is supported, and a plain ibis fallback otherwise.

    Falls back to returning the plain ibis table when the backend is not
    supported by xorq (e.g. Databricks). Idempotent: calling it on a
    xorq-vendored table is a cheap no-op.
    """
    _patch_xorq_sortkey_compat()
    if "xorq.vendor.ibis" not in type(table).__module__:
        try:
            from .._xorq import from_ibis

            return from_ibis(table)
        except Exception:
            # Backend isn't supported by xorq's map_ibis registry (e.g.
            # Databricks). Fall back so plain-ibis paths can still execute.
            logger.debug(
                "from_ibis failed for %s; using plain ibis table",
                type(table).__module__,
                exc_info=True,
            )
            return table
    return table


def _rebind_to_backend(expr, target_backend):
    """Rebind every ``DatabaseTable`` op in *expr* to *target_backend*.

    Low-level primitive shared with ``serialization.reconstruct``.
    No-op on plain ibis expressions or when xorq is unavailable for any
    reason; callers must pass a xorq-vendored ``target_backend``.
    """
    try:
        from .._xorq import relations as xorq_rel
    except ImportError:
        return expr

    def _recreate(op, _kwargs, **overrides):
        kwargs = dict(zip(op.__argnames__, op.__args__, strict=False))
        if _kwargs:
            kwargs.update(_kwargs)
        kwargs.update(overrides)
        return op.__recreate__(kwargs)

    def replacer(op, _kwargs):
        if isinstance(op, xorq_rel.DatabaseTable) and op.source is not target_backend:
            return _recreate(op, _kwargs, source=target_backend)
        if _kwargs:
            return _recreate(op, _kwargs)
        return op

    return expr.op().replace(replacer).to_expr()


def _rebind_to_canonical_backend(expr):
    """Rebind divergent ``DatabaseTable`` backends in *expr* to share one.

    ``from_ibis()`` creates a distinct ``Backend`` per call, so expressions
    built by composing separately-converted tables contain multiple
    backends. Picking the first ``DatabaseTable``'s source as canonical
    and rebinding the rest eliminates "Multiple backends found" errors
    at execution time.

    No-op on plain ibis expressions (not xorq-vendored).
    """
    try:
        from .._xorq import relations as xorq_rel, walk_nodes
    except ImportError:
        return expr

    try:
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), expr))
    except Exception:
        # walk_nodes can't traverse plain ibis trees; treat as no-op.
        logger.debug("walk_nodes failed on plain ibis expr", exc_info=True)
        return expr

    canonical = db_tables[0].source if db_tables else None
    if canonical is None:
        return expr

    return _rebind_to_backend(expr, canonical)
