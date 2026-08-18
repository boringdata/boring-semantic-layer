"""xorq/plain-ibis backend interop for the semantic ops.

Conversion of user tables into the xorq-vendored flavor, and rebinding of
DatabaseTable backends so composed expressions share one connection.
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

    from .._xorq import SortKey as XorqSortKey
    from .._xorq import map_ibis

    if IbisSortKey in map_ibis.registry:
        return  # already patched

    @map_ibis.register(IbisSortKey)
    def _map_sort_key(val, kwargs=None):
        # ibis 12 uses .arg, ibis 11 uses .expr
        sort_expr = getattr(val, "arg", None) or val.expr
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
        except Exception as exc:
            logger.debug(
                "xorq conversion unavailable for %s table (%s); continuing on the plain-ibis path.",
                type(table).__name__,
                exc,
            )
            return table
    return table


def _connection_identity(backend) -> tuple:
    """Identify the physical connection a backend reads from.

    ``from_ibis()`` mints a fresh ``Backend`` wrapper per call but reuses the
    caller's DBAPI connection object, so wrapper identity says nothing about
    which database a table lives in while connection identity does. Backends
    that expose no ``con`` fall back to their own identity, which makes
    rebinding a no-op for them rather than a guess.
    """
    con = getattr(backend, "con", None)
    if con is None:
        return ("backend", id(backend))
    return ("con", id(con))


def _rebind_to_backend(expr, target_backend):
    """Rebind ``DatabaseTable`` ops in *expr* that share *target_backend*'s connection.

    Low-level primitive shared with ``serialization.reconstruct``.
    No-op on plain ibis expressions or when xorq is unavailable for any
    reason; callers must pass a xorq-vendored ``target_backend``.

    Tables belonging to a *different* connection are left alone. Rebinding
    those used to repoint them at the canonical backend without checking,
    so joining two same-schema databases (prod and staging, or two shards)
    silently read every column from whichever one happened to be first.
    Leaving them untouched lets the engine raise its own multiple-backends
    error instead of returning plausible numbers from the wrong database.
    """
    try:
        from .._xorq import relations as xorq_rel
    except Exception:
        return expr

    target_identity = _connection_identity(target_backend)
    foreign: set[str] = set()

    def _recreate(op, _kwargs, **overrides):
        kwargs = dict(zip(op.__argnames__, op.__args__, strict=False))
        if _kwargs:
            kwargs.update(_kwargs)
        kwargs.update(overrides)
        return op.__recreate__(kwargs)

    def replacer(op, _kwargs):
        if isinstance(op, xorq_rel.DatabaseTable) and op.source is not target_backend:
            if _connection_identity(op.source) == target_identity:
                return _recreate(op, _kwargs, source=target_backend)
            foreign.add(op.name)
        if _kwargs:
            return _recreate(op, _kwargs)
        return op

    rebound = expr.op().replace(replacer).to_expr()
    if foreign:
        logger.warning(
            "Expression spans more than one database connection; tables %s were "
            "left bound to their own backend. A query mixing them will fail in "
            "the engine — read them through a single connection (or ATTACH one "
            "database to the other) if they are meant to be joined.",
            sorted(foreign),
        )
    return rebound


def _rebind_to_canonical_backend(expr):
    """Rebind divergent ``DatabaseTable`` backends in *expr* to share one.

    ``from_ibis()`` creates a distinct ``Backend`` per call, so expressions
    built by composing separately-converted tables contain multiple
    backends. Picking the first ``DatabaseTable``'s source as canonical
    and rebinding the rest eliminates "Multiple backends found" errors
    at execution time.

    No-op on plain ibis expressions (not xorq-vendored).
    """
    from .._xorq import HAS_XORQ

    # Without xorq there is only one backend, so there is nothing to rebind.
    if not HAS_XORQ:
        return expr

    try:
        from .._xorq import relations as xorq_rel
        from .._xorq import walk_nodes
    except Exception:
        return expr

    try:
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), expr))
    except Exception:
        return expr

    canonical = db_tables[0].source if db_tables else None
    if canonical is None:
        return expr

    return _rebind_to_backend(expr, canonical)
