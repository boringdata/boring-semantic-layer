"""Reduction-kind classification for fan-out-safe re-aggregation.

The single place that answers "what reduction is this expression, and how
does it re-aggregate from a finer pre-aggregate?" — mean decomposes,
count-distinct defers, sum/count re-sum, min/max re-min/max, everything
else must be computed at exact grain.
"""

from __future__ import annotations

from ibis.expr import operations as ibis_ops
from returns.result import safe

from .._xorq import operations as xorq_ops


def _reductions_for_expr(expr):
    """Return the ``reductions`` ops module matching *expr*'s ibis flavor.

    A user-supplied callable produces expressions against exactly one of
    ``ibis`` or ``xorq.vendor.ibis`` — whichever the underlying table came
    from. Pick that module so isinstance checks compare against a single
    concrete type rather than a cross-module union.
    """
    if type(expr.op()).__module__.startswith("xorq.vendor.ibis"):
        return xorq_ops.reductions
    return ibis_ops.reductions


def _is_mean_expr(expr):
    """Check if an ibis expression is a Mean/Average reduction."""
    try:
        return isinstance(expr.op(), _reductions_for_expr(expr).Mean)
    except Exception:
        return False


def _is_count_distinct_expr(expr):
    """Check if an ibis expression is a CountDistinct (nunique) reduction."""
    return safe(lambda: isinstance(expr.op(), _reductions_for_expr(expr).CountDistinct))().value_or(
        False
    )


def _is_count_expr(expr):
    """Check if an expression is COUNT(column) or COUNT(*)."""
    try:
        reductions = _reductions_for_expr(expr)
        return isinstance(expr.op(), (reductions.Count, reductions.CountStar))
    except Exception:
        return False


def _fill_missing_count_identities(table, measure_names):
    """Restore COUNT's empty-set identity after a dimension-bridge join."""
    replacements = {
        name: table[name].fill_null(0) for name in measure_names if name in table.columns
    }
    return table.mutate(**replacements) if replacements else table


def _reagg_op_for_expr(expr):
    """Return the re-aggregation operation name for an ibis expression.

    Additive measures (SUM, COUNT) re-aggregate with ``sum``; MIN and MAX
    with ``min``/``max``. MEAN never reaches here (decomposed by
    ``_is_mean_expr``), nor does COUNT DISTINCT (deferred).

    Returns ``None`` for everything else — median, stddev, variance,
    compound expressions like ``sum()/count()`` — which cannot be
    re-aggregated from a finer pre-aggregate at all. Those measures must
    be computed at the exact target grain (``_exact_grain_preagg``);
    the previous ``"sum"`` default silently summed per-key medians.
    """
    op = expr.op()
    reductions = _reductions_for_expr(expr)
    if isinstance(op, reductions.Min):
        return "min"
    if isinstance(op, reductions.Max):
        return "max"
    if isinstance(op, (reductions.Sum, reductions.Count, reductions.CountStar)):
        return "sum"
    if isinstance(op, reductions.Mean):
        raise ValueError(
            f"Mean expression {expr.get_name()!r} was not decomposed — "
            "this is a bug in the pre-aggregation logic"
        )
    if isinstance(op, reductions.CountDistinct):
        raise ValueError(
            f"CountDistinct expression {expr.get_name()!r} was not deferred — "
            "this is a bug in the pre-aggregation logic"
        )
    return None


def _build_reagg(col_ref, op_name):
    """Apply the correct re-aggregation to a column reference."""
    return getattr(col_ref, op_name)()
