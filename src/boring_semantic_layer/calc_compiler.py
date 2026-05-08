"""Ibis-native calc-measure compiler.

Replaces the curated-AST ``compile_grouped_with_all`` /
``infer_calc_dtype`` pipeline with one that accepts user-written ibis
expressions directly. The compiler relies on
:func:`boring_semantic_layer.calc_analyzer.analyze_calc_expr` to
classify each measure structurally; placement (pushable vs.
post-aggregation) is read off the analysis record rather than off
curated-AST node types.

Architecture
------------

A calc-measure lambda is evaluated against an :class:`IbisCalcScope`,
which dispatches name lookups to two tables:

- ``base_tbl`` for raw columns (used by inline aggregations like
  ``t.distance.sum()`` inside a calc measure).
- ``virtual_agg_tbl`` for measure references (a synthetic ibis table
  whose schema mirrors the post-aggregation result).

The lambda returns one ibis expression that may reference *both*
tables. The analyzer walks the resulting tree to record pushability,
totals patterns, and dependencies. At compile time, references to the
virtual aggregated table are substituted with the real aggregated
table via ``op.replace({...})``.

The ``t.all(x)`` API emits ``x.sum().over(window())`` — an empty window
over a reduction. The analyzer recognizes this structural shape as the
"totals" pattern; compilation lets the windowed-reduction shape pass
through unchanged so the SQL backend evaluates it as the windowed sum
of the post-aggregated column.
"""

from __future__ import annotations

from typing import Any

from ._xorq import Deferred, Field, Node
from ._xorq import ibis as ibis_mod
from .calc_analyzer import (
    CalcExprAnalysis,
    analyze_calc_expr,
    virtual_agg_table,
)


class IbisCalcScope:
    """Dual-table scope passed to calc-measure lambdas.

    ``t.measure_name`` returns a column on the virtual aggregated
    table; ``t.column_name`` returns a column on the base table. The
    same ``t`` supports both lookups so that calc-measure expressions
    can mix measure references with inline aggregations transparently.
    """

    __slots__ = ("_base_tbl", "_virtual_agg_tbl", "_known_measures")

    def __init__(
        self,
        base_tbl,
        virtual_agg_tbl,
        known_measures,
    ):
        object.__setattr__(self, "_base_tbl", base_tbl)
        object.__setattr__(self, "_virtual_agg_tbl", virtual_agg_tbl)
        object.__setattr__(self, "_known_measures", frozenset(known_measures))

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._known_measures:
            return self._virtual_agg_tbl[name]
        if hasattr(self._base_tbl, "columns") and name in self._base_tbl.columns:
            return self._base_tbl[name]
        # Fall through to ibis Table methods (e.g. `count`).
        return getattr(self._base_tbl, name)

    def __getitem__(self, name: str):
        if name in self._known_measures:
            return self._virtual_agg_tbl[name]
        return self._base_tbl[name]

    def all(self, x: Any):
        """Emit the totals marker recognized by the analyzer.

        For ibis values, returns ``x.sum().over(window())`` if ``x`` is
        a column-like value, or ``x.over(window())`` if ``x`` is
        already a reduction. For string names, looks the column up on
        the virtual aggregated table first (so ``t.all("measure_name")``
        works), then on the base table.
        """
        if isinstance(x, str):
            if x in self._known_measures:
                col = self._virtual_agg_tbl[x]
            else:
                col = self._base_tbl[x]
            return col.sum().over(ibis_mod.window())

        # Already a reduction (Sum, Mean, ...): wrap in window directly.
        if hasattr(x, "op") and callable(x.op):
            try:
                from ._xorq import operations as ibis_ops

                Reduction = getattr(ibis_ops, "Reduction", None)
                if Reduction is not None and isinstance(x.op(), Reduction):
                    return x.over(ibis_mod.window())
            except Exception:
                pass

        # Column-like: aggregate-then-window so the empty window applies
        # to the totals.
        if hasattr(x, "sum"):
            return x.sum().over(ibis_mod.window())

        return x


def evaluate_calc_lambda(
    fn,
    base_tbl,
    known_measures: frozenset[str],
    virtual_agg_schema: dict[str, Any] | None = None,
):
    """Run a calc-measure lambda and return the ibis expression it builds.

    Constructs an :class:`IbisCalcScope` over ``base_tbl`` and a
    synthetic virtual aggregated table whose schema is derived from
    ``virtual_agg_schema`` (or inferred from ``known_measures`` with
    placeholder dtypes when not supplied). The scope is passed to
    ``fn`` exactly once; the returned ibis expression encodes the
    full structural shape the analyzer needs.

    Deferreds are resolved against the scope.
    """
    if virtual_agg_schema is None:
        # Placeholder dtypes — analyzer cares about structure, not
        # exact types. Compile-time substitution swaps in the real
        # aggregated table whose dtypes are correct.
        virtual_agg_schema = {name: "float64" for name in known_measures}
    if not virtual_agg_schema:
        # ibis won't build a zero-column table; give it a dummy column.
        virtual_agg_schema = {"__bsl_unused__": "int64"}

    vt = virtual_agg_table(virtual_agg_schema)
    scope = IbisCalcScope(base_tbl, vt, known_measures)

    if isinstance(fn, Deferred):
        return fn.resolve(scope), vt

    if callable(fn):
        return fn(scope), vt

    return fn, vt


def classify_calc_lambda(
    fn,
    base_tbl,
    known_measures: frozenset[str],
    virtual_agg_schema: dict[str, Any] | None = None,
) -> tuple[Any, CalcExprAnalysis]:
    """Evaluate the lambda and run :func:`analyze_calc_expr` on the result.

    Returns ``(expr, analysis)`` where ``expr`` is the ibis expression
    the lambda built (with references against the virtual aggregated
    table) and ``analysis`` is the structural classification. The
    caller can then route to base-measure or calc-measure compilation
    based on ``analysis.pushable``.
    """
    expr, vt = evaluate_calc_lambda(fn, base_tbl, known_measures, virtual_agg_schema)
    base_op = base_tbl.op() if hasattr(base_tbl, "op") and callable(base_tbl.op) else None
    analysis = analyze_calc_expr(expr, known_measures=known_measures, base_table_op=base_op)
    return expr, analysis


def _substitute_table(expr_or_op, old_tbl, new_tbl):
    """Replace references to ``old_tbl`` with ``new_tbl`` in an ibis op tree."""
    op = expr_or_op.op() if hasattr(expr_or_op, "op") and callable(expr_or_op.op) else expr_or_op
    old_op = old_tbl.op() if hasattr(old_tbl, "op") and callable(old_tbl.op) else old_tbl
    new_op = new_tbl.op() if hasattr(new_tbl, "op") and callable(new_tbl.op) else new_tbl
    return op.replace({old_op: new_op}).to_expr()


def compile_calc_measure(
    expr,
    virtual_agg_tbl,
    real_agg_tbl,
):
    """Compile a calc-measure ibis expression against the real agg table.

    Substitutes references to ``virtual_agg_tbl`` with ``real_agg_tbl``.
    The resulting ibis expression is suitable for use as a column in
    ``real_agg_tbl.mutate(name=expr)``.
    """
    return _substitute_table(expr, virtual_agg_tbl, real_agg_tbl)


def compile_calc_measures(
    real_agg_tbl,
    calc_exprs: dict[str, tuple[Any, Any]],
):
    """Apply all post-aggregation calc measures to the aggregated table.

    Parameters
    ----------
    real_agg_tbl:
        The actual aggregated ibis table.
    calc_exprs:
        Mapping of ``measure_name → (expr, virtual_agg_tbl)``. Each
        ``expr`` was built against its ``virtual_agg_tbl`` during
        classification; we substitute that virtual table with
        ``real_agg_tbl`` to produce the post-agg column.

    Returns
    -------
    The aggregated table with calc-measure columns added via
    ``mutate(...)``.
    """
    if not calc_exprs:
        return real_agg_tbl
    new_cols = {
        name: compile_calc_measure(expr, vt, real_agg_tbl)
        for name, (expr, vt) in calc_exprs.items()
    }
    return real_agg_tbl.mutate(**new_cols)
