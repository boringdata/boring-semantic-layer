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

A calc-measure lambda is evaluated against an :class:`IbisCalcScope`
that dispatches name lookups to three tables:

- ``base_tbl`` for raw columns (used by inline aggregations like
  ``t.distance.sum()`` inside a calc measure).
- ``virtual_agg_tbl`` for measure references (a synthetic ibis table
  whose schema mirrors the post-aggregation result).
- ``totals_virtual_agg_tbl`` for ``t.all(measure_ref)`` — a parallel
  synthetic table representing the same measures computed without
  group_by. Compile-time substitution swaps it with a real totals
  aggregation cross-joined into the result so non-sum measures get
  correct overall values.

When a column name exists on both ``base_tbl`` and ``virtual_agg_tbl``,
the base column wins — historical curated-AST behavior where
``t.distance.sum()`` meant "sum the base column" even when ``distance``
was registered as a measure.
"""

from __future__ import annotations

import difflib
import logging
from collections.abc import Iterable
from typing import Any

from ._xorq import Field
from ._xorq import ibis as ibis_mod
from ._xorq import operations as ibis_ops
from .calc_analyzer import (
    CalcExprAnalysis,
    _walk,
    analyze_calc_expr,
    virtual_agg_table,
)
from .measure_scope import UnknownMeasureRefError

logger = logging.getLogger(__name__)


TOTALS_PREFIX = "__bsl_totals__"
"""Column prefix applied to totals-table columns when cross-joined into
the per-group result. Any column on a result table starting with this
prefix represents the same-named measure computed over the totals
aggregation; calc-measure compilation rewrites
``Field(totals_vt, name)`` references to point at these prefixed
columns."""


_EMPTY_VT_SCHEMA: dict[str, str] = {"__bsl_unused__": "int64"}
"""Placeholder schema used when a virtual aggregated table would
otherwise be empty (e.g. a model with no measures yet). ibis tables must
have at least one column; the sentinel name is unlikely to collide with
real column names and lets analyzer/compiler logic stay uniform."""


class TotalsNotAvailableError(RuntimeError):
    """Raised when a calc measure references ``t.all(measure_ref)`` but
    no totals table can be constructed in the current compilation context.

    Two situations produce this error:

    * The compilation path lacks the per-base aggregation specs needed
      to recompute totals (``apply_calc_measures`` called without
      ``agg_specs`` and no ``real_totals_tbl``).
    * The aggregation involves nested-array measures, which are
      computed at multiple grains and joined; building a totals table
      that respects all grains is not yet supported.
    """


def _to_op(x):
    """Return ``x.op()`` if ``x`` is an ibis expression-like, else ``x``.

    BSL accepts both expressions (``Table``/``Column``/...) and bare
    ops in many places. Centralizing the duck-type lets call sites
    stay focused on the substitution logic.
    """
    op = getattr(x, "op", None)
    return op() if callable(op) else x


def _drop_totals_columns(tbl, totals_prefix: str = TOTALS_PREFIX):
    """Project ``tbl`` to columns that do not carry the totals prefix.

    Used after a calc-measure ``mutate`` on a cross-joined
    ``real_with_totals`` table so the user-visible result no longer
    exposes the synthetic totals columns.
    """
    return tbl.select([c for c in tbl.columns if not c.startswith(totals_prefix)])


class IbisCalcScope:
    """Dual-table scope passed to calc-measure lambdas.

    ``t.column_name`` returns the base-table column when one exists;
    ``t.measure_name`` returns the virtual aggregated column otherwise.
    Base columns win on collision so that historical patterns like
    ``t.distance.sum()`` (where ``distance`` is also a measure name)
    still classify as a base aggregation rather than a post-aggregation
    sum.

    ``t.all(measure_ref)`` resolves to a Field on a parallel
    ``totals_virtual_agg_tbl`` that mirrors the post-aggregation schema
    but represents the same measures computed without group_by. The
    compiler later substitutes this synthetic table with a real totals
    table built from the base by re-running the aggregation without
    group keys, so non-sum measures (mean / quantile / …) get correct
    overall values rather than a windowed sum of per-group results.
    """

    __slots__ = (
        "_base_tbl",
        "_virtual_agg_tbl",
        "_totals_virtual_agg_tbl",
        "_known_measures",
        "_priority_measures",
    )

    def __init__(
        self,
        base_tbl,
        virtual_agg_tbl,
        known_measures,
        totals_virtual_agg_tbl=None,
        priority_measures=(),
    ):
        object.__setattr__(self, "_base_tbl", base_tbl)
        object.__setattr__(self, "_virtual_agg_tbl", virtual_agg_tbl)
        if totals_virtual_agg_tbl is None:
            vt_op = _to_op(virtual_agg_tbl)
            schema = (
                dict(vt_op.schema.items())
                if hasattr(vt_op, "schema")
                else {n: "float64" for n in known_measures}
            )
            if not schema:
                schema = dict(_EMPTY_VT_SCHEMA)
            totals_virtual_agg_tbl = ibis_mod.table(schema, name="__bsl_virtual_totals__")
        object.__setattr__(self, "_totals_virtual_agg_tbl", totals_virtual_agg_tbl)
        object.__setattr__(self, "_known_measures", frozenset(known_measures))
        object.__setattr__(self, "_priority_measures", frozenset(priority_measures))

    @property
    def tbl(self):
        """Backwards-compat: return the base table for callers that
        introspect ``scope.tbl`` (e.g. unnest inference)."""
        return self._base_tbl

    def _has_column(self, name: str) -> bool:
        return hasattr(self._base_tbl, "columns") and name in self._base_tbl.columns

    def _typo_suggestion(self, name: str) -> str | None:
        cutoff = 0.80
        candidates: list[tuple[str, str]] = []
        if self._known_measures:
            for match in difflib.get_close_matches(
                name, list(self._known_measures), n=3, cutoff=cutoff
            ):
                candidates.append(("measure", match))
        if hasattr(self._base_tbl, "columns"):
            for match in difflib.get_close_matches(
                name, list(self._base_tbl.columns), n=3, cutoff=cutoff
            ):
                candidates.append(("column", match))
        if not candidates:
            return None
        formatted = ", ".join(f"{kind} {match!r}" for kind, match in candidates)
        return f"Did you mean: {formatted}?"

    def _resolve_measure_name(self, name: str) -> str | None:
        """Resolve ``name`` to a known measure, including suffix matching.

        On a joined model, measure names are prefixed (``flights.flight_count``).
        A calc-measure lambda written on the un-joined model still references
        them by short name (``t.flight_count``); we transparently bridge by
        suffix-matching when a unique match exists.
        """
        if name in self._known_measures:
            return name
        suffix = f".{name}"
        matches = tuple(k for k in self._known_measures if k.endswith(suffix))
        if len(matches) == 1:
            return matches[0]
        return None

    def _resolve_priority_measure_name(self, name: str) -> str | None:
        if name in self._priority_measures and name in self._known_measures:
            return name
        suffix = f".{name}"
        matches = tuple(
            k
            for k in self._priority_measures
            if k in self._known_measures and k.endswith(suffix)
        )
        if len(matches) == 1:
            return matches[0]
        return None

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        resolved = self._resolve_priority_measure_name(name)
        if resolved is not None:
            return self._virtual_agg_tbl[resolved]
        if self._has_column(name):
            return self._base_tbl[name]
        resolved = self._resolve_measure_name(name)
        if resolved is not None:
            return self._virtual_agg_tbl[resolved]
        # Fall through to ibis Table methods (e.g. `count`).
        try:
            return getattr(self._base_tbl, name)
        except AttributeError:
            suggestion = self._typo_suggestion(name)
            if suggestion:
                raise UnknownMeasureRefError(
                    f"{name!r} is not a known measure or column. {suggestion}"
                ) from None
            raise

    def __getitem__(self, name: str):
        resolved = self._resolve_priority_measure_name(name)
        if resolved is not None:
            return self._virtual_agg_tbl[resolved]
        if self._has_column(name):
            return self._base_tbl[name]
        resolved = self._resolve_measure_name(name)
        if resolved is not None:
            return self._virtual_agg_tbl[resolved]
        return self._base_tbl[name]

    def all(self, x: Any):
        """Resolve a measure reference to its totals-table column.

        ``t.all(measure_name)`` and ``t.all(t.measure_name)`` both
        return ``Field(totals_virtual_agg_tbl, measure_name)``. The
        compiler builds a real totals table from the base aggregation
        (no group_by) and substitutes it in at compile time, so the
        result is the measure's overall value computed by the same
        formula — not a windowed sum of per-group values.

        Inline reductions (``t.all(t.distance.sum())``) keep the
        windowed-reduction shape here; the inline-reduction lift in
        :func:`lift_inline_reductions` rewrites them to totals-table
        Field references too.

        Raw base columns (``t.all("col")`` where ``col`` is not a
        measure) fall back to the legacy ``column.sum().over(window())``
        shape — there is no measure formula to re-apply.
        """
        if isinstance(x, str):
            resolved = self._resolve_measure_name(x)
            if resolved is not None:
                return self._totals_virtual_agg_tbl[resolved]
            if self._has_column(x):
                logger.warning(
                    "t.all(%r) over a raw column emits column.sum().over(window()); "
                    "this is correct only for sum semantics. Reference a measure "
                    "instead (e.g. t.all(t.measure_name)) so the totals re-aggregation "
                    "uses the measure formula.",
                    x,
                )
                return self._base_tbl[x].sum().over(ibis_mod.window())
            suggestion = self._typo_suggestion(x)
            if suggestion:
                raise UnknownMeasureRefError(
                    f"{x!r} is not a known measure or column. {suggestion}"
                )
            return self._base_tbl[x].sum().over(ibis_mod.window())

        # If x is a Field on virtual_agg_tbl (a known measure
        # reference), redirect to the parallel totals table so the
        # compiler can substitute in a properly re-aggregated value.
        if hasattr(x, "op") and callable(x.op):
            try:
                op = x.op()
                if isinstance(op, Field) and id(op.rel) == id(_to_op(self._virtual_agg_tbl)):
                    return self._totals_virtual_agg_tbl[op.name]

                Reduction = getattr(ibis_ops, "Reduction", None)
                if Reduction is not None:
                    if isinstance(op, Reduction):
                        return x.over(ibis_mod.window())
                    if any(isinstance(n, Reduction) for n in _walk(op)):
                        return x.over(ibis_mod.window())
            except Exception as exc:
                logger.debug("IbisCalcScope.all() reduction-detection swallowed: %s", exc)

        if hasattr(x, "sum"):
            return x.sum().over(ibis_mod.window())

        return x


def evaluate_calc_lambda(
    fn,
    base_tbl,
    known_measures: frozenset[str],
    virtual_agg_schema: dict[str, Any] | None = None,
    priority_measures: frozenset[str] | None = None,
):
    """Run a calc-measure lambda and return the ibis expression it builds.

    Constructs an :class:`IbisCalcScope` over ``base_tbl``, a synthetic
    virtual aggregated table whose schema is derived from
    ``virtual_agg_schema``, and a parallel synthetic totals table with
    the same schema. The scope is passed to ``fn`` exactly once; the
    returned ibis expression encodes the structural shape the analyzer
    walks — including any ``Field(totals_vt, ...)`` references emitted
    by ``t.all(measure_ref)``.

    Returns ``(expr, vt, totals_vt)``. Callers that only need the
    virtual aggregated table can ignore the third element.
    """
    if virtual_agg_schema is None:
        virtual_agg_schema = {name: "float64" for name in known_measures}
    if not virtual_agg_schema:
        virtual_agg_schema = dict(_EMPTY_VT_SCHEMA)

    vt = virtual_agg_table(virtual_agg_schema)
    totals_vt = ibis_mod.table(dict(virtual_agg_schema), name="__bsl_virtual_totals__")
    scope = IbisCalcScope(
        base_tbl,
        vt,
        known_measures,
        totals_virtual_agg_tbl=totals_vt,
        priority_measures=priority_measures or frozenset(),
    )

    if hasattr(fn, "_resolver") and hasattr(fn, "resolve"):
        return fn.resolve(scope), vt, totals_vt

    if callable(fn):
        return fn(scope), vt, totals_vt

    return fn, vt, totals_vt


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
    expr, vt, totals_vt = evaluate_calc_lambda(
        fn, base_tbl, known_measures, virtual_agg_schema
    )
    analysis = analyze_calc_expr(
        expr,
        known_measures=known_measures,
        base_table_op=_to_op(base_tbl),
        totals_vt_op=_to_op(totals_vt),
    )
    return expr, analysis


def compile_calc_measure(
    expr,
    virtual_agg_tbl,
    real_agg_tbl,
    totals_virtual_agg_tbl=None,
    real_with_totals=None,
    totals_prefix: str = TOTALS_PREFIX,
):
    """Compile a calc-measure ibis expression against the real agg table.

    Substitutes references to ``virtual_agg_tbl`` with ``real_agg_tbl``.
    When the calc references a totals virtual table, also rewrites
    each ``Field(totals_vt, name)`` to ``Field(real_with_totals,
    f"{totals_prefix}{name}")`` — i.e. the prefixed column produced by
    cross-joining the totals aggregation into the per-group result.

    The resulting ibis expression is suitable for use as a column in
    ``mutate(name=expr)`` on whichever table holds those references
    (``real_agg_tbl`` for non-totals calcs, ``real_with_totals`` for
    totals-using calcs).
    """
    op = _to_op(expr)
    vt_op = _to_op(virtual_agg_tbl)
    real_op = _to_op(real_agg_tbl)
    subs: dict = {vt_op: real_op}

    totals_vt_op = None
    if totals_virtual_agg_tbl is not None and real_with_totals is not None:
        totals_vt_op = _to_op(totals_virtual_agg_tbl)
        rwt_op = _to_op(real_with_totals)
        totals_schema = (
            dict(totals_vt_op.schema.items()) if hasattr(totals_vt_op, "schema") else {}
        )
        rwt_columns = (
            real_with_totals.columns if hasattr(real_with_totals, "columns") else ()
        )
        for col_name in totals_schema:
            prefixed = f"{totals_prefix}{col_name}"
            target_name = prefixed if prefixed in rwt_columns else col_name
            if target_name in rwt_columns:
                subs[Field(totals_vt_op, col_name)] = Field(rwt_op, target_name)

    rewritten = op.replace(subs)

    # Verify no Field reference to the totals virtual table survived the
    # rewrite; an unsubstituted reference reaches ibis as
    # ``IntegrityError: Cannot add ... to projection`` and obscures the
    # real cause (schema drift / missing totals column).
    if totals_vt_op is not None:
        unresolved = sorted(
            {
                n.name
                for n in _walk(rewritten)
                if isinstance(n, Field) and id(n.rel) == id(totals_vt_op)
            }
        )
        if unresolved:
            raise TotalsNotAvailableError(
                "Calc measure references totals columns that were not "
                f"substituted: {unresolved!r}. Expected prefixed columns "
                f"({totals_prefix}<name>) on the cross-joined real_with_totals "
                f"table but found neither prefixed nor unprefixed match in "
                f"columns: {list(real_with_totals.columns)!r}."
            )

    return rewritten.to_expr()


def compile_calc_measures(
    real_agg_tbl,
    calc_exprs: dict[str, tuple[Any, Any]],
):
    """Apply post-aggregation calc measures to the aggregated table.

    Convenience wrapper: each entry in ``calc_exprs`` is
    ``measure_name → (expr, virtual_agg_tbl)``; we substitute the
    virtual table with ``real_agg_tbl`` and add the resulting columns
    via ``mutate``. Totals-aware compilation lives in the
    full :func:`compile_calc_measure` entry point and is invoked from
    higher-level orchestration in ``ops.py``.
    """
    if not calc_exprs:
        return real_agg_tbl
    new_cols = {
        name: compile_calc_measure(expr, vt, real_agg_tbl)
        for name, (expr, vt) in calc_exprs.items()
    }
    return real_agg_tbl.mutate(**new_cols)


def apply_calc_measures(
    real_agg_tbl,
    base_tbl,
    calc_lambdas: dict[str, Any],
    known_measures: frozenset[str],
    real_totals_tbl=None,
    agg_specs: dict[str, Any] | None = None,
    totals_prefix: str = TOTALS_PREFIX,
):
    """Re-run each calc-measure lambda against the real aggregated table.

    Calc measures are applied one-at-a-time in topological order via
    successive ``mutate`` calls so calc-of-calc chains see prior results
    as columns.

    Totals handling: when a calc lambda emits ``Field(totals_vt, name)``
    references (the ``t.all(measure_ref)`` shape) we cross-join a
    no-group-by totals table into the result with prefixed column names
    and rewrite the field references to point at it. ``real_totals_tbl``
    can be supplied directly; otherwise, when ``agg_specs`` is provided
    and at least one calc actually references totals, we build the
    totals table on first need by re-running ``agg_specs`` on
    ``base_tbl`` without group keys.

    .. note::
        When ``real_totals_tbl`` is supplied directly, the caller is
        responsible for ensuring it already carries any non-AllOf calc
        columns that AllOf calcs depend on. The lazy ``agg_specs`` path
        re-applies those calc-of-calc deps automatically; the
        pre-built path does not, since rebuilding them would require
        re-running the analyzer on a table whose schema may already
        diverge from ``base_tbl`` + ``agg_specs``.

    Raises :class:`TotalsNotAvailableError` when a calc references
    totals but neither ``real_totals_tbl`` nor ``agg_specs`` lets us
    build one — surfaces the missing-totals condition with a clear
    message instead of letting the unsubstituted ``Field(totals_vt,
    ...)`` reach ibis and fail with ``IntegrityError``.
    """
    if not calc_lambdas:
        return real_agg_tbl

    calc_specs = dict(calc_lambdas)
    calc_exprs = {name: getattr(spec, "expr", spec) for name, spec in calc_specs.items()}
    priority = {
        name: frozenset(getattr(spec, "prefer_known", ()) or ())
        for name, spec in calc_specs.items()
    }

    if all(hasattr(spec, "depends_on") for spec in calc_specs.values()):
        deps = {name: set(getattr(spec, "depends_on", ()) or ()) for name, spec in calc_specs.items()}
        ordered = topological_order_from_deps(calc_exprs, deps)
    else:
        ordered = _topological_order(calc_exprs, base_tbl, known_measures)

    real_with_totals = None
    if real_totals_tbl is not None:
        real_with_totals = _join_totals(real_agg_tbl, real_totals_tbl, totals_prefix)

    base_op = _to_op(base_tbl)

    for name in ordered:
        fn = calc_exprs[name]
        cur_known = known_measures | frozenset(real_agg_tbl.columns)
        virtual_schema = {
            col: real_agg_tbl[col].type()
            for col in real_agg_tbl.columns
            if col in cur_known
        }
        expr, vt, totals_vt = evaluate_calc_lambda(
            fn,
            base_tbl,
            cur_known,
            virtual_schema,
            priority_measures=priority[name],
        )
        analysis = analyze_calc_expr(
            expr,
            known_measures=cur_known,
            base_table_op=base_op,
            totals_vt_op=_to_op(totals_vt),
        )

        if analysis.references_AllOf:
            if real_with_totals is None:
                if real_totals_tbl is None:
                    real_totals_tbl = _build_totals_from_agg_specs(
                        base_tbl, agg_specs, calc_lambdas, known_measures
                    )
                if real_totals_tbl is not None:
                    real_with_totals = _join_totals(
                        real_agg_tbl, real_totals_tbl, totals_prefix
                    )

            if real_with_totals is None:
                raise TotalsNotAvailableError(
                    f"Calc measure {name!r} references t.all(...) but no totals "
                    "table could be built. Pass `real_totals_tbl` or `agg_specs` "
                    "to apply_calc_measures, or define the calc on a model "
                    "without nested-array measures (which compile at multiple "
                    "grains and don't yet support totals)."
                )

            compiled = compile_calc_measure(
                expr,
                vt,
                real_with_totals,
                totals_virtual_agg_tbl=totals_vt,
                real_with_totals=real_with_totals,
                totals_prefix=totals_prefix,
            )
            real_with_totals = real_with_totals.mutate(**{name: compiled})
            real_agg_tbl = _drop_totals_columns(real_with_totals, totals_prefix)
            continue

        compiled = compile_calc_measure(expr, vt, real_agg_tbl)
        real_agg_tbl = real_agg_tbl.mutate(**{name: compiled})
        if real_with_totals is not None and real_totals_tbl is not None:
            real_with_totals = _join_totals(real_agg_tbl, real_totals_tbl, totals_prefix)

    return real_agg_tbl


def attach_windowed_totals(
    base_tbl,
    agg_specs: dict[str, Any],
    total_names: Iterable[str],
    totals_prefix: str = TOTALS_PREFIX,
) -> tuple[Any, dict[str, Any]]:
    """Pre-mutate ``base_tbl`` with windowed totals for the given base measures.

    For each name in ``total_names`` that has an entry in ``agg_specs``,
    evaluate the agg-spec callable on ``base_tbl`` to get the measure's
    aggregation expression (e.g. ``base.count()`` or
    ``base.distance.mean()``), wrap it in ``.over(window())`` to produce
    a window function over the entire base, and add the result as a
    new column ``f"{totals_prefix}{name}"``. Returns the mutated base
    table plus a dict of arbitrary-aggregator specs that callers should
    add to their per-group aggregation so the totals propagate as
    ordinary columns on the result.

    This expresses "ungrouped aggregate alongside a grouped one" as a
    single-pass query: the totals are computed once via window function,
    broadcast to every base row, and surface as a per-group column via
    ``arbitrary()`` in the aggregation. No cross-join, no shared-ancestor
    collapse, compiles to SQL on every backend that supports window
    functions.

    Returns
    -------
    (new_base_tbl, totals_arbitrary_specs):
        - ``new_base_tbl`` carries the original columns plus
          ``__bsl_totals__<name>`` for each requested measure.
        - ``totals_arbitrary_specs[col]`` is an agg-spec callable that
          wraps ``t[col].arbitrary()``.
    """
    new_base = base_tbl
    arbitrary_specs: dict[str, Any] = {}
    for name in total_names:
        if name not in agg_specs:
            continue
        try:
            agg_expr = agg_specs[name](new_base)
        except Exception as exc:
            logger.debug(
                "could not evaluate agg_spec for %r when attaching windowed totals: %s",
                name,
                exc,
            )
            continue
        try:
            windowed = agg_expr.over(ibis_mod.window())
        except Exception as exc:
            logger.debug(
                "could not wrap %r in window() for windowed totals: %s",
                name,
                exc,
            )
            continue
        col = f"{totals_prefix}{name}"
        new_base = new_base.mutate(**{col: windowed})
        arbitrary_specs[col] = (lambda t, _c=col: t[_c].arbitrary())
    return new_base, arbitrary_specs


class _TotalsResolvingScope:
    """Scope that resolves measure references to ``__bsl_totals__<name>`` columns.

    Used by :func:`attach_calc_totals` to evaluate a calc lambda
    against the totals columns of a per-group result. Since each
    ``__bsl_totals__<name>`` column carries the same value across all
    rows (the overall total computed via window function), applying
    a calc formula against this scope produces the calc's totals value
    on every row.
    """

    __slots__ = ("_tbl", "_totals_prefix")

    def __init__(self, tbl, totals_prefix: str):
        object.__setattr__(self, "_tbl", tbl)
        object.__setattr__(self, "_totals_prefix", totals_prefix)

    def _resolve(self, name: str):
        col = f"{self._totals_prefix}{name}"
        if hasattr(self._tbl, "columns") and col in self._tbl.columns:
            return self._tbl[col]
        # Suffix matching for joined models: ``flights.flight_count``
        # has totals column ``__bsl_totals__flights.flight_count``.
        suffix = f".{name}"
        for c in getattr(self._tbl, "columns", ()):
            if c.startswith(self._totals_prefix) and c[len(self._totals_prefix):].endswith(
                suffix
            ):
                return self._tbl[c]
        raise AttributeError(f"No totals column found for measure {name!r}")

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._resolve(name)

    def __getitem__(self, name: str):
        return self._resolve(name)

    def all(self, x):
        # Inside a totals evaluation, ``t.all(t.x)`` is just ``t.x`` —
        # we're already computing in the totals scope. Pass the value
        # through.
        if isinstance(x, str):
            return self._resolve(x)
        return x


def attach_calc_totals(
    real_agg_tbl,
    calc_specs: dict[str, Any],
    classifications: dict[str, CalcExprAnalysis],
    totals_prefix: str = TOTALS_PREFIX,
):
    """Compute ``__bsl_totals__<calc_name>`` columns for calc-of-calc-AllOf chains.

    When an AllOf-using calc references another calc (rather than a
    base measure) — e.g. ``t.all(t.avg_distance)`` where
    ``avg_distance`` is itself a calc — we need the totals value of
    the referenced calc on the per-group result so substitution can
    point at it. ``attach_windowed_totals`` only handles base measures
    via ``agg.over(window())``; this function fills the gap by
    evaluating each calc's lambda against the totals columns already
    attached to ``real_agg_tbl``, in topological order so calc-of-calc
    chains see prior totals as inputs.

    The user's calc lambda doesn't change — it's the same formula —
    but the scope it runs against returns ``__bsl_totals__<dep>``
    columns instead of regular per-group columns. Since each totals
    column carries a constant value across rows, applying the formula
    yields the corresponding constant calc-totals value.
    """
    # Identify calcs whose totals are needed: the direct AllOf targets
    # plus any transitive calc dependencies of those.
    needed: set[str] = set()
    work: list[str] = []
    for cn, c in classifications.items():
        if c.references_AllOf:
            for d in c.depends_on:
                if d in calc_specs:
                    needed.add(d)
                    work.append(d)
    while work:
        n = work.pop()
        if n not in classifications:
            continue
        for d in classifications[n].depends_on:
            if d in calc_specs and d not in needed:
                needed.add(d)
                work.append(d)

    if not needed:
        return real_agg_tbl

    # Topo-order so a calc's deps are computed before the calc itself.
    deps_map = {
        n: set(classifications[n].depends_on) & needed
        for n in needed
        if n in classifications
    }
    ordered = topological_order_from_deps(needed, deps_map)

    for calc_name in ordered:
        if calc_name not in calc_specs:
            continue
        cm = calc_specs[calc_name]
        fn = cm.expr if hasattr(cm, "expr") else cm
        try:
            scope = _TotalsResolvingScope(real_agg_tbl, totals_prefix)
            if hasattr(fn, "_resolver") and hasattr(fn, "resolve"):
                totals_expr = fn.resolve(scope)
            elif callable(fn):
                totals_expr = fn(scope)
            else:
                totals_expr = fn
        except Exception as exc:
            logger.debug(
                "calc-of-calc totals evaluation failed for %r: %s", calc_name, exc
            )
            continue
        col = f"{totals_prefix}{calc_name}"
        real_agg_tbl = real_agg_tbl.mutate(**{col: totals_expr})

    return real_agg_tbl


def _join_totals(real_agg_tbl, real_totals_tbl, totals_prefix: str):
    """Legacy cross-join path. Kept for ``apply_calc_measures`` callers
    that pass a pre-built ``real_totals_tbl``.

    .. deprecated::
       Prefer :func:`attach_windowed_totals` which avoids the
       shared-ancestor cross-join collapse some SQL backends apply
       when both sides derive from the same parent relation. This
       helper survives only for the ``apply_calc_measures(real_totals_tbl=...)``
       entry point where the totals are produced externally and the
       per-group table is already built; the windowed-totals path
       requires attaching at base-table time before the per-group
       aggregation runs.
    """
    rename_map = {f"{totals_prefix}{c}": c for c in real_totals_tbl.columns}
    totals_renamed = real_totals_tbl.rename(rename_map)
    return real_agg_tbl.cross_join(totals_renamed)


def classify_calc_lambdas(
    calc_lambdas: dict[str, Any],
    base_tbl,
    known_measures: frozenset[str],
) -> dict[str, CalcExprAnalysis]:
    """Run the analyzer once per calc lambda; return ``{name → analysis}``.

    Lets multiple passes (topological order, totals-build filtering,
    apply loop) read the same classification record without
    re-evaluating each lambda. Lambdas that fail evaluation get an
    empty ``CalcExprAnalysis`` (post_agg_only=True, no deps) so the
    surrounding orchestration still terminates.
    """
    base_op = _to_op(base_tbl)
    out: dict[str, CalcExprAnalysis] = {}
    for name, fn in calc_lambdas.items():
        try:
            virtual_schema = {n: "float64" for n in known_measures}
            expr, _vt, totals_vt = evaluate_calc_lambda(
                fn, base_tbl, known_measures, virtual_schema
            )
            out[name] = analyze_calc_expr(
                expr,
                known_measures=known_measures,
                base_table_op=base_op,
                totals_vt_op=_to_op(totals_vt),
            )
        except Exception as exc:
            logger.debug("calc-measure classification failed for %r: %s", name, exc)
            out[name] = CalcExprAnalysis(
                pushable=False,
                references_AllOf=False,
                has_window=False,
                post_agg_only=True,
            )
    return out


def _build_totals_from_agg_specs(
    base_tbl,
    agg_specs: dict[str, Any] | None,
    calc_lambdas: dict[str, Any],
    known_measures: frozenset[str],
    classifications: dict[str, CalcExprAnalysis] | None = None,
):
    """Build a no-group-by totals table when callers passed ``agg_specs``.

    Re-runs each base-aggregation callable on ``base_tbl`` without group
    keys, then applies the non-AllOf calc lambdas so calc-of-calc chains
    see correctly-recomputed dependencies. Returns ``None`` when there
    is no way to construct totals (no ``agg_specs`` supplied or the
    specs fail to evaluate against the base).
    """
    if not agg_specs:
        return None
    try:
        totals_aggs = {n: f(base_tbl) for n, f in agg_specs.items()}
    except Exception as exc:
        logger.debug("totals aggregation failed to evaluate: %s", exc)
        return None
    real_totals = base_tbl.aggregate(**totals_aggs)

    if classifications is None:
        classifications = classify_calc_lambdas(calc_lambdas, base_tbl, known_measures)
    non_allof = {
        name: fn
        for name, fn in calc_lambdas.items()
        if not classifications.get(name, _EMPTY_ANALYSIS).references_AllOf
    }
    if non_allof:
        real_totals = apply_calc_measures(real_totals, base_tbl, non_allof, known_measures)
    return real_totals


_EMPTY_ANALYSIS = CalcExprAnalysis(
    pushable=False,
    references_AllOf=False,
    has_window=False,
    post_agg_only=True,
)


def topological_order_from_deps(
    names: list[str] | tuple[str, ...] | dict[str, Any],
    deps: dict[str, set[str] | frozenset[str]],
) -> list[str]:
    """Topologically order ``names`` using ``deps`` (``name → {dep, ...}``).

    Edges to nodes outside ``names`` are ignored; cycles fall back to
    insertion order so a downstream substitution failure surfaces the
    real error rather than this helper raising. Shared by ops.py and
    apply_calc_measures so calc-of-calc ordering is consistent.
    """
    name_seq = list(names)
    name_set = set(name_seq)

    ordered: list[str] = []
    visited: set[str] = set()
    visiting: set[str] = set()

    def visit(node: str) -> None:
        if node in visited or node in visiting:
            return
        visiting.add(node)
        for dep in deps.get(node, ()):
            if dep in name_set:
                visit(dep)
        visiting.discard(node)
        visited.add(node)
        ordered.append(node)

    for n in name_seq:
        visit(n)
    return ordered


def _topological_order(
    calc_lambdas: dict[str, Any],
    base_tbl,
    known_measures: frozenset[str],
    classifications: dict[str, CalcExprAnalysis] | None = None,
) -> list[str]:
    """Order calc lambdas using analyzer-derived dependencies."""
    if classifications is None:
        classifications = classify_calc_lambdas(calc_lambdas, base_tbl, known_measures)
    deps = {
        name: set(classifications.get(name, _EMPTY_ANALYSIS).depends_on)
        for name in calc_lambdas
    }
    return topological_order_from_deps(calc_lambdas, deps)


def lift_inline_reductions(expr, virtual_agg_tbl, base_tbl, totals_virtual_agg_tbl=None):
    """Lift inline reductions over the base table out of a calc expression.

    The user's calc lambda may contain reductions that read base-table
    columns directly, e.g. ``t.distance.mean() / t.all(t.distance.mean())``.
    Straight ``mutate`` can't compile these because the reductions are
    bound to the unaggregated base relation, not the post-aggregation
    result.

    Each unique base-table reduction is named, added to both the per-group
    base aggregation and the (no-group-by) totals aggregation, then
    rewritten in the calc expression:

    - A reduction at the top level becomes ``Field(vt, anon_name)`` —
      a column reference on the per-group result.
    - A reduction that is the ``func`` of a ``WindowFunction`` (the
      ``t.all(...)`` totals shape) becomes ``Field(totals_vt, anon_name)``
      — a reference to the same reduction computed over the full
      filtered base. The compiler later substitutes ``totals_vt`` with a
      real totals table cross-joined into the result, so non-sum
      reductions (mean/quantile/…) get correct overall values.

    Returns ``(rewritten_expr, new_vt, new_totals_vt, lifted)`` where
    ``lifted`` maps anonymous names to the original scalar reduction
    expression. The caller adds those reductions to both the per-group
    aggregation and the totals aggregation.
    """
    op = _to_op(expr)
    vt_op = _to_op(virtual_agg_tbl)
    if totals_virtual_agg_tbl is None:
        totals_schema = dict(vt_op.schema.items()) if hasattr(vt_op, "schema") else {}
        totals_virtual_agg_tbl = ibis_mod.table(
            totals_schema or dict(_EMPTY_VT_SCHEMA),
            name="__bsl_virtual_totals__",
        )
    totals_vt_op = _to_op(totals_virtual_agg_tbl)
    base_op = _to_op(base_tbl)

    Reduction = getattr(ibis_ops, "Reduction", None)
    WindowFunction = getattr(ibis_ops, "WindowFunction", None)

    if Reduction is None:
        return expr, virtual_agg_tbl, totals_virtual_agg_tbl, {}

    def is_base_reduction(node):
        if not isinstance(node, Reduction):
            return False
        for c in _walk(node):
            if isinstance(c, Field) and id(c.rel) == id(base_op):
                return True
        return False

    base_reductions = [n for n in _walk(op) if is_base_reduction(n)]
    if not base_reductions:
        return expr, virtual_agg_tbl, totals_virtual_agg_tbl, {}

    name_to_reduction: dict[str, Any] = {}
    reduction_to_name: dict[int, str] = {}
    counter = 0
    for r in base_reductions:
        if id(r) in reduction_to_name:
            continue
        anon = f"__bsl_inline_{type(r).__name__.lower()}_{counter}"
        counter += 1
        name_to_reduction[anon] = r
        reduction_to_name[id(r)] = anon

    extended_schema = dict(vt_op.schema.items())
    for anon, r in name_to_reduction.items():
        extended_schema[anon] = r.dtype
    new_vt = ibis_mod.table(extended_schema, name=getattr(vt_op, "name", "__bsl_virtual_agg__"))
    new_vt_op = new_vt.op()

    totals_extended_schema = dict(totals_vt_op.schema.items()) if hasattr(
        totals_vt_op, "schema"
    ) else {}
    for anon, r in name_to_reduction.items():
        totals_extended_schema[anon] = r.dtype
    new_totals_vt = ibis_mod.table(
        totals_extended_schema or dict(_EMPTY_VT_SCHEMA),
        name=getattr(totals_vt_op, "name", "__bsl_virtual_totals__"),
    )
    new_totals_vt_op = new_totals_vt.op()

    # Two-pass substitution. The same ``Reduction`` node may appear both
    # at top level (where we want ``Field(vt, anon)`` — the per-group
    # value) and as a ``WindowFunction.func`` (the ``t.all(...)`` totals
    # shape, where we want ``Field(totals_vt, anon)`` — the overall
    # value). ``op.replace`` dedupes by equality, so we can't tell those
    # apart in one pass: handle WindowFunctions first, then the bare
    # Reductions.
    if WindowFunction is not None:
        window_subs: dict = {}
        for n in _walk(op):
            if not isinstance(n, WindowFunction):
                continue
            inner = getattr(n, "func", None)
            if inner is None or id(inner) not in reduction_to_name:
                continue
            anon = reduction_to_name[id(inner)]
            window_subs[n] = Field(new_totals_vt_op, anon)
        intermediate = op.replace(window_subs) if window_subs else op
    else:
        intermediate = op

    field_subs = {r: Field(new_vt_op, reduction_to_name[id(r)]) for r in base_reductions}
    new_op = intermediate.replace(field_subs)

    lifted_aggs = {anon: r.to_expr() for anon, r in name_to_reduction.items()}
    return new_op.to_expr(), new_vt, new_totals_vt, lifted_aggs


def rename_measure_refs(expr, virtual_agg_tbl, name_map: dict[str, str]):
    """Rename measure references inside a calc-measure ibis expression.

    Used when joining tables: a calc measure declared on a model named
    ``flights`` may reference ``flight_count``, but after the join the
    aggregated column is ``flights.flight_count``. This function rebuilds
    the calc expression so that field references on the virtual aggregated
    table map to their prefixed names.

    Parameters
    ----------
    expr:
        Calc-measure ibis expression built against ``virtual_agg_tbl``.
    virtual_agg_tbl:
        The synthetic table the expression was built against.
    name_map:
        Mapping of ``old_name → new_name`` for measure references that
        need renaming. Names not in the map are left untouched.

    Returns
    -------
    A new ibis expression with prefixed field names. The returned
    expression now references a *new* virtual table whose schema includes
    the renamed columns, so callers must compile against that new virtual
    table (use :func:`build_renamed_virtual_table` to get it).
    """
    if not name_map:
        return expr, virtual_agg_tbl

    vt_op = virtual_agg_tbl.op() if hasattr(virtual_agg_tbl, "op") else virtual_agg_tbl
    old_schema = dict(vt_op.schema.items()) if hasattr(vt_op, "schema") else {}
    new_schema = {name_map.get(k, k): v for k, v in old_schema.items()}
    new_vt = ibis_mod.table(new_schema, name=getattr(vt_op, "name", "__bsl_virtual_agg__"))
    new_vt_op = new_vt.op()

    field_substitutions = {}
    for old_name, dtype in old_schema.items():
        new_name = name_map.get(old_name, old_name)
        old_field = Field(vt_op, old_name)
        new_field = Field(new_vt_op, new_name)
        field_substitutions[old_field] = new_field

    op = expr.op() if hasattr(expr, "op") and callable(expr.op) else expr
    return op.replace(field_substitutions).to_expr(), new_vt
