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

When a name exists on *both* (e.g. user defines a measure called
``distance`` and the base table also has a ``distance`` column), the
base column wins. This matches the historical curated-AST behavior
where ``t.distance.sum()`` always meant "sum the base column" even when
``distance`` was registered as a measure.

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

import difflib
import hashlib
from typing import Any

from ._xorq import Deferred, Field, Node
from ._xorq import ibis as ibis_mod
from ._xorq import operations as ibis_ops
from .calc_analyzer import (
    CalcExprAnalysis,
    _walk,
    _walk_children,
    analyze_calc_expr,
    virtual_agg_table,
)
from .measure_scope import UnknownMeasureRefError


class IbisCalcScope:
    """Dual-table scope passed to calc-measure lambdas.

    ``t.column_name`` returns the base-table column when one exists;
    ``t.measure_name`` returns the virtual aggregated column otherwise.
    Base columns win on collision so that historical patterns like
    ``t.distance.sum()`` (where ``distance`` is also a measure name)
    still classify as a base aggregation rather than a post-aggregation
    sum.
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

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
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
        if self._has_column(name):
            return self._base_tbl[name]
        resolved = self._resolve_measure_name(name)
        if resolved is not None:
            return self._virtual_agg_tbl[resolved]
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
            resolved = self._resolve_measure_name(x)
            if resolved is not None:
                col = self._virtual_agg_tbl[resolved]
            elif self._has_column(x):
                col = self._base_tbl[x]
            else:
                suggestion = self._typo_suggestion(x)
                if suggestion:
                    raise UnknownMeasureRefError(
                        f"{x!r} is not a known measure or column. {suggestion}"
                    )
                col = self._base_tbl[x]
            return col.sum().over(ibis_mod.window())

        # Already a reduction (Sum, Mean, ...) or a scalar built on top
        # of one (e.g. ``Sum(...).coalesce(0)``): wrap in a window. The
        # analyzer recognizes the ``WindowFunction(reduction, empty)``
        # shape as the totals pattern; the inline-aggregation lift then
        # rewrites ``Reduction(base)`` to ``Sum(Field(vt, anon))`` so the
        # post-agg result computes the correct windowed totals.
        if hasattr(x, "op") and callable(x.op):
            try:
                Reduction = getattr(ibis_ops, "Reduction", None)
                if Reduction is not None:
                    if isinstance(x.op(), Reduction):
                        return x.over(ibis_mod.window())
                    # Scalar shape that wraps a Reduction (Coalesce, Cast,
                    # arithmetic on a reduction). Walk the op tree to
                    # confirm a Reduction is present, then wrap.
                    if any(isinstance(n, Reduction) for n in _walk(x.op())):
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

    # Duck-typed deferred check covers both regular ``ibis._`` and
    # ``xorq.vendor.ibis._`` shapes; ``isinstance(fn, Deferred)`` would
    # only match the xorq flavor since BSL imports Deferred from there.
    if hasattr(fn, "_resolver") and hasattr(fn, "resolve"):
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


def apply_calc_measures(
    real_agg_tbl,
    base_tbl,
    calc_lambdas: dict[str, Any],
    known_measures: frozenset[str],
):
    """Re-run each calc-measure lambda against the real aggregated table.

    Used at query time after the base aggregation has been computed.
    Calc measures may reference other calc measures (calc-of-calc), so
    we order them topologically by their analyzed dependencies and apply
    them one at a time via successive ``mutate`` calls. Each calc sees
    previously-computed calcs as columns on ``real_agg_tbl``.
    """
    if not calc_lambdas:
        return real_agg_tbl

    ordered = _topological_order(calc_lambdas, base_tbl, known_measures)

    for name in ordered:
        fn = calc_lambdas[name]
        virtual_schema = {
            col: real_agg_tbl[col].type()
            for col in real_agg_tbl.columns
            if col in known_measures
        }
        expr, vt = evaluate_calc_lambda(fn, base_tbl, known_measures, virtual_schema)
        compiled = compile_calc_measure(expr, vt, real_agg_tbl)
        real_agg_tbl = real_agg_tbl.mutate(**{name: compiled})

    return real_agg_tbl


def _topological_order(
    calc_lambdas: dict[str, Any],
    base_tbl,
    known_measures: frozenset[str],
) -> list[str]:
    """Order calc measures so dependencies are compiled before their consumers.

    Runs the analyzer once per lambda to read ``depends_on``. Cycles fall
    back to the original insertion order with a debug log; the substitution
    pass will surface a clear error if the cycle was real.
    """
    deps: dict[str, set[str]] = {}
    for name, fn in calc_lambdas.items():
        try:
            virtual_schema = {n: "float64" for n in known_measures}
            expr, _vt = evaluate_calc_lambda(fn, base_tbl, known_measures, virtual_schema)
            base_op = base_tbl.op() if hasattr(base_tbl, "op") and callable(base_tbl.op) else None
            analysis = analyze_calc_expr(
                expr, known_measures=known_measures, base_table_op=base_op
            )
            deps[name] = set(analysis.depends_on) & set(calc_lambdas.keys())
        except Exception:
            deps[name] = set()

    ordered: list[str] = []
    visited: set[str] = set()
    visiting: set[str] = set()

    def visit(node: str):
        if node in visited:
            return
        if node in visiting:
            return  # cycle — break here
        visiting.add(node)
        for dep in deps.get(node, ()):
            visit(dep)
        visiting.discard(node)
        visited.add(node)
        ordered.append(node)

    for name in calc_lambdas:
        visit(name)
    return ordered


def lift_inline_reductions(expr, virtual_agg_tbl, base_tbl):
    """Lift inline reductions over the base table out of a calc expression.

    The user's calc lambda may contain reductions that read base-table
    columns directly, e.g. ``t.distance.sum() / t.all(t.distance.sum())``.
    The straight ``mutate`` path can't compile these because the resulting
    aggregations are bound to the unaggregated base relation rather than
    the post-aggregation result.

    This pass walks the expression tree, lifts each base-table reduction
    to an anonymous base measure, and rewrites the expression so each
    reduction becomes a column reference on the (extended) virtual
    aggregated table:

    - A reduction at the top level becomes ``Field(vt, anon_name)``.
    - A reduction that is the ``func`` of a ``WindowFunction`` (the
      ``t.all(...)`` totals shape) becomes ``Sum(Field(vt, anon_name))``,
      so the windowed totals re-aggregate the post-agg column.

    Returns ``(rewritten_expr, new_vt, lifted)`` where ``lifted`` maps
    anonymous names to the original scalar reduction expression. The
    caller adds those reductions to the base aggregation so the
    extended virtual table's columns line up with the real aggregated
    result.
    """
    op = expr.op() if hasattr(expr, "op") and callable(expr.op) else expr
    vt_op = (
        virtual_agg_tbl.op()
        if hasattr(virtual_agg_tbl, "op") and callable(virtual_agg_tbl.op)
        else virtual_agg_tbl
    )
    base_op = base_tbl.op() if hasattr(base_tbl, "op") and callable(base_tbl.op) else base_tbl

    Reduction = getattr(ibis_ops, "Reduction", None)
    WindowFunction = getattr(ibis_ops, "WindowFunction", None)
    Sum = getattr(getattr(ibis_ops, "reductions", None), "Sum", None)

    if Reduction is None:
        return expr, virtual_agg_tbl, {}

    def is_base_reduction(node):
        if not isinstance(node, Reduction):
            return False
        for c in _walk(node):
            if isinstance(c, Field) and id(c.rel) == id(base_op):
                return True
        return False

    # Parent map so we can detect "Reduction is the func of a WindowFunction".
    parent_map: dict[int, list] = {}

    def visit(node):
        for child in _walk_children(node):
            parent_map.setdefault(id(child), []).append(node)
            visit(child)

    visit(op)

    base_reductions = [n for n in _walk(op) if is_base_reduction(n)]
    if not base_reductions:
        return expr, virtual_agg_tbl, {}

    name_to_reduction: dict[str, Any] = {}
    reduction_to_name: dict[int, str] = {}
    for r in base_reductions:
        if id(r) in reduction_to_name:
            continue
        sig = hashlib.md5(repr(r).encode()).hexdigest()[:8]
        anon = f"__bsl_inline_{type(r).__name__.lower()}_{sig}"
        suffix_idx = 0
        base_anon = anon
        while anon in name_to_reduction and name_to_reduction[anon] is not r:
            suffix_idx += 1
            anon = f"{base_anon}_{suffix_idx}"
        if anon not in name_to_reduction:
            name_to_reduction[anon] = r
        reduction_to_name[id(r)] = anon

    extended_schema = dict(vt_op.schema.items())
    for anon, r in name_to_reduction.items():
        extended_schema[anon] = r.dtype
    new_vt = ibis_mod.table(extended_schema, name=getattr(vt_op, "name", "__bsl_virtual_agg__"))
    new_vt_op = new_vt.op()

    # Two-pass substitution. The same ``Reduction`` node may appear both
    # at top level (where we want ``Field(vt, anon)``) and as a
    # WindowFunction.func (where we want ``Sum(Field(vt, anon))`` so the
    # windowed totals re-aggregate the post-agg column). ``op.replace``
    # dedupes by equality, so we can't distinguish those roles in a
    # single pass — handle WindowFunctions wholesale first, then the
    # remaining bare Reductions.
    if WindowFunction is not None and Sum is not None:
        window_subs: dict = {}
        for n in _walk(op):
            if not isinstance(n, WindowFunction):
                continue
            inner = getattr(n, "func", None)
            if inner is None or id(inner) not in reduction_to_name:
                continue
            anon = reduction_to_name[id(inner)]
            field_op = Field(new_vt_op, anon)
            new_window = WindowFunction(
                func=Sum(field_op),
                how=n.how,
                start=n.start,
                end=n.end,
                group_by=n.group_by,
                order_by=n.order_by,
            )
            window_subs[n] = new_window
        intermediate = op.replace(window_subs) if window_subs else op
    else:
        intermediate = op

    field_subs = {r: Field(new_vt_op, reduction_to_name[id(r)]) for r in base_reductions}
    new_op = intermediate.replace(field_subs)

    lifted_aggs = {anon: r.to_expr() for anon, r in name_to_reduction.items()}
    return new_op.to_expr(), new_vt, lifted_aggs


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
