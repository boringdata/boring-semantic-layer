"""Analyzer for calc measures expressed as ibis expressions.

Replaces the curated calc-measure AST classification (`MeasureRef` /
`AllOf` / `BinOp` / `MethodCall` / `AggregationExpr`) with structural
analysis of an ibis expression tree.

The analyzer walks the ibis tree and returns a :class:`CalcExprAnalysis`
record describing properties relevant to the planner:

- ``pushable``: every column reference targets one source table and there
  is no window or aggregation-in-aggregation; the expression can be
  computed pre-aggregation as a base measure.
- ``references_AllOf``: an aggregation node appears as a scalar inside
  another aggregation context (the "totals" pattern). The compiler lifts
  this to a window aggregation (or a cross-joined totals table).
- ``has_window``: any window node anywhere. Forces post-aggregation.
- ``post_agg_only``: the expression cannot be pushed pre-aggregation.
- ``depends_on``: set of names this expression references on the
  *aggregated* virtual scope (i.e. measure references).

Anything not classifiable falls back to ``post_agg_only=True`` with a
warning, never an error — see the ADR's "v1 analyzer scope" open
question.
"""

from __future__ import annotations

import warnings
from typing import Any

from attrs import field, frozen

from ._xorq import (
    Deferred,
    Field,
    Node,
    operations as ibis_ops,
)
from ._xorq import ibis as ibis_mod


@frozen(kw_only=True)
class CalcExprAnalysis:
    """Structural classification of a calc-measure ibis expression.

    Produced by :func:`analyze_calc_expr`. The planner reads ``pushable``
    to decide pre-agg pushdown, ``references_AllOf`` to decide whether
    to compute totals, ``has_window`` and ``post_agg_only`` to decide
    placement, and ``depends_on`` to order calc-measure compilation.
    """

    pushable: bool
    references_AllOf: bool
    has_window: bool
    post_agg_only: bool
    depends_on: frozenset[str] = field(factory=frozenset, converter=frozenset)
    inline_aggs: frozenset[str] = field(factory=frozenset, converter=frozenset)


def _to_node(expr: Any) -> Node | None:
    """Best-effort coercion of an arbitrary value to an ibis ``Node``.

    Returns ``None`` for primitives (int, float, str, None) and for
    Deferreds that haven't been resolved yet — callers must resolve
    Deferreds against an actual table before analysis.
    """
    if expr is None:
        return None
    if isinstance(expr, (int, float, str, bool)):
        return None
    if isinstance(expr, Deferred):
        return None
    if hasattr(expr, "op") and callable(expr.op):
        try:
            return expr.op()
        except Exception:
            return None
    if isinstance(expr, Node):
        return expr
    return None


def _is_reduction(node: Node) -> bool:
    """True if ``node`` is an ibis ``Reduction`` (sum/mean/count/...)."""
    Reduction = getattr(ibis_ops, "Reduction", None)
    if Reduction is not None and isinstance(node, Reduction):
        return True
    name = type(node).__name__
    return name in (
        "Sum",
        "Mean",
        "Count",
        "CountStar",
        "CountDistinct",
        "Min",
        "Max",
        "Variance",
        "StandardDev",
        "Median",
        "Quantile",
        "ApproxCountDistinct",
        "Mode",
        "First",
        "Last",
        "Arbitrary",
        "Any",
        "All",
        "GroupConcat",
        "ArrayCollect",
    )


def _is_window(node: Node) -> bool:
    """True if ``node`` is any ibis window operation."""
    WindowFunction = getattr(ibis_ops, "WindowFunction", None)
    if WindowFunction is not None and isinstance(node, WindowFunction):
        return True
    name = type(node).__name__
    return "Window" in name


def _walk_children(node: Node):
    """Yield direct child Nodes of ``node``. Robust to ibis API drift.

    Walks ``__children__`` if present, otherwise ``__args__``. Skips
    non-Node leaves (literals, schemas, etc.). Skips ``Relation`` nodes
    so the analyzer does not descend into base-table expressions whose
    body may itself contain window functions or aggregations unrelated
    to the calc measure being classified.
    """
    from ._xorq import operations as ibis_ops

    Relation = getattr(ibis_ops.relations, "Relation", None)

    children = getattr(node, "__children__", None)
    if children is not None:
        for c in children:
            if isinstance(c, Node) and not (Relation is not None and isinstance(c, Relation)):
                yield c
        return
    args = getattr(node, "__args__", None)
    if args is None:
        return
    for arg in args:
        if isinstance(arg, Node):
            if Relation is not None and isinstance(arg, Relation):
                continue
            yield arg
        elif isinstance(arg, tuple):
            for inner in arg:
                if isinstance(inner, Node):
                    if Relation is not None and isinstance(inner, Relation):
                        continue
                    yield inner


def _walk(node: Node):
    """Iterate ``node`` and all descendants (preorder, deduped).

    Does not descend into ``Relation`` subtrees — those are the table
    references the calc expression sits on top of, not part of its
    structural shape.
    """
    seen: set[int] = set()
    stack = [node]
    while stack:
        cur = stack.pop()
        key = id(cur)
        if key in seen:
            continue
        seen.add(key)
        yield cur
        stack.extend(_walk_children(cur))


def _collect_field_names(node: Node) -> set[str]:
    """Collect all ``Field`` names referenced anywhere under ``node``."""
    return {n.name for n in _walk(node) if isinstance(n, Field)}


def _collect_source_tables(node: Node) -> set[int]:
    """Identify distinct source-table ops referenced under ``node``.

    Returns a set of ``id()`` for ``Field.rel`` ops. Used to detect
    expressions that span multiple tables (post-agg only).
    """
    return {id(n.rel) for n in _walk(node) if isinstance(n, Field)}


def _is_empty_window(node: Node) -> bool:
    """True if ``node`` is a window with no partitioning or ordering.

    The ``t.all(x)`` API emits ``x.sum().over(window())`` — an empty
    window — to mean "take the totals over the whole post-agg result."
    A partitioned or ordered window is a real window function (moving
    average, rank, etc.) and is treated separately as ``has_window``.
    """
    if not _is_window(node):
        return False
    group_by = getattr(node, "group_by", ())
    order_by = getattr(node, "order_by", ())
    return not group_by and not order_by


def _scan_tree(node: Node) -> tuple[bool, bool, bool]:
    """Single-pass tree walk returning ``(has_reduction, has_window, has_totals)``.

    Combining the three checks avoids the O(K) full subtree walks the
    original ``_has_totals_pattern`` did once per encountered reduction —
    structural classification is a hot path called once per
    ``with_measures`` lambda. Definition of ``has_totals``:

    * a ``Reduction`` whose subtree contains another ``Reduction`` (an
      aggregation-inside-aggregation), or
    * an empty (no group_by, no order_by) ``WindowFunction`` over a
      ``Reduction`` — the ``x.sum().over(window())`` shape ``t.all(x)``
      emits today.
    """
    has_reduction = False
    has_window = False
    has_totals = False
    seen: set[int] = set()
    stack = [(node, 0)]  # (node, depth_of_enclosing_reduction)
    while stack:
        cur, agg_depth = stack.pop()
        key = id(cur)
        if key in seen:
            continue
        seen.add(key)

        cur_is_reduction = _is_reduction(cur)
        cur_is_window = _is_window(cur)

        if cur_is_reduction:
            has_reduction = True
            if agg_depth > 0:
                has_totals = True
            agg_depth += 1
        elif cur_is_window:
            has_window = True
            if _is_empty_window(cur):
                agg_depth += 1

        for child in _walk_children(cur):
            stack.append((child, agg_depth))
    return has_reduction, has_window, has_totals


def analyze_calc_expr(
    expr: Any,
    known_measures: frozenset[str] = frozenset(),
    base_table_op: Node | None = None,
    totals_vt_op: Node | None = None,
) -> CalcExprAnalysis:
    """Classify a calc-measure ibis expression.

    Parameters
    ----------
    expr:
        An ibis expression, ``Deferred``, or primitive. Deferreds must
        be resolved by the caller against the analysis scope before the
        walker can inspect them.
    known_measures:
        Names of measures defined on the model. Field references on the
        synthetic post-aggregation virtual table whose names are in
        this set are recorded as ``depends_on``.
    base_table_op:
        Optional. The base table's ibis op. When provided, fields
        referencing this exact table are not treated as measure
        dependencies — they are inline base columns (used by inline
        aggregations like ``t.distance.sum()`` in calc-measure form).
    totals_vt_op:
        Optional. The totals virtual table's ibis op (parallel to
        ``base_table_op`` but representing no-group-by aggregation).
        Field references on this table mark the totals pattern; the
        compiler later substitutes them with a real totals aggregation
        cross-joined into the result.

    Returns
    -------
    CalcExprAnalysis
        Structural classification. On unrecognized inputs the analyzer
        returns ``post_agg_only=True`` with a warning rather than
        raising.
    """
    node = _to_node(expr)

    if node is None:
        # Primitive (int/float/str). Pure constants are pushable
        # trivially — they fold into both grouped and ungrouped contexts.
        if isinstance(expr, (int, float, bool)):
            return CalcExprAnalysis(
                pushable=True,
                references_AllOf=False,
                has_window=False,
                post_agg_only=False,
            )
        warnings.warn(
            f"calc-measure analyzer could not classify {type(expr).__name__}; "
            "treating as post-aggregation-only.",
            stacklevel=2,
        )
        return CalcExprAnalysis(
            pushable=False,
            references_AllOf=False,
            has_window=False,
            post_agg_only=True,
        )

    _, has_window, references_AllOf = _scan_tree(node)

    field_names = _collect_field_names(node)
    source_tables = _collect_source_tables(node)

    depends_on: set[str] = set()
    inline_aggs: set[str] = set()
    base_id = id(base_table_op) if base_table_op is not None else None
    totals_id = id(totals_vt_op) if totals_vt_op is not None else None
    for fld in (n for n in _walk(node) if isinstance(n, Field)):
        if totals_id is not None and id(fld.rel) == totals_id:
            references_AllOf = True
            depends_on.add(fld.name)
        elif base_id is not None and id(fld.rel) == base_id:
            inline_aggs.add(fld.name)
        elif fld.name in known_measures:
            depends_on.add(fld.name)

    # Pushability heuristic: single source table, no windows, no
    # cross-aggregation patterns, no measure refs (since measures are
    # already aggregated and can't push pre-agg).
    pushable = (
        not has_window
        and not references_AllOf
        and not depends_on
        and len(source_tables) <= 1
    )

    post_agg_only = has_window or references_AllOf or bool(depends_on)

    return CalcExprAnalysis(
        pushable=pushable,
        references_AllOf=references_AllOf,
        has_window=has_window,
        post_agg_only=post_agg_only,
        depends_on=depends_on,
        inline_aggs=inline_aggs,
    )


def virtual_agg_table(
    schema: dict[str, Any],
    name: str = "__bsl_virtual_agg__",
):
    """Build a synthetic ibis table representing the post-aggregation
    schema. Calc-measure lambdas evaluate against this table to produce
    an ibis expression the analyzer can walk.
    """
    return ibis_mod.table(schema, name=name)
