"""Tests for the ibis-tree analyzer that replaces the curated calc-measure
AST classifier.

The analyzer walks an ibis expression tree and returns a
:class:`CalcExprAnalysis` record. These tests exercise each branch of
the classification — pushable base, post-agg measure refs, totals
pattern (``t.all``-style empty window over reduction), real windowed
expressions (moving avg, rank), and inline aggregations — to lock in
the structural shapes the planner reads downstream.
"""

from __future__ import annotations

import pytest

xorq = pytest.importorskip("xorq", reason="xorq not installed")

from boring_semantic_layer._xorq import ibis as xibis  # noqa: E402
from boring_semantic_layer.calc_analyzer import (  # noqa: E402
    CalcExprAnalysis,
    analyze_calc_expr,
    virtual_agg_table,
)


def _vt():
    return virtual_agg_table(
        {"flight_count": "int64", "total_distance": "float64", "date": "date"}
    )


def _base():
    return xibis.table(
        {"distance": "float64", "passengers": "int64", "carrier": "string"},
        "flights",
    )


def test_literal_is_pushable():
    r = analyze_calc_expr(42)
    assert r.pushable is True
    assert r.post_agg_only is False
    assert r.depends_on == frozenset()


def test_plain_reduction_on_base_is_pushable():
    base = _base()
    r = analyze_calc_expr(base.distance.sum(), base_table_op=base.op())
    assert r.pushable is True
    assert r.post_agg_only is False
    assert r.inline_aggs == frozenset({"distance"})


def test_arith_of_aggs_on_same_base_is_pushable():
    base = _base()
    r = analyze_calc_expr(
        base.distance.sum() / base.passengers.sum(), base_table_op=base.op()
    )
    assert r.pushable is True
    assert r.inline_aggs == frozenset({"distance", "passengers"})


def test_measure_ratio_is_post_agg_only():
    vt = _vt()
    r = analyze_calc_expr(
        vt.flight_count / vt.total_distance,
        known_measures=frozenset({"flight_count", "total_distance"}),
    )
    assert r.post_agg_only is True
    assert r.pushable is False
    assert r.depends_on == frozenset({"flight_count", "total_distance"})
    assert r.references_AllOf is False
    assert r.has_window is False


def test_empty_window_over_reduction_is_totals_reference():
    """``t.all(x)`` shape: x.sum().over(empty window) reads as references_AllOf."""
    vt = _vt()
    r = analyze_calc_expr(
        vt.flight_count / vt.flight_count.sum().over(xibis.window()),
        known_measures=frozenset({"flight_count"}),
    )
    assert r.references_AllOf is True
    assert r.has_window is True
    assert r.post_agg_only is True
    assert "flight_count" in r.depends_on


def test_ordered_window_is_window_not_totals():
    """Moving average / rank-style windows must not be classified as totals."""
    vt = _vt()
    r = analyze_calc_expr(
        vt.flight_count.mean().over(xibis.window(order_by="date", preceding=2)),
        known_measures=frozenset({"flight_count", "date"}),
    )
    assert r.has_window is True
    assert r.references_AllOf is False
    assert r.post_agg_only is True


def test_unknown_input_warns_and_falls_back():
    class Weird:
        pass

    with pytest.warns(UserWarning, match="post-aggregation-only"):
        r = analyze_calc_expr(Weird())
    assert r.post_agg_only is True
    assert r.pushable is False


def test_returns_frozen_dataclass():
    r = analyze_calc_expr(1)
    assert isinstance(r, CalcExprAnalysis)
    with pytest.raises(Exception):
        r.pushable = False  # frozen
