"""Tests for the ibis-native calc-measure compiler.

Exercises :class:`IbisCalcScope` dispatch, lambda evaluation, structural
classification via the analyzer, and compile-time substitution of the
virtual aggregated table with the real one.
"""

from __future__ import annotations

import pandas as pd
import pytest

xorq = pytest.importorskip("xorq", reason="xorq not installed")

import xorq.api as xo  # noqa: E402

from boring_semantic_layer._xorq import ibis as xibis  # noqa: E402
from boring_semantic_layer.calc_compiler import (  # noqa: E402
    IbisCalcScope,
    classify_calc_lambda,
    compile_calc_measure,
    compile_calc_measures,
    evaluate_calc_lambda,
)


@pytest.fixture(scope="module")
def base_tbl():
    con = xo.duckdb.connect()
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA", "DL"],
            "distance": [100, 200, 150, 250, 300],
            "passengers": [10, 20, 30, 40, 50],
        }
    )
    return con.create_table("flights", df)


def test_scope_dispatches_measure_to_virtual_table(base_tbl):
    vt = xibis.table({"flight_count": "int64"}, "__virt__")
    scope = IbisCalcScope(base_tbl, vt, frozenset({"flight_count"}))
    expr = scope.flight_count
    # Field's relation should be the virtual table
    assert expr.op().rel == vt.op()


def test_scope_dispatches_column_to_base_table(base_tbl):
    vt = xibis.table({"flight_count": "int64"}, "__virt__")
    scope = IbisCalcScope(base_tbl, vt, frozenset({"flight_count"}))
    expr = scope.distance
    assert expr.op().rel == base_tbl.op()


def test_all_with_string_measure_name(base_tbl):
    vt = xibis.table({"flight_count": "int64"}, "__virt__")
    scope = IbisCalcScope(base_tbl, vt, frozenset({"flight_count"}))
    expr = scope.all("flight_count")
    # Should be a window function over a sum
    op_name = type(expr.op()).__name__
    assert "Window" in op_name or "Sum" in op_name


def test_classify_pct_calc_measure(base_tbl):
    """``flight_count / t.all(flight_count)`` is a post-agg measure with
    ``references_AllOf`` set."""
    fn = lambda t: t.flight_count / t.all(t.flight_count)
    _, analysis = classify_calc_lambda(fn, base_tbl, frozenset({"flight_count"}))
    assert analysis.post_agg_only is True
    assert analysis.pushable is False
    assert analysis.references_AllOf is True
    assert "flight_count" in analysis.depends_on


def test_classify_inline_agg_pushable(base_tbl):
    """``t.distance.sum() / t.passengers.sum()`` references only the base
    table — the analyzer reports it as pushable (a base measure)."""
    fn = lambda t: t.distance.sum() / t.passengers.sum()
    _, analysis = classify_calc_lambda(fn, base_tbl, frozenset())
    assert analysis.pushable is True
    assert analysis.post_agg_only is False
    assert analysis.inline_aggs == frozenset({"distance", "passengers"})


def test_compile_substitutes_virtual_for_real(base_tbl):
    fn = lambda t: t.flight_count / t.all(t.flight_count)
    expr, vt = evaluate_calc_lambda(fn, base_tbl, frozenset({"flight_count"}))

    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
    )
    compiled = compile_calc_measure(expr, vt, real_agg)
    # The compiled expression's column references should now point at real_agg
    op = compiled.op()
    # Walk and assert no remaining Field refers to vt
    seen_rels = set()
    stack = [op]
    visited = set()
    while stack:
        cur = stack.pop()
        if id(cur) in visited:
            continue
        visited.add(id(cur))
        if hasattr(cur, "rel") and cur.rel is not None:
            seen_rels.add(id(cur.rel))
        children = getattr(cur, "__children__", None) or getattr(cur, "__args__", ())
        for c in children:
            if hasattr(c, "__children__") or hasattr(c, "__args__"):
                stack.append(c)
    assert id(vt.op()) not in seen_rels


def test_compile_pct_calc_measure_end_to_end(base_tbl):
    fn = lambda t: t.flight_count / t.all(t.flight_count)
    expr, vt = evaluate_calc_lambda(fn, base_tbl, frozenset({"flight_count"}))

    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
    )
    final = compile_calc_measures(real_agg, {"pct": (expr, vt)})
    df = final.execute().sort_values("carrier").reset_index(drop=True)
    assert pytest.approx(df["pct"].sum()) == 1.0


def test_compile_no_calcs_passes_through(base_tbl):
    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
    )
    out = compile_calc_measures(real_agg, {})
    assert out is real_agg


def test_compile_multiple_calc_measures(base_tbl):
    """Two independent calcs apply together via a single mutate."""
    fn1 = lambda t: t.flight_count / t.all(t.flight_count)
    fn2 = lambda t: t.total_distance / t.flight_count

    known = frozenset({"flight_count", "total_distance"})
    e1, vt1 = evaluate_calc_lambda(fn1, base_tbl, known)
    e2, vt2 = evaluate_calc_lambda(fn2, base_tbl, known)

    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
        total_distance=base_tbl.distance.sum(),
    )
    final = compile_calc_measures(real_agg, {"pct": (e1, vt1), "avg_dist": (e2, vt2)})
    df = final.execute().sort_values("carrier").reset_index(drop=True)
    assert "pct" in df.columns
    assert "avg_dist" in df.columns
    assert pytest.approx(df["pct"].sum()) == 1.0
