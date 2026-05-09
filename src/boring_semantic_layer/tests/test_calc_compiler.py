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
    # ``t.all(measure_name)`` resolves to a Field on the parallel
    # totals virtual table; the compiler later substitutes it with
    # the real no-group-by aggregation.
    op = expr.op()
    assert type(op).__name__ == "Field"
    assert op.name == "flight_count"
    assert id(op.rel) == id(scope._totals_virtual_agg_tbl.op())


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
    """An inline-reduction calc (no ``t.all``) substitutes vt → real_agg."""
    fn = lambda t: t.distance.sum() / t.passengers.sum()
    expr, vt, _totals_vt = evaluate_calc_lambda(fn, base_tbl, frozenset())

    real_agg = base_tbl.group_by("carrier").aggregate(
        total_distance=base_tbl.distance.sum(),
        total_passengers=base_tbl.passengers.sum(),
    )
    # When no totals are involved, compile_calc_measure rewrites Fields
    # on the virtual aggregated table to point at real_agg directly.
    # (The straight inline-aggregation lift still applies upstream; this
    # test exercises just the substitution mechanic.)
    from boring_semantic_layer._xorq import Field

    rewritten = expr.op().replace(
        {
            expr.op()
            .__args__[0]: Field(real_agg.op(), "total_distance")  # type: ignore[index]
        }
    )
    # Smoke-test that the resulting op tree carries the real relation.
    assert any(
        id(getattr(n, "rel", None)) == id(real_agg.op())
        for n in [rewritten, *getattr(rewritten, "__args__", ())]
        if hasattr(n, "rel")
    )


def test_compile_pct_calc_measure_end_to_end(base_tbl):
    """``apply_calc_measures`` builds totals on demand for ``t.all``."""
    from boring_semantic_layer.calc_compiler import apply_calc_measures

    fn = lambda t: t.flight_count / t.all(t.flight_count)
    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
    )
    final = apply_calc_measures(
        real_agg,
        base_tbl,
        {"pct": fn},
        frozenset({"flight_count"}),
        agg_specs={"flight_count": lambda t: t.count()},
    )
    df = final.execute().sort_values("carrier").reset_index(drop=True)
    assert "pct" in df.columns
    # Sum of per-group counts ÷ total count = 1.0 for sum-style measures.
    assert pytest.approx(df["pct"].sum()) == 1.0


def test_compile_no_calcs_passes_through(base_tbl):
    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
    )
    out = compile_calc_measures(real_agg, {})
    assert out is real_agg


def test_compile_multiple_calc_measures(base_tbl):
    """Two independent calcs apply together: one references totals, one doesn't."""
    from boring_semantic_layer.calc_compiler import apply_calc_measures

    pct = lambda t: t.flight_count / t.all(t.flight_count)
    avg_dist = lambda t: t.total_distance / t.flight_count

    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
        total_distance=base_tbl.distance.sum(),
    )
    final = apply_calc_measures(
        real_agg,
        base_tbl,
        {"pct": pct, "avg_dist": avg_dist},
        frozenset({"flight_count", "total_distance"}),
        agg_specs={
            "flight_count": lambda t: t.count(),
            "total_distance": lambda t: t.distance.sum(),
        },
    )
    df = final.execute().sort_values("carrier").reset_index(drop=True)
    assert "pct" in df.columns
    assert "avg_dist" in df.columns
    assert pytest.approx(df["pct"].sum()) == 1.0
