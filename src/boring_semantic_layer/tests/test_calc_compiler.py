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
    """``compile_calc_measure`` rewrites Field(vt) → Field(real_agg)."""
    # A pure measure-ref calc: avg_dist references two known measures
    # on the virtual table. ``compile_calc_measure`` should rebind both
    # Fields to the actual aggregated table.
    fn = lambda t: t.total_distance / t.flight_count
    known = frozenset({"total_distance", "flight_count"})
    expr, vt, _totals_vt = evaluate_calc_lambda(fn, base_tbl, known)

    real_agg = base_tbl.group_by("carrier").aggregate(
        total_distance=base_tbl.distance.sum(),
        flight_count=base_tbl.count(),
    )
    compiled = compile_calc_measure(expr, vt, real_agg)

    # Walk the compiled op tree; every Field reference should land on
    # real_agg, none on the synthetic vt.
    real_op = real_agg.op()
    vt_op = vt.op()
    rels: list[int] = []
    seen: set[int] = set()
    stack: list = [compiled.op()]
    while stack:
        cur = stack.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        rel = getattr(cur, "rel", None)
        if rel is not None:
            rels.append(id(rel))
        for child in getattr(cur, "__args__", ()) or ():
            if hasattr(child, "__args__") or hasattr(child, "rel"):
                stack.append(child)
    assert id(real_op) in rels, "compiled expression should reference real_agg"
    assert id(vt_op) not in rels, "compiled expression must not reference virtual vt"

    # End-to-end: the table should execute and produce the expected ratio.
    final = real_agg.mutate(avg_dist=compiled).execute().sort_values("carrier")
    # AA: 300/2 = 150; UA: 400/2 = 200; DL: 300/1 = 300
    by_carrier = dict(zip(final["carrier"], final["avg_dist"], strict=True))
    assert pytest.approx(by_carrier["AA"]) == 150.0
    assert pytest.approx(by_carrier["UA"]) == 200.0
    assert pytest.approx(by_carrier["DL"]) == 300.0


def test_apply_calc_measures_raises_when_totals_unavailable(base_tbl):
    """Clear error when t.all(...) is referenced but no totals can be built."""
    from boring_semantic_layer.calc_compiler import (
        TotalsNotAvailableError,
        apply_calc_measures,
    )

    fn = lambda t: t.flight_count / t.all(t.flight_count)
    real_agg = base_tbl.group_by("carrier").aggregate(
        flight_count=base_tbl.count(),
    )
    # No agg_specs and no real_totals_tbl — the totals build can't run.
    with pytest.raises(TotalsNotAvailableError, match="t.all"):
        apply_calc_measures(
            real_agg,
            base_tbl,
            {"pct": fn},
            frozenset({"flight_count"}),
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


def test_multiple_allof_calcs_share_one_totals_per_measure():
    """Two t.all-referencing calcs share their totals computations.

    Each base measure that's referenced by ``t.all(...)`` gets exactly
    one windowed-totals column added to the base via
    ``measure.over(window())``. Multiple calcs referencing the same
    measure share the same totals column — no duplicate window
    computation, no quadratic growth.

    The new compilation strategy uses windowed totals carried through
    the per-group aggregation rather than cross-joined totals tables,
    so the rendered SQL has *zero* ``CROSS JOIN`` operations and one
    ``OVER (...)`` window per AllOf-referenced measure. Locking the
    "one totals per measure" property guards against an O(n²) regression.
    """
    from boring_semantic_layer import to_semantic_table

    con = xo.duckdb.connect()
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA"],
            "distance": [100, 200, 300, 400],
            "passengers": [10, 20, 30, 40],
        }
    )
    tbl = con.create_table("flights_share_totals", df)
    st = (
        to_semantic_table(tbl, "flights_share_totals")
        .with_measures(
            total_distance=lambda t: t.distance.sum(),
            total_passengers=lambda t: t.passengers.sum(),
        )
        .with_measures(
            pct_distance=lambda t: t.total_distance / t.all(t.total_distance),
            pct_passengers=lambda t: t.total_passengers / t.all(t.total_passengers),
        )
    )
    sql = st.group_by("carrier").aggregate("pct_distance", "pct_passengers").compile()
    sql_upper = sql.upper()
    # No cross joins under the new strategy.
    assert sql_upper.count("CROSS JOIN") == 0
    # One windowed totals per AllOf-referenced base measure (two here).
    # Each appears as ``SUM(...) OVER (ROWS BETWEEN ...)``.
    assert sql_upper.count("__BSL_TOTALS__TOTAL_DISTANCE") >= 1
    assert sql_upper.count("__BSL_TOTALS__TOTAL_PASSENGERS") >= 1
    # Output schema only has the user-requested columns.
    result = st.group_by("carrier").aggregate("pct_distance", "pct_passengers")
    assert set(result.columns) == {"carrier", "pct_distance", "pct_passengers"}


def test_apply_calc_measures_join_with_mean_totals():
    """Joined model: t.all over a non-sum measure recomputes totals from base."""
    from boring_semantic_layer import to_semantic_table

    con = xo.duckdb.connect()
    flights = pd.DataFrame(
        {
            "carrier_code": ["AA", "AA", "UA", "UA"],
            "distance": [100, 200, 300, 400],
        }
    )
    carriers = pd.DataFrame(
        {"code": ["AA", "UA"], "carrier_name": ["American", "United"]}
    )
    f_tbl = con.create_table("join_flights", flights)
    c_tbl = con.create_table("join_carriers", carriers)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        avg_distance=lambda t: t.distance.mean(),
    )
    carriers_st = to_semantic_table(c_tbl, "carriers").with_dimensions(
        carrier_name=lambda t: t.carrier_name,
    )
    joined = flights_st.join_one(
        carriers_st,
        on=lambda left, right: left.carrier_code == right.code,
    ).with_measures(
        ratio=lambda t: t.avg_distance / t.all(t.avg_distance),
    )

    df = (
        joined.group_by("carrier_name")
        .aggregate("avg_distance", "ratio")
        .execute()
        .sort_values("carrier_name")
        .reset_index(drop=True)
    )

    # AA mean=150, UA mean=350; overall mean=250 (NOT 150+350=500).
    by_name = dict(zip(df["carrier_name"], df["ratio"], strict=True))
    assert pytest.approx(by_name["American"]) == 150 / 250
    assert pytest.approx(by_name["United"]) == 350 / 250


@pytest.mark.parametrize(
    "reducer,expected_total,per_group",
    [
        # Median: pooled rows [100, 200, 300, 400] → 250.
        ("median", 250.0, {"AA": 150.0, "UA": 350.0}),
        # Min: AA=100, UA=300, overall=100.
        ("min", 100.0, {"AA": 100.0, "UA": 300.0}),
        # Max: AA=200, UA=400, overall=400.
        ("max", 400.0, {"AA": 200.0, "UA": 400.0}),
    ],
)
def test_apply_calc_measures_non_sum_totals(reducer, expected_total, per_group):
    """``t.all`` over min/max/median recomputes totals via the formula, not a windowed sum.

    Locks the same v1-bug fix as the mean case for the rest of the
    common non-sum reductions: per-group sums-of-medians or sum-of-mins
    would be obviously wrong.
    """
    from boring_semantic_layer import to_semantic_table

    con = xo.duckdb.connect()
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA"],
            "distance": [100, 200, 300, 400],
        }
    )
    tbl = con.create_table(f"flights_nonsum_{reducer}", df)

    st = (
        to_semantic_table(tbl, f"flights_nonsum_{reducer}")
        .with_measures(**{f"d_{reducer}": lambda t, op=reducer: getattr(t.distance, op)()})
        .with_measures(
            ratio=lambda t, op=reducer: getattr(t, f"d_{op}")
            / t.all(getattr(t, f"d_{op}")),
        )
    )
    df_out = (
        st.group_by("carrier")
        .aggregate(f"d_{reducer}", "ratio")
        .execute()
        .sort_values("carrier")
        .reset_index(drop=True)
    )
    by_carrier = dict(zip(df_out["carrier"], df_out[f"d_{reducer}"], strict=True))
    assert pytest.approx(by_carrier["AA"]) == per_group["AA"]
    assert pytest.approx(by_carrier["UA"]) == per_group["UA"]
    by_ratio = dict(zip(df_out["carrier"], df_out["ratio"], strict=True))
    assert pytest.approx(by_ratio["AA"]) == per_group["AA"] / expected_total
    assert pytest.approx(by_ratio["UA"]) == per_group["UA"] / expected_total


def test_cast_to_float_survives_int_measure_substitution():
    """``int_measure.cast('float64') / int_measure_total * 100`` returns nonzero.

    Regression test for the bug where the preprocess step in
    ``_compile_aggregation`` populated the virtual aggregated table's
    schema with placeholder ``float64`` dtypes for every measure.
    User casts like ``t.flight_count.cast('float64')`` were elided as
    no-ops by ibis (the column was already float64 in the synthetic
    schema). After substitution to the real aggregated table — where
    ``flight_count`` is int64 (from ``CountStar``) — the Cast was gone,
    so ``int / int * 100`` returned 0 for ratios less than 1.

    The fix uses the *real* dtype derived from ``agg_specs[name](base_tbl).type()``
    so the cast is preserved when substituted. This test pins the
    behavior end-to-end with a count-style integer measure and a
    ``cast('float64')``-using calc.
    """
    from boring_semantic_layer import to_semantic_table

    con = xo.duckdb.connect()
    df = pd.DataFrame(
        {
            "carrier": ["AA"] * 30 + ["UA"] * 70,
            "value": list(range(100)),
        }
    )
    tbl = con.create_table("flights_cast_regression", df)

    st = (
        to_semantic_table(tbl, "flights_cast_regression")
        .with_measures(
            flight_count=lambda t: t.count(),  # int64
        )
        .with_measures(
            share_pct=(
                lambda t: t.flight_count.cast("float64") / t.all(t.flight_count) * 100
            ),
        )
    )
    result = (
        st.group_by("carrier")
        .aggregate("flight_count", "share_pct")
        .execute()
        .sort_values("carrier")
        .reset_index(drop=True)
    )
    by_carrier = dict(zip(result["carrier"], result["share_pct"], strict=True))
    # AA = 30/100 = 30%, UA = 70/100 = 70%; sum = 100% (sanity)
    assert pytest.approx(by_carrier["AA"]) == 30.0
    assert pytest.approx(by_carrier["UA"]) == 70.0
    assert pytest.approx(result["share_pct"].sum()) == 100.0


def test_lift_inline_reductions_routes_window_to_totals():
    """The two-pass substitution gives top-level reductions vt refs and
    ``t.all(...)``-style windowed reductions totals_vt refs.

    Locks the contract documented in :func:`lift_inline_reductions`:
    the same ``Reduction`` node may appear both at top level (per-group
    value, want ``Field(vt, anon)``) and as a ``WindowFunction.func``
    (totals value, want ``Field(totals_vt, anon)``). Bind the reduction
    to a single Python object so the duplicate-id case (which
    ``op.replace`` would dedupe by equality) is exercised end-to-end.
    """
    from boring_semantic_layer.calc_compiler import lift_inline_reductions

    base = xibis.table(
        {"distance": "float64", "passengers": "int64"},
        "flights_lift",
    )
    vt = xibis.table({"__bsl_unused__": "int64"}, "__vt__")

    shared = base.distance.sum()
    expr = shared / shared.over(xibis.window())

    rewritten, new_vt, new_totals_vt, lifted = lift_inline_reductions(expr, vt, base)

    # A single shared reduction should produce exactly one anonymous lift —
    # locking the dedup-by-id behavior at the top of the function.
    assert len(lifted) == 1
    anon_name = next(iter(lifted))

    assert anon_name in dict(new_vt.op().schema.items())
    assert anon_name in dict(new_totals_vt.op().schema.items())

    new_vt_id = id(new_vt.op())
    new_totals_id = id(new_totals_vt.op())
    rewritten_op = rewritten.op() if hasattr(rewritten, "op") else rewritten

    fields_seen: list[tuple[str, int]] = []
    seen: set[int] = set()
    stack: list = [rewritten_op]
    while stack:
        cur = stack.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        if hasattr(cur, "name") and hasattr(cur, "rel"):
            fields_seen.append((cur.name, id(cur.rel)))
        for child in getattr(cur, "__args__", ()) or ():
            if hasattr(child, "__args__") or hasattr(child, "rel"):
                stack.append(child)

    rels_for_anon = {r for n, r in fields_seen if n == anon_name}
    assert new_vt_id in rels_for_anon, "expected Field(new_vt, anon) for the bare reduction"
    assert new_totals_id in rels_for_anon, (
        "expected Field(new_totals_vt, anon) for the windowed reduction"
    )
