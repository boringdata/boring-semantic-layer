"""
Test different styles of referencing measures in lambdas:
1. String name via attribute access: t.flight_count
2. String name via bracket notation: t["flight_count"]
3. String name passed to t.all(): t.all("flight_count")
4. MeasureRef object: t.all(t.flight_count)
5. Ibis column: t.all(t.distance)
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


def test_measure_ref_via_attribute():
    """Test referencing measures via attribute access (current behavior)."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(
            # Reference measure via attribute access
            pct=lambda t: t.flight_count / t.all(t.flight_count),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct").execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_measure_ref_via_bracket_notation():
    """Test referencing measures via bracket notation."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(
            # Reference measure via bracket notation
            pct=lambda t: t["flight_count"] / t.all(t["flight_count"]),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct").execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_all_with_string_name():
    """Test t.all() with string measure name."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(
            # Pass string name to t.all()
            pct=lambda t: t.flight_count / t.all("flight_count"),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct").execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_all_with_measure_ref_object():
    """Test t.all() with MeasureRef object (current behavior)."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(
            # Pass MeasureRef object to t.all()
            pct=lambda t: t.flight_count / t.all(t.flight_count),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct").execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_all_with_ibis_column_post_aggregation():
    """Test t.all() with ibis column in post-aggregation context."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
    )

    # Post-aggregation: use t.all() with ibis columns
    result = (
        flights_st.group_by("carrier")
        .aggregate("flight_count", "total_distance")
        .mutate(
            # t.flight_count is now an ibis column (post-aggregation)
            pct=lambda t: t.flight_count / t.all(t.flight_count),
        )
    )

    df = result.execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_all_with_string_in_post_aggregation():
    """Test t.all() with string name in post-aggregation context."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
    )

    # Post-aggregation: use t.all() with string name
    result = (
        flights_st.group_by("carrier")
        .aggregate("flight_count")
        .mutate(
            # Pass string name to t.all() in post-aggregation context
            pct=lambda t: t.flight_count / t.all("flight_count"),
        )
    )

    df = result.execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_mixed_reference_styles():
    """Test mixing different reference styles in the same expression."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
        .with_measures(
            # Mix attribute access and string name
            pct1=lambda t: t.flight_count / t.all("flight_count"),
            # Mix bracket notation and MeasureRef
            pct2=lambda t: t["flight_count"] / t.all(t.flight_count),
            # All styles together
            avg_distance_pct=lambda t: (t["total_distance"] / t.flight_count)
            / t.all("total_distance"),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct1", "pct2").execute()
    # Both pct1 and pct2 should give same results
    assert pytest.approx(df.pct1.sum()) == 1.0
    assert pytest.approx(df.pct2.sum()) == 1.0
    # pct1 and pct2 should be equal
    assert all(pytest.approx(p1) == p2 for p1, p2 in zip(df.pct1, df.pct2, strict=False))


def test_prefixed_measures_with_string():
    """Test string-based reference with prefixed measures after join."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    carriers = pd.DataFrame({"code": ["AA", "UA"], "name": ["American", "United"]})
    f_tbl = con.create_table("flights", flights)
    c_tbl = con.create_table("carriers", carriers)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
    )
    carriers_st = to_semantic_table(c_tbl, "carriers").with_dimensions(
        code=lambda t: t.code,
        name=lambda t: t.name,
    )

    joined = (
        flights_st.join_many(carriers_st, lambda f, c: f.carrier == c.code)
        .with_dimensions(name=lambda t: t.name)
        .with_measures(
            # Reference prefixed measure with bracket notation (dots not allowed in Python identifiers)
            pct_full=lambda t: t["flights.flight_count"] / t.all("flights.flight_count"),
            # Reference with short name (should resolve to flights.flight_count)
            pct_short=lambda t: t["flights.flight_count"] / t.all("flights.flight_count"),
        )
    )

    df = joined.group_by("name").aggregate("pct_full", "pct_short").execute()
    # Both should give same results
    assert all(pytest.approx(p1) == p2 for p1, p2 in zip(df.pct_full, df.pct_short, strict=False))


def test_inline_measure_with_different_reference_styles():
    """Test inline measures in aggregate() using different reference styles."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
    )

    # Define measures inline with different reference styles
    df = (
        flights_st.group_by("carrier")
        .aggregate(
            "flight_count",
            pct_attr=lambda t: t.flight_count / t.all(t.flight_count),
            pct_string=lambda t: t.flight_count / t.all("flight_count"),
            pct_bracket=lambda t: t["flight_count"] / t.all(t["flight_count"]),
        )
        .execute()
    )

    # All three should give same results
    assert pytest.approx(df.pct_attr.sum()) == 1.0
    assert pytest.approx(df.pct_string.sum()) == 1.0
    assert pytest.approx(df.pct_bracket.sum()) == 1.0
    assert all(pytest.approx(p1) == p2 for p1, p2 in zip(df.pct_attr, df.pct_string, strict=False))
    assert all(pytest.approx(p1) == p2 for p1, p2 in zip(df.pct_attr, df.pct_bracket, strict=False))


def test_all_of_multilayer_calc_measure():
    """``t.all()`` over a calc-of-calc chain compiles via the analyzer path.

    The analyzer-based compiler lifts the totals shape to a windowed sum
    over the post-aggregation column (``x.sum().over(window())``).  For
    sum-style measures this matches the legacy curated-AST behavior of
    re-aggregating on the unfiltered base table; for non-sum measures
    (e.g. ``avg``) it differs, which is documented as a v1 limitation —
    see ADR 0001 design decision #1.
    """
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA"],
            "distance": [100, 200, 300, 400],
        }
    )
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            total_distance=lambda t: t.distance.sum(),
            total_flights=lambda t: t.count(),
            distance_plus_one=lambda t: t.total_distance + 1,
        )
        .with_measures(
            pct_of_total=lambda t: t.distance_plus_one / t.all(t.distance_plus_one),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct_of_total").execute()

    assert len(df) == 2
    assert "pct_of_total" in df.columns
    # AA total_distance=300, +1=301; UA total_distance=700, +1=701.
    # windowed sum over the carriers = 301 + 701 = 1002.
    assert pytest.approx(df.pct_of_total.sum()) == 1.0


# --- Tests for .values / .schema / .columns with calc measures ---


def test_calc_measure_values_schema_columns_single_block():
    """Composed measures defined in a single with_measures block should not
    break .values, .schema, or .columns on the aggregate op."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: t.total_distance / t.flight_count,
    )

    agg = st.group_by("carrier").aggregate("avg_distance")
    op = agg.op()

    # These all previously raised IbisTypeError for calc measures
    assert "avg_distance" in op.values
    assert "carrier" in op.values
    assert "avg_distance" in op.schema
    assert "avg_distance" in agg.columns


def test_calc_measure_values_chained_with_measures():
    """Calc measures defined across chained with_measures calls should work
    with .values, .schema, and .columns."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = (
        to_semantic_table(tbl, "flights")
        .with_measures(
            total_distance=lambda t: t.distance.sum(),
            flight_count=lambda t: t.count(),
        )
        .with_measures(
            avg_distance=lambda t: t.total_distance / t.flight_count,
        )
    )

    agg = st.group_by("carrier").aggregate("avg_distance")
    op = agg.op()

    assert "avg_distance" in op.values
    assert "avg_distance" in op.schema
    assert "avg_distance" in agg.columns


def test_base_measures_only_values():
    """Regression guard: .values should still work for base-measures-only aggregations."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
    )

    agg = st.group_by("carrier").aggregate("flight_count")
    op = agg.op()

    assert "flight_count" in op.values
    assert "carrier" in op.values
    assert "flight_count" in op.schema
    assert set(agg.columns) == {"carrier", "flight_count"}


def test_calc_measure_downstream_chaining():
    """Downstream operations (order_by, limit) should work after aggregating
    calc measures, confirming .schema/.columns propagate correctly."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame(
        {"carrier": ["AA", "AA", "UA", "DL"], "distance": [100, 200, 300, 400]}
    )
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: t.total_distance / t.flight_count,
    )

    result = (
        st.group_by("carrier")
        .aggregate("avg_distance")
        .order_by("avg_distance")
        .limit(2)
    )

    df = result.execute()
    assert len(df) == 2
    assert "avg_distance" in df.columns


# --- Tests for MethodCall AST node (e.g. .round(), .cast(), chaining) ---


def test_method_call_round_on_calc_measure():
    """Calling .round() on a calculated measure expression should work."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: (t.total_distance / t.flight_count).round(1),
    )

    df = st.group_by("carrier").aggregate("avg_distance").execute()
    assert len(df) == 2
    assert "avg_distance" in df.columns
    # AA: (100+200)/2 = 150.0, UA: 300/1 = 300.0
    vals = sorted(df.avg_distance.tolist())
    assert vals == [150.0, 300.0]


def test_method_call_cast_on_calc_measure():
    """Calling .cast() on a calculated measure expression should work."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: (t.total_distance / t.flight_count).cast("float32"),
    )

    df = st.group_by("carrier").aggregate("avg_distance").execute()
    assert len(df) == 2
    assert "avg_distance" in df.columns


def test_method_call_chained_round_cast():
    """Chaining .round().cast() on a calc measure should work."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: (t.total_distance / t.flight_count).round(2).cast("float32"),
    )

    df = st.group_by("carrier").aggregate("avg_distance").execute()
    assert len(df) == 2
    assert "avg_distance" in df.columns


def test_method_call_round_on_measure_ref():
    """Calling .round() on a MeasureRef directly should work."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: t.total_distance / t.flight_count,
        avg_distance_rounded=lambda t: t.avg_distance.round(1),
    )

    df = st.group_by("carrier").aggregate("avg_distance_rounded").execute()
    assert len(df) == 2
    assert "avg_distance_rounded" in df.columns
    vals = sorted(df.avg_distance_rounded.tolist())
    assert vals == [150.0, 300.0]


def test_method_call_arithmetic_after():
    """Arithmetic after a method call: t.x.round(2) * 100."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        pct=lambda t: (t.total_distance / t.flight_count).round(0) * 100,
    )

    df = st.group_by("carrier").aggregate("pct").execute()
    assert len(df) == 2
    vals = sorted(df.pct.tolist())
    # AA: 150*100=15000, UA: 300*100=30000
    assert vals == [15000.0, 30000.0]


def test_method_call_fillna_on_calc_measure():
    """Calling .fillna() on a calc measure expression should work."""
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", data)

    st = to_semantic_table(tbl, "flights").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: (t.total_distance / t.flight_count).fillna(0),
    )

    df = st.group_by("carrier").aggregate("avg_distance").execute()
    assert len(df) == 2
    assert "avg_distance" in df.columns


def test_method_call_serialization_roundtrip():
    """A method-call calc measure survives serialize/deserialize roundtrip.

    Replaces the legacy curated-AST direct construction with the
    behavioral round-trip through ``to_tagged`` / ``from_tagged``.
    """
    from boring_semantic_layer import to_semantic_table
    from boring_semantic_layer.serialization import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100.0, 200.0, 300.0]})
    tbl = con.create_table("flights_ms", df)

    st = to_semantic_table(tbl, "flights_ms").with_measures(
        total_distance=lambda t: t.distance.sum(),
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: (t.total_distance / t.flight_count).round(2),
    )
    reconstructed = from_tagged(to_tagged(st))
    df_orig = st.group_by("carrier").aggregate("avg_distance").execute().sort_values("carrier")
    df_round = (
        reconstructed.group_by("carrier")
        .aggregate("avg_distance")
        .execute()
        .sort_values("carrier")
    )
    pd.testing.assert_frame_equal(
        df_orig.reset_index(drop=True),
        df_round.reset_index(drop=True),
    )


def test_calc_dtype_inference_with_inline_aggregation():
    """``schema`` should know the dtype of a calc measure built with t.all(t.col.sum())."""
    con = ibis.duckdb.connect(":memory:")
    events = pd.DataFrame({"grp": ["a", "b"], "value": [1.0, 2.0]})
    tbl = con.create_table("events", events)

    st = to_semantic_table(tbl, "events").with_measures(
        pct_of_total=lambda t: t.value.sum() / t.all(t.value.sum()),
    )
    # Before infer_calc_dtype handled the rewrite path, this was silently
    # dropped from values and the dtype was unknown.
    assert "pct_of_total" in st.schema.names


# ---------------------------------------------------------------------------
# Broader base-measure recognition (#1)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method, expected_kind",
    [
        ("var", "base"),
        ("std", "base"),
        ("median", "base"),
        ("nunique", "base"),
        ("approx_nunique", "base"),
        ("first", "base"),
        ("last", "base"),
    ],
)
def test_extra_reductions_classified_as_base(method, expected_kind):
    """Measures named the same as a column then aggregated with a non-sum/mean
    reduction should still be classified as base, not silently routed through
    the calc compiler.
    """
    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table(f"flights_{method}", data)

    # Define a measure named ``distance`` (matching the column name) so that
    # ``t.distance.<method>()`` resolves through the MeasureRef path that
    # ``_AGG_METHODS`` guards.
    st = to_semantic_table(tbl, f"flights_{method}").with_measures(
        distance=lambda t: t.distance.sum(),
    )
    st = st.with_measures(
        derived=lambda t, _m=method: getattr(t.distance, _m)(),
    )

    op = st.op()
    base_names = set(op.get_measures().keys())
    calc_names = set(op.get_calculated_measures().keys())
    assert "derived" in base_names, (
        f"{method!r} measure should classify as base, got calc"
    )
    assert "derived" not in calc_names


# ---------------------------------------------------------------------------
# Typo'd measure references (#2)
# ---------------------------------------------------------------------------


def test_typo_measure_ref_raises_with_suggestion():
    """A calc-measure lambda referencing a misspelled measure name should
    fail loudly at construction with a "did you mean?" suggestion."""
    from boring_semantic_layer.measure_scope import UnknownMeasureRefError

    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA"], "distance": [100]})
    tbl = con.create_table("flights_typo", data)

    st = to_semantic_table(tbl, "flights_typo").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
    )

    with pytest.raises(UnknownMeasureRefError, match="flight_count"):
        st.with_measures(
            # ``flight_konut`` typo of ``flight_count``
            ratio=lambda t: t.flight_konut / t.total_distance,
        )


def test_typo_in_t_all_raises():
    """Same loud-fail behavior when the typo is the argument to ``t.all``."""
    from boring_semantic_layer.measure_scope import UnknownMeasureRefError

    con = ibis.duckdb.connect(":memory:")
    data = pd.DataFrame({"carrier": ["AA"], "distance": [100]})
    tbl = con.create_table("flights_typo_all", data)

    st = to_semantic_table(tbl, "flights_typo_all").with_measures(
        flight_count=lambda t: t.count(),
    )

    with pytest.raises(UnknownMeasureRefError, match="flight_count"):
        st.with_measures(
            ratio=lambda t: t.flight_count / t.all(t.flight_konut),
        )


def test_substring_measure_name_does_not_trigger_typo():
    """Names that are substrings of other measures should not trip the typo
    detector. Asking for a known measure name returns its column on the
    virtual aggregated table without firing the typo path.
    """
    from boring_semantic_layer.calc_compiler import IbisCalcScope
    from boring_semantic_layer.calc_analyzer import virtual_agg_table
    import ibis as i

    tbl = i.table({"col": "int64"}, name="t")
    vt = virtual_agg_table({"net_revenue": "float64", "total_net_revenue": "float64"})
    scope = IbisCalcScope(
        base_tbl=tbl,
        virtual_agg_tbl=vt,
        known_measures=frozenset({"net_revenue", "total_net_revenue"}),
    )
    assert scope.net_revenue is not None
    # Asking for 'col' is fine — it's a column.
    assert scope.col is not None
