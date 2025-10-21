"""
Test different styles of referencing measures in lambdas:
1. String name via attribute access: t.flight_count
2. String name via bracket notation: t["flight_count"]
3. String name passed to t.all(): t.all("flight_count")
4. MeasureRef object: t.all(t.flight_count)
5. Ibis column: t.all(t.distance)
"""
import pandas as pd
import ibis
import pytest

from boring_semantic_layer.semantic_api import to_semantic_table


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
            pct=lambda t: t.flight_count / t.all(t.flight_count)
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
            pct=lambda t: t["flight_count"] / t.all(t["flight_count"])
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
            pct=lambda t: t.flight_count / t.all("flight_count")
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
            pct=lambda t: t.flight_count / t.all(t.flight_count)
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
        flight_count=lambda t: t.count(), total_distance=lambda t: t.distance.sum()
    )

    # Post-aggregation: use t.all() with ibis columns
    result = (
        flights_st.group_by("carrier")
        .aggregate("flight_count", "total_distance")
        .mutate(
            # t.flight_count is now an ibis column (post-aggregation)
            pct=lambda t: t.flight_count / t.all(t.flight_count)
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
        flight_count=lambda t: t.count()
    )

    # Post-aggregation: use t.all() with string name
    result = (
        flights_st.group_by("carrier")
        .aggregate("flight_count")
        .mutate(
            # Pass string name to t.all() in post-aggregation context
            pct=lambda t: t.flight_count / t.all("flight_count")
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
            flight_count=lambda t: t.count(), total_distance=lambda t: t.distance.sum()
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
    assert all(pytest.approx(p1) == p2 for p1, p2 in zip(df.pct1, df.pct2))


def test_prefixed_measures_with_string():
    """Test string-based reference with prefixed measures after join."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    carriers = pd.DataFrame({"code": ["AA", "UA"], "name": ["American", "United"]})
    f_tbl = con.create_table("flights", flights)
    c_tbl = con.create_table("carriers", carriers)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count()
    )
    carriers_st = to_semantic_table(c_tbl, "carriers").with_dimensions(
        code=lambda t: t.code, name=lambda t: t.name
    )

    joined = (
        flights_st.join(carriers_st, on=lambda f, c: f.carrier == c.code)
        .with_dimensions(name=lambda t: t.name)
        .with_measures(
            # Reference prefixed measure with string
            pct_full=lambda t: t.flights__flight_count / t.all("flights__flight_count"),
            # Reference with short name (should resolve to flights__flight_count)
            pct_short=lambda t: t.flight_count / t.all("flight_count"),
        )
    )

    df = joined.group_by("name").aggregate("pct_full", "pct_short").execute()
    # Both should give same results
    assert all(pytest.approx(p1) == p2 for p1, p2 in zip(df.pct_full, df.pct_short))


def test_inline_measure_with_different_reference_styles():
    """Test inline measures in aggregate() using different reference styles."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count()
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
    assert all(
        pytest.approx(p1) == p2 for p1, p2 in zip(df.pct_attr, df.pct_string)
    )
    assert all(
        pytest.approx(p1) == p2 for p1, p2 in zip(df.pct_attr, df.pct_bracket)
    )
