"""
Tests for both dot notation and bracket notation support in BSL v2.

This module verifies that both t.measure_name and t["measure_name"] work
consistently across all contexts:
- In .with_dimensions()
- In .with_measures() (pre-aggregation)
- In .mutate() (post-aggregation)
- With t.all() for percent calculations
"""

import pandas as pd
import ibis
import pytest

from boring_semantic_layer.api import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def flights_data(con):
    """Sample flights data for testing."""
    flights_df = pd.DataFrame({
        "carrier": ["AA", "AA", "UA", "DL", "DL", "DL"],
        "distance": [100, 200, 150, 300, 250, 400],
    })
    carriers_df = pd.DataFrame({
        "code": ["AA", "UA", "DL"],
        "nickname": ["American", "United", "Delta"],
    })
    return {
        "flights": con.create_table("flights", flights_df),
        "carriers": con.create_table("carriers", carriers_df),
    }


class TestDimensionNotation:
    """Test both notations for dimension references."""

    def test_dot_notation_in_dimensions(self, flights_data):
        """Test t.column in with_dimensions."""
        tbl = flights_data["flights"]
        st = to_semantic_table(tbl, "flights").with_dimensions(
            carrier=lambda t: t.carrier,  # dot notation
            distance=lambda t: t.distance,
        )

        result = st._dims
        assert "carrier" in result
        assert "distance" in result

    def test_bracket_notation_in_dimensions(self, flights_data):
        """Test t['column'] in with_dimensions."""
        tbl = flights_data["flights"]
        st = to_semantic_table(tbl, "flights").with_dimensions(
            carrier=lambda t: t["carrier"],  # bracket notation
            distance=lambda t: t["distance"],
        )

        result = st._dims
        assert "carrier" in result
        assert "distance" in result

    def test_mixed_notation_in_dimensions(self, flights_data):
        """Test mixing both notations in with_dimensions."""
        tbl = flights_data["flights"]
        st = to_semantic_table(tbl, "flights").with_dimensions(
            carrier=lambda t: t.carrier,      # dot
            distance=lambda t: t["distance"], # bracket
        )

        result = st._dims
        assert "carrier" in result
        assert "distance" in result


class TestMeasureNotationPreAggregation:
    """Test both notations for measure references before aggregation."""

    def test_dot_notation_in_with_measures(self, flights_data):
        """Test t.measure in with_measures."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
            .with_measures(
                # Reference existing measure with dot notation
                pct=lambda t: t.flight_count / t.all(t.flight_count)
            )
        )

        assert "flight_count" in st._base_measures
        assert "pct" in st._calc_measures

    def test_bracket_notation_in_with_measures(self, flights_data):
        """Test t['measure'] in with_measures."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t["distance"].sum(),  # bracket for column
            )
            .with_measures(
                # Reference existing measure with bracket notation
                pct=lambda t: t["flight_count"] / t.all(t["flight_count"])
            )
        )

        assert "flight_count" in st._base_measures
        assert "pct" in st._calc_measures

    def test_mixed_notation_in_with_measures(self, flights_data):
        """Test mixing both notations in with_measures."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
            .with_measures(
                # Mix dot and bracket notation in same expression
                mixed=lambda t: t.flight_count / t["total_distance"]
            )
        )

        assert "mixed" in st._calc_measures


class TestMeasureNotationPostAggregation:
    """Test both notations in post-aggregation context (mutate after aggregate)."""

    def test_dot_notation_in_post_agg_mutate(self, flights_data):
        """Test t.column in mutate after aggregate."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
        )

        result = (
            st
            .group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(
                avg_distance=lambda t: t.total_distance / t.flight_count  # dot notation
            )
            .execute()
        )

        assert "avg_distance" in result.columns
        assert len(result) == 3  # 3 carriers

    def test_bracket_notation_in_post_agg_mutate(self, flights_data):
        """Test t['column'] in mutate after aggregate."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
        )

        result = (
            st
            .group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(
                avg_distance=lambda t: t["total_distance"] / t["flight_count"]  # bracket
            )
            .execute()
        )

        assert "avg_distance" in result.columns
        assert len(result) == 3

    def test_t_all_with_bracket_notation_post_agg(self, flights_data):
        """Test t.all(t['column']) in post-aggregation context."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(flight_count=lambda t: t.count())
        )

        result = (
            st
            .group_by("carrier")
            .aggregate("flight_count")
            .mutate(
                pct=lambda t: t["flight_count"] / t.all(t["flight_count"])  # bracket
            )
            .execute()
        )

        assert "pct" in result.columns
        assert pytest.approx(result["pct"].sum()) == 1.0

    def test_t_all_with_dot_notation_post_agg(self, flights_data):
        """Test t.all(t.column) in post-aggregation context."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(flight_count=lambda t: t.count())
        )

        result = (
            st
            .group_by("carrier")
            .aggregate("flight_count")
            .mutate(
                pct=lambda t: t.flight_count / t.all(t.flight_count)  # dot
            )
            .execute()
        )

        assert "pct" in result.columns
        assert pytest.approx(result["pct"].sum()) == 1.0


class TestEndToEndNotationConsistency:
    """Test that both notations produce identical results in complete workflows."""

    def test_dot_vs_bracket_same_result_simple(self, flights_data):
        """Test that dot and bracket notation produce the same results."""
        flights_tbl = flights_data["flights"]
        carriers_tbl = flights_data["carriers"]

        # Version 1: Using dot notation
        carriers_st_dot = to_semantic_table(carriers_tbl, "carriers").with_dimensions(
            code=lambda t: t.code,
            nickname=lambda t: t.nickname,
        )

        flights_st_dot = (
            to_semantic_table(flights_tbl, "flights")
            .with_measures(flight_count=lambda t: t.count())
            .join(carriers_st_dot, on=lambda f, c: f.carrier == c.code)
            .with_dimensions(nickname=lambda t: t.nickname)
            .with_measures(pct=lambda t: t.flight_count / t.all(t.flight_count))
        )

        result_dot = (
            flights_st_dot
            .group_by("nickname")
            .aggregate("pct")
            .order_by("nickname")
            .execute()
        )

        # Version 2: Using bracket notation
        carriers_st_bracket = to_semantic_table(carriers_tbl, "carriers").with_dimensions(
            code=lambda t: t["code"],
            nickname=lambda t: t["nickname"],
        )

        flights_st_bracket = (
            to_semantic_table(flights_tbl, "flights")
            .with_measures(flight_count=lambda t: t.count())
            .join(carriers_st_bracket, on=lambda f, c: f["carrier"] == c["code"])
            .with_dimensions(nickname=lambda t: t["nickname"])
            .with_measures(pct=lambda t: t["flight_count"] / t.all(t["flight_count"]))
        )

        result_bracket = (
            flights_st_bracket
            .group_by("nickname")
            .aggregate("pct")
            .order_by("nickname")
            .execute()
        )

        # Results should be identical
        pd.testing.assert_frame_equal(result_dot, result_bracket)

    def test_dot_vs_bracket_same_result_post_agg(self, flights_data):
        """Test that both notations work the same in post-aggregation mutate."""
        tbl = flights_data["flights"]

        # Dot notation version
        result_dot = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
            .group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(
                ratio=lambda t: t.total_distance / t.flight_count,
                pct=lambda t: t.flight_count / t.all(t.flight_count),
            )
            .order_by("carrier")
            .execute()
        )

        # Bracket notation version
        result_bracket = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t["carrier"])
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t["distance"].sum(),
            )
            .group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(
                ratio=lambda t: t["total_distance"] / t["flight_count"],
                pct=lambda t: t["flight_count"] / t.all(t["flight_count"]),
            )
            .order_by("carrier")
            .execute()
        )

        # Results should be identical
        pd.testing.assert_frame_equal(result_dot, result_bracket)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
