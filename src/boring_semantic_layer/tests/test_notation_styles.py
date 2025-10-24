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


class TestDictBasedMetadata:
    """Test the dict-based API for dimensions and measures with metadata."""

    def test_dimension_with_metadata(self, flights_data):
        """Test that dimensions accept dict with metadata."""
        tbl = flights_data["flights"]
        st = to_semantic_table(tbl, "flights").with_dimensions(
            carrier={"expr": lambda t: t.carrier, "description": "Carrier code"},
            distance={
                "expr": lambda t: t.distance,
                "description": "Flight distance in miles",
                "is_time_dimension": False,
            },
        )

        # Verify metadata is stored
        assert st._dims_dict()["carrier"].description == "Carrier code"
        assert st._dims_dict()["distance"].description == "Flight distance in miles"
        assert st._dims_dict()["distance"].is_time_dimension is False

        # Verify it still works in queries
        result = (
            st.group_by("carrier").aggregate(flight_count=lambda t: t.count()).execute()
        )
        assert len(result) > 0

    def test_measure_with_metadata(self, flights_data):
        """Test that measures accept dict with metadata."""
        tbl = flights_data["flights"]
        st = to_semantic_table(tbl, "flights").with_measures(
            flight_count={
                "expr": lambda t: t.count(),
                "description": "Total number of flights",
            },
            total_distance={
                "expr": lambda t: t.distance.sum(),
                "description": "Sum of all flight distances",
            },
        )

        # Verify metadata is stored
        assert (
            st._measures_dict()["flight_count"].description == "Total number of flights"
        )
        assert (
            st._measures_dict()["total_distance"].description
            == "Sum of all flight distances"
        )

        # Verify it still works in queries
        result = (
            st.with_dimensions(carrier=lambda t: t.carrier)
            .group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .execute()
        )
        assert len(result) > 0

    def test_mixed_callable_and_dict(self, flights_data):
        """Test mixing simple callables and dicts with metadata."""
        tbl = flights_data["flights"]
        st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(
                carrier=lambda t: t.carrier,  # Simple callable
                distance={  # Dict with metadata
                    "expr": lambda t: t.distance,
                    "description": "Distance traveled",
                },
            )
            .with_measures(
                flight_count=lambda t: t.count(),  # Simple callable
                avg_distance={  # Dict with metadata
                    "expr": lambda t: t.distance.mean(),
                    "description": "Average flight distance",
                },
            )
        )

        # Simple callable should have None description
        assert st._dims_dict()["carrier"].description is None
        # Dict should have description
        assert st._dims_dict()["distance"].description == "Distance traveled"

        # Same for measures
        assert st._measures_dict()["flight_count"].description is None
        assert (
            st._measures_dict()["avg_distance"].description == "Average flight distance"
        )

        # Both should work in queries
        result = (
            st.group_by("carrier").aggregate("flight_count", "avg_distance").execute()
        )
        assert len(result) > 0

    def test_descriptions_preserved_through_filter(self, flights_data):
        """Test that descriptions are preserved through filter operations."""
        tbl = flights_data["flights"]

        # Create semantic table with descriptions
        flights_st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(
                carrier={
                    "expr": lambda t: t.carrier,
                    "description": "Airline carrier code",
                }
            )
            .with_measures(
                flight_count={
                    "expr": lambda t: t.count(),
                    "description": "Total number of flights",
                },
                total_distance={
                    "expr": lambda t: t.distance.sum(),
                    "description": "Sum of all flight distances",
                },
            )
        )

        # Filter and verify descriptions are preserved (access via source)
        filtered = flights_st.filter(lambda t: t.carrier == "AA")
        # SemanticFilter delegates to its source for dimensions and measures
        assert (
            filtered.source._dims_dict()["carrier"].description
            == "Airline carrier code"
        )
        assert (
            filtered.source._measures_dict()["flight_count"].description
            == "Total number of flights"
        )

    def test_descriptions_preserved_through_aggregate(self, flights_data):
        """Test that dimensions are preserved in source before aggregation."""
        tbl = flights_data["flights"]

        flights_st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(
                carrier={
                    "expr": lambda t: t.carrier,
                    "description": "Airline carrier code",
                }
            )
            .with_measures(
                flight_count={
                    "expr": lambda t: t.count(),
                    "description": "Total number of flights",
                },
                total_distance={
                    "expr": lambda t: t.distance.sum(),
                    "description": "Sum of all flight distances",
                },
            )
        )

        # After aggregation, dimensions should be preserved in source
        aggregated = flights_st.group_by("carrier").aggregate(
            "flight_count", "total_distance"
        )
        # SemanticAggregate -> SemanticGroupBy -> SemanticTable chain
        # Access the root table through the chain
        assert (
            aggregated.source.source._dims_dict()["carrier"].description
            == "Airline carrier code"
        )

    def test_descriptions_preserved_through_join(self, flights_data):
        """Test that descriptions are preserved through join operations."""
        tbl = flights_data["flights"]
        carriers_tbl = flights_data["carriers"]

        flights_st = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(
                carrier={
                    "expr": lambda t: t.carrier,
                    "description": "Airline carrier code",
                }
            )
            .with_measures(
                flight_count={
                    "expr": lambda t: t.count(),
                    "description": "Total number of flights",
                }
            )
        )

        carriers_st = to_semantic_table(carriers_tbl, "carriers").with_dimensions(
            code={"expr": lambda t: t.code, "description": "Carrier code for joining"}
        )

        # Join and verify descriptions are preserved with prefixes
        joined = flights_st.join_one(carriers_st, "carrier", "code")
        assert (
            joined._dims_dict()["flights.carrier"].description
            == "Airline carrier code"
        )
        assert (
            joined._dims_dict()["carriers.code"].description
            == "Carrier code for joining"
        )
        assert (
            joined._measures_dict()["flights.flight_count"].description
            == "Total number of flights"
        )

    def test_time_dimension_metadata(self, flights_data):
        """Test that time dimension metadata is preserved."""
        tbl = flights_data["flights"]

        st = to_semantic_table(tbl, "flights").with_dimensions(
            carrier={
                "expr": lambda t: t.carrier,
                "description": "Carrier code",
                "is_time_dimension": False,
            },
            distance={
                "expr": lambda t: t.distance,
                "description": "Flight distance",
                "is_time_dimension": False,
                "smallest_time_grain": None,
            },
        )

        # Verify time dimension metadata
        assert st._dims_dict()["carrier"].is_time_dimension is False
        assert st._dims_dict()["carrier"].smallest_time_grain is None
        assert st._dims_dict()["distance"].is_time_dimension is False
        assert st._dims_dict()["distance"].smallest_time_grain is None

        # After filter, metadata should be preserved (access via source)
        filtered = st.filter(lambda t: t.carrier == "AA")
        assert filtered.source._dims_dict()["carrier"].is_time_dimension is False
        assert filtered.source._dims_dict()["distance"].is_time_dimension is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
