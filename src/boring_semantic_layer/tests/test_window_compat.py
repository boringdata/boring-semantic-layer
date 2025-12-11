"""Tests for window_compat module - ibis/xorq compatibility layer."""

import ibis
import pytest

from boring_semantic_layer import to_semantic_table


class TestIbisCasesWithXorq:
    """Test that ibis.cases() works correctly with xorq expressions."""

    @pytest.fixture
    def flights_st(self):
        """Create a simple flights semantic table."""
        flights_tbl = ibis.memtable(
            {
                "carrier": ["WN", "AA", "WN", "UA", "WN", "DL", "AA", "WN"],
                "origin": ["DAL", "DFW", "HOU", "IAH", "DAL", "DFW", "DFW", "HOU"],
                "state": ["TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX"],
                "distance": [100, 200, 150, 300, 120, 250, 180, 140],
            }
        )
        return (
            to_semantic_table(flights_tbl, name="flights")
            .with_dimensions(
                carrier=lambda t: t.carrier,
                origin=lambda t: t.origin,
                state=lambda t: t.state,
            )
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
        )

    def test_ibis_cases_in_with_dimensions(self, flights_st):
        """Test ibis.cases() works in with_dimensions lambda."""
        result = (
            flights_st.with_dimensions(
                carrier_type=lambda t: ibis.cases(
                    (t.carrier == "WN", "Southwest"),
                    else_="Other",
                ),
            )
            .group_by("carrier_type")
            .aggregate("flight_count")
            .execute()
        )

        # Should have 2 groups: Southwest and Other
        assert len(result) == 2
        carrier_types = set(result["carrier_type"].tolist())
        assert carrier_types == {"Southwest", "Other"}

        # Southwest (WN) has 4 flights
        southwest_count = result[result["carrier_type"] == "Southwest"]["flight_count"].iloc[0]
        assert southwest_count == 4

        # Other carriers have 4 flights
        other_count = result[result["carrier_type"] == "Other"]["flight_count"].iloc[0]
        assert other_count == 4

    def test_ibis_cases_multiple_conditions(self, flights_st):
        """Test ibis.cases() with multiple conditions."""
        result = (
            flights_st.with_dimensions(
                distance_bucket=lambda t: ibis.cases(
                    (t.distance < 150, "Short"),
                    (t.distance < 250, "Medium"),
                    else_="Long",
                ),
            )
            .group_by("distance_bucket")
            .aggregate("flight_count")
            .execute()
        )

        # Should have 3 buckets
        assert len(result) == 3
        buckets = set(result["distance_bucket"].tolist())
        assert buckets == {"Short", "Medium", "Long"}

    def test_ibis_cases_no_else(self, flights_st):
        """Test ibis.cases() without else clause."""
        result = (
            flights_st.with_dimensions(
                is_southwest=lambda t: ibis.cases(
                    (t.carrier == "WN", "Yes"),
                ),
            )
            .group_by("is_southwest")
            .aggregate("flight_count")
            .execute()
        )

        # Should have Southwest flagged, others will be null
        assert "Yes" in result["is_southwest"].tolist()

    def test_ibis_cases_with_filter(self, flights_st):
        """Test ibis.cases() combined with filter."""
        result = (
            flights_st.filter(lambda t: t.state == "TX")
            .with_dimensions(
                carrier_group=lambda t: ibis.cases(
                    (t.carrier == "WN", "Southwest"),
                    (t.carrier == "AA", "American"),
                    else_="Other",
                ),
            )
            .group_by("carrier_group")
            .aggregate("flight_count")
            .execute()
        )

        # Should have Southwest, American, and Other
        groups = set(result["carrier_group"].tolist())
        assert "Southwest" in groups
        assert "American" in groups
