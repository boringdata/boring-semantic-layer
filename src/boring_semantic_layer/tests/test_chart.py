"""Tests for chart functionality with SemanticAggregate."""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def flights_model(con):
    """Create a sample flights semantic table for testing."""
    flights_df = pd.DataFrame(
        {
            "origin": ["JFK", "LAX", "ORD", "JFK", "LAX", "ORD"] * 5,
            "destination": ["LAX", "JFK", "DEN", "ORD", "DEN", "LAX"] * 5,
            "carrier": ["AA", "UA", "DL"] * 10,
            "flight_date": pd.date_range("2024-01-01", periods=30, freq="D"),
            "distance": [2475, 2475, 920, 740, 862, 987] * 5,
            "dep_delay": [5.2, 8.1, 3.5, 2.0, 6.3, 1.8] * 5,
        },
    )

    flights_tbl = con.create_table("flights", flights_df, overwrite=True)

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
            flight_date={
                "expr": lambda t: t.flight_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_distance=lambda t: t.distance.mean(),
            avg_delay=lambda t: t.dep_delay.mean(),
        )
    )
    return flights


class TestAltairChart:
    """Test Altair chart generation."""

    def test_basic_bar_chart(self, flights_model):
        """Test basic bar chart with single dimension and measure."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_chart_with_multiple_measures(self, flights_model):
        """Test chart with multiple measures."""
        result = flights_model.group_by("carrier").aggregate(
            "flight_count",
            "avg_distance",
        )
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_time_series_chart(self, flights_model):
        """Test time series line chart."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_manual_chart_type(self, flights_model):
        """Test manual chart type override."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="altair", spec={"mark": "line"})

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_heatmap_chart(self, flights_model):
        """Test heatmap with two dimensions."""
        result = flights_model.group_by("origin", "destination").aggregate(
            "flight_count",
        )
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)


class TestPlotlyChart:
    """Test Plotly chart generation."""

    def test_basic_bar_chart(self, flights_model):
        """Test basic Plotly bar chart."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)

    def test_line_chart(self, flights_model):
        """Test Plotly line chart with time series."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)

    def test_heatmap_chart(self, flights_model):
        """Test Plotly heatmap."""
        result = flights_model.group_by("origin", "carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)

    def test_multiple_measures(self, flights_model):
        """Test Plotly chart with multiple measures."""
        result = flights_model.group_by("carrier").aggregate(
            "flight_count",
            "avg_delay",
        )
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)

    def test_manual_chart_type(self, flights_model):
        """Test manual chart type override with Plotly."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly", spec={"chart_type": "scatter"})

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)


class TestChartErrors:
    """Test error handling in chart generation."""

    def test_invalid_backend(self, flights_model):
        """Test error on invalid backend."""
        result = flights_model.group_by("carrier").aggregate("flight_count")

        with pytest.raises(ValueError, match="Unsupported backend"):
            result.chart(backend="invalid")

    def test_chart_method_exists(self, flights_model):
        """Test that chart method exists on SemanticAggregate."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        assert hasattr(result, "chart")
        assert callable(result.chart)


class TestChartFieldNameSanitization:
    """Test that field names with dots are sanitized for Vega-Lite compatibility."""

    def test_dotted_field_names_sanitized(self, con):
        """Test that field names with dots are converted to underscores."""
        # Create test data with carriers table
        carriers_df = pd.DataFrame(
            {
                "code": ["AA", "UA", "DL"],
                "name": ["American Airlines", "United Airlines", "Delta Air Lines"],
            }
        )
        carriers_tbl = con.create_table("carriers", carriers_df, overwrite=True)

        flights_df = pd.DataFrame({"carrier": ["AA", "UA", "DL"], "distance": [2475, 337, 382]})
        flights_tbl = con.create_table("flights_for_join", flights_df, overwrite=True)

        # Create semantic tables
        carriers_sm = (
            to_semantic_table(carriers_tbl, name="carriers")
            .with_dimensions(code=lambda t: t.code, name=lambda t: t.name)
            .with_measures(carrier_count=lambda t: t.count())
        )

        flights_sm = (
            to_semantic_table(flights_tbl, name="flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(
                flight_count=lambda t: t.count(), total_distance=lambda t: t.distance.sum()
            )
        )

        # Join tables - this creates a "carriers.name" field
        joined = flights_sm.join_many(carriers_sm, lambda f, c: f.carrier == c.code)

        # Query with the joined field
        result = joined.group_by("carriers.name").aggregate("flight_count", "total_distance")

        # Create chart - this should sanitize the dotted field name
        chart = result.chart(backend="altair")

        # Verify the chart was created
        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

        # Verify the spec uses sanitized field names
        spec = chart.to_dict()

        # The X field should be "carriers_name" not "carriers.name"
        assert spec["encoding"]["x"]["field"] == "carriers_name"

        # The datasets should have "carriers_name" as a key
        dataset_key = list(spec["datasets"].keys())[0]
        first_row = spec["datasets"][dataset_key][0]
        assert "carriers_name" in first_row
        assert "carriers.name" not in first_row


class TestChartFormats:
    """Test chart output formats (PNG, SVG, JSON)."""

    def test_chart_png_format_altair(self, flights_model):
        """Test PNG export with Altair backend."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        png_bytes = result.chart(backend="altair", format="png")

        # Verify we got bytes back
        assert isinstance(png_bytes, bytes)
        # PNG files start with this signature
        assert png_bytes[:8] == b"\x89PNG\r\n\x1a\n"

    def test_chart_svg_format_altair(self, flights_model):
        """Test SVG export with Altair backend."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        svg_bytes = result.chart(backend="altair", format="svg")

        # Verify we got bytes back
        assert isinstance(svg_bytes, bytes)
        # SVG files are XML, should contain <svg
        assert b"<svg" in svg_bytes

    def test_chart_json_format_altair(self, flights_model):
        """Test JSON export with Altair backend."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        json_spec = result.chart(backend="altair", format="json")

        # Verify we got a dict back
        assert isinstance(json_spec, dict)
        # Should have Vega-Lite schema
        assert "$schema" in json_spec
        assert "vega-lite" in json_spec["$schema"]

    def test_chart_png_format_plotly(self, flights_model):
        """Test PNG export with Plotly backend."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        png_bytes = result.chart(backend="plotly", format="png")

        # Verify we got bytes back
        assert isinstance(png_bytes, bytes)
        # PNG files start with this signature
        assert png_bytes[:8] == b"\x89PNG\r\n\x1a\n"

    def test_chart_json_format_plotly(self, flights_model):
        """Test JSON export with Plotly backend."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        json_spec = result.chart(backend="plotly", format="json")

        # Verify we got a JSON string back
        assert isinstance(json_spec, str)
        # Should be valid JSON with Plotly structure
        import json

        parsed = json.loads(json_spec)
        assert "data" in parsed

    def test_chart_json_roundtrip_plotly(self, flights_model):
        """Test JSON export/import roundtrip with Plotly."""
        import plotly.io

        result = flights_model.group_by("carrier").aggregate("flight_count")
        json_spec = result.chart(backend="plotly", format="json")

        # Verify we can reconstruct the figure from JSON
        fig = plotly.io.from_json(json_spec)
        assert fig is not None
        assert hasattr(fig, "data")
        assert len(fig.data) > 0


class TestChartWithFilters:
    """Test chart generation with filtered data."""

    def test_chart_with_filter(self, flights_model):
        """Test chart generation on filtered data."""
        result = (
            flights_model.filter(lambda t: t.carrier == "AA")
            .group_by("origin")
            .aggregate("flight_count")
        )
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_chart_with_order_by(self, flights_model):
        """Test chart generation with ordered data."""
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .order_by(ibis.desc("flight_count"))
        )
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)

    def test_chart_with_limit(self, flights_model):
        """Test chart generation with limited results."""
        result = flights_model.group_by("carrier").aggregate("flight_count").limit(2)
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_chart_with_mutate(self, flights_model):
        """Test chart generation after mutate operation."""
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance_per_flight=lambda t: t.total_distance / t.flight_count)
        )
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)


class TestFilterOperators:
    """Test new filter operators: eq, equals, ilike, not ilike."""

    @pytest.fixture(scope="class")
    def operator_model(self, con):
        """Create a model for testing filter operators."""
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "charlie", "DAVID", "Eve"],
                "email": [
                    "alice@example.com",
                    "bob@test.org",
                    "charlie@example.com",
                    "david@TEST.ORG",
                    "eve@example.com",
                ],
                "value": [10, 20, 30, 40, 50],
            }
        )

        tbl = con.create_table("test_operators", df, overwrite=True)

        return (
            to_semantic_table(tbl, name="test_operators")
            .with_dimensions(
                name=lambda t: t.name,
                email=lambda t: t.email,
            )
            .with_measures(
                sum_value=lambda t: t.value.sum(),
                count=lambda t: t.count(),
            )
        )

    def test_eq_operator(self, operator_model):
        """Test 'eq' operator (should work same as '=')."""
        result = (
            operator_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "eq", "value": "Alice"}],
            )
            .execute()
            .reset_index(drop=True)
        )

        expected = pd.DataFrame({"name": ["Alice"], "sum_value": [10]})
        pd.testing.assert_frame_equal(result, expected)

    def test_equals_operator(self, operator_model):
        """Test 'equals' operator (should work same as '=')."""
        result = (
            operator_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "equals", "value": "Bob"}],
            )
            .execute()
            .reset_index(drop=True)
        )

        expected = pd.DataFrame({"name": ["Bob"], "sum_value": [20]})
        pd.testing.assert_frame_equal(result, expected)

    def test_ilike_operator(self, operator_model):
        """Test 'ilike' operator (case-insensitive LIKE)."""
        # Should match alice, Alice (case insensitive 'a')
        result = (
            operator_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "ilike", "value": "%a%"}],
            )
            .execute()
            .reset_index(drop=True)
            .sort_values("name")
            .reset_index(drop=True)
        )

        expected = pd.DataFrame({"name": ["Alice", "DAVID", "charlie"], "sum_value": [10, 40, 30]})
        pd.testing.assert_frame_equal(result, expected)

    def test_not_ilike_operator(self, operator_model):
        """Test 'not ilike' operator."""
        # Should exclude names containing 'e' (case insensitive): Alice, charlie, Eve
        result = (
            operator_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "not ilike", "value": "%e%"}],
            )
            .execute()
            .reset_index(drop=True)
            .sort_values("name")
            .reset_index(drop=True)
        )

        expected = pd.DataFrame({"name": ["Bob", "DAVID"], "sum_value": [20, 40]})
        pd.testing.assert_frame_equal(result, expected)

    def test_operators_in_compound_filters(self, operator_model):
        """Test new operators work in compound AND/OR filters."""
        # Test with AND: email contains example AND name equals Alice
        result = (
            operator_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[
                    {"field": "email", "operator": "ilike", "value": "%example%"},
                    {"field": "name", "operator": "equals", "value": "Alice"},
                ],
            )
            .execute()
            .reset_index(drop=True)
        )

        expected = pd.DataFrame({"name": ["Alice"], "sum_value": [10]})
        pd.testing.assert_frame_equal(result, expected)

    @pytest.mark.parametrize(
        "operator,expected_count",
        [
            ("=", 1),
            ("eq", 1),
            ("equals", 1),
        ],
    )
    def test_equality_operators_equivalence(self, operator_model, operator, expected_count):
        """Test that =, eq, and equals all produce the same results."""
        result = operator_model.query(
            dimensions=["name"],
            measures=["count"],
            filters=[{"field": "name", "operator": operator, "value": "Bob"}],
        ).execute()

        assert len(result) == expected_count
        assert result["count"].sum() == 1
