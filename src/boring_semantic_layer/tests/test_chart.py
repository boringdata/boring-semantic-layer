"""Tests for chart functionality with SemanticAggregate."""

import pytest
import pandas as pd
import ibis
from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def flights_model(con):
    """Create a sample flights semantic table for testing."""
    flights_df = pd.DataFrame({
        "origin": ["JFK", "LAX", "ORD", "JFK", "LAX", "ORD"] * 5,
        "destination": ["LAX", "JFK", "DEN", "ORD", "DEN", "LAX"] * 5,
        "carrier": ["AA", "UA", "DL"] * 10,
        "flight_date": pd.date_range("2024-01-01", periods=30, freq="D"),
        "distance": [2475, 2475, 920, 740, 862, 987] * 5,
        "dep_delay": [5.2, 8.1, 3.5, 2.0, 6.3, 1.8] * 5,
    })

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
        result = flights_model.group_by("carrier").aggregate("flight_count", "avg_distance")
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
        chart = result.chart(backend="altair", chart_type="line")

        assert chart is not None
        import altair as alt
        assert isinstance(chart, alt.Chart)

    def test_heatmap_chart(self, flights_model):
        """Test heatmap with two dimensions."""
        result = flights_model.group_by("origin", "destination").aggregate("flight_count")
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
        result = flights_model.group_by("carrier").aggregate("flight_count", "avg_delay")
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go
        assert isinstance(chart, go.Figure)

    def test_manual_chart_type(self, flights_model):
        """Test manual chart type override with Plotly."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly", chart_type="scatter")

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


class TestChartWithFilters:
    """Test chart generation with filtered data."""

    def test_chart_with_filter(self, flights_model):
        """Test chart generation on filtered data."""
        result = (
            flights_model
            .filter(lambda t: t.carrier == "AA")
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
            flights_model
            .group_by("carrier")
            .aggregate("flight_count")
            .order_by(ibis.desc("flight_count"))
        )
        chart = result.chart(backend="plotly")

        assert chart is not None
        import plotly.graph_objects as go
        assert isinstance(chart, go.Figure)

    def test_chart_with_limit(self, flights_model):
        """Test chart generation with limited results."""
        result = (
            flights_model
            .group_by("carrier")
            .aggregate("flight_count")
            .limit(2)
        )
        chart = result.chart(backend="altair")

        assert chart is not None
        import altair as alt
        assert isinstance(chart, alt.Chart)
