"""Comprehensive tests for all chart backends and chart types."""

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


class TestAltairChartTypes:
    """Test all chart types with Altair backend."""

    def test_single_value_indicator(self, flights_model):
        """Test single value display (no dimensions)."""
        result = flights_model.aggregate("flight_count")
        chart = result.chart(backend="altair")

        import altair as alt

        assert isinstance(chart, alt.Chart)
        spec = chart.to_dict()
        assert spec["mark"]["type"] == "text"

    def test_bar_chart_categorical(self, flights_model):
        """Test bar chart for categorical dimension."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="altair")

        import altair as alt

        assert isinstance(chart, alt.Chart)
        # Just verify it's an Altair chart, don't check internals

    def test_line_chart_time_series(self, flights_model):
        """Test line chart for time series."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart = result.chart(backend="altair")

        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_grouped_bar_multiple_measures(self, flights_model):
        """Test grouped bar chart with multiple measures."""
        result = flights_model.group_by("carrier").aggregate("flight_count", "avg_distance")
        chart = result.chart(backend="altair")

        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_multi_line_time_plus_category(self, flights_model):
        """Test multi-line chart with time and category dimensions."""
        result = flights_model.group_by("flight_date", "carrier").aggregate("flight_count")
        chart = result.chart(backend="altair")

        import altair as alt

        assert isinstance(chart, alt.Chart)

    def test_heatmap_two_dimensions(self, flights_model):
        """Test heatmap with two categorical dimensions."""
        result = flights_model.group_by("origin", "destination").aggregate("flight_count")
        chart = result.chart(backend="altair")

        import altair as alt

        assert isinstance(chart, alt.Chart)


class TestPlotlyChartTypes:
    """Test all chart types with Plotly backend."""

    def test_single_value_indicator(self, flights_model):
        """Test indicator for single value."""
        result = flights_model.aggregate("flight_count")
        chart = result.chart(backend="plotly")

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        assert len(chart.data) > 0
        assert chart.data[0].type == "indicator"

    def test_bar_chart_categorical(self, flights_model):
        """Test bar chart for categorical dimension."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        assert chart.data[0].type == "bar"

    def test_line_chart_time_series(self, flights_model):
        """Test line chart for time series."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        assert chart.data[0].type in ("scatter", "scattergl")
        assert chart.data[0].mode == "lines"

    def test_grouped_bar_multiple_measures(self, flights_model):
        """Test grouped bar with multiple measures."""
        result = flights_model.group_by("carrier").aggregate("flight_count", "avg_delay")
        chart = result.chart(backend="plotly")

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        # Should have multiple traces (one per measure)
        assert len(chart.data) >= 2

    def test_multi_line_time_plus_category(self, flights_model):
        """Test multi-line chart with time and category."""
        result = flights_model.group_by("flight_date", "carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        # Should have multiple traces (one per carrier)
        assert len(chart.data) >= 2

    def test_heatmap_two_dimensions(self, flights_model):
        """Test heatmap with two dimensions."""
        result = flights_model.group_by("origin", "carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly")

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        assert chart.data[0].type == "heatmap"

    def test_scatter_chart_override(self, flights_model):
        """Test scatter chart with manual type override."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="plotly", spec={"chart_type": "scatter"})

        import plotly.graph_objects as go

        assert isinstance(chart, go.Figure)
        assert chart.data[0].type in ("scatter", "scattergl")


class TestPlotextChartTypes:
    """Test all chart types with Plotext backend."""

    def test_single_value_simple(self, flights_model):
        """Test simple value display."""
        result = flights_model.aggregate("flight_count")
        # Plotext prints to terminal, just verify it doesn't crash
        chart = result.chart(backend="plotext", format="string")
        assert chart is not None

    def test_bar_chart_categorical(self, flights_model):
        """Test bar chart for categorical dimension."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart_str = result.chart(backend="plotext", format="string")
        assert chart_str is not None
        assert isinstance(chart_str, str)

    def test_line_chart_time_series(self, flights_model):
        """Test line chart for time series."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart_str = result.chart(backend="plotext", format="string")
        assert chart_str is not None
        assert isinstance(chart_str, str)

    def test_grouped_bar_multiple_measures(self, flights_model):
        """Test grouped bar with multiple measures."""
        result = flights_model.group_by("carrier").aggregate("flight_count", "avg_distance")
        chart_str = result.chart(backend="plotext", format="string")
        assert chart_str is not None
        assert isinstance(chart_str, str)

    def test_multi_line_time_plus_category(self, flights_model):
        """Test multi-line chart."""
        result = flights_model.group_by("flight_date", "carrier").aggregate("flight_count")
        chart_str = result.chart(backend="plotext", format="string")
        assert chart_str is not None
        assert isinstance(chart_str, str)

    def test_scatter_chart_two_dims(self, flights_model):
        """Test scatter chart with two non-time dimensions."""
        result = flights_model.group_by("origin", "destination").aggregate("flight_count")
        # This should create a scatter plot
        chart_str = result.chart(backend="plotext", format="string")
        assert chart_str is not None
        assert isinstance(chart_str, str)

    def test_complex_query_skips_charting(self, flights_model):
        """Test that complex queries skip charting (return None)."""
        result = flights_model.group_by("origin", "destination", "carrier").aggregate(
            "flight_count", "avg_distance"
        )
        chart_str = result.chart(backend="plotext", format="string")
        # Complex queries with 3+ dimensions now return None instead of table chart
        assert chart_str is None


class TestChartDetectionLogic:
    """Test chart type auto-detection across backends."""

    @pytest.mark.parametrize("backend", ["altair", "plotly", "plotext"])
    def test_single_dim_single_measure_categorical(self, flights_model, backend):
        """Test that categorical + single measure = bar chart."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend=backend, format="string" if backend == "plotext" else "static")
        assert chart is not None

    @pytest.mark.parametrize("backend", ["altair", "plotly", "plotext"])
    def test_time_dimension_detection(self, flights_model, backend):
        """Test that time dimension is properly detected."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart = result.chart(backend=backend, format="string" if backend == "plotext" else "static")
        assert chart is not None

    @pytest.mark.parametrize("backend", ["altair", "plotly", "plotext"])
    def test_no_dimensions_single_measure(self, flights_model, backend):
        """Test single value display."""
        result = flights_model.aggregate("flight_count")
        chart = result.chart(backend=backend, format="string" if backend == "plotext" else "static")
        assert chart is not None


class TestChartFormats:
    """Test different output formats."""

    def test_altair_json_format(self, flights_model):
        """Test Altair JSON export."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        json_spec = result.chart(backend="altair", format="json")
        assert isinstance(json_spec, dict)
        assert "$schema" in json_spec

    def test_plotly_json_format(self, flights_model):
        """Test Plotly JSON export."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        json_str = result.chart(backend="plotly", format="json")
        assert isinstance(json_str, str)
        import json

        parsed = json.loads(json_str)
        assert "data" in parsed

    def test_plotext_string_format(self, flights_model):
        """Test Plotext string output."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart_str = result.chart(backend="plotext", format="string")
        assert isinstance(chart_str, str)
        assert len(chart_str) > 0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_result(self, con):
        """Test chart with no data."""
        empty_df = pd.DataFrame({"carrier": [], "value": []})
        empty_tbl = con.create_table("empty", empty_df, overwrite=True)
        empty_model = (
            to_semantic_table(empty_tbl, name="empty")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_value=lambda t: t.value.sum())
        )

        result = empty_model.group_by("carrier").aggregate("total_value")
        # Should not crash even with empty data
        chart = result.chart(backend="altair")
        assert chart is not None

    def test_single_row_result(self, con):
        """Test chart with single row."""
        single_df = pd.DataFrame({"carrier": ["AA"], "distance": [1000]})
        single_tbl = con.create_table("single", single_df, overwrite=True)
        single_model = (
            to_semantic_table(single_tbl, name="single")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(avg_distance=lambda t: t.distance.mean())
        )

        result = single_model.group_by("carrier").aggregate("avg_distance")
        chart = result.chart(backend="plotly")
        assert chart is not None

    def test_many_categories(self, con):
        """Test chart with many categorical values."""
        many_df = pd.DataFrame(
            {"category": [f"cat_{i}" for i in range(100)], "value": list(range(100))}
        )
        many_tbl = con.create_table("many", many_df, overwrite=True)
        many_model = (
            to_semantic_table(many_tbl, name="many")
            .with_dimensions(category=lambda t: t.category)
            .with_measures(sum_value=lambda t: t.value.sum())
        )

        result = many_model.group_by("category").aggregate("sum_value")
        chart = result.chart(backend="plotext", format="string")
        assert chart is not None
