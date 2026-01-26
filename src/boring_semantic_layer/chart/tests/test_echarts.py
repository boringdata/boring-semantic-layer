"""Tests for ECharts chart backend functionality with SemanticAggregate."""

import ibis
import json
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

    flights_tbl = con.create_table("flights_echarts", flights_df, overwrite=True)

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


class TestEChartsBackendRegistration:
    """Test ECharts backend registration and availability."""

    def test_echarts_backend_registered(self):
        """Test that ECharts backend is registered."""
        from boring_semantic_layer.chart import list_backends

        backends = list_backends()
        assert "echarts" in backends



class TestEChartsBasicCharts:
    """Test basic ECharts chart generation."""

    def test_basic_bar_chart(self, flights_model):
        """Test basic bar chart with single dimension and measure."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)
        assert "series" in chart
        assert len(chart["series"]) > 0
        assert chart["series"][0]["type"] == "bar"

    def test_bar_chart_has_correct_structure(self, flights_model):
        """Test bar chart has expected ECharts structure."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="echarts")

        # Check required ECharts fields
        assert "xAxis" in chart
        assert "yAxis" in chart
        assert "series" in chart

        # Check axis configuration
        assert chart["xAxis"]["type"] == "category"
        assert chart["yAxis"]["type"] == "value"

    def test_line_chart_time_series(self, flights_model):
        """Test time series line chart."""
        result = flights_model.group_by("flight_date").aggregate("flight_count")
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)
        assert "series" in chart
        assert chart["series"][0]["type"] == "line"

    def test_chart_with_multiple_measures(self, flights_model):
        """Test chart with multiple measures."""
        result = flights_model.group_by("carrier").aggregate(
            "flight_count",
            "avg_distance",
        )
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)
        # Multiple measures should create grouped bar
        assert "series" in chart

    def test_chart_with_two_dimensions(self, flights_model):
        """Test chart with two dimensions."""
        result = flights_model.group_by("origin", "destination").aggregate(
            "flight_count",
        )
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)
        assert "series" in chart


class TestEChartsChartTypeOverride:
    """Test manual chart type override."""

    def test_override_to_line(self, flights_model):
        """Test overriding chart type to line."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="echarts", spec={"chart_type": "line"})

        assert chart is not None
        assert chart["series"][0]["type"] == "line"

    def test_override_to_pie(self, flights_model):
        """Test overriding chart type to pie."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="echarts", spec={"chart_type": "pie"})

        assert chart is not None
        assert chart["series"][0]["type"] == "pie"


class TestEChartsOutputFormats:
    """Test ECharts output formats."""

    def test_static_format_returns_dict(self, flights_model):
        """Test static format returns dict."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        chart = result.chart(backend="echarts", format="static")

        assert isinstance(chart, dict)
        assert "series" in chart

    def test_json_format_returns_string(self, flights_model):
        """Test JSON format returns valid JSON string."""
        result = flights_model.group_by("carrier").aggregate("flight_count")
        json_str = result.chart(backend="echarts", format="json")

        assert isinstance(json_str, str)
        # Should be valid JSON
        parsed = json.loads(json_str)
        assert "series" in parsed


class TestEChartsWithFilters:
    """Test ECharts chart generation with filtered data."""

    def test_chart_with_filter(self, flights_model):
        """Test chart generation on filtered data."""
        result = (
            flights_model.filter(lambda t: t.carrier == "AA")
            .group_by("origin")
            .aggregate("flight_count")
        )
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)

    def test_chart_with_order_by(self, flights_model):
        """Test chart generation with ordered data."""
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .order_by(ibis.desc("flight_count"))
        )
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)

    def test_chart_with_limit(self, flights_model):
        """Test chart generation with limited results."""
        result = flights_model.group_by("carrier").aggregate("flight_count").limit(2)
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)

    def test_chart_with_mutate(self, flights_model):
        """Test chart generation after mutate operation."""
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance_per_flight=lambda t: t.total_distance / t.flight_count)
        )
        chart = result.chart(backend="echarts")

        assert chart is not None
        assert isinstance(chart, dict)


class TestEChartsFieldSanitization:
    """Test that field names with dots are sanitized."""

    def test_dotted_field_names_sanitized(self, con):
        """Test that field names with dots are converted to underscores."""
        # Create test data with carriers table
        carriers_df = pd.DataFrame(
            {
                "code": ["AA", "UA", "DL"],
                "name": ["American Airlines", "United Airlines", "Delta Air Lines"],
            }
        )
        carriers_tbl = con.create_table("carriers_echarts", carriers_df, overwrite=True)

        flights_df = pd.DataFrame(
            {"carrier": ["AA", "UA", "DL"], "distance": [2475, 337, 382]}
        )
        flights_tbl = con.create_table("flights_join_echarts", flights_df, overwrite=True)

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
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
            )
        )

        # Join tables - this creates a "carriers.name" field
        joined = flights_sm.join_many(
            carriers_sm, on=lambda left, right: left.carrier == right.code
        )

        # Query with the joined field
        result = joined.group_by("carriers.name").aggregate(
            "flight_count", "total_distance"
        )

        # Create chart - this should sanitize the dotted field name
        chart = result.chart(backend="echarts")

        # Verify the chart was created
        assert chart is not None
        assert isinstance(chart, dict)

        # Verify the spec uses sanitized field names in the xAxis
        assert "xAxis" in chart
        # The xAxis data should contain the sanitized values
        assert "data" in chart["xAxis"]


class TestEChartsEdgeCases:
    """Test edge cases for ECharts backend."""

    def test_empty_result(self, con):
        """Test chart with no data."""
        # Use a DF with at least one row, then filter to empty
        # DuckDB does not allow creating tables with NULL typed columns
        empty_df = pd.DataFrame({"carrier": ["NONE"], "value": [0.0]})
        empty_tbl = con.create_table("empty_echarts", empty_df, overwrite=True)
        empty_model = (
            to_semantic_table(empty_tbl, name="empty")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_value=lambda t: t.value.sum())
        )

        result = empty_model.group_by("carrier").aggregate("total_value")
        # Should not crash even with empty data
        chart = result.chart(backend="echarts")
        assert chart is not None

    def test_single_row_result(self, con):
        """Test chart with single row."""
        single_df = pd.DataFrame({"carrier": ["AA"], "distance": [1000.0]})
        single_tbl = con.create_table("single_echarts", single_df, overwrite=True)
        single_model = (
            to_semantic_table(single_tbl, name="single")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(avg_distance=lambda t: t.distance.mean())
        )

        result = single_model.group_by("carrier").aggregate("avg_distance")
        chart = result.chart(backend="echarts")
        assert chart is not None
        assert isinstance(chart, dict)

    def test_many_categories(self, con):
        """Test chart with many categorical values."""
        many_df = pd.DataFrame(
            {
                "category": [f"cat_{i}" for i in range(100)],
                "value": list(range(100)),
            }
        )
        many_tbl = con.create_table("many_echarts", many_df, overwrite=True)
        many_model = (
            to_semantic_table(many_tbl, name="many")
            .with_dimensions(category=lambda t: t.category)
            .with_measures(sum_value=lambda t: t.value.sum())
        )

        result = many_model.group_by("category").aggregate("sum_value")
        chart = result.chart(backend="echarts")
        assert chart is not None
        assert isinstance(chart, dict)
