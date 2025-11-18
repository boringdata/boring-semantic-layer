"""Tests for chart utility functions."""

import pandas as pd
import pytest

from boring_semantic_layer.chart.utils import (
    clean_field_name,
    convert_datetime_to_strings,
    detect_chart_type_generic,
    detect_time_dimension_from_dtype,
    get_non_time_dimensions,
    has_time_dimension,
    melt_dataframe_for_multiple_measures,
    pivot_dataframe_for_heatmap,
    sanitize_field_name_for_vega,
    sanitize_spec_for_vega,
    sort_dataframe_for_line_chart,
)


class TestFieldNameUtils:
    """Test field name utilities."""

    def test_clean_field_name_with_prefix(self):
        """Test removing model prefix from field names."""
        assert clean_field_name("model.field") == "field"
        assert clean_field_name("flights.carrier") == "carrier"

    def test_clean_field_name_without_prefix(self):
        """Test field name without prefix."""
        assert clean_field_name("carrier") == "carrier"

    def test_clean_field_name_no_remove(self):
        """Test keeping prefix when remove_prefix=False."""
        assert clean_field_name("model.field", remove_prefix=False) == "model.field"

    def test_sanitize_field_name_for_vega(self):
        """Test Vega-Lite field name sanitization."""
        assert sanitize_field_name_for_vega("model.field") == "model_field"
        assert sanitize_field_name_for_vega("flights.carrier.name") == "flights_carrier_name"

    def test_sanitize_spec_for_vega_field(self):
        """Test spec sanitization for field references."""
        spec = {"encoding": {"x": {"field": "model.field"}}}
        result = sanitize_spec_for_vega(spec)
        assert result["encoding"]["x"]["field"] == "model_field"

    def test_sanitize_spec_for_vega_fold(self):
        """Test spec sanitization for fold transforms."""
        spec = {"transform": [{"fold": ["model.a", "model.b"]}]}
        result = sanitize_spec_for_vega(spec)
        assert result["transform"][0]["fold"] == ["model_a", "model_b"]


class TestDimensionUtils:
    """Test dimension utility functions."""

    def test_has_time_dimension_true(self):
        """Test time dimension detection when present."""
        assert has_time_dimension(["date", "carrier"], "date") is True

    def test_has_time_dimension_false_not_in_list(self):
        """Test time dimension not in dimensions list."""
        assert has_time_dimension(["carrier", "origin"], "date") is False

    def test_has_time_dimension_false_none(self):
        """Test time dimension is None."""
        assert has_time_dimension(["carrier"], None) is False

    def test_get_non_time_dimensions(self):
        """Test filtering out time dimension."""
        dims = ["date", "carrier", "origin"]
        result = get_non_time_dimensions(dims, "date")
        assert result == ["carrier", "origin"]

    def test_get_non_time_dimensions_no_time(self):
        """Test when no time dimension."""
        dims = ["carrier", "origin"]
        result = get_non_time_dimensions(dims, None)
        assert result == ["carrier", "origin"]

    def test_detect_time_dimension_from_dtype_datetime64(self):
        """Test detecting datetime64 column as time dimension."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "carrier": ["AA", "UA"],
                "count": [10, 20],
            }
        )
        result = detect_time_dimension_from_dtype(df, ["date", "carrier"])
        assert result == "date"

    def test_detect_time_dimension_from_dtype_timestamp(self):
        """Test detecting timestamp column as time dimension."""
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01 14:30:00", "2024-01-02 16:45:00"]),
                "value": [1, 2],
            }
        )
        result = detect_time_dimension_from_dtype(df, ["timestamp"])
        assert result == "timestamp"

    def test_detect_time_dimension_from_dtype_no_datetime(self):
        """Test when no datetime columns exist."""
        df = pd.DataFrame({"carrier": ["AA", "UA"], "count": [10, 20]})
        result = detect_time_dimension_from_dtype(df, ["carrier", "count"])
        assert result is None

    def test_detect_time_dimension_from_dtype_first_match(self):
        """Test that first datetime column is returned."""
        df = pd.DataFrame(
            {
                "date1": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "date2": pd.to_datetime(["2024-02-01", "2024-02-02"]),
                "value": [1, 2],
            }
        )
        result = detect_time_dimension_from_dtype(df, ["date1", "date2"])
        assert result == "date1"


class TestDataFrameUtils:
    """Test DataFrame manipulation utilities."""

    def test_sort_dataframe_for_line_chart_with_time(self):
        """Test sorting with time dimension."""
        df = pd.DataFrame(
            {
                "date": ["2024-03-01", "2024-01-01", "2024-02-01"],
                "carrier": ["AA", "AA", "AA"],
                "count": [3, 1, 2],
            }
        )
        result = sort_dataframe_for_line_chart(df, ["date", "carrier"], "date")
        assert result["count"].tolist() == [1, 2, 3]

    def test_sort_dataframe_for_line_chart_no_time(self):
        """Test sorting without time dimension."""
        df = pd.DataFrame({"carrier": ["UA", "AA", "DL"], "count": [2, 1, 3]})
        result = sort_dataframe_for_line_chart(df, ["carrier"], None)
        # Sorted by carrier: AA(1), DL(3), UA(2)
        assert result["count"].tolist() == [1, 3, 2]

    def test_convert_datetime_to_strings(self):
        """Test datetime column conversion."""
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-01", "2024-01-02"]), "value": [1, 2]})
        result = convert_datetime_to_strings(df)
        assert result["date"].dtype == object
        assert result["date"].tolist() == ["2024-01-01", "2024-01-02"]

    def test_convert_datetime_with_time(self):
        """Test datetime conversion preserves time."""
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01 14:30:00", "2024-01-02 16:45:00"]),
                "value": [1, 2],
            }
        )
        result = convert_datetime_to_strings(df)
        assert "14:30:00" in result["timestamp"].iloc[0]

    def test_melt_dataframe_for_multiple_measures(self):
        """Test melting DataFrame for multiple measures."""
        df = pd.DataFrame({"carrier": ["AA", "UA"], "count": [10, 20], "avg_delay": [5.2, 8.1]})
        result_df, measure_col, value_col = melt_dataframe_for_multiple_measures(
            df, ["count", "avg_delay"]
        )

        assert measure_col == "measure"
        assert value_col == "value"
        assert len(result_df) == 4
        assert set(result_df[measure_col]) == {"count", "avg_delay"}

    def test_pivot_dataframe_for_heatmap(self):
        """Test pivoting DataFrame for heatmap."""
        df = pd.DataFrame(
            {
                "origin": ["JFK", "JFK", "LAX", "LAX"],
                "carrier": ["AA", "UA", "AA", "UA"],
                "count": [10, 20, 30, 40],
            }
        )
        result = pivot_dataframe_for_heatmap(df, ["carrier", "origin"], "count")

        assert result.shape == (2, 2)  # 2 origins x 2 carriers
        assert result.loc["JFK", "AA"] == 10
        assert result.loc["LAX", "UA"] == 40

    def test_pivot_dataframe_insufficient_dimensions(self):
        """Test heatmap pivot with insufficient dimensions."""
        df = pd.DataFrame({"carrier": ["AA", "UA"], "count": [10, 20]})

        with pytest.raises(ValueError, match="at least 2 dimensions"):
            pivot_dataframe_for_heatmap(df, ["carrier"], "count")


class TestChartTypeDetection:
    """Test shared chart type detection logic."""

    def test_detect_single_value_indicator(self):
        """Test indicator for single value (no dimensions, 1 measure)."""
        result = detect_chart_type_generic([], ["count"], None)
        assert result == "indicator"

    def test_detect_bar_chart_categorical(self):
        """Test bar chart for categorical dimension."""
        result = detect_chart_type_generic(["carrier"], ["count"], None)
        assert result == "bar"

    def test_detect_line_chart_time_series(self):
        """Test line chart for time series."""
        result = detect_chart_type_generic(["date"], ["count"], "date")
        assert result == "line"

    def test_detect_line_chart_multiple_measures(self):
        """Test line chart with multiple measures and time."""
        result = detect_chart_type_generic(["date"], ["count", "avg_delay"], "date")
        assert result == "line"

    def test_detect_bar_chart_multiple_measures(self):
        """Test bar chart with multiple measures (no time)."""
        result = detect_chart_type_generic(["carrier"], ["count", "avg_delay"], None)
        assert result == "bar"

    def test_detect_multi_line_time_plus_category(self):
        """Test multi-line chart with time and category."""
        result = detect_chart_type_generic(["date", "carrier"], ["count"], "date")
        assert result == "line"

    def test_detect_heatmap_two_dimensions(self):
        """Test heatmap with two categorical dimensions."""
        result = detect_chart_type_generic(["origin", "destination"], ["count"], None)
        assert result == "heatmap"

    def test_detect_line_two_dimensions_with_time(self):
        """Test line chart with two dimensions (one is time)."""
        result = detect_chart_type_generic(["date", "carrier"], ["count"], "date")
        assert result == "line"

    def test_detect_table_complex_query(self):
        """Test table for complex queries."""
        result = detect_chart_type_generic(["origin", "dest", "carrier"], ["count"], None)
        assert result == "table"

    def test_detect_table_multiple_dimensions_and_measures(self):
        """Test table for multiple dimensions and measures."""
        result = detect_chart_type_generic(
            ["origin", "dest", "carrier"], ["count", "avg_delay"], None
        )
        assert result == "table"
