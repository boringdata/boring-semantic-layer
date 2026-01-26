"""Comprehensive tests for ECharts backend."""

import pytest
import pandas as pd

from boring_semantic_layer.chart.echarts import (
    EChartsBackend,
    EChartsChartType,
    EChartsOption,
    DEFAULT_COLOR_PALETTE,
)


class TestEChartsBackend:
    """Test suite for EChartsBackend."""

    @pytest.fixture
    def backend(self):
        """Create an EChartsBackend instance."""
        return EChartsBackend()

    @pytest.fixture
    def sample_data(self):
        """Sample data as list of dicts."""
        return [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "C", "value": 15},
        ]

    @pytest.fixture
    def sample_df(self, sample_data):
        """Sample data as pandas DataFrame."""
        return pd.DataFrame(sample_data)

    @pytest.fixture
    def multi_series_data(self):
        """Data for multi-series charts."""
        return [
            {"category": "A", "value": 10, "group": "X"},
            {"category": "B", "value": 20, "group": "X"},
            {"category": "C", "value": 15, "group": "X"},
            {"category": "A", "value": 12, "group": "Y"},
            {"category": "B", "value": 18, "group": "Y"},
            {"category": "C", "value": 22, "group": "Y"},
        ]

    @pytest.fixture
    def scatter_data(self):
        """Data for scatter charts."""
        return [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 3.5},
            {"x": 4.0, "y": 5.0},
        ]

    # =========================================================================
    # 1. Basic chart generation tests
    # =========================================================================

    def test_generate_bar_chart(self, backend, sample_data):
        """Test basic bar chart generation."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
        )

        assert spec["series"][0]["type"] == "bar"
        assert spec["xAxis"]["data"] == ["A", "B", "C"]
        assert spec["series"][0]["data"] == [10, 20, 15]

    def test_generate_line_chart(self, backend, sample_data):
        """Test basic line chart generation."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="line",
            x="category",
            y="value",
        )

        assert spec["series"][0]["type"] == "line"
        assert spec["xAxis"]["data"] == ["A", "B", "C"]
        assert spec["series"][0]["data"] == [10, 20, 15]

    def test_generate_area_chart(self, backend, sample_data):
        """Test area chart generation (virtual type)."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="area",
            x="category",
            y="value",
        )

        # Area is line with areaStyle
        assert spec["series"][0]["type"] == "line"
        assert "areaStyle" in spec["series"][0]
        assert spec["xAxis"]["data"] == ["A", "B", "C"]

    def test_generate_pie_chart(self, backend, sample_data):
        """Test pie chart generation."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="pie",
            x="category",
            y="value",
        )

        assert spec["series"][0]["type"] == "pie"
        pie_data = spec["series"][0]["data"]
        assert len(pie_data) == 3
        assert {"name": "A", "value": 10} in pie_data

    def test_generate_scatter_chart(self, backend, scatter_data):
        """Test scatter chart generation."""
        spec = backend.generate_spec(
            scatter_data,
            chart_type="scatter",
            x="x",
            y="y",
        )

        assert spec["series"][0]["type"] == "scatter"
        # Scatter data is [[x, y], ...]
        assert [1.0, 2.0] in spec["series"][0]["data"]

    # =========================================================================
    # 2. Data input format tests
    # =========================================================================

    def test_list_of_dicts_input(self, backend, sample_data):
        """Test list of dicts as input."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert spec["series"][0]["data"] == [10, 20, 15]

    def test_dataframe_input(self, backend, sample_df):
        """Test pandas DataFrame as input."""
        spec = backend.generate_spec(sample_df, "bar", x="category", y="value")
        assert spec["series"][0]["data"] == [10, 20, 15]

    def test_dict_with_data_key_input(self, backend, sample_data):
        """Test dict with 'data' key as input."""
        wrapped_data = {"data": sample_data}
        spec = backend.generate_spec(wrapped_data, "bar", x="category", y="value")
        assert spec["series"][0]["data"] == [10, 20, 15]

    def test_invalid_data_format_raises(self, backend):
        """Test that invalid data format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported data type"):
            backend.generate_spec("invalid", "bar", x="category", y="value")

    # =========================================================================
    # 3. Multi-series with color grouping tests
    # =========================================================================

    def test_bar_chart_multi_series(self, backend, multi_series_data):
        """Test bar chart with color grouping (multi-series)."""
        spec = backend.generate_spec(
            multi_series_data,
            chart_type="bar",
            x="category",
            y="value",
            color="group",
        )

        # Should have two series (X and Y)
        assert len(spec["series"]) == 2
        series_names = {s["name"] for s in spec["series"]}
        assert series_names == {"X", "Y"}

    def test_line_chart_multi_series(self, backend, multi_series_data):
        """Test line chart with color grouping (multi-series)."""
        spec = backend.generate_spec(
            multi_series_data,
            chart_type="line",
            x="category",
            y="value",
            color="group",
        )

        assert len(spec["series"]) == 2
        for series in spec["series"]:
            assert series["type"] == "line"

    def test_area_chart_multi_series(self, backend, multi_series_data):
        """Test area chart with multi-series."""
        spec = backend.generate_spec(
            multi_series_data,
            chart_type="area",
            x="category",
            y="value",
            color="group",
        )

        assert len(spec["series"]) == 2
        for series in spec["series"]:
            assert "areaStyle" in series

    def test_scatter_chart_multi_series(self, backend):
        """Test scatter chart with color grouping."""
        scatter_grouped = [
            {"x": 1, "y": 2, "group": "A"},
            {"x": 2, "y": 3, "group": "A"},
            {"x": 3, "y": 1, "group": "B"},
            {"x": 4, "y": 4, "group": "B"},
        ]
        spec = backend.generate_spec(
            scatter_grouped,
            chart_type="scatter",
            x="x",
            y="y",
            color="group",
        )

        assert len(spec["series"]) == 2

    def test_multi_series_legend(self, backend, multi_series_data):
        """Test that multi-series charts show legend by default."""
        spec = backend.generate_spec(
            multi_series_data,
            chart_type="bar",
            x="category",
            y="value",
            color="group",
        )

        assert "legend" in spec
        assert spec["legend"]["show"] is True
        assert set(spec["legend"]["data"]) == {"X", "Y"}

    def test_multi_series_value_alignment(self, backend):
        """Test that multi-series aligns values correctly with categories."""
        # Data with different categories per group
        data = [
            {"cat": "A", "val": 10, "g": "X"},
            {"cat": "B", "val": 20, "g": "X"},
            {"cat": "A", "val": 15, "g": "Y"},
            {"cat": "C", "val": 25, "g": "Y"},
        ]
        spec = backend.generate_spec(data, "bar", x="cat", y="val", color="g")

        # Categories should be A, B, C (unique across all)
        assert spec["xAxis"]["data"] == ["A", "B", "C"]

        # Each series should have correct values (0 for missing)
        for series in spec["series"]:
            assert len(series["data"]) == 3

    # =========================================================================
    # 4. Options tests (title, subtitle, labels, legend, tooltip, animation)
    # =========================================================================

    def test_title_option(self, backend, sample_data):
        """Test chart title option."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            title="Sales by Category",
        )

        assert spec["title"]["text"] == "Sales by Category"

    def test_subtitle_option(self, backend, sample_data):
        """Test chart subtitle option."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            title="Main Title",
            subtitle="Subtitle here",
        )

        assert spec["title"]["subtext"] == "Subtitle here"

    def test_axis_labels(self, backend, sample_data):
        """Test x and y axis labels."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            x_label="Categories",
            y_label="Values",
        )

        assert spec["xAxis"]["name"] == "Categories"
        assert spec["yAxis"]["name"] == "Values"

    def test_show_legend_false(self, backend, multi_series_data):
        """Test hiding legend."""
        spec = backend.generate_spec(
            multi_series_data,
            chart_type="bar",
            x="category",
            y="value",
            color="group",
            show_legend=False,
        )

        # Legend should not be present or show=False
        assert "legend" not in spec or spec.get("legend", {}).get("show") is False

    def test_show_tooltip_default(self, backend, sample_data):
        """Test tooltip is shown by default."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert spec["tooltip"]["show"] is True

    def test_show_tooltip_false(self, backend, sample_data):
        """Test hiding tooltip."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            show_tooltip=False,
        )

        assert spec["tooltip"]["show"] is False

    def test_animation_default(self, backend, sample_data):
        """Test animation is enabled by default."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert spec["animation"] is True

    def test_animation_disabled(self, backend, sample_data):
        """Test disabling animation."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            animation=False,
        )

        assert spec["animation"] is False

    # =========================================================================
    # 5. Custom color palette tests
    # =========================================================================

    def test_default_color_palette(self, backend, sample_data):
        """Test default color palette is applied."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert spec["color"] == DEFAULT_COLOR_PALETTE

    def test_custom_color_palette(self, backend, sample_data):
        """Test custom color palette."""
        custom_colors = ["#ff0000", "#00ff00", "#0000ff"]
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            colors=custom_colors,
        )

        assert spec["color"] == custom_colors

    # =========================================================================
    # 6. Option overrides/merging tests
    # =========================================================================

    def test_option_overrides_simple(self, backend, sample_data):
        """Test simple option overrides."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            overrides={"backgroundColor": "#f5f5f5"},
        )

        assert spec["backgroundColor"] == "#f5f5f5"

    def test_option_overrides_nested_merge(self, backend, sample_data):
        """Test that nested dicts are shallow-merged."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            title="Original",
            overrides={"title": {"left": "center"}},
        )

        # Original text should be preserved, left should be added
        assert spec["title"]["text"] == "Original"
        assert spec["title"]["left"] == "center"

    def test_option_overrides_replace_value(self, backend, sample_data):
        """Test overriding a non-dict value."""
        spec = backend.generate_spec(
            sample_data,
            chart_type="bar",
            x="category",
            y="value",
            animation=True,
            overrides={"animation": False},
        )

        assert spec["animation"] is False

    def test_merge_options_method(self, backend):
        """Test merge_options method directly."""
        base: EChartsOption = {
            "title": {"text": "Hello"},
            "animation": True,
        }
        overrides = {
            "title": {"subtext": "World"},
            "animation": False,
        }
        result = backend.merge_options(base, overrides)

        assert result["title"]["text"] == "Hello"
        assert result["title"]["subtext"] == "World"
        assert result["animation"] is False

    def test_merge_options_none(self, backend):
        """Test merge_options with None overrides."""
        base: EChartsOption = {"animation": True}
        result = backend.merge_options(base, None)
        assert result == base

    # =========================================================================
    # 7. Error handling tests
    # =========================================================================

    def test_missing_x_field_bar(self, backend, sample_data):
        """Test error when x field is missing for bar chart."""
        with pytest.raises(ValueError, match="requires 'x' and 'y'"):
            backend.generate_spec(sample_data, "bar", y="value")

    def test_missing_y_field_bar(self, backend, sample_data):
        """Test error when y field is missing for bar chart."""
        with pytest.raises(ValueError, match="requires 'x' and 'y'"):
            backend.generate_spec(sample_data, "bar", x="category")

    def test_missing_fields_line(self, backend, sample_data):
        """Test error when fields are missing for line chart."""
        with pytest.raises(ValueError, match="requires 'x' and 'y'"):
            backend.generate_spec(sample_data, "line", x="category")

    def test_missing_fields_scatter(self, backend, scatter_data):
        """Test error when fields are missing for scatter chart."""
        with pytest.raises(ValueError, match="requires 'x' and 'y'"):
            backend.generate_spec(scatter_data, "scatter")

    def test_missing_fields_pie(self, backend, sample_data):
        """Test error when fields are missing for pie chart."""
        with pytest.raises(ValueError, match="requires 'x'/'name' and 'y'/'value'"):
            backend.generate_spec(sample_data, "pie")

    def test_invalid_chart_type(self, backend, sample_data):
        """Test error for unsupported chart type."""
        with pytest.raises(ValueError, match="Unsupported chart type"):
            backend.generate_spec(sample_data, "invalid_type", x="category", y="value")

    def test_enum_chart_type(self, backend, sample_data):
        """Test using EChartsChartType enum instead of string."""
        spec = backend.generate_spec(
            sample_data,
            chart_type=EChartsChartType.BAR,
            x="category",
            y="value",
        )

        assert spec["series"][0]["type"] == "bar"

    def test_recognized_but_unimplemented_chart_type(self, backend, sample_data):
        """Test chart type that exists in enum but isn't implemented yet."""
        # heatmap is in EChartsChartType but not implemented
        with pytest.raises(ValueError, match="recognized but not yet implemented"):
            backend.generate_spec(
                sample_data, EChartsChartType.HEATMAP, x="category", y="value"
            )

    # =========================================================================
    # 8. Edge cases
    # =========================================================================

    def test_single_row_data(self, backend):
        """Test chart with single data point."""
        single_row = [{"category": "Only", "value": 42}]
        spec = backend.generate_spec(single_row, "bar", x="category", y="value")

        assert spec["xAxis"]["data"] == ["Only"]
        assert spec["series"][0]["data"] == [42]

    def test_single_column_scatter(self, backend):
        """Test scatter with single point."""
        single_point = [{"x": 1, "y": 2}]
        spec = backend.generate_spec(single_point, "scatter", x="x", y="y")

        assert spec["series"][0]["data"] == [[1, 2]]

    def test_null_values_in_data(self, backend):
        """Test handling of null/None values."""
        data_with_nulls = [
            {"category": "A", "value": 10},
            {"category": "B", "value": None},
            {"category": "C", "value": 15},
        ]
        spec = backend.generate_spec(data_with_nulls, "bar", x="category", y="value")

        assert spec["series"][0]["data"] == [10, None, 15]

    def test_empty_data(self, backend):
        """Test chart with empty data."""
        empty_data: list[dict] = []
        spec = backend.generate_spec(empty_data, "bar", x="category", y="value")

        assert spec["xAxis"]["data"] == []
        assert spec["series"][0]["data"] == []

    def test_missing_field_in_records(self, backend):
        """Test handling when a record is missing a field."""
        incomplete_data = [
            {"category": "A", "value": 10},
            {"category": "B"},  # missing 'value'
            {"category": "C", "value": 15},
        ]
        spec = backend.generate_spec(incomplete_data, "bar", x="category", y="value")

        # Should get None for missing value
        assert spec["series"][0]["data"] == [10, None, 15]

    def test_numeric_categories(self, backend):
        """Test categories that are numbers."""
        numeric_cats = [
            {"category": 2020, "value": 100},
            {"category": 2021, "value": 150},
            {"category": 2022, "value": 200},
        ]
        spec = backend.generate_spec(numeric_cats, "bar", x="category", y="value")

        assert spec["xAxis"]["data"] == [2020, 2021, 2022]

    def test_special_characters_in_values(self, backend):
        """Test categories with special characters."""
        special_data = [
            {"category": "A & B", "value": 10},
            {"category": "C < D", "value": 20},
            {"category": "E > F", "value": 15},
        ]
        spec = backend.generate_spec(special_data, "bar", x="category", y="value")

        assert "A & B" in spec["xAxis"]["data"]

    def test_large_dataset(self, backend):
        """Test with a larger dataset."""
        large_data = [{"category": f"Cat_{i}", "value": i * 10} for i in range(100)]
        spec = backend.generate_spec(large_data, "bar", x="category", y="value")

        assert len(spec["xAxis"]["data"]) == 100
        assert len(spec["series"][0]["data"]) == 100

    # =========================================================================
    # 9. Chart-specific behavior tests
    # =========================================================================

    def test_bar_chart_tooltip_trigger_axis(self, backend, sample_data):
        """Test bar chart uses axis tooltip trigger."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert spec["tooltip"]["trigger"] == "axis"

    def test_line_chart_tooltip_trigger_axis(self, backend, sample_data):
        """Test line chart uses axis tooltip trigger."""
        spec = backend.generate_spec(sample_data, "line", x="category", y="value")
        assert spec["tooltip"]["trigger"] == "axis"

    def test_pie_chart_tooltip_trigger_item(self, backend, sample_data):
        """Test pie chart uses item tooltip trigger."""
        spec = backend.generate_spec(sample_data, "pie", x="category", y="value")
        assert spec["tooltip"]["trigger"] == "item"

    def test_scatter_chart_tooltip_trigger_item(self, backend, scatter_data):
        """Test scatter chart uses item tooltip trigger."""
        spec = backend.generate_spec(scatter_data, "scatter", x="x", y="y")
        assert spec["tooltip"]["trigger"] == "item"

    def test_pie_chart_legend_position(self, backend, sample_data):
        """Test pie chart legend is at bottom."""
        spec = backend.generate_spec(sample_data, "pie", x="category", y="value")
        assert spec["legend"]["top"] == "bottom"

    def test_pie_chart_label_formatter(self, backend, sample_data):
        """Test pie chart has percentage formatter."""
        spec = backend.generate_spec(sample_data, "pie", x="category", y="value")
        assert "{d}%" in spec["series"][0]["label"]["formatter"]

    def test_pie_chart_name_value_aliases(self, backend):
        """Test pie chart accepts 'name' and 'value' as field names."""
        data = [
            {"name": "A", "value": 10},
            {"name": "B", "value": 20},
        ]
        spec = backend.generate_spec(data, "pie", name="name", value="value")
        assert spec["series"][0]["type"] == "pie"

    def test_scatter_chart_value_axes(self, backend, scatter_data):
        """Test scatter chart uses value type for both axes."""
        spec = backend.generate_spec(scatter_data, "scatter", x="x", y="y")
        assert spec["xAxis"]["type"] == "value"
        assert spec["yAxis"]["type"] == "value"

    def test_bar_chart_category_axis(self, backend, sample_data):
        """Test bar chart uses category type for x-axis."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert spec["xAxis"]["type"] == "category"
        assert spec["yAxis"]["type"] == "value"

    def test_grid_config_present(self, backend, sample_data):
        """Test grid config is set for cartesian charts."""
        spec = backend.generate_spec(sample_data, "bar", x="category", y="value")
        assert "grid" in spec
        assert spec["grid"]["containLabel"] is True

    # =========================================================================
    # 10. Supported chart types tests
    # =========================================================================

    def test_supported_chart_types_includes_base_types(self, backend):
        """Test that supported_chart_types includes all base types."""
        supported = backend.supported_chart_types()
        assert "bar" in supported
        assert "line" in supported
        assert "pie" in supported
        assert "scatter" in supported

    def test_supported_chart_types_includes_virtual_types(self, backend):
        """Test that supported_chart_types includes virtual types like area."""
        supported = backend.supported_chart_types()
        assert "area" in supported

    def test_validate_chart_type_valid(self, backend):
        """Test validate_chart_type with valid type."""
        result = backend.validate_chart_type("bar")
        assert result == "bar"

    def test_validate_chart_type_enum(self, backend):
        """Test validate_chart_type with enum."""
        result = backend.validate_chart_type(EChartsChartType.LINE)
        assert result == "line"

    def test_validate_chart_type_invalid(self, backend):
        """Test validate_chart_type with invalid type."""
        with pytest.raises(ValueError, match="Unsupported chart type"):
            backend.validate_chart_type("not_a_chart")


class TestEChartsBackendIntegration:
    """Integration tests for ECharts backend with realistic scenarios."""

    @pytest.fixture
    def sales_data(self):
        """Realistic sales data."""
        return pd.DataFrame({
            "month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun"],
            "revenue": [10000, 12000, 15000, 14000, 18000, 20000],
            "expenses": [8000, 9000, 11000, 10000, 12000, 13000],
            "region": ["East", "East", "West", "West", "East", "West"],
        })

    def test_sales_bar_chart(self, sales_data):
        """Test creating a sales bar chart."""
        backend = EChartsBackend()
        spec = backend.generate_spec(
            sales_data,
            chart_type="bar",
            x="month",
            y="revenue",
            title="Monthly Revenue",
            x_label="Month",
            y_label="Revenue ($)",
        )

        assert spec["title"]["text"] == "Monthly Revenue"
        assert spec["xAxis"]["name"] == "Month"
        assert spec["yAxis"]["name"] == "Revenue ($)"
        assert len(spec["series"][0]["data"]) == 6

    def test_sales_trend_line(self, sales_data):
        """Test creating a sales trend line chart."""
        backend = EChartsBackend()
        spec = backend.generate_spec(
            sales_data,
            chart_type="line",
            x="month",
            y="revenue",
            title="Revenue Trend",
        )

        assert spec["series"][0]["type"] == "line"
        assert spec["xAxis"]["data"][0] == "Jan"

    def test_sales_by_region_pie(self):
        """Test creating a pie chart of sales by region."""
        backend = EChartsBackend()
        region_totals = [
            {"region": "East", "total": 40000},
            {"region": "West", "total": 49000},
            {"region": "North", "total": 35000},
        ]
        spec = backend.generate_spec(
            region_totals,
            chart_type="pie",
            x="region",
            y="total",
            title="Sales by Region",
        )

        assert spec["series"][0]["type"] == "pie"
        assert len(spec["series"][0]["data"]) == 3

    def test_multi_measure_comparison(self, sales_data):
        """Test creating a chart comparing multiple measures."""
        backend = EChartsBackend()
        # For multi-measure, we'd need to reshape data
        # This tests the color grouping feature
        reshaped = [
            {"month": row["month"], "amount": row["revenue"], "type": "Revenue"}
            for _, row in sales_data.iterrows()
        ] + [
            {"month": row["month"], "amount": row["expenses"], "type": "Expenses"}
            for _, row in sales_data.iterrows()
        ]

        spec = backend.generate_spec(
            reshaped,
            chart_type="bar",
            x="month",
            y="amount",
            color="type",
            title="Revenue vs Expenses",
        )

        assert len(spec["series"]) == 2
        series_names = {s["name"] for s in spec["series"]}
        assert "Revenue" in series_names
        assert "Expenses" in series_names
