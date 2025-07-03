"""Tests for visualization module."""

import pytest
import pandas as pd
from boring_semantic_layer.visualization import ChartProcessor, ChartTypeDetector


class TestChartProcessor:
    """Test ChartProcessor functionality."""

    def test_validate_vega_lite_spec_valid(self):
        """Test validation of valid Vega-Lite specs."""
        # Valid spec with mark
        spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": "category", "type": "nominal"},
                "y": {"field": "value", "type": "quantitative"},
            },
        }
        ChartProcessor.validate_vega_lite_spec(spec)  # Should not raise

        # Valid spec with layer
        spec = {"layer": [{"mark": "line"}, {"mark": "point"}]}
        ChartProcessor.validate_vega_lite_spec(spec)  # Should not raise

    def test_validate_vega_lite_spec_invalid(self):
        """Test validation of invalid Vega-Lite specs."""
        # Not a dictionary
        with pytest.raises(ValueError, match="must be a dictionary"):
            ChartProcessor.validate_vega_lite_spec("not a dict")

        # Missing required fields
        with pytest.raises(ValueError, match="must have either"):
            ChartProcessor.validate_vega_lite_spec({})

        # Invalid encoding
        with pytest.raises(ValueError, match="'encoding' must be a dictionary"):
            ChartProcessor.validate_vega_lite_spec(
                {"mark": "bar", "encoding": "invalid"}
            )

        # Empty encoding
        with pytest.raises(
            ValueError, match="'encoding' must have at least one channel"
        ):
            ChartProcessor.validate_vega_lite_spec({"mark": "bar", "encoding": {}})

    def test_extract_referenced_fields(self):
        """Test extraction of field references from specs."""
        # Simple spec
        spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": "category", "type": "nominal"},
                "y": {"field": "value", "type": "quantitative"},
            },
        }
        fields = ChartProcessor.extract_referenced_fields(spec)
        assert fields == {"category", "value"}

        # Layered spec
        spec = {
            "layer": [
                {
                    "mark": "line",
                    "encoding": {
                        "x": {"field": "date", "type": "temporal"},
                        "y": {"field": "sales", "type": "quantitative"},
                    },
                },
                {
                    "mark": "point",
                    "encoding": {
                        "x": {"field": "date", "type": "temporal"},
                        "y": {"field": "sales", "type": "quantitative"},
                        "color": {"field": "region", "type": "nominal"},
                    },
                },
            ]
        }
        fields = ChartProcessor.extract_referenced_fields(spec)
        assert fields == {"date", "sales", "region"}

        # Faceted spec
        spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": "x", "type": "nominal"},
                "y": {"field": "y", "type": "quantitative"},
            },
            "facet": {"field": "category"},
        }
        fields = ChartProcessor.extract_referenced_fields(spec)
        assert fields == {"x", "y", "category"}

    def test_validate_fields_exist(self):
        """Test field existence validation."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        # Valid fields
        ChartProcessor.validate_fields_exist({"a", "b"}, df)  # Should not raise

        # Invalid fields
        with pytest.raises(ValueError, match="fields not found in data: x, y"):
            ChartProcessor.validate_fields_exist({"x", "y"}, df)

    def test_inject_data(self):
        """Test data injection into specs."""
        df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})

        spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": "category", "type": "nominal"},
                "y": {"field": "value", "type": "quantitative"},
            },
        }

        result = ChartProcessor.inject_data(spec, df)

        assert "data" in result
        assert "values" in result["data"]
        assert len(result["data"]["values"]) == 3
        assert result["data"]["values"][0] == {"category": "A", "value": 10}

    def test_process_chart_full(self):
        """Test full chart processing pipeline."""
        df = pd.DataFrame({"month": ["Jan", "Feb", "Mar"], "sales": [100, 150, 200]})

        spec = {
            "mark": "line",
            "encoding": {
                "x": {"field": "month", "type": "nominal"},
                "y": {"field": "sales", "type": "quantitative"},
            },
        }

        result = ChartProcessor.process_chart(spec, df)

        # Should have data injected
        assert "data" in result
        assert len(result["data"]["values"]) == 3

        # Original spec structure should be preserved
        assert result["mark"] == "line"
        assert result["encoding"]["x"]["field"] == "month"


class TestChartTypeDetector:
    """Test ChartTypeDetector functionality."""

    def test_detect_single_value(self):
        """Test detection for single value queries."""
        spec = ChartTypeDetector.detect_chart_type([], ["total_sales"])
        assert spec["mark"] == "text"
        assert spec["encoding"]["text"]["field"] == "total_sales"

    def test_detect_time_series(self):
        """Test detection for time series data."""
        spec = ChartTypeDetector.detect_chart_type(
            ["date"], ["sales"], time_dimension="date"
        )
        assert spec["mark"] == "line"
        assert spec["encoding"]["x"]["type"] == "temporal"
        assert spec["encoding"]["y"]["type"] == "quantitative"

    def test_detect_categorical_bar(self):
        """Test detection for categorical bar charts."""
        spec = ChartTypeDetector.detect_chart_type(["category"], ["value"])
        assert spec["mark"] == "bar"
        assert spec["encoding"]["x"]["type"] == "nominal"
        assert spec["encoding"]["y"]["type"] == "quantitative"

    def test_detect_grouped_bar(self):
        """Test detection for grouped bar charts."""
        spec = ChartTypeDetector.detect_chart_type(["category"], ["sales", "profit"])
        assert spec["mark"] == "bar"
        # Should default to showing first measure
        assert spec["encoding"]["y"]["field"] == "sales"

    def test_detect_heatmap(self):
        """Test detection for heatmap visualization."""
        spec = ChartTypeDetector.detect_chart_type(["x_dim", "y_dim"], ["value"])
        assert spec["mark"] == "rect"
        assert spec["encoding"]["color"]["type"] == "quantitative"
