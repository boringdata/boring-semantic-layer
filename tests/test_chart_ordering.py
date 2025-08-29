"""Test that charts preserve data ordering from queries."""

from boring_semantic_layer.chart import _detect_altair_spec


class TestChartOrdering:
    """Test chart ordering behavior to ensure data order is preserved."""

    def test_bar_chart_uses_ordinal_with_sort_none(self):
        """Test that single dimension bar charts use ordinal type with sort: None."""
        spec = _detect_altair_spec(
            dimensions=["destination"], measures=["avg_distance"]
        )

        assert spec["mark"] == "bar"
        x_encoding = spec["encoding"]["x"]
        assert x_encoding["type"] == "ordinal"
        assert x_encoding["sort"] is None
        assert x_encoding["field"] == "destination"

    def test_grouped_bar_chart_uses_ordinal_with_sort_none(self):
        """Test that grouped bar charts use ordinal type with sort: None."""
        spec = _detect_altair_spec(
            dimensions=["destination"], measures=["avg_distance", "flight_count"]
        )

        assert spec["mark"] == "bar"
        x_encoding = spec["encoding"]["x"]
        assert x_encoding["type"] == "ordinal"
        assert x_encoding["sort"] is None
        assert x_encoding["field"] == "destination"

    def test_heatmap_uses_ordinal_with_sort_none(self):
        """Test that heatmaps use ordinal type with sort: None for both dimensions."""
        spec = _detect_altair_spec(
            dimensions=["origin", "destination"], measures=["flight_count"]
        )

        assert spec["mark"] == "rect"
        x_encoding = spec["encoding"]["x"]
        y_encoding = spec["encoding"]["y"]

        assert x_encoding["type"] == "ordinal"
        assert x_encoding["sort"] is None
        assert x_encoding["field"] == "origin"

        assert y_encoding["type"] == "ordinal"
        assert y_encoding["sort"] is None
        assert y_encoding["field"] == "destination"

    def test_time_series_chart_uses_temporal_type(self):
        """Test that time series charts still use temporal type (not ordinal)."""
        spec = _detect_altair_spec(
            dimensions=["flight_date"],
            measures=["flight_count"],
            time_dimension="flight_date",
            time_grain="TIME_GRAIN_DAY",
        )

        assert spec["mark"] == "line"
        x_encoding = spec["encoding"]["x"]
        assert x_encoding["type"] == "temporal"
        assert x_encoding["field"] == "flight_date"
        # Temporal charts don't use sort: None, they use axis configuration
        assert "sort" not in x_encoding
