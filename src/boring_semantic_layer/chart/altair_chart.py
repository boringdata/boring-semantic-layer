"""
Altair backend for chart visualization.

Provides Vega-Lite based charting through the Altair library.
"""

from collections.abc import Sequence
from typing import Any

from .base import ChartBackend


def _sanitize_field_name(field: str) -> str:
    """
    Sanitize field names for Vega-Lite compatibility.

    Vega-Lite interprets dots as nested field accessors, which causes issues
    with transforms like fold. Replace dots with underscores to avoid this.

    Args:
        field: Field name that may contain dots

    Returns:
        Sanitized field name safe for Vega-Lite
    """
    return field.replace(".", "_")


def _sanitize_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively sanitize field names in a chart specification.

    Args:
        spec: Chart specification dictionary

    Returns:
        Sanitized specification with field names converted
    """
    if isinstance(spec, dict):
        result = {}
        for key, value in spec.items():
            # Sanitize field names in encoding and other field references
            if key == "field" and isinstance(value, str):
                result[key] = _sanitize_field_name(value)
            # Sanitize field names in fold transforms
            elif key == "fold" and isinstance(value, list):
                result[key] = [_sanitize_field_name(f) for f in value]
            else:
                result[key] = _sanitize_spec(value)
        return result
    elif isinstance(spec, list):
        return [_sanitize_spec(item) for item in spec]
    else:
        return spec


class AltairBackend(ChartBackend):
    """Altair/Vega-Lite chart backend implementation."""

    def detect_chart_type(
        self,
        dimensions: Sequence[str],
        measures: Sequence[str],
        time_dimension: str | None = None,
        time_grain: str | None = None,
    ) -> dict[str, Any]:
        """
        Detect an appropriate chart type and return an Altair specification.

        Args:
            dimensions: List of dimension names
            measures: List of measure names
            time_dimension: Optional name of the time dimension
            time_grain: Optional time grain for temporal formatting

        Returns:
            An Altair specification dict with appropriate chart type
        """
        num_dims = len(dimensions)
        num_measures = len(measures)

        # Single value - text display
        if num_dims == 0 and num_measures == 1:
            return {
                "mark": {"type": "text", "size": 40},
                "encoding": {"text": {"field": measures[0], "type": "quantitative"}},
            }

        # Check if we have a time dimension
        has_time = time_dimension and time_dimension in dimensions
        time_dim_index = dimensions.index(time_dimension) if has_time else -1

        # Determine appropriate date format and axis config based on time grain
        if has_time and time_grain:
            if "YEAR" in time_grain:
                date_format = "%Y"
                axis_config = {"format": date_format, "labelAngle": 0}
            elif "QUARTER" in time_grain:
                date_format = "%Y Q%q"
                axis_config = {"format": date_format, "labelAngle": -45}
            elif "MONTH" in time_grain:
                date_format = "%Y-%m"
                axis_config = {"format": date_format, "labelAngle": -45}
            elif "WEEK" in time_grain:
                date_format = "%Y W%W"
                axis_config = {"format": date_format, "labelAngle": -45, "tickCount": 10}
            elif "DAY" in time_grain:
                date_format = "%Y-%m-%d"
                axis_config = {"format": date_format, "labelAngle": -45}
            elif "HOUR" in time_grain:
                date_format = "%m-%d %H:00"
                axis_config = {"format": date_format, "labelAngle": -45, "tickCount": 12}
            else:
                date_format = "%Y-%m-%d"
                axis_config = {"format": date_format, "labelAngle": -45}
        else:
            date_format = "%Y-%m-%d"
            axis_config = {"format": date_format, "labelAngle": -45}

        # Single dimension, single measure
        if num_dims == 1 and num_measures == 1:
            if has_time:
                # Time series - line chart
                return {
                    "mark": "line",
                    "encoding": {
                        "x": {
                            "field": dimensions[0],
                            "type": "temporal",
                            "axis": axis_config,
                        },
                        "y": {"field": measures[0], "type": "quantitative"},
                        "tooltip": [
                            {
                                "field": dimensions[0],
                                "type": "temporal",
                                "format": date_format,
                            },
                            {"field": measures[0], "type": "quantitative"},
                        ],
                    },
                }
            else:
                # Categorical - bar chart
                return {
                    "mark": "bar",
                    "encoding": {
                        "x": {"field": dimensions[0], "type": "ordinal", "sort": None},
                        "y": {"field": measures[0], "type": "quantitative"},
                        "tooltip": [
                            {"field": dimensions[0], "type": "nominal"},
                            {"field": measures[0], "type": "quantitative"},
                        ],
                    },
                }

        # Single dimension, multiple measures - grouped bar chart
        if num_dims == 1 and num_measures >= 2:
            return {
                "transform": [{"fold": measures, "as": ["measure", "value"]}],
                "mark": "bar",
                "encoding": {
                    "x": {"field": dimensions[0], "type": "ordinal", "sort": None},
                    "y": {"field": "value", "type": "quantitative"},
                    "color": {"field": "measure", "type": "nominal"},
                    "xOffset": {"field": "measure"},
                    "tooltip": [
                        {"field": dimensions[0], "type": "nominal"},
                        {"field": "measure", "type": "nominal"},
                        {"field": "value", "type": "quantitative"},
                    ],
                },
            }

        # Time series with additional dimension(s) - multi-line chart
        if has_time and num_dims >= 2 and num_measures == 1:
            non_time_dims = [d for i, d in enumerate(dimensions) if i != time_dim_index]
            tooltip_fields = [
                {"field": time_dimension, "type": "temporal", "format": date_format},
                {"field": non_time_dims[0], "type": "nominal"},
                {"field": measures[0], "type": "quantitative"},
            ]
            return {
                "mark": "line",
                "encoding": {
                    "x": {"field": time_dimension, "type": "temporal", "axis": axis_config},
                    "y": {"field": measures[0], "type": "quantitative"},
                    "color": {"field": non_time_dims[0], "type": "nominal"},
                    "tooltip": tooltip_fields,
                },
            }

        # Time series with multiple measures
        if has_time and num_dims == 1 and num_measures >= 2:
            return {
                "transform": [{"fold": measures, "as": ["measure", "value"]}],
                "mark": "line",
                "encoding": {
                    "x": {"field": dimensions[0], "type": "temporal", "axis": axis_config},
                    "y": {"field": "value", "type": "quantitative"},
                    "color": {"field": "measure", "type": "nominal"},
                    "tooltip": [
                        {"field": dimensions[0], "type": "temporal", "format": date_format},
                        {"field": "measure", "type": "nominal"},
                        {"field": "value", "type": "quantitative"},
                    ],
                },
            }

        # Two dimensions, one measure - heatmap
        if num_dims == 2 and num_measures == 1:
            return {
                "mark": "rect",
                "encoding": {
                    "x": {"field": dimensions[0], "type": "ordinal", "sort": None},
                    "y": {"field": dimensions[1], "type": "ordinal", "sort": None},
                    "color": {"field": measures[0], "type": "quantitative"},
                    "tooltip": [
                        {"field": dimensions[0], "type": "nominal"},
                        {"field": dimensions[1], "type": "nominal"},
                        {"field": measures[0], "type": "quantitative"},
                    ],
                },
            }

        # Default for complex queries
        return {
            "mark": "text",
            "encoding": {
                "text": {"value": "Complex query - consider custom visualization"},
            },
        }

    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: dict[str, Any],
        time_dimension: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Prepare data for Altair chart creation.

        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: Chart specification dict
            time_dimension: Optional time dimension name

        Returns:
            tuple: (sanitized_dataframe, empty_params_dict)
        """
        # Sanitize column names to avoid Vega-Lite issues with dotted field names
        column_mapping = {col: _sanitize_field_name(col) for col in df.columns}
        df = df.rename(columns=column_mapping)

        # Return empty params dict as Altair uses the spec directly
        return df, {}

    def create_chart(
        self,
        df: Any,
        params: dict[str, Any],
        chart_type: dict[str, Any],
        spec: dict[str, Any] | None = None,
    ) -> Any:
        """
        Create Altair chart object.

        Args:
            df: Sanitized DataFrame
            params: Unused for Altair (kept for interface consistency)
            chart_type: Auto-detected Altair specification
            spec: Optional custom specification to override/merge

        Returns:
            altair.Chart object
        """
        import altair as alt

        # Start with auto-detected spec as base (must be sanitized)
        base_spec = _sanitize_spec(chart_type)

        # Merge with custom spec if provided
        if spec is None:
            spec = base_spec
        else:
            # Sanitize custom spec
            spec = _sanitize_spec(spec)

            # Intelligent merging: fill in missing parts with auto-detected values
            if "mark" not in spec:
                spec["mark"] = base_spec.get("mark", "point")

            if "encoding" not in spec:
                spec["encoding"] = base_spec.get("encoding", {})

            if "transform" not in spec:
                spec["transform"] = base_spec.get("transform", [])

        # Create chart object
        chart_obj = alt.Chart(df)

        # Apply mark type
        mark = spec.get("mark")
        if isinstance(mark, str):
            chart_obj = getattr(chart_obj, f"mark_{mark}")()
        elif isinstance(mark, dict):
            mark_type = mark.get("type", "bar")
            chart_obj = getattr(chart_obj, f"mark_{mark_type}")(
                **{k: v for k, v in mark.items() if k != "type"},
            )

        # Apply encoding
        encoding = spec.get("encoding", {})
        if encoding:
            chart_obj = chart_obj.encode(**encoding)

        # Apply transform if present
        if "transform" in spec:
            for transform in spec["transform"]:
                if "fold" in transform:
                    chart_obj = chart_obj.transform_fold(
                        transform["fold"],
                        as_=transform.get("as", ["key", "value"]),
                    )

        return chart_obj

    def format_output(self, chart_obj: Any, format: str = "static") -> Any:
        """
        Format Altair chart output.

        Args:
            chart_obj: Altair Chart object
            format: Output format ("static", "interactive", "json", "png", "svg")

        Returns:
            Formatted chart
        """
        if format == "static":
            return chart_obj
        elif format == "interactive":
            return chart_obj.interactive()
        elif format == "json":
            return chart_obj.to_dict()
        elif format in ["png", "svg"]:
            try:
                import io

                if format == "svg":
                    # SVG is returned as a string by Altair
                    buffer = io.StringIO()
                    chart_obj.save(buffer, format=format)
                    return buffer.getvalue().encode("utf-8")
                else:
                    # PNG is returned as bytes
                    buffer = io.BytesIO()
                    chart_obj.save(buffer, format=format)
                    return buffer.getvalue()
            except Exception as e:
                raise ImportError(
                    f"{format} export requires additional dependencies: {e}. "
                    "Install with: pip install 'altair[all]' or pip install vl-convert-python"
                ) from e
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )
