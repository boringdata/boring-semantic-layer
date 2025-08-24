"""
Auto-detect Vega-Lite chart specifications based on query dimensions and measures.
"""

from typing import Any, Dict, List, Optional


def _detect_chart_spec(
    dimensions: List[str],
    measures: List[str],
    time_dimension: Optional[str] = None,
    time_grain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Detect an appropriate chart type and return a Vega-Lite specification.

    Args:
        dimensions: List of dimension names
        measures: List of measure names
        time_dimension: Optional name of the time dimension
        time_grain: Optional time grain for temporal formatting

    Returns:
        A Vega-Lite specification dict with appropriate chart type
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
            "text": {"value": "Complex query - consider custom visualization"}
        },
    }
