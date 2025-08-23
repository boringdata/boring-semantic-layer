"""
Auto-detect Vega-Lite chart specifications based on query dimensions and measures.
"""

from typing import Any, Dict, List, Optional
import pandas as pd


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
                    "x": {"field": dimensions[0], "type": "nominal"},
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
                "x": {"field": dimensions[0], "type": "nominal"},
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
                "x": {"field": dimensions[0], "type": "nominal"},
                "y": {"field": dimensions[1], "type": "nominal"},
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


# Native Plotly Detection Functions (Approach 2)


def _detect_plotly_spec(
    dimensions: List[str],
    measures: List[str],
    time_dimension: Optional[str] = None,
    time_grain: Optional[str] = None,
    query_filters: Optional[List] = None,
    data_sample: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Detect appropriate chart type and return native Plotly Express specification.

    Args:
        dimensions: List of dimension field names
        measures: List of measure field names
        time_dimension: Name of time dimension if present
        time_grain: Time aggregation level for formatting
        query_filters: Applied filters for context
        data_sample: Optional sample data for cardinality analysis

    Returns:
        Dictionary with chart_type, parameters, and metadata for Plotly Express
    """
    num_dims = len(dimensions)
    num_measures = len(measures)

    # Single value - KPI display
    if num_dims == 0 and num_measures == 1:
        return {
            "chart_type": "indicator",
            "parameters": {
                "mode": "number",
                "value": measures[0],
                "title": {"text": measures[0].title()},
            },
            "plotly_function": "go.Indicator",
            "data_requirements": {"single_value": True},
            "chart_config": {"autosize": True, "height": 200},
        }

    # Check if we have a time dimension
    has_time = time_dimension and time_dimension in dimensions
    time_dim_index = dimensions.index(time_dimension) if has_time else -1

    # Get time formatting options
    time_formatting = {}
    if has_time and time_grain:
        if "YEAR" in time_grain:
            time_formatting = {"x_tickformat": "%Y"}
        elif "QUARTER" in time_grain:
            time_formatting = {"x_tickformat": "%Y Q%q", "x_tickangle": -45}
        elif "MONTH" in time_grain:
            time_formatting = {"x_tickformat": "%Y-%m", "x_tickangle": -45}
        elif "WEEK" in time_grain:
            time_formatting = {"x_tickformat": "%Y W%W", "x_tickangle": -45}
        elif "DAY" in time_grain:
            time_formatting = {"x_tickformat": "%m-%d", "x_tickangle": -45}
        else:
            time_formatting = {"x_tickangle": -45}

    # Single dimension, single measure
    if num_dims == 1 and num_measures == 1:
        if has_time:
            # Time series - line chart
            parameters = {
                "x": dimensions[0],
                "y": measures[0],
                "title": f"{measures[0]} over time",
                "labels": {dimensions[0]: "Time", measures[0]: measures[0]},
            }
            parameters.update(time_formatting)
            return {
                "chart_type": "line",
                "parameters": parameters,
                "plotly_function": "px.line",
                "data_requirements": {"data_frame": True},
                "chart_config": {"autosize": True, "height": 400},
            }
        else:
            # Categorical - bar chart
            return {
                "chart_type": "bar",
                "parameters": {
                    "x": dimensions[0],
                    "y": measures[0],
                    "title": f"{measures[0]} by {dimensions[0]}",
                    "labels": {
                        dimensions[0]: dimensions[0].title(),
                        measures[0]: measures[0],
                    },
                },
                "plotly_function": "px.bar",
                "data_requirements": {"data_frame": True},
                "chart_config": {"autosize": True, "height": 400},
            }

    # Single dimension, multiple measures - grouped chart
    if num_dims == 1 and num_measures >= 2:
        if has_time:
            # Multi-line time series
            parameters = {
                "x": dimensions[0],
                "y": "value",
                "color": "measure",
                "title": "Multiple metrics over time",
                "labels": {
                    dimensions[0]: "Time",
                    "value": "Value",
                    "measure": "Metric",
                },
            }
            parameters.update(time_formatting)
            chart_type = "line"
        else:
            # Grouped bar chart
            parameters = {
                "x": dimensions[0],
                "y": "value",
                "color": "measure",
                "barmode": "group",
                "title": f"Multiple metrics by {dimensions[0]}",
                "labels": {
                    dimensions[0]: dimensions[0].title(),
                    "value": "Value",
                    "measure": "Metric",
                },
            }
            chart_type = "bar"

        return {
            "chart_type": chart_type,
            "parameters": parameters,
            "plotly_function": f"px.{chart_type}",
            "data_requirements": {
                "data_frame": True,
                "reshape": "melt",
                "melt_config": {
                    "id_vars": [dimensions[0]],
                    "value_vars": measures,
                    "var_name": "measure",
                    "value_name": "value",
                },
            },
            "chart_config": {"autosize": True, "height": 400},
        }

    # Time series with additional dimension(s) - multi-line chart
    if has_time and num_dims >= 2 and num_measures == 1:
        non_time_dims = [d for i, d in enumerate(dimensions) if i != time_dim_index]
        parameters = {
            "x": time_dimension,
            "y": measures[0],
            "color": non_time_dims[0],
            "title": f"{measures[0]} over time by {non_time_dims[0]}",
            "labels": {
                time_dimension: "Time",
                measures[0]: measures[0],
                non_time_dims[0]: non_time_dims[0].title(),
            },
        }
        parameters.update(time_formatting)
        return {
            "chart_type": "line",
            "parameters": parameters,
            "plotly_function": "px.line",
            "data_requirements": {"data_frame": True},
            "chart_config": {"autosize": True, "height": 400},
        }

    # Time series with multiple measures
    if has_time and num_dims == 1 and num_measures >= 2:
        parameters = {
            "x": dimensions[0],
            "y": "value",
            "color": "measure",
            "title": "Multiple metrics over time",
            "labels": {dimensions[0]: "Time", "value": "Value", "measure": "Metric"},
        }
        parameters.update(time_formatting)
        return {
            "chart_type": "line",
            "parameters": parameters,
            "plotly_function": "px.line",
            "data_requirements": {
                "data_frame": True,
                "reshape": "melt",
                "melt_config": {
                    "id_vars": [dimensions[0]],
                    "value_vars": measures,
                    "var_name": "measure",
                    "value_name": "value",
                },
            },
            "chart_config": {"autosize": True, "height": 400},
        }

    # Two dimensions, one measure - heatmap
    if num_dims == 2 and num_measures == 1:
        return {
            "chart_type": "heatmap",
            "parameters": {
                "x": dimensions[0],
                "y": dimensions[1],
                "z": measures[0],
                "title": f"{measures[0]} by {dimensions[0]} and {dimensions[1]}",
                "labels": {
                    dimensions[0]: dimensions[0].title(),
                    dimensions[1]: dimensions[1].title(),
                    measures[0]: measures[0],
                },
            },
            "plotly_function": "go.Heatmap",
            "data_requirements": {
                "data_frame": True,
                "reshape": "pivot",
                "pivot_config": {
                    "index": dimensions[0],
                    "columns": dimensions[1],
                    "values": measures[0],
                },
            },
            "chart_config": {"autosize": True, "height": 500},
        }

    # Default for complex queries - scatter plot or table
    if len(dimensions) >= 3 or len(measures) >= 3:
        # Complex scatter plot
        x_axis = dimensions[0]
        y_axis = measures[0] if measures else dimensions[1]
        color_by = dimensions[1] if len(dimensions) > 1 else None
        size_by = measures[1] if len(measures) > 1 else None

        return {
            "chart_type": "scatter",
            "parameters": {
                "x": x_axis,
                "y": y_axis,
                "color": color_by,
                "size": size_by,
                "title": "Multi-dimensional Analysis",
                "labels": {
                    x_axis: x_axis.title(),
                    y_axis: y_axis if isinstance(y_axis, str) else y_axis.title(),
                },
            },
            "plotly_function": "px.scatter",
            "data_requirements": {"data_frame": True},
            "chart_config": {"autosize": True, "height": 500},
        }

    # Ultimate fallback - table view
    return {
        "chart_type": "table",
        "parameters": {"columns": dimensions + measures, "title": "Data Table"},
        "plotly_function": "custom_table",
        "data_requirements": {"data_frame": True},
        "chart_config": {"autosize": True},
    }


def execute_plotly_spec(query_expr, plotly_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute query and prepare data according to plotly spec requirements.

    Args:
        query_expr: BSL QueryExpr object
        plotly_spec: Plotly specification from _detect_plotly_spec

    Returns:
        Dictionary with prepared data and final chart specification
    """
    # Execute query to get data
    data = query_expr.execute()

    # Apply required data transformations
    data_reqs = plotly_spec.get("data_requirements", {})

    if data_reqs.get("reshape") == "melt":
        # Reshape for multi-measure charts
        melt_config = data_reqs["melt_config"]
        data = data.melt(**melt_config)

    elif data_reqs.get("reshape") == "pivot":
        # Reshape for heatmaps
        pivot_config = data_reqs["pivot_config"]
        data = data.pivot(**pivot_config)

    elif data_reqs.get("single_value"):
        # Extract single value for KPIs
        data = data.iloc[0, -1]  # Last column, first row

    # Prepare final specification
    final_spec = {
        "chart_type": plotly_spec["chart_type"],
        "plotly_function": plotly_spec["plotly_function"],
        "parameters": plotly_spec["parameters"].copy(),
        "data": data,
        "config": plotly_spec.get("chart_config", {}),
    }

    # Add data to parameters if needed
    if "data_frame" in data_reqs:
        final_spec["parameters"]["data_frame"] = data

    return final_spec


def _merge_plotly_specs(base_spec: Dict, user_spec: Dict) -> Dict:
    """Merge user customizations with auto-detected spec."""
    import copy

    merged = copy.deepcopy(base_spec)

    # Allow overriding parameters
    if "parameters" in user_spec:
        merged["parameters"].update(user_spec["parameters"])

    # Handle Vega-Lite mark translation to Plotly chart_type
    if "mark" in user_spec:
        # Map Vega-Lite marks to Plotly chart types
        mark_to_chart_type = {
            "bar": "bar",
            "line": "line",
            "point": "scatter",
            "circle": "scatter",
            "square": "scatter",
            "rect": "imshow",  # For heatmaps
            "area": "area",
        }
        mark = user_spec["mark"]
        if mark in mark_to_chart_type:
            new_chart_type = mark_to_chart_type[mark]
            merged["chart_type"] = new_chart_type
            merged["plotly_function"] = f"px.{new_chart_type}"

            # Clean up parameters that don't apply to the new chart type
            if new_chart_type == "line":
                # Line charts don't use barmode
                merged["parameters"].pop("barmode", None)
            elif new_chart_type == "scatter":
                # Scatter plots don't use barmode
                merged["parameters"].pop("barmode", None)

    # Allow chart type override (direct Plotly chart_type)
    if "chart_type" in user_spec:
        merged["chart_type"] = user_spec["chart_type"]
        # Update function reference
        merged["plotly_function"] = f"px.{user_spec['chart_type']}"

    # Allow config overrides
    if "chart_config" in user_spec:
        merged["chart_config"].update(user_spec["chart_config"])

    return merged
