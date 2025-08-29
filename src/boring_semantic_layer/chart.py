"""
Auto-detect Altair chart specifications based on query dimensions and measures.
"""

from typing import Any, Dict, List, Optional


def _detect_altair_spec(
    dimensions: List[str],
    measures: List[str],
    time_dimension: Optional[str] = None,
    time_grain: Optional[str] = None,
) -> Dict[str, Any]:
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
            "text": {"value": "Complex query - consider custom visualization"}
        },
    }


# Plotly backend


def _detect_plotly_chart_type(
    dimensions: List[str], measures: List[str], time_dimension: Optional[str] = None
) -> str:
    """
    Auto-detect appropriate chart type based on query structure for Plotly backend.

    Args:
        dimensions: List of dimension field names from the query
        measures: List of measure field names from the query
        time_dimension: Optional time dimension field name for temporal detection

    Returns:
        str: Chart type identifier ("bar", "line", "heatmap", "table", "indicator")

    """
    num_dims = len(dimensions)
    num_measures = len(measures)

    # Single value - indicator
    if num_dims == 0 and num_measures == 1:
        return "indicator"

    # Check if we have a time dimension
    has_time = time_dimension and time_dimension in dimensions

    # Single dimension, single measure
    if num_dims == 1 and num_measures == 1:
        return "line" if has_time else "bar"

    # Single dimension, multiple measures - grouped chart
    if num_dims == 1 and num_measures >= 2:
        return "line" if has_time else "bar"

    # Time series with additional dimension(s) - multi-line chart
    if has_time and num_dims >= 2 and num_measures == 1:
        return "line"

    # Two dimensions, one measure - heatmap
    if num_dims == 2 and num_measures == 1:
        return "heatmap"

    # Default for complex queries - table
    return "table"


def _prepare_plotly_data_and_params(query_expr, chart_type: str) -> tuple:
    """
    Execute query and prepare base parameters for Plotly Express.

    Args:
        query_expr: The QueryExpr instance containing query details
        chart_type: The chart type string (bar, line, heatmap, etc.)

    Returns:
        tuple: (dataframe, base_params) where:
            - dataframe: Processed pandas DataFrame ready for plotting
            - base_params: Dict of parameters for Plotly Express functions

    Design Notes:
        - Parameters are validated and processed according to chart type requirements
        - Data transformations ensure compatibility with Plotly Express expectations
        - Chart type is passed separately to maintain flexibility in chart creation
        - All returned parameters are safe to pass to any Plotly Express function
    """
    import pandas as pd

    # Execute the query to get data
    df = query_expr.execute()

    # Get dimensions and measures from query
    dimensions = list(query_expr.dimensions)
    measures = list(query_expr.measures)
    time_dimension = query_expr.model.time_dimension

    # Handle data sorting for line charts to avoid zigzag connections
    if chart_type == "line" and dimensions:
        if time_dimension and time_dimension in dimensions:
            # Sort by time dimension for temporal data
            df = df.sort_values(by=time_dimension)
        elif query_expr.order_by:
            # Query already applied order_by, but when switching chart types
            # we might need to re-sort for proper line connections
            pass  # df is already sorted by the query execution
        else:
            # For categorical data converted to line, sort by x-axis for consistency
            df = df.sort_values(by=dimensions[0])

    # Build minimal base parameters that Plotly Express needs
    base_params = {"data_frame": df}

    if chart_type in ["bar", "line", "scatter"]:
        if dimensions:
            base_params["x"] = dimensions[0]
        if measures:
            if len(measures) == 1:
                base_params["y"] = measures[0]
            else:
                # For multiple measures, we need to reshape data for grouped charts
                # Melt the dataframe to long format

                id_cols = [col for col in df.columns if col not in measures]
                df_melted = pd.melt(
                    df,
                    id_vars=id_cols,
                    value_vars=measures,
                    var_name="measure",
                    value_name="value",
                )
                base_params["data_frame"] = df_melted
                base_params["y"] = "value"
                base_params["color"] = "measure"
                # Update df reference for return
                df = df_melted

        # Handle multiple traces for time series with categories
        if time_dimension and len(dimensions) >= 2:
            non_time_dims = [d for d in dimensions if d != time_dimension]
            if non_time_dims:
                base_params["color"] = non_time_dims[0]

    elif chart_type == "heatmap":
        if len(dimensions) >= 2 and measures:
            # Use pivot table to create proper heatmap matrix with NaN for missing values

            pivot_df = df.pivot(
                index=dimensions[1], columns=dimensions[0], values=measures[0]
            )

            # For go.Heatmap, we need to pass the matrix directly, not through px parameters
            base_params = {
                "z": pivot_df.values,
                "x": pivot_df.columns.tolist(),
                "y": pivot_df.index.tolist(),
                "hoverongaps": False,  # Don't show hover on NaN values
            }
            # Update df reference for return
            df = pivot_df

    return df, base_params
