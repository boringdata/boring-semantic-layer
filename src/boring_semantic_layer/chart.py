"""
Chart functionality for semantic API.

Auto-detect Altair chart specifications based on query dimensions and measures.
Provides chart() method for SemanticAggregate results.
"""

from collections.abc import Sequence
from typing import Any

from ibis.common.collections import FrozenDict


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


def _detect_altair_spec(
    dimensions: Sequence[str],
    measures: Sequence[str],
    time_dimension: str | None = None,
    time_grain: str | None = None,
) -> FrozenDict:
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


# Plotly backend


def _detect_plotly_chart_type(
    dimensions: Sequence[str],
    measures: Sequence[str],
    time_dimension: str | None = None,
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

    # Workaround for Plotly/Kaleido datetime rendering bug:
    # Convert datetime columns to ISO format strings to ensure proper rendering in PNG/SVG exports
    # The interactive HTML/JSON outputs work fine, but static image exports have issues with datetime64
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            # If all times are midnight, strip the time part for cleaner labels
            if df[col].str.endswith(" 00:00:00").all():
                df[col] = df[col].str.replace(" 00:00:00", "")

    # Handle data sorting for line charts to avoid zigzag connections
    if chart_type == "line" and dimensions:
        if time_dimension and time_dimension in dimensions:
            # Sort by time dimension for temporal data
            # If there are multiple dimensions, sort by all of them to ensure
            # proper line ordering (e.g., first by time, then by category)
            sort_cols = [time_dimension]
            non_time_dims = [d for d in dimensions if d != time_dimension]
            if non_time_dims:
                sort_cols.extend(non_time_dims)
            df = df.sort_values(by=sort_cols)
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
                index=dimensions[1],
                columns=dimensions[0],
                values=measures[0],
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


def chart(
    semantic_aggregate: Any,
    spec: dict[str, Any] | None = None,
    backend: str = "altair",
    format: str = "static",
):
    """
    Generate a chart visualization for semantic aggregate query results.

    Args:
        semantic_aggregate: The SemanticAggregate object to visualize
        spec: Optional chart specification dict (backend-specific format).
              If partial spec is provided (e.g., only "mark" or only "encoding"),
              missing parts will be auto-detected and merged.
        backend: Visualization backend ("altair" or "plotly")
        format: Output format ("static", "interactive", "json", "png", "svg")

    Returns:
        Chart object (altair.Chart or plotly Figure) or formatted output

    Examples:
        # Auto-detect chart type with Altair
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result)

        # Use Plotly backend
        result = flights.group_by("dep_month").aggregate("flight_count")
        chart(result, backend="plotly")

        # Custom mark with auto-detected encoding
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, spec={"mark": "line"})

        # Custom encoding with auto-detected mark
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, spec={"encoding": {"color": {"field": "carrier"}}})

        # Export as JSON
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, format="json")
    """
    from .ops import _find_all_root_models, _get_merged_fields

    # Extract dimensions and measures from the operation chain
    # The semantic_aggregate might be wrapped by limit/order_by/mutate operations,
    # so walk back to find the SemanticAggregateOp (has both keys and aggs).
    aggregate_op = semantic_aggregate.op()
    while hasattr(aggregate_op, "source") and not hasattr(aggregate_op, "aggs"):
        aggregate_op = aggregate_op.source

    dimensions = list(aggregate_op.keys)
    measures = list(aggregate_op.aggs.keys())

    # Try to detect time dimension from source
    time_dimension = None
    time_grain = None
    all_roots = _find_all_root_models(aggregate_op.source)
    if all_roots:
        dims_dict = _get_merged_fields(all_roots, "dimensions")
        for dim_name in dimensions:
            if dim_name in dims_dict:
                dim_obj = dims_dict[dim_name]
                if hasattr(dim_obj, "is_time_dimension") and dim_obj.is_time_dimension:
                    time_dimension = dim_name
                    break

    if backend == "altair":
        import altair as alt

        # Execute query to get data - the expression includes full chain
        # with limit, order_by, and other transformations
        df = semantic_aggregate.execute()

        # Sanitize column names to avoid Vega-Lite issues with dotted field names
        # This is necessary because Vega-Lite transforms (like fold) don't handle
        # dotted field names correctly - they interpret dots as nested accessors
        column_mapping = {col: _sanitize_field_name(col) for col in df.columns}
        df = df.rename(columns=column_mapping)

        # Update dimensions and measures to use sanitized names
        sanitized_dimensions = [_sanitize_field_name(d) for d in dimensions]
        sanitized_measures = [_sanitize_field_name(m) for m in measures]
        sanitized_time_dimension = _sanitize_field_name(time_dimension) if time_dimension else None

        # Always start with auto-detected spec as base
        base_spec = _detect_altair_spec(
            sanitized_dimensions,
            sanitized_measures,
            sanitized_time_dimension,
            time_grain,
        )

        # Merge with custom spec if provided
        if spec is None:
            spec = base_spec
        else:
            # Intelligent merging: fill in missing parts with auto-detected values
            if "mark" not in spec:
                spec["mark"] = base_spec.get("mark", "point")

            if "encoding" not in spec:
                spec["encoding"] = base_spec.get("encoding", {})

            if "transform" not in spec:
                spec["transform"] = base_spec.get("transform", [])

        # Create and return Altair chart
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

        # Handle different output formats
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
                    import io

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

    elif backend == "plotly":
        import plotly.express as px
        import plotly.graph_objects as go

        # Auto-detect chart type if not provided in spec
        chart_type = _detect_plotly_chart_type(dimensions, measures, time_dimension)

        # Override with spec if provided
        if spec and "chart_type" in spec:
            chart_type = spec["chart_type"]

        # Execute query to get data - the expression includes full chain
        # with limit, order_by, and other transformations
        df = semantic_aggregate.execute()

        # Create a minimal query expression object for _prepare_plotly_data_and_params
        class QueryExpr:
            def __init__(self, dimensions, measures, time_dimension, df):
                self.dimensions = dimensions
                self.measures = measures
                self.df = df

                class Model:
                    pass

                self.model = Model()
                self.model.time_dimension = time_dimension
                self.order_by = None

            def execute(self):
                return self.df

        query_expr = QueryExpr(dimensions, measures, time_dimension, df)
        df, base_params = _prepare_plotly_data_and_params(query_expr, chart_type)

        # Create chart based on type
        if chart_type == "bar":
            fig = px.bar(**base_params)
        elif chart_type == "line":
            fig = px.line(**base_params)
        elif chart_type == "scatter":
            fig = px.scatter(**base_params)
        elif chart_type == "heatmap":
            fig = go.Figure(data=go.Heatmap(**base_params))
        elif chart_type == "indicator":
            value = df[measures[0]].iloc[0] if measures else 0
            fig = go.Figure(go.Indicator(mode="number", value=value))
        else:
            # Default to table
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(values=list(df.columns)),
                        cells=dict(values=[df[col] for col in df.columns]),
                    ),
                ],
            )

        # Handle different output formats
        if format == "static" or format == "interactive":
            return fig
        elif format == "json":
            import plotly.io

            return plotly.io.to_json(fig)
        elif format in ["png", "svg"]:
            return fig.to_image(format=format)
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )
    else:
        raise ValueError(f"Unsupported backend: {backend}. Use 'altair' or 'plotly'")
