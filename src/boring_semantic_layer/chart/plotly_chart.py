"""
Plotly backend for chart visualization.

Provides interactive charting through the Plotly library.
"""

from collections.abc import Sequence
from typing import Any

from .base import ChartBackend
from .utils import (
    convert_datetime_to_strings,
    detect_chart_type_generic,
    get_non_time_dimensions,
    melt_dataframe_for_multiple_measures,
    override_chart_type_from_spec,
    pivot_dataframe_for_heatmap,
    sort_dataframe_for_line_chart,
    validate_format,
)


class PlotlyBackend(ChartBackend):
    """Plotly chart backend implementation."""

    def detect_chart_type(
        self,
        dimensions: Sequence[str],
        measures: Sequence[str],
        time_dimension: str | None = None,
        time_grain: str | None = None,
    ) -> str:
        """
        Auto-detect appropriate chart type based on query structure for Plotly backend.

        Uses shared detection logic. Plotly supports all standard chart types:
        indicator, bar, line, heatmap, scatter, and table.

        Args:
            dimensions: List of dimension field names from the query
            measures: List of measure field names from the query
            time_dimension: Optional time dimension field name for temporal detection
            time_grain: Optional time grain (unused in Plotly)

        Returns:
            str: Chart type identifier ("bar", "line", "heatmap", "table", "indicator")
        """
        # Use shared generic detection - Plotly supports all types
        return detect_chart_type_generic(list(dimensions), list(measures), time_dimension)

    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: str,
        time_dimension: str | None = None,
        time_grain: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Execute query and prepare base parameters for Plotly Express.

        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: The chart type string (bar, line, heatmap, etc.)
            time_dimension: Optional time dimension name
            time_grain: Optional requested time grain (unused)

        Returns:
            tuple: (dataframe, base_params) where:
                - dataframe: Processed pandas DataFrame ready for plotting
                - base_params: Dict of parameters for Plotly Express functions
        """

        # Convert datetime columns for Plotly compatibility
        df = convert_datetime_to_strings(df)

        # Sort line charts to avoid zigzag connections
        if chart_type == "line":
            df = sort_dataframe_for_line_chart(df, list(dimensions), time_dimension)

        # Build minimal base parameters that Plotly Express needs
        base_params = {"data_frame": df}

        if chart_type in ["bar", "line", "scatter"]:
            if dimensions:
                base_params["x"] = dimensions[0]
            if measures:
                if len(measures) == 1:
                    base_params["y"] = measures[0]
                else:
                    # Melt for multiple measures
                    df, measure_col, value_col = melt_dataframe_for_multiple_measures(
                        df, list(measures)
                    )
                    base_params["data_frame"] = df
                    base_params["y"] = value_col
                    base_params["color"] = measure_col

            # Handle multiple traces for time series with categories
            if time_dimension and len(dimensions) >= 2:
                non_time_dims = get_non_time_dimensions(list(dimensions), time_dimension)
                if non_time_dims:
                    base_params["color"] = non_time_dims[0]

        elif chart_type == "heatmap":
            if len(dimensions) >= 2 and measures:
                # Pivot for heatmap
                pivot_df = pivot_dataframe_for_heatmap(df, list(dimensions), measures[0])

                # For go.Heatmap, we need to pass the matrix directly
                base_params = {
                    "z": pivot_df.values,
                    "x": pivot_df.columns.tolist(),
                    "y": pivot_df.index.tolist(),
                    "hoverongaps": False,
                }
                df = pivot_df

        return df, base_params

    def create_chart(
        self,
        df: Any,
        params: dict[str, Any],
        chart_type: str,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        """
        Create Plotly chart object.

        Args:
            df: Processed DataFrame
            params: Base parameters from prepare_data
            chart_type: Chart type string
            spec: Optional custom specification (can override chart_type)

        Returns:
            plotly Figure object
        """
        import plotly.express as px
        import plotly.graph_objects as go

        # Override chart type from spec if provided
        chart_type = override_chart_type_from_spec(chart_type, spec)

        # Get measures from params if available
        if (
            "data_frame" in params
            and hasattr(params["data_frame"], "columns")
            and "y" in params
            and isinstance(params["y"], str)
        ):
            # Try to infer measures from params
            [params["y"]]

        # Create chart based on type
        if chart_type == "bar":
            fig = px.bar(**params)
        elif chart_type == "line":
            fig = px.line(**params)
        elif chart_type == "scatter":
            fig = px.scatter(**params)
        elif chart_type == "heatmap":
            fig = go.Figure(data=go.Heatmap(**params))
        elif chart_type == "indicator":
            # Extract value from DataFrame
            value = df.iloc[0, 0] if len(df) > 0 and len(df.columns) > 0 else 0
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

        return fig

    def format_output(self, chart_obj: Any, format: str = "static") -> Any:
        """
        Format Plotly chart output.

        Args:
            chart_obj: Plotly Figure object
            format: Output format ("static", "interactive", "json", "png", "svg")

        Returns:
            Formatted chart
        """
        # Validate format
        validate_format(format, ["static", "interactive", "json", "png", "svg"])

        if format in ("static", "interactive"):
            return chart_obj
        elif format == "json":
            import plotly.io

            return plotly.io.to_json(chart_obj)
        else:  # png or svg
            return chart_obj.to_image(format=format)
