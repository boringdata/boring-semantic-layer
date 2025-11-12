"""
Plotly backend for chart visualization.

Provides interactive charting through the Plotly library.
"""

from collections.abc import Sequence
from typing import Any

from .base import ChartBackend


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

        Args:
            dimensions: List of dimension field names from the query
            measures: List of measure field names from the query
            time_dimension: Optional time dimension field name for temporal detection
            time_grain: Optional time grain (unused in Plotly)

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

    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: str,
        time_dimension: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Execute query and prepare base parameters for Plotly Express.

        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: The chart type string (bar, line, heatmap, etc.)
            time_dimension: Optional time dimension name

        Returns:
            tuple: (dataframe, base_params) where:
                - dataframe: Processed pandas DataFrame ready for plotting
                - base_params: Dict of parameters for Plotly Express functions
        """
        import pandas as pd

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
        if spec and "chart_type" in spec:
            chart_type = spec["chart_type"]

        # Get measures from params if available
        measures = []
        if "data_frame" in params and hasattr(params["data_frame"], "columns"):
            # Try to infer measures from params
            if "y" in params and isinstance(params["y"], str):
                measures = [params["y"]]

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
        if format == "static" or format == "interactive":
            return chart_obj
        elif format == "json":
            import plotly.io

            return plotly.io.to_json(chart_obj)
        elif format in ["png", "svg"]:
            return chart_obj.to_image(format=format)
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )
