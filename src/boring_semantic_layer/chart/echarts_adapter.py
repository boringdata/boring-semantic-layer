"""
ECharts adapter for the ChartBackend interface.

Provides an adapter that wraps EChartsBackend to implement the ChartBackend
interface, allowing ECharts to be used as a chart backend in the semantic layer.
"""

from collections.abc import Sequence
import json
from typing import Any

from .base import ChartBackend
from .echarts import EChartsBackend
from .utils import (
    get_non_time_dimensions,
    has_time_dimension,
    sanitize_field_name_for_vega,
)


class EChartsAdapter(ChartBackend):
    """
    Adapter that wraps EChartsBackend to implement ChartBackend interface.
    
    This allows ECharts to be used as a standard chart backend in the
    semantic layer visualization system.
    """
    
    def __init__(self):
        self._backend = EChartsBackend()
    
    def detect_chart_type(
        self,
        dimensions: Sequence[str],
        measures: Sequence[str],
        time_dimension: str | None = None,
    ) -> str:
        """
        Detect appropriate chart type based on query structure.
        
        Args:
            dimensions: List of dimension field names from the query
            measures: List of measure field names from the query
            time_dimension: Optional time dimension field name
            
        Returns:
            Chart type string (bar, line, pie, scatter)
        """
        num_dims = len(dimensions)
        num_measures = len(measures)
        
        # Single value - no chart
        if num_dims == 0 and num_measures == 1:
            return None
        
        # Check if we have a time dimension
        has_time = has_time_dimension(list(dimensions), time_dimension)
        
        # Time series - line chart
        if has_time:
            return "line"
        
        # Single dimension, single measure - bar chart
        if num_dims == 1 and num_measures == 1:
            return "bar"
        
        # Single dimension, multiple measures - bar chart (grouped)
        if num_dims == 1 and num_measures >= 2:
            return "bar"
        
        # Two dimensions, one measure - heatmap not yet supported, use bar
        if num_dims == 2 and num_measures == 1:
            return "bar"
        
        # Default to bar
        return "bar"
    
    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: str,
        time_dimension: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Prepare dataframe and parameters for chart creation.
        
        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: Chart type string
            time_dimension: Optional time dimension name
            
        Returns:
            tuple: (processed_dataframe, parameters_dict)
        """
        # Sanitize column names (replace dots with underscores)
        column_mapping = {col: sanitize_field_name_for_vega(col) for col in df.columns}
        df = df.rename(columns=column_mapping)
        
        # Map old names to new names for dimensions and measures
        sanitized_dims = [sanitize_field_name_for_vega(d) for d in dimensions]
        sanitized_measures = [sanitize_field_name_for_vega(m) for m in measures]
        sanitized_time_dim = (
            sanitize_field_name_for_vega(time_dimension) if time_dimension else None
        )
        
        params = {
            "dimensions": sanitized_dims,
            "measures": sanitized_measures,
            "time_dimension": sanitized_time_dim,
        }
        
        return df, params
    
    def create_chart(
        self,
        df: Any,
        params: dict[str, Any],
        chart_type: str,
        spec: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Create ECharts specification.
        
        Args:
            df: Processed dataframe from prepare_data
            params: Parameters dict from prepare_data
            chart_type: Chart type string
            spec: Optional custom specification to merge
            
        Returns:
            ECharts option specification dict
        """
        dimensions = params.get("dimensions", [])
        measures = params.get("measures", [])
        time_dimension = params.get("time_dimension")
        
        # Determine x and y fields
        if dimensions:
            x_field = dimensions[0]
        else:
            x_field = None
            
        if measures:
            y_field = measures[0]
        else:
            y_field = None
        
        # Handle color field for multi-series
        color_field = None
        if len(dimensions) > 1 and time_dimension and dimensions[0] == time_dimension:
            # Time series with color grouping
            non_time_dims = get_non_time_dimensions(dimensions, time_dimension)
            if non_time_dims:
                color_field = non_time_dims[0]
        elif len(dimensions) > 1:
            # Multiple dimensions - use second as color
            color_field = dimensions[1]
        
        # Build kwargs for generate_spec
        kwargs = {}
        if x_field:
            kwargs["x"] = x_field
        if y_field:
            kwargs["y"] = y_field
        if color_field:
            kwargs["color"] = color_field
        
        # Handle multiple measures by folding/transforming
        if len(measures) > 1 and len(dimensions) == 1:
            # Transform data to long format for multiple measures
            import pandas as pd
            df_long = df.melt(
                id_vars=dimensions,
                value_vars=measures,
                var_name="measure",
                value_name="value"
            )
            kwargs["x"] = dimensions[0]
            kwargs["y"] = "value"
            kwargs["color"] = "measure"
            df = df_long
        
        # Merge any custom spec options
        if spec:
            # Extract chart_type override if present
            if "chart_type" in spec:
                chart_type = spec["chart_type"]
            # Pass other options as overrides
            overrides = {k: v for k, v in spec.items() if k != "chart_type"}
            kwargs["overrides"] = overrides
        
        # Generate the spec
        echarts_spec = self._backend.generate_spec(
            df,
            chart_type=chart_type,
            **kwargs
        )
        
        return echarts_spec
    
    def format_output(
        self,
        chart_obj: dict[str, Any],
        format: str = "static",
    ) -> Any:
        """
        Format chart output according to requested format.
        
        Args:
            chart_obj: ECharts option specification dict
            format: Output format ("static", "json", "interactive")
            
        Returns:
            Formatted chart (dict for static/interactive, JSON string for json)
        """
        if format == "json":
            return json.dumps(chart_obj, default=str)
        
        # For static and interactive, return the spec dict
        # The consumer (e.g., frontend) will handle rendering
        return chart_obj
