"""
Auto-detect Altair chart specifications based on query dimensions and measures.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from boring_semantic_layer.semantic_api.ops import SemanticTable


class SemanticChartRenderer(ABC):
    """Base class for semantic chart renderers."""

    def __init__(self, semantic_table: "SemanticTable"):
        self.semantic_table = semantic_table
        
    @property
    def dimensions_list(self) -> list:
        """Get dimensions as a list for chart operations."""
        return list(self.semantic_table.dimensions.keys()) if self.semantic_table.dimensions else []
    
    @property
    def measures_list(self) -> list:
        """Get measures as a list for chart operations.""" 
        return list(self.semantic_table.measures.keys()) if self.semantic_table.measures else []
    
    def _detect_time_dimension(self) -> tuple[bool, str | None]:
        """Detect time dimension using the semantic table's time_dimensions property."""
        time_dims = self.semantic_table.time_dimensions
        if time_dims:
            # Return the first time dimension found
            first_time_dim = next(iter(time_dims.keys()))
            return True, first_time_dim
        return False, None

    def _has_order_by(self) -> bool:
        """Check if the semantic table has order_by applied."""
        node = self.semantic_table._node
        while node is not None:
            if hasattr(node, "__class__") and "OrderBy" in node.__class__.__name__:
                return True
            if hasattr(node, "source"):
                node = node.source
            else:
                break
        return False

    @abstractmethod
    def _detect_chart_type(self, spec: Optional[Dict[str, Any]]) -> str:
        """Detect the appropriate chart type based on dimensions and measures."""
        pass

    @abstractmethod
    def _create_chart(self, chart_type: str, spec: Optional[Dict[str, Any]]) -> Any:
        """Create the chart object using the backend-specific library."""
        pass

    @abstractmethod
    def _format_output(
        self, chart: Any, format: str
    ) -> Union[Any, Dict[str, Any], bytes, str]:
        """Format the chart output according to the requested format."""
        pass

    def render(
        self, spec: Optional[Dict[str, Any]] = None, format: str = "static"
    ) -> Union[Any, Dict[str, Any], bytes, str]:
        """Main method to render a chart."""
        # Detect chart type
        chart_type = self._detect_chart_type(spec)

        # Create chart
        chart = self._create_chart(chart_type, spec)

        # Format output
        return self._format_output(chart, format)


class AltairChartRenderer(SemanticChartRenderer):
    """Altair-specific chart renderer."""

    def _detect_chart_type(self, spec: Optional[Dict[str, Any]]) -> str:
        """Detect chart type by generating Altair spec and extracting mark type."""
        base_spec = self._detect_altair_spec()
        return base_spec.get("mark", "bar")

    def _detect_altair_spec(self) -> Dict[str, Any]:
        """
        Detect an appropriate chart type and return an Altair specification.

        Returns:
            An Altair specification dict with appropriate chart type
        """
        num_dims = len(self.dimensions_list)
        num_measures = len(self.measures_list)

        # Single value - text display
        if num_dims == 0 and num_measures == 1:
            return {
                "mark": {"type": "text", "size": 40},
                "encoding": {
                    "text": {
                        "field": self.measures_list[0],
                        "type": "quantitative",
                    }
                },
            }

        # Use centralized time dimension detection
        has_time, time_dimension_name = self._detect_time_dimension()

        # Determine appropriate date format and axis config based on time grain
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
                            "field": time_dimension_name or self.dimensions_list[0],
                            "type": "temporal",
                            "axis": axis_config,
                        },
                        "y": {
                            "field": self.measures_list[0],
                            "type": "quantitative",
                        },
                        "tooltip": [
                            {
                                "field": time_dimension_name or self.dimensions_list[0],
                                "type": "temporal",
                                "format": date_format,
                            },
                            {
                                "field": self.measures_list[0],
                                "type": "quantitative",
                            },
                        ],
                    },
                }
            else:
                # Categorical - bar chart
                return {
                    "mark": "bar",
                    "encoding": {
                        "x": {
                            "field": self.dimensions_list[0],
                            "type": "ordinal",
                            "sort": None,
                        },
                        "y": {
                            "field": self.measures_list[0],
                            "type": "quantitative",
                        },
                        "tooltip": [
                            {
                                "field": self.dimensions_list[0],
                                "type": "nominal",
                            },
                            {
                                "field": self.measures_list[0],
                                "type": "quantitative",
                            },
                        ],
                    },
                }

        # Single dimension, multiple measures - grouped bar chart
        if num_dims == 1 and num_measures >= 2:
            return {
                "transform": [
                    {"fold": self.semantic_table.measures, "as": ["measure", "value"]}
                ],
                "mark": "bar",
                "encoding": {
                    "x": {
                        "field": self.dimensions_list[0],
                        "type": "ordinal",
                        "sort": None,
                    },
                    "y": {"field": "value", "type": "quantitative"},
                    "color": {"field": "measure", "type": "nominal"},
                    "xOffset": {"field": "measure"},
                    "tooltip": [
                        {"field": self.dimensions_list[0], "type": "nominal"},
                        {"field": "measure", "type": "nominal"},
                        {"field": "value", "type": "quantitative"},
                    ],
                },
            }

        # Time series with additional dimension(s) - multi-line chart
        if has_time and num_dims >= 2 and num_measures == 1:
            non_time_dims = [
                d
                for i, d in enumerate(self.semantic_table.dimensions)
                if i != time_dim_index
            ]
            tooltip_fields = [
                {
                    "field": time_dimension_name,
                    "type": "temporal", 
                    "format": date_format,
                },
                {"field": non_time_dims[0], "type": "nominal"},
                {"field": self.measures_list[0], "type": "quantitative"},
            ]
            return {
                "mark": "line",
                "encoding": {
                    "x": {
                        "field": time_dimension_name,
                        "type": "temporal",
                        "axis": axis_config,
                    },
                    "y": {
                        "field": self.measures_list[0],
                        "type": "quantitative",
                    },
                    "color": {"field": non_time_dims[0], "type": "nominal"},
                    "tooltip": tooltip_fields,
                },
            }

        # Two dimensions, one measure - heatmap
        if num_dims == 2 and num_measures == 1:
            return {
                "mark": "rect",
                "encoding": {
                    "x": {
                        "field": self.dimensions_list[0],
                        "type": "ordinal",
                        "sort": None,
                    },
                    "y": {
                        "field": self.dimensions_list[1],
                        "type": "ordinal",
                        "sort": None,
                    },
                    "color": {
                        "field": self.measures_list[0],
                        "type": "quantitative",
                    },
                    "tooltip": [
                        {"field": self.dimensions_list[0], "type": "nominal"},
                        {"field": self.dimensions_list[1], "type": "nominal"},
                        {
                            "field": self.measures_list[0],
                            "type": "quantitative",
                        },
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

    def _create_chart(
        self, chart_type: str, spec: Optional[Dict[str, Any]]
    ) -> "altair.Chart":
        """Create Altair chart."""
        try:
            import altair as alt
        except ImportError:
            raise ImportError(
                "Altair is required for chart creation. "
                "Install it with: pip install 'boring-semantic-layer[viz-altair]'"
            )

        # Always start with auto-detected spec as base
        base_spec = self._detect_altair_spec()

        if spec is None:
            spec = base_spec
        else:
            if "mark" not in spec.keys():
                spec["mark"] = base_spec["mark"]
            if "encoding" not in spec.keys():
                spec["encoding"] = base_spec["encoding"]
            else:
                # Deep merge encodings - user spec overrides base
                base_enc = base_spec.get("encoding", {})
                user_enc = spec["encoding"]
                spec["encoding"] = {**base_enc, **user_enc}

        # Execute data and create Altair chart from specification
        df = self.semantic_table.execute()
        chart = alt.Chart(df).mark_text()  # Default mark
        if "mark" in spec:
            if isinstance(spec["mark"], dict):
                chart = alt.Chart(df).mark_text(**spec["mark"])
            else:
                mark_method = getattr(alt.Chart(df), f"mark_{spec['mark']}", None)
                if mark_method:
                    chart = mark_method()

        # Apply encoding
        if "encoding" in spec:
            chart = chart.encode(**spec["encoding"])

        # Apply any transforms
        if "transform" in spec:
            for transform in spec["transform"]:
                if "fold" in transform:
                    chart = chart.transform_fold(
                        transform["fold"], as_=transform.get("as", ["key", "value"])
                    )

        return chart

    def _format_output(
        self, chart: "altair.Chart", format: str
    ) -> Union["altair.Chart", Dict[str, Any], bytes, str]:
        """Format Altair chart output."""
        if format in ["static", "interactive"]:
            if format == "interactive":
                chart = chart.interactive()
            return chart
        elif format == "json":
            return chart.to_dict()
        elif format in ["png", "svg"]:
            return chart.save(format=format, fp=None)
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )


class PlotlyChartRenderer(SemanticChartRenderer):
    """Plotly-specific chart renderer."""

    def _detect_chart_type(self, spec: Optional[Dict[str, Any]]) -> str:
        """Detect chart type for Plotly."""
        # Extract chart_type from spec if provided
        if spec is not None and "chart_type" in spec:
            return spec["chart_type"]

        return self._detect_plotly_chart_type()

    def _detect_plotly_chart_type(self) -> str:
        """
        Auto-detect appropriate chart type based on query structure for Plotly backend.

        Returns:
            str: Chart type identifier ("bar", "line", "heatmap", "table", "indicator")
        """
        num_dims = len(self.semantic_table.dimensions)
        num_measures = len(self.semantic_table.measures)

        # Single value - indicator
        if num_dims == 0 and num_measures == 1:
            return "indicator"

        # Use centralized time dimension detection  
        has_time, _ = self._detect_time_dimension()

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

    def _prepare_data_and_params(self, chart_type: str) -> tuple:
        """
        Prepare data and base parameters for Plotly Express.

        Args:
            chart_type: The chart type string (bar, line, heatmap, etc.)

        Returns:
            tuple: (dataframe, base_params) where:
                - dataframe: Processed pandas DataFrame ready for plotting
                - base_params: Dict of parameters for Plotly Express functions
        """
        import pandas as pd

        df = self.semantic_table.execute()

        # Handle data sorting for line charts to avoid zigzag connections
        if chart_type == "line" and self.semantic_table.dimensions:
            has_time, time_dimension_name = self._detect_time_dimension()
            if has_time and time_dimension_name:
                # Sort by time dimension for temporal data
                df = df.sort_values(by=time_dimension_name)
            elif self._has_order_by():
                # Data is already sorted by the query execution
                pass
            else:
                # For categorical data converted to line, sort by x-axis for consistency
                df = df.sort_values(by=self.dimensions_list[0])

        # Build minimal base parameters that Plotly Express needs
        base_params = {"data_frame": df}

        if chart_type in ["bar", "line", "scatter"]:
            if self.semantic_table.dimensions:
                base_params["x"] = self.dimensions_list[0]
            if self.semantic_table.measures:
                if len(self.semantic_table.measures) == 1:
                    base_params["y"] = self.measures_list[0]
                else:
                    # For multiple measures, we need to reshape data for grouped charts
                    # Melt the dataframe to long format

                    id_cols = [
                        col
                        for col in df.columns
                        if col not in self.semantic_table.measures
                    ]
                    df_melted = pd.melt(
                        df,
                        id_vars=id_cols,
                        value_vars=self.semantic_table.measures,
                        var_name="measure",
                        value_name="value",
                    )
                    base_params["data_frame"] = df_melted
                    base_params["y"] = "value"
                    base_params["color"] = "measure"
                    # Update df reference for return
                    df = df_melted

            # Handle multiple traces for time series with categories
            has_time, time_dimension_name = self._detect_time_dimension()
            if has_time and len(self.semantic_table.dimensions) >= 2:
                non_time_dims = [
                    d
                    for d in self.semantic_table.dimensions
                    if d != time_dimension_name
                ]
                if non_time_dims:
                    base_params["color"] = non_time_dims[0]

        elif chart_type == "heatmap":
            if (
                len(self.semantic_table.dimensions) >= 2
                and self.semantic_table.measures
            ):
                # Use pivot table to create proper heatmap matrix with NaN for missing values

                pivot_df = df.pivot(
                    index=self.dimensions_list[1],
                    columns=self.dimensions_list[0],
                    values=self.measures_list[0],
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

    def _create_chart(
        self, chart_type: str, spec: Optional[Dict[str, Any]]
    ) -> "go.Figure":
        """Create Plotly chart."""
        try:
            import plotly
            import plotly.graph_objects as go
            import plotly.express as px
        except ImportError:
            raise ImportError(
                "plotly is required for chart creation. "
                "Install it with: pip install 'boring-semantic-layer[viz-plotly]'"
            )

        # Prepare data and base parameters
        df, base_params = self._prepare_data_and_params(chart_type)

        # Merge base params with user-provided Plotly Express parameters
        user_params = {}
        layout_params = {}
        config_params = {}

        if spec is not None:
            for k, v in spec.items():
                if k == "chart_type":
                    pass  # Already handled above
                elif k == "layout":
                    layout_params = v
                elif k == "config":
                    config_params = v
                else:
                    user_params[k] = v

        # Final parameters for Plotly Express - user params override base params
        final_params = {**base_params, **user_params}

        # Create the actual Plotly figure
        if chart_type == "indicator":
            raise NotImplementedError(
                "Indicator charts are not yet supported for Plotly backend"
            )
        elif chart_type == "bar":
            fig = px.bar(**final_params)
            # For multiple measures, set barmode to 'group'
            if len(self.semantic_table.measures) > 1:
                fig.update_layout(barmode="group")
        elif chart_type == "line":
            fig = px.line(**final_params)
        elif chart_type == "scatter":
            fig = px.scatter(**final_params)
        elif chart_type == "heatmap":
            fig = go.Figure(data=go.Heatmap(**final_params))
        elif chart_type == "table":
            # Special case for table - doesn't use Plotly Express
            columns = user_params.get(
                "columns", self.dimensions_list + self.measures_list
            )
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(values=columns),
                        cells=dict(
                            values=[df[col] for col in columns if col in df.columns]
                        ),
                    )
                ]
            )
        else:
            # Fallback
            fig = px.scatter(**final_params)

        if layout_params:
            fig.update_layout(**layout_params)
        if config_params:
            fig.update_layout(**config_params)

        return fig

    def _format_output(
        self, fig: "go.Figure", format: str
    ) -> Union["go.Figure", Dict[str, Any], bytes, str]:
        """Format Plotly chart output."""
        try:
            import plotly
        except ImportError:
            raise ImportError("plotly is required")

        if format == "static":
            return fig
        elif format == "interactive":
            return fig
        elif format == "json":
            return plotly.io.to_json(fig)
        elif format in ["png", "svg"]:
            return fig.to_image(format=format)
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )
