"""MCP functionality for semantic models."""

from collections.abc import Mapping, Sequence
from typing import Any

from fastmcp import FastMCP

from .query import _find_time_dimension


class MCPSemanticModel(FastMCP):
    """
    MCP server specialized for semantic models using SemanticTable.

    Provides tools:
    - list_models: list all model names
    - get_model: get model metadata (dimensions, measures, time dimensions)
    - get_time_range: get available time range for time dimensions
    - query_model: execute queries with time_grain, time_range, and chart_spec support
    """

    def __init__(
        self,
        models: Mapping[str, Any],
        name: str = "Semantic Layer MCP Server",
        *args,
        **kwargs,
    ):
        super().__init__(name, *args, **kwargs)
        self.models = models
        self._register_tools()

    def _register_tools(self):
        @self.tool()
        def list_models() -> Mapping[str, str]:
            """List all available semantic model names."""
            return {name: f"Semantic model: {name}" for name in self.models}

        @self.tool()
        def get_model(model_name: str) -> Mapping[str, Any]:
            """Get details about a specific semantic model including dimensions and measures."""
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Build dimension info with metadata
            dimensions = {}
            for name, dim in model.get_dimensions().items():
                dimensions[name] = {
                    "description": dim.description,
                    "is_time_dimension": dim.is_time_dimension,
                    "smallest_time_grain": dim.smallest_time_grain,
                }

            # Build measure info with metadata
            measures = {}
            for name, meas in model.get_measures().items():
                measures[name] = {"description": meas.description}

            return {
                "name": model.name or "unnamed",
                "dimensions": dimensions,
                "measures": measures,
                "calculated_measures": list(model.get_calculated_measures().keys()),
            }

        @self.tool()
        def get_time_range(model_name: str) -> Mapping[str, Any]:
            """Get the available time range for a model's time dimension."""
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Find first time dimension
            all_dims = list(model.dimensions)  # dimensions is now a tuple
            time_dim_name = _find_time_dimension(model, all_dims)

            if not time_dim_name:
                raise ValueError(f"Model {model_name} has no time dimension")

            # Get the dimension expression
            time_dim = model.get_dimensions()[time_dim_name]

            # Get min/max from base table
            tbl = model.table  # Already an expression
            time_col = time_dim.expr(tbl)
            result = tbl.aggregate(start=time_col.min(), end=time_col.max()).execute()

            return {
                "start": result["start"].iloc[0].isoformat(),
                "end": result["end"].iloc[0].isoformat(),
            }

        @self.tool()
        def query_model(
            model_name: str,
            dimensions: Sequence[str] | None = None,
            measures: Sequence[str] | None = None,
            filters: Sequence[Mapping[str, Any]] | None = None,
            order_by: Sequence[tuple[str, str]] | None = None,
            limit: int | None = None,
            time_grain: str | None = None,
            time_range: Mapping[str, str] | None = None,
            chart_spec: Mapping[str, Any] | None = None,
        ) -> str:
            """
            Query a semantic model with support for filters and time dimensions.

            Args:
                model_name: Name of the model to query
                dimensions: List of dimension names to group by
                measures: List of measure names to aggregate
                filters: List of filter dicts (e.g., [{"field": "carrier", "operator": "=", "value": "AA"}])
                order_by: List of (field, direction) tuples
                limit: Maximum number of rows to return
                time_grain: Optional time grain (e.g., "TIME_GRAIN_MONTH")
                time_range: Optional time range with 'start' and 'end' keys
                chart_spec: Optional chart specification dict. When provided, returns both data and chart.
                           Format: {"backend": "altair"|"plotly", "spec": {...}, "format": "json"|"static"}

            Returns:
                When chart_spec is None: Query results as JSON string ({"records": [...]})
                When chart_spec is provided: JSON with both records and chart ({"records": [...], "chart": {...}})
            """
            import json

            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Execute query using the query interface
            query_result = model.query(
                dimensions=dimensions,
                measures=measures,
                filters=filters or [],
                order_by=order_by,
                limit=limit,
                time_grain=time_grain,
                time_range=time_range,
            )

            # Get the data
            result_df = query_result.execute()
            records = json.loads(result_df.to_json(orient="records", date_format="iso"))

            # If chart_spec is not provided, return only records
            if chart_spec is None:
                return json.dumps({"records": records})

            # Generate chart if chart_spec is provided
            backend = chart_spec.get("backend", "altair")
            spec = chart_spec.get("spec")
            format_type = chart_spec.get("format", "json")

            chart_result = query_result.chart(spec=spec, backend=backend, format=format_type)

            # For JSON format, extract the spec
            if format_type == "json":
                if backend == "altair":
                    chart_data = chart_result
                else:  # plotly returns JSON string, need to parse it
                    chart_data = (
                        json.loads(chart_result) if isinstance(chart_result, str) else chart_result
                    )

                return json.dumps({"records": records, "chart": chart_data})
            else:
                # For other formats (static, interactive), we can't serialize directly
                # Return a message indicating the chart type
                return json.dumps(
                    {
                        "records": records,
                        "chart": {
                            "backend": backend,
                            "format": format_type,
                            "message": f"Chart generated as {format_type} format. Use format='json' for serializable output.",
                        },
                    }
                )


def create_mcp_server(
    models: Mapping[str, Any],
    name: str = "Semantic Layer MCP Server",
) -> MCPSemanticModel:
    """
    Create an MCP server for semantic models.

    Args:
        models: Dictionary mapping model names to SemanticTable instances
        name: Name of the MCP server

    Returns:
        MCPSemanticModel instance ready to serve
    """
    return MCPSemanticModel(models=models, name=name)
