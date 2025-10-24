"""MCP functionality for semantic models."""

from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List, Optional, Tuple, Mapping, Sequence
from .query import _find_time_dimension


class MCPSemanticModel(FastMCP):
    """
    MCP server specialized for semantic models using SemanticTable.

    Provides tools:
    - list_models: list all model names
    - get_model: get model metadata (dimensions, measures, time dimensions)
    - get_time_range: get available time range for time dimensions
    - query_model: execute queries with time_grain and time_range support
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
            return {name: f"Semantic model: {name}" for name in self.models.keys()}

        @self.tool()
        def get_model(model_name: str) -> Mapping[str, Any]:
            """Get details about a specific semantic model including dimensions and measures."""
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Build dimension info with metadata
            dimensions = {}
            for name, dim in model._dims_dict().items():
                dimensions[name] = {
                    "description": dim.description,
                    "is_time_dimension": dim.is_time_dimension,
                    "smallest_time_grain": dim.smallest_time_grain,
                }

            # Build measure info with metadata
            measures = {}
            for name, meas in model._measures_dict().items():
                measures[name] = {"description": meas.description}

            return {
                "name": model.name or "unnamed",
                "dimensions": dimensions,
                "measures": measures,
                "calculated_measures": list(model._calc_measures_dict().keys()),
            }

        @self.tool()
        def get_time_range(model_name: str) -> Mapping[str, Any]:
            """Get the available time range for a model's time dimension."""
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Find first time dimension
            all_dims = list(model._dims_dict().keys())
            time_dim_name = _find_time_dimension(model, all_dims)

            if not time_dim_name:
                raise ValueError(f"Model {model_name} has no time dimension")

            # Get the dimension expression
            time_dim = model._dims_dict()[time_dim_name]

            # Get min/max from base table
            tbl = model.table.to_expr()
            time_col = time_dim.expr(tbl)
            result = tbl.aggregate(start=time_col.min(), end=time_col.max()).execute()

            return {
                "start": result["start"].iloc[0].isoformat(),
                "end": result["end"].iloc[0].isoformat(),
            }

        @self.tool()
        def query_model(
            model_name: str,
            dimensions: Optional[Sequence[str]] = None,
            measures: Optional[Sequence[str]] = None,
            filters: Optional[Sequence[Mapping[str, Any]]] = None,
            order_by: Optional[Sequence[Tuple[str, str]]] = None,
            limit: Optional[int] = None,
            time_grain: Optional[str] = None,
            time_range: Optional[Mapping[str, str]] = None,
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

            Returns:
                Query results as JSON string
            """
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Execute query using the query interface
            result = model.query(
                dimensions=dimensions,
                measures=measures,
                filters=filters or [],
                order_by=order_by,
                limit=limit,
                time_grain=time_grain,
                time_range=time_range,
            ).execute()

            # Format as JSON
            return result.to_json(orient="records", date_format="iso")


def create_mcp_server(
    models: Mapping[str, Any], name: str = "Semantic Layer MCP Server"
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
