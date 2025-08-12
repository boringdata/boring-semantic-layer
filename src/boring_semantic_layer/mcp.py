"""MCP functionality for semantic models."""

from mcp.server.fastmcp import FastMCP
from typing import Annotated, Any, Dict, List, Optional, Union, Tuple, Literal
from .time_grain import TIME_GRAIN_ORDER

from .semantic_model import SemanticModel

class MCPSemanticModel(FastMCP):
    """
    MCP server specialized for semantic models.

    Provides tools:
    - list_models: list all model names
    - get_model: get model metadata
    - get_time_range: get available time range
    - query_model: execute queries and return records and optional charts
    """

    def __init__(
        self,
        models: Dict[str, SemanticModel],
        name: str = "Semantic Layer MCP Server",
        *args,
        **kwargs,
    ):
        super().__init__(name, *args, **kwargs)
        self.models = models
        self._register_tools()

    def _register_tools(self):
        @self.tool()
        def list_models() -> List[str]:
            return list(self.models.keys())

        @self.tool()
        def get_model(model_name: str) -> Dict[str, Any]:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            return self.models[model_name].json_definition

        @self.tool()
        def get_time_range(model_name: str) -> Dict[str, Any]:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            return self.models[model_name].get_time_range()

        @self.tool()
        def query_model(
            model_name: str,
            dimensions: List[str] = [],
            measures: List[str] = [],
            filters: Union[Dict[str, Any], List[Dict[str, Any]]] = [],
            order_by: List[Tuple[str, str]] = [],
            limit: Optional[int] = None,
            time_range: Optional[Dict[str, str]] = None,
            time_grain: Optional[
                Literal[
                    "TIME_GRAIN_YEAR",
                    "TIME_GRAIN_QUARTER",
                    "TIME_GRAIN_MONTH",
                    "TIME_GRAIN_WEEK",
                    "TIME_GRAIN_DAY",
                    "TIME_GRAIN_HOUR",
                    "TIME_GRAIN_MINUTE",
                    "TIME_GRAIN_SECOND",
                ]
            ] = None,
            chart_spec: Union[bool, Dict[str, Any], None] = None,
            chart_format: Literal["altair", "interactive", "json", "png", "svg"] = "json",
        ) -> Dict[str, Any]:
            # Validate model existence
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            # Validate time_grain is not finer than allowed
            smallest = self.models[model_name].smallest_time_grain
            if time_grain is not None and smallest is not None:
                if TIME_GRAIN_ORDER.index(time_grain) < TIME_GRAIN_ORDER.index(smallest):
                    raise ValueError(
                        f"Time grain {time_grain} is smaller than model's smallest allowed grain {smallest}"
                    )
            # Validate order_by directions
            for item in order_by:
                # item is a tuple (field, direction)
                _, direction = item
                if not isinstance(direction, str) or direction not in ("asc", "desc"):
                    raise ValueError(
                        "Each order_by tuple must be (field: str, direction: 'asc' or 'desc')"
                    )
            # Build and execute query
            query = self.models[model_name].query(
                dimensions=dimensions,
                measures=measures,
                filters=filters,
                order_by=order_by,
                limit=limit,
                time_range=time_range,
                time_grain=time_grain,
            )
            records = query.execute().to_dict(orient="records")
            output: Dict[str, Any] = {"records": records}
            # Generate chart if requested
            if chart_spec is not None:
                spec = None if chart_spec is True else chart_spec  # type: ignore
                chart = query.chart(spec=spec, format=chart_format)  # type: ignore
                # Decode bytes (e.g., PNG) to string
                if isinstance(chart, (bytes, bytearray)):
                    chart = chart.decode("utf-8")
                output["chart"] = chart
            return output