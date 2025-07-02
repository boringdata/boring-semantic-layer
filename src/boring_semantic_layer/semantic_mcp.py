"""MCP server implementation for semantic models."""

from mcp.server.fastmcp import FastMCP
from typing import Dict, List, Optional, Union, Tuple, Any, Literal
from typing import Annotated

from .semantic_model import SemanticModel


class MCPSemanticModel(FastMCP):
    """
    MCP server specialized for semantic models.

    Provides pre-defined tools for interacting with semantic models:
    - list_models: List all available semantic model names
    - get_model: Get model metadata and schema information
    - get_time_range: Get available time range for time-series data
    - query_model: Execute queries with dimensions, measures, and filters

    Example:
        >>> from boring_semantic_layer import SemanticModel, MCPSemanticModel
        >>>
        >>> # Create semantic models
        >>> flights_sm = SemanticModel(
        ...     name="flights",
        ...     table=flights_table,
        ...     dimensions={...},
        ...     measures={...}
        ... )
        >>>
        >>> # Create MCP server
        >>> mcp_server = MCPSemanticModel(
        ...     models={"flights": flights_sm},
        ...     name="Flight Data Server"
        ... )
        >>>
        >>> # Run server
        >>> mcp_server.run()
    """

    def __init__(
        self,
        models: Dict[str, SemanticModel],
        name: str = "Semantic Layer MCP Server",
        *args,
        **kwargs,
    ):
        """
        Initialize MCP server with semantic models.

        Args:
            models: Dictionary mapping model names to SemanticModel instances
            name: Name of the MCP server
            *args, **kwargs: Additional arguments passed to FastMCP
        """
        super().__init__(name, *args, **kwargs)
        self.models = models
        self._register_tools()

    def _register_tools(self):
        """Register the standard semantic layer tools."""

        @self.tool()
        def list_models() -> List[str]:
            """List all available semantic model names."""
            return list(self.models.keys())

        @self.tool()
        def get_model(model_name: str) -> Dict[str, Any]:
            """Get details about a specific semantic model including available dimensions and measures."""
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            return self.models[model_name].json_definition

        @self.tool()
        def get_time_range(model_name: str) -> Dict[str, Any]:
            """Get the available time range for a model's time dimension.

            Returns:
                A dictionary with 'start' and 'end' dates in ISO format, or an error if the model has no time dimension
            """
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            return self.models[model_name].get_time_range()

        @self.tool()
        def query_model(
            model_name: str,
            dimensions: Optional[List[str]] = [],
            measures: Optional[List[str]] = [],
            filters: Annotated[
                Optional[Union[Dict, List[Dict]]],
                """
                List of JSON filter objects with the following structure:
                   
                Simple Filter:
                {
                    "field": "dimension_name",  # Can include join references like "customer.country" or time dimensions like "order_date"
                    "operator": "=",            # One of: =, !=, >, >=, <, <=, in, not in, like, not like, is null, is not null
                    "value": "value"            # For non-'in' operators. For dates use ISO format: "2024-03-21" or "2024-03-21T14:30:00"
                    # OR
                    "values": ["val1", "val2"]  # For 'in' operator only
                }
                   
                Compound Filter (AND/OR):
                {
                    "operator": "AND",          # or "OR"
                    "conditions": [             # Non-empty list of other filter objects
                        {
                            "field": "country",
                            "operator": "=",
                            "value": "US"
                        },
                        {
                            "field": "tier",
                            "operator": "in",
                            "values": ["gold", "platinum"]
                        }
                    ]
                }
                   
                Example of a complex nested filter with time ranges:
                [{
                    "operator": "AND",
                    "conditions": [
                        {
                            "operator": "AND",
                            "conditions": [
                                {"field": "flight_date", "operator": ">=", "value": "2024-01-01"},
                                {"field": "flight_date", "operator": "<", "value": "2024-04-01"}
                            ]
                        },
                        {"field": "carrier.country", "operator": "=", "value": "US"}
                    ]
                }]
                """,
            ] = [],
            order_by: Annotated[
                List[Tuple[str, str]],
                "The order by clause to apply to the query (list of tuples: [('field', 'asc|desc')]",
            ] = [],
            limit: Annotated[int, "The limit to apply to the query"] = None,
            time_range: Annotated[
                Optional[Dict[str, str]],
                """Optional time range filter with format:
                    {
                        "start": "2024-01-01T00:00:00Z",  # ISO 8601 format
                        "end": "2024-12-31T23:59:59Z"     # ISO 8601 format
                    }
                    
                    Using time_range is preferred over using filters for time-based filtering because:
                    1. It automatically applies to the model's primary time dimension
                    2. It ensures proper time zone handling with ISO 8601 format
                    3. It's more concise than creating complex filter conditions
                    4. It works seamlessly with time_grain parameter for time-based aggregations
                """,
            ] = None,
            time_grain: Annotated[
                Optional[
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
                ],
                "Optional time grain to use for time-based dimensions",
            ] = None,
        ) -> List[Dict[str, Any]]:
            """Query a semantic model with JSON-based filtering.

            Args:
                model_name: The name of the model to query.
                dimensions: The dimensions to group by. Can include time dimensions like "flight_date", "flight_month", "flight_year".
                measures: The measures to aggregate.
                filters: List of JSON filter objects (see detailed description above).
                order_by: The order by clause to apply to the query (list of tuples: [("field", "asc|desc")]).
                limit: The limit to apply to the query (integer).
                time_range: Optional time range filter for time dimensions. Preferred over using filters for time-based filtering.
                time_grain: Optional time grain for time-based dimensions (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).

            Example queries:
            ```python
            # Query with time dimension grouping and time range (preferred approach)
            query_model(
                model_name="flights",
                dimensions=["flight_month", "carrier"],  # Group by month and carrier
                measures=["total_delay", "avg_delay"],
                time_range={
                    "start": "2024-01-01T00:00:00Z",  # ISO 8601 format ensures proper timezone handling
                    "end": "2024-03-31T23:59:59Z"
                },
                time_grain="TIME_GRAIN_DAY",  # Automatically applies to time dimensions
                order_by=[("avg_delay", "desc")],
                limit=10
            )

            # Query combining time_range with regular filters
            query_model(
                model_name="flights",
                dimensions=["carrier", "destination"],
                measures=["total_delay", "avg_delay"],
                time_range={
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-03-31T23:59:59Z"
                },
                time_grain="TIME_GRAIN_DAY",
                filters=[{
                    "field": "carrier.country",
                    "operator": "=",
                    "value": "US"
                }],
                order_by=[("avg_delay", "desc")],
                limit=10
            )
            ```

            Raises:
                ValueError: If any filter object doesn't match the required structure or model not found
            """
            if not isinstance(order_by, list):
                raise ValueError("order_by must be a list of tuples")
            for item in order_by:
                if not (isinstance(item, (list, tuple)) and len(item) == 2):
                    raise ValueError(
                        "Each order_by item must be a tuple of (field, direction)"
                    )
                field, direction = item
                if not isinstance(field, str) or direction not in ("asc", "desc"):
                    raise ValueError(
                        "Each order_by tuple must be (field: str, direction: 'asc' or 'desc')"
                    )

            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Validate time grain if provided
            if time_grain and model.smallest_time_grain:
                grain_order = [
                    "TIME_GRAIN_SECOND",
                    "TIME_GRAIN_MINUTE",
                    "TIME_GRAIN_HOUR",
                    "TIME_GRAIN_DAY",
                    "TIME_GRAIN_WEEK",
                    "TIME_GRAIN_MONTH",
                    "TIME_GRAIN_QUARTER",
                    "TIME_GRAIN_YEAR",
                ]
                if grain_order.index(time_grain) < grain_order.index(
                    model.smallest_time_grain
                ):
                    raise ValueError(
                        f"Time grain {time_grain} is smaller than model's smallest allowed grain {model.smallest_time_grain}"
                    )

            output_df = model.query(
                dimensions=dimensions,
                measures=measures,
                filters=filters,
                order_by=order_by,
                limit=limit,
                time_range=time_range,
                time_grain=time_grain,
            ).execute()
            return output_df.to_dict(orient="records")
