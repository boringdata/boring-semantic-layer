"""MCP functionality for semantic models."""

from fastmcp import FastMCP
from typing import Annotated, Any, Dict, List, Optional, Union, Literal

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
        def list_models() -> Dict[str, str]:
            """List all available semantic model names with their descriptions."""
            return {
                name: model.description or "No description available"
                for name, model in self.models.items()
            }

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
            dimensions: List[str] = [],
            measures: List[str] = [],
            filters: Annotated[
                Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
                """
                List of JSON filter objects with the following structure:

                Simple Filter:
                {
                    "field": "dimension_name",  # Must be an existing dimension (check model schema first!).
                    "operator": "=",            # See operator list below - MUST use exact symbols shown
                    "value": "single_value"     # For single-value operators (=, !=, >, >=, <, <=, ilike, not ilike, like, not like)
                    # OR for 'in'/'not in' operators only:
                    "values": ["val1", "val2"]  # REQUIRED for 'in' and 'not in' operators
                }

                ⚠️ CRITICAL: OPERATOR SYMBOLS - Use EXACT symbols, NOT abbreviations:
                - Comparisons: Use ">", ">=", "<", "<=" (NOT "gt", "gte", "lt", "lte")
                - Equality: Use "=" or "eq" or "equals" (all work identically)
                - Not equal: Use "!=" or "ne" or "not equals"

                ⚠️ CRITICAL: value vs values field:
                - Single-value operators (=, !=, >, >=, <, <=, like, ilike): Use "value" (singular)
                - Multi-value operators (in, not in): Use "values" (plural) with array
                - WRONG: {"field": "x", "operator": "=", "values": ["y"]} ❌
                - RIGHT: {"field": "x", "operator": "=", "value": "y"} ✅
                - WRONG: {"field": "x", "operator": "in", "value": "y"} ❌
                - RIGHT: {"field": "x", "operator": "in", "values": ["y", "z"]} ✅

                Available operators (use exact symbols):
                - "=" / "eq" / "equals": exact match → use "value" field
                - "!=" / "ne" / "not equals": not equal → use "value" field
                - ">": greater than → use "value" field (NOT "gt")
                - ">=": greater than or equal → use "value" field (NOT "gte")
                - "<": less than → use "value" field (NOT "lt")
                - "<=": less than or equal → use "value" field (NOT "lte")
                - "in": value is in list → use "values" array (NOT "value")
                - "not in": value not in list → use "values" array (NOT "value")
                - "ilike": case-insensitive pattern → use "value" field with % wildcards
                - "not ilike": negated case-insensitive → use "value" field with % wildcards
                - "like": case-sensitive pattern → use "value" field with % wildcards
                - "not like": negated case-sensitive → use "value" field with % wildcards
                - "is null": field is null → no value/values field needed
                - "is not null": field is not null → no value/values field needed

                COMMON MISTAKES TO AVOID:
                1. ❌ Using "gt", "gte", "lt", "lte" - Use ">", ">=", "<", "<=" instead
                2. ❌ Using "values" with "=" or comparison operators - Use "value" (singular)
                3. ❌ Using "value" with "in"/"not in" - Use "values" (plural) array
                4. ❌ Filtering on measures - Only filter on dimensions
                5. ❌ Using .month(), .year() etc. - Use time_grain parameter instead
                6. For text search, prefer "ilike" over "like" for case-insensitive matching
                    
                Compound Filter (AND/OR):
                {
                    "operator": "AND",          # or "OR"
                    "conditions": [             # Non-empty list of other filter objects
                        {
                            "field": "country",
                            "operator": "equals",   # or "=" or "eq" - all equivalent
                            "value": "US"           # Note: "value" singular
                        },
                        {
                            "field": "tier",
                            "operator": "in",       # MUST use "values" array for "in"
                            "values": ["gold", "platinum"]  # Note: "values" plural
                        },
                        {
                            "field": "name",
                            "operator": "ilike",    # Case-insensitive pattern matching
                            "value": "%john%"       # Note: "value" singular
                        }
                    ]
                }

                Correct examples:
                [
                    {"field": "status", "operator": "in", "values": ["active", "pending"]},
                    {"field": "name", "operator": "ilike", "value": "%smith%"},
                    {"field": "created_date", "operator": ">=", "value": "2024-01-01"},
                    {"field": "amount", "operator": ">", "value": 1000000},
                    {"field": "email", "operator": "not ilike", "value": "%spam%"}
                ]

                Wrong examples (DO NOT USE):
                [
                    {"field": "amount", "operator": "gt", "value": 1000000},  # ❌ Use ">" not "gt"
                    {"field": "status", "operator": "equals", "values": ["active"]},  # ❌ Use "value" not "values"
                    {"field": "tier", "operator": "in", "value": "gold"}  # ❌ Use "values" array not "value"
                ]
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
                        {"field": "carrier.country", "operator": "eq", "value": "US"}
                    ]
                }]
                """,
            ] = [],
            order_by: Annotated[
                List[List[str]],
                "The order by clause to apply to the query (list of lists: [['field', 'asc|desc']])",
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
                """Time grain for aggregating time-based dimensions.
                
                IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
                use this time_grain parameter to aggregate by time periods. The system will 
                automatically handle time dimension transformations.
                
                Examples:
                - For monthly data: time_grain="TIME_GRAIN_MONTH" 
                - For yearly data: time_grain="TIME_GRAIN_YEAR"
                - For daily data: time_grain="TIME_GRAIN_DAY"
                
                Then filter using the time_range parameter or regular date filters like:
                {"field": "date_column", "operator": ">=", "value": "2024-01-01"}
                """,
            ] = None,
            chart_spec: Annotated[
                Optional[Union[bool, Dict[str, Any]]],
                """Controls chart generation:
                - None/False: Returns {"records": [...]} (default)
                - True: Returns {"records": [...], "chart": {...}} with auto-detected chart
                - Dict: Returns {"records": [...], "chart": {...}} with custom Vega-Lite specification
                    Can be partial (e.g., just {"mark": "line"} or {"encoding": {"y": {"scale": {"zero": False}}}}).
                    BSL intelligently merges partial specs with auto-detected defaults.
                
                Common chart specifications:
                - {"mark": "bar"} - Bar chart
                - {"mark": "line"} - Line chart  
                - {"mark": "point"} - Scatter plot
                - {"mark": "rect"} - Heatmap
                - {"title": "My Chart"} - Add title
                - {"width": 600, "height": 400} - Set size
                - {"encoding": {"color": {"field": "category"}}} - Color by field
                - {"encoding": {"y": {"scale": {"zero": False}}}} - Don't start Y-axis at zero
                
                BSL auto-detection logic:
                - Time series (time dimension + measure) → Line chart
                - Categorical (1 dimension + 1 measure) → Bar chart
                - Multiple measures → Multi-series chart
                - Two dimensions → Heatmap
                """,
            ] = None,
            chart_format: Annotated[
                Optional[
                    Literal[
                        "altair",
                        "interactive",
                        "json",
                        "png",
                        "svg",
                    ]
                ],
                """Chart output format when chart_spec is provided:
                    - "altair": Altair Chart object (default)
                    - "interactive": Interactive Altair Chart with tooltips
                    - "json": Vega-Lite JSON specification
                    - "png": Base64-encoded PNG image {"format": "png", "data": "base64..."} (requires altair[all])
                    - "svg": SVG string {"format": "svg", "data": "svg..."} (requires altair[all])
                    """,
            ] = "json",
        ) -> Dict[str, Any]:
            """Query a semantic model with JSON-based filtering and optional visualization.

            Args:
                model_name: The name of the model to query.
                dimensions: The dimensions to group by. Can include time dimensions like "flight_date", "flight_month", "flight_year".
                measures: The measures to aggregate.
                filters: List of JSON filter objects (see detailed description above).
                order_by: The order by clause to apply to the query (list of lists: [["field", "asc|desc"]]).
                limit: The limit to apply to the query (integer).
                time_range: Optional time range filter for time dimensions. Preferred over using filters for time-based filtering.
                time_grain: Optional time grain for time-based dimensions (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
                chart_spec: Controls chart generation - None/False for data, True for auto-detected chart, or Dict for custom spec.
                chart_format: Output format when chart_spec is provided.

            Example queries:
            ```python
            # 1. Get data as records (default)
            query_model(
                model_name="flights",
                dimensions=["flight_month", "carrier"],
                measures=["total_delay", "avg_delay"],
                time_range={"start": "2024-01-01", "end": "2024-03-31"},
                time_grain="TIME_GRAIN_MONTH",
                order_by=[["avg_delay", "desc"]],
                limit=10
            )  # Returns {"records": [...]}

            # 2. Get data with auto-detected chart
            query_model(
                model_name="flights",
                dimensions=["date"],
                measures=["flight_count"],
                time_grain="TIME_GRAIN_WEEK",
                chart_spec=True  # Returns {"records": [...], "chart": {...}}
            )

            # 3. Get data with custom chart styling
            query_model(
                model_name="flights",
                dimensions=["date", "carrier"],
                measures=["on_time_rate"],
                filters=[{"field": "carrier", "operator": "in", "values": ["AA", "UA", "DL"]}],
                time_grain="TIME_GRAIN_MONTH",
                chart_spec={
                    "encoding": {"y": {"scale": {"zero": False}}},
                    "title": "Carrier Performance Comparison"
                },
                chart_format="interactive"  # Chart format in the response
            )

            # 4. Get data with PNG chart image
            query_model(
                model_name="flights",
                dimensions=["carrier"],
                measures=["flight_count"],
                chart_spec={"mark": "bar", "title": "Flight Count by Carrier"},
                chart_format="png"  # Chart will be {"format": "png", "data": "base64..."}
            )
            ```

            Raises:
                ValueError: If any filter object doesn't match the required structure or model not found
            """
            # Validate model existence
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            # Validate time_grain is not finer than allowed
            smallest = self.models[model_name].smallest_time_grain
            if time_grain is not None and smallest is not None:
                if TIME_GRAIN_ORDER.index(time_grain) < TIME_GRAIN_ORDER.index(
                    smallest
                ):
                    raise ValueError(
                        f"Time grain {time_grain} is smaller than model's smallest allowed grain {smallest}"
                    )
            # Validate order_by directions
            for item in order_by:
                # item is a list [field, direction]
                if len(item) != 2:
                    raise ValueError(
                        "Each order_by item must be a list with 2 elements: [field, direction]"
                    )
                _, direction = item
                if not isinstance(direction, str) or direction not in ("asc", "desc"):
                    raise ValueError(
                        "Each order_by item must be [field: str, direction: 'asc' or 'desc']"
                    )
            # Build and execute query
            # Convert order_by from list of lists to list of tuples for internal API
            order_by_tuples = [tuple(item) for item in order_by] if order_by else []
            query = self.models[model_name].query(
                dimensions=dimensions,
                measures=measures,
                filters=filters,
                order_by=order_by_tuples,
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
