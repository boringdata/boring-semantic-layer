"""MCP functionality for semantic models."""

import json
from collections.abc import Mapping
from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field
from pydantic.functional_validators import BeforeValidator

from .query import _find_time_dimension


def _parse_json_string(v: Any) -> Any:
    """
    Parse JSON-stringified parameters that some MCP clients send.

    Some MCP clients (like Claude Desktop) send JSON-stringified arrays
    instead of actual arrays (e.g., '["a","b"]' instead of ["a","b"]).
    This validator handles that case while still accepting proper types.
    """
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, ValueError):
            # If it's not valid JSON, return as-is and let Pydantic handle validation
            return v
    return v


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

            result = {
                "name": model.name or "unnamed",
                "dimensions": dimensions,
                "measures": measures,
                "calculated_measures": list(model.get_calculated_measures().keys()),
            }

            # Include model description
            # For base models, use the description directly
            # For joined models, construct a description from the underlying tables
            description = getattr(model, "description", None)

            if description:
                result["description"] = description
            elif hasattr(model, "op") and type(model.op()).__name__ == "SemanticJoinOp":
                # For joined models, construct description from base tables
                from .ops import _find_all_root_models

                roots = _find_all_root_models(model.op())
                base_descriptions = []

                for root in roots:
                    root_name = getattr(root, "name", None) or "unnamed"
                    root_desc = getattr(root, "description", None)
                    if root_desc:
                        base_descriptions.append(f"{root_name} ({root_desc})")
                    else:
                        base_descriptions.append(root_name)

                if base_descriptions:
                    result["description"] = "Joined model combining: " + ", ".join(
                        base_descriptions
                    )

            return result

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
            dimensions: Annotated[
                list[str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description="List of dimension names to group by (e.g., ['flights.origin', 'flights.destination'])",
                ),
            ] = None,
            measures: Annotated[
                list[str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description="List of measure names to aggregate (e.g., ['flights.flight_count', 'flights.avg_distance'])",
                ),
            ] = None,
            filters: Annotated[
                list[dict[str, Any]] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description="""List of JSON filter objects with the following structure:

                Simple Filter:
                {
                    "field": "dimension_name",  # Must be an existing dimension (check model schema first!).
                    "operator": "=",            # See operator list below
                    "value": "single_value"     # For single-value operators (=, !=, >, >=, <, <=, ilike, not ilike, like, not like)
                    # OR for 'in'/'not in' operators only:
                    "values": ["val1", "val2"]  # REQUIRED for 'in' and 'not in' operators
                }

                IMPORTANT OPERATOR GUIDELINES:
                - Equality: Use "=" (preferred), "eq", or "equals" - all work identically
                - Text matching: Use "ilike" (case-insensitive) instead of "like" for better results
                - List membership: "in" requires "values" field (array), NOT "value"
                - Negated list: "not in" requires "values" field (array), NOT "value"
                - Pattern matching: "ilike" and "not ilike" support wildcards (%, _)
                - Null checks: "is null" and "is not null" need no value/values field

                Available operators:
                - "=" / "eq" / "equals": exact match (use "value")
                - "!=": not equal (use "value")
                - ">", ">=", "<", "<=": comparisons (use "value")
                - "in": value is in list (use "values" array)
                - "not in": value not in list (use "values" array)
                - "ilike": case-insensitive pattern match (use "value" with % wildcards)
                - "not ilike": negated case-insensitive pattern (use "value" with % wildcards)
                - "like": case-sensitive pattern match (use "value" with % wildcards)
                - "not like": negated case-sensitive pattern (use "value" with % wildcards)
                - "is null": field is null (no value/values needed)
                - "is not null": field is not null (no value/values needed)

                COMMON MISTAKES TO AVOID:
                1. Don't use "value" with "in"/"not in" - use "values" array instead
                2. Don't filter on measures - only filter on dimensions
                3. Don't use .month(), .year() etc. - use time_grain parameter instead
                4. For case-insensitive text search, prefer "ilike" over "like"

                Compound Filter (AND/OR):
                {
                    "operator": "AND",          # or "OR"
                    "conditions": [             # Non-empty list of other filter objects
                        {
                            "field": "country",
                            "operator": "equals",   # or "=" or "eq" - all equivalent
                            "value": "US"
                        },
                        {
                            "field": "tier",
                            "operator": "in",       # MUST use "values" array for "in"
                            "values": ["gold", "platinum"]  # NOT "value"
                        },
                        {
                            "field": "name",
                            "operator": "ilike",    # Case-insensitive pattern matching
                            "value": "%john%"       # Wildcards supported
                        }
                    ]
                }

                Example filters:
                [
                    {"field": "status", "operator": "in", "values": ["active", "pending"]},
                    {"field": "name", "operator": "ilike", "value": "%smith%"},
                    {"field": "created_date", "operator": ">=", "value": "2024-01-01"},
                    {"field": "email", "operator": "not ilike", "value": "%spam%"}
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
                ),
            ] = None,
            order_by: Annotated[
                list[list[str]] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description="List of [field, direction] pairs for sorting (e.g., [['flights.flight_count', 'desc']])",
                    json_schema_extra={"items": {"type": "array", "items": {"type": "string"}}},
                ),
            ] = None,
            limit: Annotated[
                int | None,
                Field(
                    default=None,
                    description="Maximum number of rows to return",
                ),
            ] = None,
            time_grain: Annotated[
                str | None,
                Field(
                    default=None,
                    description="""Time grain for aggregating time-based dimensions (e.g., "TIME_GRAIN_DAY", "TIME_GRAIN_MONTH").

                IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
                use this time_grain parameter to aggregate by time periods. The system will
                automatically handle time dimension transformations.

                Available time grains:
                - TIME_GRAIN_YEAR
                - TIME_GRAIN_QUARTER
                - TIME_GRAIN_MONTH
                - TIME_GRAIN_WEEK
                - TIME_GRAIN_DAY
                - TIME_GRAIN_HOUR
                - TIME_GRAIN_MINUTE
                - TIME_GRAIN_SECOND

                Examples:
                - For monthly data: time_grain="TIME_GRAIN_MONTH"
                - For yearly data: time_grain="TIME_GRAIN_YEAR"
                - For daily data: time_grain="TIME_GRAIN_DAY"

                Then filter using the time_range parameter or regular date filters like:
                {"field": "date_column", "operator": ">=", "value": "2024-01-01"}
                """,
                ),
            ] = None,
            time_range: Annotated[
                dict[str, str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description="""Optional time range filter with format:
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
                ),
            ] = None,
            chart_spec: Annotated[
                dict[str, Any] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description="""Chart specification dictionary for generating visualizations.

                Format: {"backend": "altair"|"plotly"|"plotext", "spec": {...}, "format": "json"|"static"|"interactive"}

                When provided, returns both data and chart: {"records": [...], "chart": {...}}
                When None, returns only data: {"records": [...]}

                Backend options:
                - "altair": Vega-Lite charts (default, works everywhere)
                - "plotly": Plotly charts (interactive, rich features)
                - "plotext": Terminal charts (ASCII, for CLI display)

                Format options:
                - "json": Returns chart specification as JSON (serializable)
                - "static": Returns static image (PNG/SVG)
                - "interactive": Returns interactive chart object

                Spec examples:
                - {"mark": "bar"} - Bar chart
                - {"mark": "line"} - Line chart
                - {"mark": "point"} - Scatter plot
                - {"title": "My Chart"} - Add title
                - {"width": 600, "height": 400} - Set size

                Complete example:
                {
                    "backend": "altair",
                    "spec": {"mark": "line", "title": "Monthly Trends"},
                    "format": "json"
                }
                """,
                ),
            ] = None,
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
