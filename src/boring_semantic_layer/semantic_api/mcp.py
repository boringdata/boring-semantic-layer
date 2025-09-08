"""MCP server implementation for the new semantic API."""

from mcp.server.fastmcp import FastMCP
from typing import Annotated, Any, Dict, List, Optional
from .ops import SemanticTable
from .query import query


class MCPSemanticAPI(FastMCP):
    """
    MCP server for the new semantic API using list-based query operations.

    This is the new implementation specifically for the semantic API,
    separate from the legacy MCP implementation.

    Provides tools:
    - list_tables: list all semantic table names
    - get_table_schema: get table schema information with descriptions
    - query_table: execute queries using list-based operations
    - query_with_chart: execute queries and generate charts
    """

    def __init__(
        self,
        tables: Dict[str, Any],  # Raw Ibis tables
        yaml_configs: Dict[str, str],  # Mapping of table names to YAML config paths
        name: str = "Semantic API MCP Server",
        *args,
        **kwargs,
    ):
        super().__init__(name, *args, **kwargs)
        self.tables = tables
        self.yaml_configs = yaml_configs
        self.semantic_tables = {}

        # Load semantic tables from YAML configs
        for table_name, yaml_path in yaml_configs.items():
            self.semantic_tables[table_name] = SemanticTable.from_yaml(
                yaml_path, tables
            )

        self._register_tools()

    def _register_tools(self):
        @self.tool()
        def list_tables() -> List[str]:
            """
            List all available semantic table names.

            Returns a list of table names that can be queried.
            """
            return list(self.semantic_tables.keys())

        @self.tool()
        def get_table_schema(table_name: str) -> Dict[str, Any]:
            """
            Get schema information about a specific semantic table.

            Returns detailed information including:
            - Table description
            - All columns with their data types
            - Dimensions with descriptions
            - Measures with descriptions
            - YAML configuration path
            """
            if table_name not in self.semantic_tables:
                available = ", ".join(self.semantic_tables.keys())
                raise ValueError(
                    f"Table '{table_name}' not found. Available tables: {available}"
                )

            semantic_table = self.semantic_tables[table_name]

            # Extract schema info from the semantic table node
            node = semantic_table._node

            # Build dimensions info with descriptions
            dimensions_info = {}
            if hasattr(node, "dimensions") and hasattr(node, "dimension_descriptions"):
                for dim_name in node.dimensions:
                    dimensions_info[dim_name] = {
                        "type": "dimension",
                        "description": node.dimension_descriptions.get(dim_name, ""),
                    }

            # Build measures info with descriptions
            measures_info = {}
            if hasattr(node, "measures") and hasattr(node, "measure_descriptions"):
                for meas_name in node.measures:
                    measures_info[meas_name] = {
                        "type": "measure",
                        "description": node.measure_descriptions.get(meas_name, ""),
                    }

            return {
                "name": table_name,
                "description": getattr(node, "description", ""),
                "columns": list(semantic_table.columns),
                "dimensions": dimensions_info,
                "measures": measures_info,
                "yaml_config": self.yaml_configs.get(table_name),
            }

        @self.tool()
        def query_table(
            table_name: str,
            operations: Annotated[
                List[Dict[str, Any]],
                """
                SEMANTIC TABLE QUERY OPERATIONS
                
                Execute queries using a list of operations applied sequentially.
                Each operation is a dictionary with EXACTLY ONE key-value pair.
                Operations are applied in the order provided in the list.
                
                ═══════════════════════════════════════════════════════════════
                OPERATION TYPES:
                ═══════════════════════════════════════════════════════════════
                
                1. GROUP_BY - Group data by dimensions
                   Format: {"group_by": <list> | <dict>}
                   
                   List format (existing fields):
                   {"group_by": ["carrier", "origin", "destination"]}
                   
                   Dict format (inline dimensions with expressions):
                   {"group_by": {"flight_month": "_.flight_date.month()", "flight_year": "_.flight_date.year()"}}
                
                2. AGGREGATE - Calculate measures and metrics
                   Format: {"aggregate": {<field_name>: <expression>}}
                   
                   {"aggregate": {"flight_count": "_.count()"}}
                   {"aggregate": {"avg_delay": "_.dep_delay.mean()", "total_distance": "_.distance.sum()"}}
                   {"aggregate": {"delayed_flights": "lambda t: (t.dep_delay > 15).sum()"}}
                
                3. FILTER - Apply conditions to filter data
                   Format: {"filter": <expression_string>}
                   
                   {"filter": "_.dep_delay > 0"}
                   {"filter": "_.carrier.isin(['AA', 'UA', 'DL'])"}
                   {"filter": "lambda t: t.flight_date >= '2024-01-01'"}
                
                4. MUTATE - Add calculated fields (post-aggregation)
                   Format: {"mutate": {<field_name>: <expression>}}
                   
                   {"mutate": {"delay_rate": "_.delayed_flights / _.total_flights * 100"}}
                   {"mutate": {"efficiency_score": "lambda t: 100 - (t.avg_delay / 10)"}}
                
                5. SELECT - Choose specific columns
                   Format: {"select": [<field_names>]}
                   
                   {"select": ["carrier", "flight_count", "avg_delay"]}
                
                6. ORDER_BY - Sort results
                   Format: {"order_by": [<field_specs>]}
                   
                   {"order_by": ["flight_count desc", "carrier"]}
                   {"order_by": ["avg_delay desc"]}
                
                7. LIMIT - Restrict number of results  
                   Format: {"limit": <integer>}
                   
                   {"limit": 10}
                   {"limit": 100}
                
                ═══════════════════════════════════════════════════════════════
                EXPRESSION FORMATS:
                ═══════════════════════════════════════════════════════════════
                
                1. UNBOUND EXPRESSIONS (using '_'):
                   Simple field operations using the underscore syntax:
                   - "_.count()" - Count all records
                   - "_.field.mean()" - Average of a field
                   - "_.field.sum()" - Sum of a field  
                   - "_.field.max()" - Maximum value
                   - "_.field > 10" - Boolean condition
                   - "_.field.isin(['A', 'B'])" - Check if field is in list
                   - "_.date.month()" - Extract month from date
                   - "_.date.year()" - Extract year from date
                
                2. LAMBDA EXPRESSIONS:
                   Complex calculations using lambda functions:
                   - "lambda t: t.count()" - Count records
                   - "lambda t: (t.delay > 15).sum()" - Count records meeting condition
                   - "lambda t: t.field1 / t.field2" - Calculate ratios
                   - "lambda t: t.date >= '2024-01-01'" - Date comparisons
                   - "lambda t: t.field.fillna(0).mean()" - Handle null values
                
                ═══════════════════════════════════════════════════════════════
                COMMON QUERY PATTERNS:
                ═══════════════════════════════════════════════════════════════
                
                BASIC AGGREGATION:
                [
                    {"group_by": ["carrier"]},
                    {"aggregate": {"flight_count": "_.count()", "avg_delay": "_.dep_delay.mean()"}},
                    {"order_by": ["avg_delay desc"]},
                    {"limit": 10}
                ]
                
                TIME SERIES ANALYSIS:
                [
                    {"filter": "_.date >= '2024-01-01'"},
                    {"group_by": {"month": "_.date.month()", "year": "_.date.year()"}},
                    {"aggregate": {"total": "_.amount.sum()", "count": "_.count()"}},
                    {"mutate": {"avg_per_transaction": "_.total / _.count"}},
                    {"order_by": ["year", "month"]}
                ]
                
                MULTI-DIMENSION ANALYSIS:
                [
                    {"group_by": ["category", "region"]},
                    {"aggregate": {"revenue": "_.revenue.sum()", "orders": "_.count()"}},
                    {"mutate": {"revenue_per_order": "_.revenue / _.orders"}},
                    {"filter": "_.orders >= 10"},
                    {"select": ["category", "region", "revenue", "revenue_per_order"]},
                    {"order_by": ["revenue desc"]}
                ]
                
                ═══════════════════════════════════════════════════════════════
                CRITICAL RULES:
                ═══════════════════════════════════════════════════════════════
                
                1. Each operation MUST have exactly ONE key-value pair
                2. Operations are applied sequentially - order matters!
                3. group_by typically comes before aggregate
                4. mutate operates on aggregated results (post-aggregation)
                5. filter can be used before or after aggregation
                6. Use field references (_.field) for the current table context
                """,
            ],
            limit: Optional[int] = None,
        ) -> Dict[str, Any]:
            """
            Execute a query on a semantic table using list-based operations.

            Returns query results as a dictionary with 'records' key containing
            the data rows.
            """
            if table_name not in self.semantic_tables:
                available = ", ".join(self.semantic_tables.keys())
                raise ValueError(
                    f"Table '{table_name}' not found. Available tables: {available}"
                )

            semantic_table = self.semantic_tables[table_name]

            # Apply operations
            result = query(semantic_table, operations)

            # Apply limit if specified as parameter
            if limit:
                result = result.limit(limit)

            # Execute and convert to records format
            df = result.execute()

            # Convert to JSON-serializable format
            records = df.to_dict(orient="records")

            return {"records": records}

        @self.tool()
        def query_with_chart(
            table_name: str,
            operations: List[Dict[str, Any]],
            chart_spec: Optional[Dict[str, Any]] = None,
            limit: Optional[int] = None,
        ) -> Dict[str, Any]:
            """
            Execute a query and optionally generate a chart visualization.

            Args:
                table_name: Name of the semantic table to query
                operations: List of query operations (same format as query_table)
                chart_spec: Optional Vega-Lite specification for the chart
                limit: Optional row limit

            Returns:
                Dictionary with 'records' containing data and optionally 'chart'
                containing the Vega-Lite specification if chart_spec was provided.
            """
            if table_name not in self.semantic_tables:
                available = ", ".join(self.semantic_tables.keys())
                raise ValueError(
                    f"Table '{table_name}' not found. Available tables: {available}"
                )

            semantic_table = self.semantic_tables[table_name]

            # Apply operations
            result = query(semantic_table, operations)

            # Apply limit if specified
            if limit:
                result = result.limit(limit)

            # Execute query
            df = result.execute()
            records = df.to_dict(orient="records")

            response = {"records": records}

            # Generate chart if requested
            if chart_spec is not None:
                try:
                    chart_json = result.chart(spec=chart_spec, format="json")
                    response["chart"] = chart_json
                except Exception as e:
                    # Include error but don't fail the whole query
                    response["chart_error"] = str(e)

            return response
