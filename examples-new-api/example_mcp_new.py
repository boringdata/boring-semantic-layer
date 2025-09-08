#!/usr/bin/env python3
"""Example exposing semantic table from example_basic.yml via MCP."""

import ibis

try:
    from boring_semantic_layer.semantic_api.mcp import MCPSemanticAPI
except ImportError:
    print("MCP support not available. Install with: pip install mcp")
    exit(1)

# Connect and load data
con = ibis.duckdb.connect(":memory:")
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
tables = {
    "flights_tbl": con.read_parquet(f"{BASE_URL}/flights.parquet"),
    "carriers_tbl": con.read_parquet(f"{BASE_URL}/carriers.parquet"),
}

# Create MCP server
mcp_server = MCPSemanticAPI(
    tables=tables,
    yaml_configs={"flights": "example_basic.yml"},
    name="Flights Semantic Table MCP Server",
)

# Example usage
if __name__ == "__main__":
    print("🚀 MCP Server for example_basic.yml")
    print("=" * 50)

    # The semantic table is loaded from YAML
    print("\n📊 Available semantic tables:")
    for table_name in mcp_server.semantic_tables:
        print(f"  - {table_name}")

    # Show available MCP tools
    print("\n🔧 Available MCP tools:")
    print("  - list_tables")
    print("  - get_table_schema")
    print("  - query_table")
    print("  - query_with_chart")

    print("\n✅ MCP server is ready to run!")
    print("\nTo use with Claude Desktop, add to your config:")
    print("```")
    print("mcpServers:")
    print("  flights:")
    print("    command: 'uv'")
    print("    args: ['run', 'python', '" + __file__ + "']")
    print("```")

    print("\nExample queries via MCP:")
    print("  - Tool: list_tables")
    print("  - Tool: get_table_schema, table_name: 'flights'")
    print("  - Tool: query_table, table_name: 'flights', operations: [")
    print('      {"group_by": ["carrier"]},')
    print('      {"aggregate": {"flight_count": "_.count()"}},')
    print('      {"order_by": ["flight_count desc"]},')
    print('      {"limit": 5}')
    print("    ]")
