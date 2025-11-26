#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boring-semantic-layer[examples] >= 0.2.0",
#     "boring-semantic-layer[fastmcp] >= 0.2.0"
# ]
# ///

"""
Basic MCP server example using semantic tables (BSL v2).

This example demonstrates how to create an MCP server that exposes semantic models
for querying flight and carrier data. The server provides tools for:
- Listing available models
- Getting model metadata
- Querying models with dimensions, measures, and filters
- Getting time ranges for time-series data

Usage:
    Add the following config to your MCP configuration file:

    For Claude Desktop (~/.config/Claude/claude_desktop_config.json):
    {
        "mcpServers": {
            "flight-semantic-layer": {
                "command": "uv",
                "args": ["--directory", "/path/to/boring-semantic-layer/examples", "run", "example_mcp.py"]
            }
        }
    }

The server will start and listen for MCP connections.
"""

import ibis

from boring_semantic_layer import MCPSemanticModel, to_semantic_table

con = ibis.duckdb.connect(":memory:")

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

# Define carriers semantic table
carriers = (
    to_semantic_table(carriers_tbl, name="carriers")
    .with_dimensions(
        code={
            "expr": lambda t: t.code,
            "description": "Carrier code (e.g., AA, UA, DL)",
        },
        name={
            "expr": lambda t: t.name,
            "description": "Full carrier name",
        },
        nickname={
            "expr": lambda t: t.nickname,
            "description": "Carrier nickname or short name",
        },
    )
    .with_measures(
        carrier_count={
            "expr": lambda t: t.count(),
            "description": "Total number of carriers",
        }
    )
)

# Define flights semantic table with join to carriers
flights = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        origin={
            "expr": lambda t: t.origin,
            "description": "Origin airport code",
        },
        destination={
            "expr": lambda t: t.destination,
            "description": "Destination airport code",
        },
        carrier={
            "expr": lambda t: t.carrier,
            "description": "Carrier code",
        },
        tail_num={
            "expr": lambda t: t.tail_num,
            "description": "Aircraft tail number",
        },
        arr_time={
            "expr": lambda t: t.arr_time,
            "description": "Arrival time",
            "is_time_dimension": True,
            "smallest_time_grain": "TIME_GRAIN_SECOND",
        },
    )
    .with_measures(
        flight_count={
            "expr": lambda t: t.count(),
            "description": "Total number of flights",
        },
        avg_dep_delay={
            "expr": lambda t: t.dep_delay.mean(),
            "description": "Average departure delay in minutes",
        },
        avg_distance={
            "expr": lambda t: t.distance.mean(),
            "description": "Average flight distance in miles",
        },
    )
    .join_one(carriers, lambda f, c: f.carrier == c.code)
)

# Create MCP server
server = MCPSemanticModel(
    models={"flights": flights, "carriers": carriers},
    name="Flight Data Semantic Layer Server (BSL v2)",
)

if __name__ == "__main__":
    server.run()
