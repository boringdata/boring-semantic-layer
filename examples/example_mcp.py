# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boring-semantic-layer[examples] >= 0.1.9",
#     "boring-semantic-layer[fastmcp] >= 0.1.9"
# ]
# ///

"""
Basic MCP server example using semantic models.

This example demonstrates how to create an MCP server that exposes semantic models
for querying flight and carrier data. The server provides tools for:
- Listing available models
- Getting model metadata
- Querying models with dimensions, measures, and filters
- Getting time ranges for time-series data

Usage:
    1: add the following config to the .cursor/mcp.json file:
    {
        "mcpServers": {
            "flight-semantic-layer": {
                "command": "uv run  mcp_basic_example.py",
                "language": "python"
            }
        }
    }

The server will start and listen for MCP connections.

"""

from boring_semantic_layer import MCPSemanticModel, SemanticModel, Join
import ibis

con = ibis.duckdb.connect(":memory:")

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

carriers_sm = SemanticModel(
    name="carriers",
    description="Airline carrier reference data containing carrier codes, names, and nicknames",
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "nickname": lambda t: t.nickname,
    },
    measures={
        "carrier_count": lambda t: t.count(),
    },
    primary_key="code",
)

flights_sm = SemanticModel(
    name="flights",
    description="Flight data with arrival times, origins, destinations, carriers, and performance metrics including delays and distances",
    table=flights_tbl,
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier,
        "tail_num": lambda t: t.tail_num,
        "arr_time": lambda t: t.arr_time,
    },
    time_dimension="arr_time",
    smallest_time_grain="TIME_GRAIN_SECOND",
    measures={
        "flight_count": lambda t: t.count(),
        "avg_dep_delay": lambda t: t.dep_delay.mean(),
        "avg_distance": lambda t: t.distance.mean(),
    },
    joins={
        "carriers": Join.one(
            alias="carriers",
            model=carriers_sm,
            with_=lambda left: left.carrier,
        ),
    },
)

server = MCPSemanticModel(
    models={"flights": flights_sm, "carriers": carriers_sm},
    name="Flight Data Semantic Layer Server",
)

if __name__ == "__main__":
    server.run()
