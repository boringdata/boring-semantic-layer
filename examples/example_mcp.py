#!/usr/bin/env python3
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

from boring_semantic_layer import MCPSemanticModel
from example_basic import flights_sm, carriers_sm

server = MCPSemanticModel(
    models={
        "flights": flights_sm,
        "carriers": carriers_sm,
    },
    name="Flight Data Semantic Layer Server",
)

if __name__ == "__main__":
    server.run()
