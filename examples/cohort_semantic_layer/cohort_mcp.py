#!/usr/bin/env python3
"""
Basic MCP server example using semantic models.

This example demonstrates how to create an MCP server that exposes semantic models
for querying customer, order, and cohort data. The server provides tools for:
- Listing available models
- Getting model metadata
- Querying models with dimensions, measures, and filters
- Getting time ranges for time-series data

Usage:
    1: add the following config to the .cursor/mcp.json file:
    {
        "mcpServers": {
             "cohort_mcp": {
                "command": "uv",
                "args": [
                "--directory",
                "examples/cohort_semantic_layer",
                "run",
                "cohort_mcp.py"
                ]
            }
        }
    }

The server will start and listen for MCP connections.

"""

from boring_semantic_layer import MCPSemanticModel
from example_cohort import customers_model, orders_model, cohort_model

server = MCPSemanticModel(
    models={
        "customers": customers_model,
        "orders": orders_model,
        "cohorts": cohort_model,
    },
    name="Cohort Data Semantic Layer Server",
)

if __name__ == "__main__":
    server.run()
