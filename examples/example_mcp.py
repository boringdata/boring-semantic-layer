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

from pathlib import Path

from boring_semantic_layer import MCPSemanticModel, from_yaml

# Load semantic models from YAML with profile
yaml_path = Path(__file__).parent / "flights.yml"
profile_file = Path(__file__).parent / "profiles.yml"
models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

# Create MCP server with all models from YAML
server = MCPSemanticModel(
    models=models,
    name="Flight Data Semantic Layer Server (BSL v2)",
)

if __name__ == "__main__":
    server.run()
