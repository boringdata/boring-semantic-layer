# Query Agent: BSL as MCP

Expose any set of `SemanticTable` objects through the Model Context Protocol (MCP) so Claude Desktop can call your semantic layer as if it were a native data source. The `MCPSemanticModel` class in [`src/boring_semantic_layer/agents/backends/mcp.py`](../../src/boring_semantic_layer/agents/backends/mcp.py) wraps the BSL semantics and registers four tools that map directly to what an LLM needs to stay grounded:

1. `list_models` -> discover which semantic models are available.
2. `get_model` -> inspect dimensions, measures, calculated measures, and join metadata.
3. `get_time_range` -> fetch the min/max timestamps so the agent can ask for the right filters.
4. `query_model` -> run an actual BSL query with dimensions, measures, filters, order, limit, and Vega or Plotly chart specs.

## Minimal MCP server

```python
from boring_semantic_layer import MCPSemanticModel, to_semantic_table
import ibis

con = ibis.duckdb.connect(":memory:")
flights_raw = con.read_parquet("flights.parquet")

flights = (
    to_semantic_table(flights_raw, name="flights")
    .with_dimensions(origin=lambda t: t.origin, carrier=lambda t: t.carrier)
    .with_measures(flight_count=lambda t: t.count())
)

mcp_server = MCPSemanticModel(models={"flights": flights}, name="Flight Data Server")

if __name__ == "__main__":
    mcp_server.run(transport="stdio")
```

Drop this script into your Claude Desktop configuration (`claude_desktop_config.json`) and relaunch. Claude now has structured knowledge of every dimension description, smallest time grain, and measure you defined.

## Usage tips

- Author rich descriptions inside your semantic tables; Claude surfaces them inside the MCP tooltips.
- When you join multiple semantic tables, remind users to prefix fields (`flights.origin`). The `get_model` response explains this for you.
- Reuse the same YAML or Python models you load into the CLI. MCP simply adds a transport layer for Claude.

Want end-to-end screenshots and setup steps? The standalone [MCP documentation](/querying/mcp) walks through credential setup, `claude_desktop_config.json`, and server lifecycle management.
