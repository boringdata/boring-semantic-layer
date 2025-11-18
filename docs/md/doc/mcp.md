# Model Context Protocol (MCP) Integration

BSL includes built-in support for the [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol/python-sdk), allowing you to expose your semantic models to Large Language Models like Claude.

<note type="info">
**Pro tip:** Use [descriptions in dimensions and measures](/building/semantic-tables#adding-descriptions) to make your models more AI-friendly. Descriptions help provide context to LLMs, enabling them to understand what each field represents and when to use them.
</note>

## Installation

To use MCP functionality, install BSL with the `fastmcp` extra:

```bash
pip install 'boring-semantic-layer[fastmcp]'
```

## Setting up an MCP Server

Create an MCP server script that exposes your semantic models:

```python
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table
from boring_semantic_layer.api.mcp import MCPSemanticModel

# Create synthetic flights data
flights_data = ibis.memtable({
    "flight_id": list(range(1, 101)),
    "origin": ["JFK", "LAX", "ORD", "ATL", "DFW"] * 20,
    "dest": ["LAX", "JFK", "DFW", "ORD", "ATL"] * 20,
    "carrier": ["AA", "UA", "DL", "WN", "B6"] * 20,
    "distance": [2475, 2475, 801, 606, 732] * 20,
})

# Define your semantic table with descriptions
flights = (
    to_semantic_table(flights_data, name="flights")
    .with_dimensions(
        origin={
            "expr": lambda t: t.origin,
            "description": "Origin airport code where the flight departed from"
        },
        destination={
            "expr": lambda t: t.dest,
            "description": "Destination airport code where the flight arrived"
        },
        carrier={
            "expr": lambda t: t.carrier,
            "description": "Airline carrier code (e.g., AA, UA, DL)"
        },
    )
    .with_measures(
        total_flights={
            "expr": lambda t: t.count(),
            "description": "Total number of flights"
        },
        avg_distance={
            "expr": lambda t: t.distance.mean(),
            "description": "Average flight distance in miles"
        },
    )
)

# Create the MCP server
mcp_server = MCPSemanticModel(
    models={"flights": flights},
    name="Flight Data Server"
)

if __name__ == "__main__":
    mcp_server.run(transport="stdio")
```

Save this as `example_mcp.py` in your project directory.

## Configuring Claude Desktop

To use your MCP server with Claude Desktop, add it to your configuration file.

**Configuration file location:**
- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

**Example configuration:**

```json
{
  "mcpServers": {
    "flight_sm": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/your/project/",
        "run",
        "example_mcp.py"
      ]
    }
  }
}
```

Replace `/path/to/your/project/` with the actual path to your project directory.

<note type="info">
This example uses [uv](https://docs.astral.sh/uv/) to run the MCP server. You can also use `python` directly if you have BSL installed in your environment:

```json
{
  "mcpServers": {
    "flight_sm": {
      "command": "python",
      "args": ["/path/to/your/project/example_mcp.py"]
    }
  }
}
```
</note>

After updating the configuration:
1. Restart Claude Desktop
2. Look for the MCP server indicator in the Claude Desktop interface
3. You should see "flight_sm" listed as an available server

## Available MCP Tools

Once configured, Claude will have access to these tools for interacting with your semantic models:

### list_models

List all available semantic model names in the MCP server.

**Example usage in Claude:**
> "What models are available?"

**Returns:** Array of model names (e.g., `["flights", "carriers"]`)

### get_model

Get detailed information about a specific model including its dimensions, measures, and descriptions.

**Parameters:**
- `model_name` (str): Name of the model to inspect

**Example usage in Claude:**
> "Show me the details of the flights model"

**Returns:** Model schema including:
- Model name and description
- List of dimensions with their descriptions
- List of measures with their descriptions
- Available joins (if any)

### get_time_range

Get the available time range for time-series data in a model.

**Parameters:**
- `model_name` (str): Name of the model
- `time_dimension` (str): Name of the time dimension

**Example usage in Claude:**
> "What's the time range available in the flights model?"

**Returns:** Dictionary with `min_time` and `max_time` values

### query_model

Execute queries against a semantic model with dimensions, measures, filters, and optional chart specifications.

**Parameters:**
- `model_name` (str): Name of the model to query
- `dimensions` (list[str]): List of dimension names to group by
- `measures` (list[str]): List of measure names to aggregate
- `filters` (list[str], optional): List of filter expressions (e.g., `["origin == 'JFK'"]`)
- `limit` (int, optional): Maximum number of rows to return
- `order_by` (list[str], optional): List of columns to sort by
- `chart_spec` (dict, optional): Vega-Lite chart specification

**Example usage in Claude:**
> "Show me the top 10 origins by flight count"
> "Create a bar chart of average distance by carrier"

**Returns:**
- When `chart_spec` is provided: `{"records": [...], "chart": {...}}`
- When `chart_spec` is not provided: `{"records": [...]}`

### Example Interactions

Here are some example questions you can ask Claude when the MCP server is configured:

**Data Exploration:**
- "What models are available in the flight data server?"
- "Show me all dimensions and measures in the flights model"
- "What is the time range covered by the flights data?"

**Basic Queries:**
- "How many flights departed from JFK?"
- "Show me the top 5 destinations by flight count"
- "What's the average flight distance for each carrier?"

**Filtered Queries:**
- "Show me flights from California airports (starting with 'S')"
- "What carriers have an average distance over 1000 miles?"
- "List the top 10 busiest routes"

**Visualizations:**
- "Create a bar chart showing flights by origin airport"
- "Make a line chart of flights over time"
- "Show me a heatmap of routes between origins and destinations"

## Best Practices

### 1. Add Descriptions to All Fields

Descriptions are crucial for LLMs to understand your data model:

```python
flights = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        origin={
            "expr": lambda t: t.origin,
            "description": "Origin airport code (3-letter IATA code)"
        }
    )
    .with_measures(
        total_flights={
            "expr": lambda t: t.count(),
            "description": "Total number of flights in the dataset"
        }
    )
)
```

### 2. Use Descriptive Model Names

Choose clear, descriptive names for your models:

```python
# Good
mcp_server = MCPSemanticModel(
    models={"flights": flights, "carriers": carriers},
    name="Aviation Analytics Server"
)

# Less clear
mcp_server = MCPSemanticModel(
    models={"f": flights, "c": carriers},
    name="Server"
)
```

### 3. Define Time Dimensions for MCP Time-Series Queries

When exposing models through MCP, you need to explicitly define time dimensions to enable LLMs to query time ranges and perform time-based aggregations. This is specific to MCP—when using BSL's fluent API directly, you can simply use Ibis functions like `.year()` and `.month()`.

To define a time dimension, set `is_time_dimension=True` and specify the `smallest_time_grain`:

```python
from boring_semantic_layer.semantic_api import to_semantic_table

flights = (
    to_semantic_table(flights_data, name="flights")
    .with_dimensions(
        arr_time={
            "expr": lambda t: t.arr_time,
            "description": "Arrival time of the flight",
            "is_time_dimension": True,
            "smallest_time_grain": "TIME_GRAIN_SECOND",
        },
        origin={
            "expr": lambda t: t.origin,
            "description": "Origin airport code"
        },
    )
    .with_measures(
        flight_count={
            "expr": lambda t: t.count(),
            "description": "Total number of flights"
        }
    )
)
```

**Available time grains:**
- `TIME_GRAIN_SECOND` - For second-level precision
- `TIME_GRAIN_MINUTE` - For minute-level precision
- `TIME_GRAIN_HOUR` - For hourly data
- `TIME_GRAIN_DAY` - For daily data
- `TIME_GRAIN_WEEK` - For weekly data
- `TIME_GRAIN_MONTH` - For monthly data
- `TIME_GRAIN_QUARTER` - For quarterly data
- `TIME_GRAIN_YEAR` - For yearly data

<note type="info">
If you define multiple time dimensions in your model, the `.query()` method and MCP tools will use the first time dimension that appears in your query's dimensions list.
</note>

**Example time-based queries:**

With time dimensions defined, you can use the `.query()` method with time ranges and grains:

```python
# Query with a specific time range
result = flights.query(
    dimensions=["origin"],
    measures=["flight_count"],
    time_range={"start": "2024-01-01", "end": "2024-12-31"}
)

# Query with time grain aggregation
result = flights.query(
    dimensions=["arr_time"],
    measures=["flight_count"],
    time_grain="TIME_GRAIN_MONTH"
)
```

LLMs can then perform similar queries through MCP:
```
> "What's the time range available in the flights data?"
> "Show me flights from January 2024"
> "Give me monthly flight counts for the last year"
```

### 4. Structure Your Data Logically

Organize related dimensions and measures together, and use joins to connect related models:

```python
# Flights model focuses on flight operations
flights = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(origin=..., destination=..., date=...)
    .with_measures(flight_count=..., avg_delay=...)
)

# Carriers model focuses on airline information
carriers = (
    to_semantic_table(carriers_tbl, name="carriers")
    .with_dimensions(code=..., name=..., country=...)
    .with_measures(carrier_count=...)
)

# Connect them with joins
flights_with_carriers = flights.join_one(
    carriers,
    left_on="carrier",
    right_on="code"
)
```

## Troubleshooting

### Server Not Appearing in Claude Desktop

1. Check the configuration file path is correct
2. Verify JSON syntax in `claude_desktop_config.json`
3. Ensure BSL is installed with MCP support: `pip install 'boring-semantic-layer[fastmcp]'`
4. Restart Claude Desktop completely
5. Check Claude Desktop logs for error messages

### Import Errors

If you see import errors when the server starts:

```bash
# Ensure all dependencies are installed
pip install 'boring-semantic-layer[fastmcp]'

# Or install specific dependencies
pip install fastmcp ibis-framework
```

### Path Issues

Make sure file paths in your configuration are absolute paths, not relative:

```json
{
  "mcpServers": {
    "flight_sm": {
      "command": "python",
      "args": ["/Users/username/projects/my-project/example_mcp.py"]
    }
  }
}
```

## Next Steps

- Learn about [YAML Configuration](/building/yaml) for managing multiple models
- Explore [Query Methods](/querying/methods) to understand what queries LLMs can perform
- See [Charting](/querying/charting) for visualization capabilities
- Review the [full API Reference](/reference) for advanced features
