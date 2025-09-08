# Boring Semantic Layer (BSL)

The Boring Semantic Layer (BSL) is a lightweight semantic layer based on [Ibis](https://ibis-project.org/).

**Key Features:**
- **Lightweight**: `pip install boring-semantic-layer`
- **Ibis-powered**: Built on top of [Ibis](https://ibis-project.org/), supporting any database engine that Ibis integrates with (DuckDB, Snowflake, BigQuery, PostgreSQL, and more)
- **MCP-friendly**: Perfect for connecting Large Language Models to structured data sources


*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs/xorq) and [boringdata](https://www.boringdata.io/).*

We welcome feedback and contributions!

# Quick Example

**1. Define your ibis input table**

```python
import ibis

flights_tbl = ibis.table(
    name="flights",
    schema={"origin": "string", "carrier": "string"}
)
```

**2. Define a semantic table**

```python
from boring_semantic_layer.semantic_api import to_semantic_table

flights_st = (
    to_semantic_table(flights_tbl)
    .with_dimensions(origin=lambda t: t.origin)
    .with_measures(flight_count=lambda t: t.count())
)
```

**3. Query it**

```python
result = (
    flights_st
    .group_by("origin")
    .aggregate(flight_count="_.count()")
    .execute()
)
```

**Example output (dataframe):**

| origin | flight_count |
|--------|--------------|
| JFK    | 3689         |
| LGA    | 2941         |
| ...    | ...          |


-----

## Table of Contents

- [Installation](#installation)
- [Get Started](#get-started)
  1. [Get Sample Data](#1-get-sample-data)
  2. [Build a Semantic Table](#2-build-a-semantic-table)
      - [Adding Descriptions to Semantic Tables, Dimensions, and Measures](#adding-descriptions-to-semantic-tables-dimensions-and-measures)
  3. [Query a Semantic Table](#3-query-a-semantic-table)
- [Features](#features)
  - [Filters](#filters)
    - [Ibis Expression](#ibis-expression)
    - [JSON-based (MCP & LLM friendly)](#json-based-mcp-llm-friendly)
  - [Time-Based Dimensions and Queries](#time-based-dimensions-and-queries)
  - [Joins Across Semantic Models](#joins-across-semantic-models)
    - [Classic SQL Joins](#classic-sql-joins)
    - [join_one](#join_one)
    - [join_many](#join_many)
    - [join_cross](#join_cross)
- [Model Context Protocol (MCP) Integration](#model-context-protocol-mcp-integration)
- [Chart Visualization](#chart-visualization)
  - [Installation](#installation-1)
  - [Smart Chart Creation](#smart-chart-creation)
  - [Automatic Chart Detection](#automatic-chart-detection)
  - [Advanced Chart Examples](#advanced-chart-examples)
- [Reference](#reference)
  - [SemanticModel](#semanticmodel)
  - [Query (SemanticModel.query / QueryExpr)](#query-semanticmodelquery--queryexpr)
  - [Chart API Reference](#chart-api-reference)

-----

## Installation

```bash
pip install boring-semantic-layer
```

-----

## Get Started

### 1. Get Sample Data

We expose some test data in a public bucket. You can download it with:

```bash
curl -L https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev/flights.parquet -o flights.parquet
curl -L https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev/carriers.parquet -o carriers.parquet
```

### 2. Build a Semantic Table

Define your data source and create a semantic table that describes your data in terms of dimensions and measures.

```python
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table

# Connect to your database (here, DuckDB in-memory for demo)
con = ibis.duckdb.connect(":memory:")
flights_tbl = con.read_parquet("flights.parquet")
carriers_tbl = con.read_parquet("carriers.parquet")

# Define the semantic table
flights_st = (
    to_semantic_table(flights_tbl)
    .with_dimensions(
        origin=lambda t: t.origin,
        destination=lambda t: t.dest,
        year=lambda t: t.year
    )
    .with_measures(
        total_flights=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_distance=lambda t: t.distance.mean()
    )
)
```

- **Dimensions** are attributes to group or filter by (e.g., origin, destination).
- **Measures** are aggregations or calculations (e.g., total flights, average distance).

All dimensions and measures are defined as Ibis expressions.

Ibis expressions are Python functions that represent database operations.

They allow you to write database queries using familiar Python syntax while Ibis handles the translation to optimized SQL for your specific database backend (like DuckDB, PostgreSQL, BigQuery, etc.).

For example, in our semantic model:

- `lambda t: t.origin` is an Ibis expression that references the "origin" column
- `lambda t: t.count()` is an Ibis expression that counts rows
- `lambda t: t.distance.mean()` is an Ibis expression that calculates the average distance

The `t` parameter represents the table, and you can chain operations like `t.origin.upper()` or `t.dep_delay > 0` to create complex expressions. Ibis ensures these expressions are translated to efficient SQL queries.

### Adding Descriptions to Semantic Tables, Dimensions, and Measures

BSL supports adding human-readable descriptions to dimensions, measures, and semantic tables. This helps in documenting your data model and making it easier for others to understand and AI agents to interact with.

Descriptions can be added through YAML configuration or when building tables programmatically.

**Why use descriptions?**
- **Human-readable**: Makes your models self documenting for team members.
- **AI friendly**: Perfect for MCP agents and LLM's that need to understand your models in more detail and nuances between similar dimensions and measures.
- **Flexible**: Define models in YAML or code with the same descriptive capabilities.

**YAML Configuration (Malloy-style):**

Define your semantic table with inline joins in a YAML file:

```yaml
flights:
  table: flights_tbl
  description: "Flight data with departure and arrival information"
  
  dimensions:
    origin: _.origin
    destination: _.dest
    carrier: _.carrier
    
  measures:
    flight_count: _.count()
    avg_distance: _.distance.mean()
    total_distance: _.distance.sum()
    
  joins:
    carriers:
      table: carriers_tbl
      type: one
      with: _.carrier
      dimensions:
        code: _.code
        name: _.name
        nickname: _.nickname
```

Load the YAML model:
```python
from boring_semantic_layer.semantic_api import SemanticTable

tables = {"flights_tbl": flights_tbl, "carriers_tbl": carriers_tbl}
flights_st = SemanticTable.from_yaml("flights.yml", tables)
```

---

### 3. Query a Semantic Table

Use your semantic table to run queries with the fluent interface or list-based operations.

**Fluent Interface:**
```python
result = (
    flights_st
    .group_by("origin")
    .aggregate(
        total_flights="_.count()",
        avg_distance="_.distance.mean()"
    )
    .limit(10)
    .execute()
)
```

**List-Based Operations:**
```python
operations = [
    {"group_by": ["origin"]},
    {"aggregate": {
        "total_flights": "_.count()",
        "avg_distance": "_.distance.mean()"
    }},
    {"limit": 10}
]

result = flights_st.query(operations).execute()
```

Example output:

| origin | total_flights | avg_distance |
|--------|---------------|--------------|
| JFK    | 3689          | 1047.71      |
| PHL    | 7708          | 1044.97      |
| ...    | ...           | ...          |

-----

## Features

### Filters

#### Ibis Expression

The fluent interface supports filtering data using Ibis expressions for full flexibility.

```python
result = (
    flights_st
    .filter(lambda t: t.origin == 'JFK')
    .group_by("origin")
    .aggregate(total_flights="_.count()")
    .execute()
)
```


| origin | total_flights |
|--------|---------------|
| JFK    | 3689          |

#### JSON-based (MCP & LLM friendly)

A format that's easy to serialize, good for dynamic queries or LLM integration.
```python
# Using list-based operations
operations = [
    {"filter": {
        "operator": "AND",
        "conditions": [
            {"field": "origin", "operator": "in", "values": ["JFK", "LGA", "PHL"]}
        ]
    }},
    {"group_by": ["origin"]},
    {"aggregate": {"total_flights": "_.count()"}}
]

result = flights_st.query(operations).execute()
```
**Example output (dataframe):**

| origin | total_flights |
|--------|---------------|
| LGA    | 7000          |
| PHL    | 7708          |

BSL supports the following operators: `=`, `!=`, `>`, `>=`, `in`, `not in`, `like`, `not like`, `is null`, `is not null`, `AND`, `OR`

**Note on filtering measures:** filters only work with dimensions. 
```

### Time-Based Dimensions and Queries

The Semantic API has built-in support for flexible time-based analysis.

```python
# Define time-based grouping inline
result = (
    flights_st
    .group_by(
        origin="_.origin",
        flight_date="_.dep_time.date()"
    )
    .aggregate(total_flights="_.count()")
    .filter(lambda t: t.flight_date >= '2013-01-01')
    .filter(lambda t: t.flight_date <= '2013-01-31')
    .order_by("flight_date")
    .execute()
)
```
Example output:

| origin | flight_date | total_flights |
|--------|-------------|---------------|
| PHL    | 2013-01-01  | 5             |
| CLE    | 2013-01-01  | 5             |
| DFW    | 2013-01-01  | 7             |
| DFW    | 2013-01-02  | 9             |
| DFW    | 2013-01-03  | 13            |

### Joins

The Semantic API allows you to join multiple tables to enrich your data. Joins can be defined in YAML configuration or dynamically in code.

#### Inline Joins in YAML

Define joins directly in your YAML configuration (Malloy-style):

```yaml
flights:
  table: flights_tbl
  
  dimensions:
    origin: _.origin
    destination: _.dest
    carrier: _.carrier
    
  measures:
    flight_count: _.count()
    
  joins:
    carriers:
      table: carriers_tbl
      type: one  # one-to-one join
      with: _.carrier  # join on flights.carrier = carriers.code
      dimensions:
        code: _.code
        name: _.name
        nickname: _.nickname
```

Query across joined tables:
```python
result = (
    flights_st
    .group_by("carriers.name", "origin")
    .aggregate(flight_count="_.count()")
    .limit(10)
    .execute()
)
```

| carriers_name              | origin | flight_count |
|---------------------------|--------|--------------|
| Delta Air Lines           | MDT    | 235          |
| Delta Air Lines           | ATL    | 8419         |
| Comair (Delta Connections)| ATL    | 239          |
| American Airlines         | DFW    | 8742         |
| American Eagle Airlines   | JFK    | 418          |

#### Dynamic Joins

Use the `.join()` method to join tables dynamically:

```python
carriers_st = to_semantic_table(carriers_tbl)

result = (
    flights_st
    .join(carriers_st, on=lambda l, r: l.carrier == r.code)
    .group_by("name")
    .aggregate(total_flights="_.count()")
    .execute()
)
```

## Model Context Protocol (MCP) Integration

BSL includes built-in support for the [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol/python-sdk), allowing you to expose your semantic models to LLMs like Claude.

**💡 Pro tip:** Use [descriptions in semantic models, dimensions and measures](#adding-descriptions-to-semantic-models-dimensions-and-measures) to make your models more AI-friendly. Descriptions help provide context to LLM's, enabling them to understand what each field represents and when to use them.

### Installation

To use MCP functionality, install with the `mcp` extra:

```bash
pip install 'boring-semantic-layer[mcp]'
```

### Setting up an MCP Server

Create an MCP server script that exposes your semantic tables:

```python
# example_mcp_new.py
import ibis
from boring_semantic_layer.semantic_api import SemanticTable, MCPSemanticAPI

# Connect to your database
con = ibis.duckdb.connect(":memory:")
flights_tbl = con.read_parquet("path/to/flights.parquet")
carriers_tbl = con.read_parquet("path/to/carriers.parquet")

# Load semantic table from YAML
tables = {"flights_tbl": flights_tbl, "carriers_tbl": carriers_tbl}
flights_st = SemanticTable.from_yaml("flights.yml", tables)

# Create and run the MCP server
mcp_server = MCPSemanticAPI(
    tables={"flights": flights_st},
    name="Flight Data Server (New API)"
)

if __name__ == "__main__":
    mcp_server.run(transport="stdio")
```

### Configuring Claude Desktop

To use your MCP server with Claude Desktop, add it to your configuration file:

**Location:** `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)

```json
{
  "mcpServers": {
    "flight_api": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/your/project/examples/",
        "run",
        "example_mcp_new.py"
      ]
    }
  }
}
```

Replace `/path/to/your/project/` with the actual path to your project directory.

### Available MCP Tools

Once configured, Claude will have access to these tools:

- `list_tables`: List all available semantic table names
- `get_table_schema`: Get details about a specific table including dimensions and measures
- `query_table`: Execute queries using list-based operations
  - When `chart_spec` is provided, returns both data and chart: `{"records": [...], "chart": {...}}`
  - When `chart_spec` is not provided, returns only data: `{"records": [...]}`

For more information on running MCP servers, see the [MCP Python SDK documentation](https://github.com/modelcontextprotocol/python-sdk).

## Chart Visualization

BSL includes built-in support for generating data visualizations using native Ibis-Altair integration. This allows you to create Altair charts directly from Ibis expressions without converting to pandas DataFrames first.

### Installation

To use chart visualization functionality, install with the `visualization` extra:

```bash
pip install 'boring-semantic-layer[visualization]'
```

### How BSL Charting Works

BSL's charting system is built on top of **[Vega-Lite](https://vega.github.io/vega-lite/)** and its Python wrapper **[Altair](https://altair-viz.github.io/)**.

Vega-Lite is a JSON-based grammar for creating interactive visualizations that provides a declarative approach to chart creation.

BSL supports multiple output formats including interactive Altair charts, static images (PNG/SVG), and raw JSON specifications for web embedding.

#### Quick Start Example

Here's a minimal example showing how to create a chart with custom styling:

```python
# Query and create chart
chart = (
    flights_st
    .group_by("origin")
    .aggregate(flight_count="_.count()")
    .limit(5)
    .chart(spec={
        "mark": {"type": "bar", "color": "steelblue"},
        "title": "Flights by Origin"
    })
)
```

![Quick Start Chart](docs/chart_quickstart.png)

#### How It Works

BSL exposes a `chart()` method on query results that accepts a Vega-Lite JSON specification and returns charts in various formats:

- **Auto-detection**: If you don't provide a spec, BSL automatically selects the best chart type
- **Partial specs**: Provide only what you want to customize, BSL fills in the rest
- **Multiple formats**: Output as Altair objects, PNG/SVG images, or JSON specifications

This design enables you to work at any level of abstraction - from full auto-detection to complete manual control.

### Smart Chart Creation

BSL automatically detects appropriate chart types and intelligently merges any specifications you provide.

BSL's detection logic:
- **Time series** (time dimension + measure) → Line chart with time-grain aware formatting
- **Categorical** (1 dimension + 1 measure) → Bar chart
- **Multiple measures** → Multi-series chart with automatic color encoding
- **Two dimensions** → Heatmap
- **Multiple dimensions with time** → Multi-line chart colored by dimension

Here are examples showing different chart types and customization options:

#### 1. Auto-detected Bar Chart

BSL automatically creates a bar chart for categorical data:

```python
# Query top destinations by flight count
result = (
    flights_st
    .group_by("destination")
    .aggregate(flight_count="_.count()")
    .order_by(ibis.desc("flight_count"))
    .limit(10)
)

# Auto-detects bar chart
chart = result.chart()
```

![Bar Chart](docs/chart_bar.png)

#### 2. Auto-detected Time Series Chart

For time-based queries, BSL automatically creates line charts with proper time formatting:

```python
# Time series query
time_result = (
    flights_st
    .group_by(week="_.arr_time.week()")
    .aggregate(flight_count="_.count()")
    .filter(lambda t: t.arr_time >= '2003-01-01')
    .filter(lambda t: t.arr_time <= '2003-03-31')
    .order_by("week")
)

# Auto-detects time series line chart
chart = time_result.chart()
```

![Time Series Chart](docs/chart_timeseries.png)

#### 3. Auto-detected Heatmap

When querying two categorical dimensions with a measure, BSL creates a heatmap:

```python
# Two dimensions create a heatmap
heatmap_result = (
    flights_st
    .group_by("destination", "origin")
    .aggregate(flight_count="_.count()")
    .limit(50)
)

# Auto-detects heatmap with custom sizing
chart = heatmap_result.chart(spec={
    "height": 300,
    "width": 400
})
```

![Heatmap Chart](docs/chart_heatmap.png)

#### 4. Custom Mark with Auto-detection

Mix your preferences with BSL's auto-detection by specifying only what you want to change:

```python
# Change only the mark type, keep auto-detected encoding
line_result = (
    flights_st
    .group_by("destination")
    .aggregate(avg_distance="_.distance.mean()")
    .order_by(ibis.desc("avg_distance"))
    .limit(15)
)

# Just change to line chart, encoding auto-detected
chart = line_result.chart(spec={"mark": "line"})
```

![Line Chart](docs/chart_line.png)

#### 5. Full Custom Specification

For complete control, specify everything you need:

```python
# Full custom specification  
custom_result = (
    flights_st
    .join(carriers_st, on=lambda l, r: l.carrier == r.code)
    .group_by("name")
    .aggregate(flight_count="_.count()")
    .order_by(ibis.desc("flight_count"))
    .limit(8)
)

# Complete custom chart specification
chart = custom_result.chart(spec={
    "title": "Top Airlines by Flight Count",
    "mark": {"type": "bar", "color": "steelblue"},
    "encoding": {
        "x": {"field": "name", "type": "nominal", "sort": "-y"},
        "y": {"field": "flight_count", "type": "quantitative"}
    },
    "width": 500,
    "height": 300
})
```

![Custom Chart](docs/chart_custom.png)

#### Export Formats

BSL supports multiple export formats:

```python
# Different export formats
altair_chart = result.chart()                # Altair Chart object (default)
interactive = result.chart(format="interactive")  # With interactive tooltips
json_spec = result.chart(format="json")      # Vega-Lite specification
png_bytes = result.chart(format="png")       # PNG image (requires altair[all])
svg_str = result.chart(format="svg")         # SVG markup (requires altair[all])

# Save as file
with open("my_chart.png", "wb") as f:
    f.write(png_bytes)
```

## Reference

### SemanticTable

#### Loading and Creating Tables

| Method | Description | Example |
|--------|-------------|---------|
| `SemanticTable.from_yaml(path, tables)` | Load from YAML config | `SemanticTable.from_yaml("config.yml", {"flights": tbl})` |
| `to_semantic_table(ibis_table)` | Convert Ibis table to semantic table | `to_semantic_table(flights_tbl)` |
| `with_dimensions(**dims)` | Add dimensions to table | `.with_dimensions(origin=lambda t: t.origin)` |
| `with_measures(**measures)` | Add measures to table | `.with_measures(count=lambda t: t.count())` |

### Query Operations

All operations return a new `SemanticTableExpr` object, allowing method chaining:

| Operation | Description | Example |
|-----------|-------------|---------|

| `group_by(*fields, **inline_dims)` | Group by dimensions | `.group_by("origin", month="_.arr_time.month()")` |
| `aggregate(**measures)` | Calculate aggregations | `.aggregate(count="_.count()", avg="_.delay.mean()")` |
| `filter(predicate)` | Apply filter conditions | `.filter(lambda t: t.delay > 0)` |
| `mutate(**fields)` | Add calculated fields | `.mutate(rate="_.delayed / _.total * 100")` |
| `select(*fields)` | Choose specific columns | `.select("origin", "count")` |
| `order_by(*keys)` | Sort results | `.order_by(ibis.desc("count"))` |
| `limit(n, offset=0)` | Limit results | `.limit(10, offset=20)` |
| `join(other, how='inner', on=None)` | Join with another table | `.join(carriers, on=lambda l, r: l.carrier == r.code)` |
| `execute()` | Execute query and return DataFrame | `.execute()` |
| `query(operations)` | Execute list-based operations | `.query([{"group_by": ["origin"]}])` |
#### Expression Formats

The Semantic API supports two expression formats:

1. **Unbound Expressions (using `_`)**:
   - `"_.count()"` - Count all records
   - `"_.field.mean()"` - Average of a field
   - `"_.field > 10"` - Boolean condition

2. **Lambda Expressions**:
   - `"lambda t: t.count()"` - Count records
   - `"lambda t: (t.delay > 15).sum()"` - Conditional count
   - `"lambda t: t.field1 / t.field2"` - Calculate ratio

### Chart API Reference

The `SemanticTableExpr` object provides the `chart()` method for visualization:

| Parameter | Type                     | Required | Allowed Values / Notes                                                                                      |
|-----------|--------------------------|----------|------------------------------------------------------------------------------------------------------------|
| `spec`    | dict or None             | No       | Vega-Lite specification dict. If not provided, will auto-detect chart type.<br>If partial spec is provided (e.g., only encoding or only mark), missing parts will be auto-detected and merged. |
| `format`  | str                      | No       | Output format of the chart:<br>- `"altair"` (default): Returns Altair Chart object<br>- `"interactive"`: Returns interactive Altair Chart with tooltip<br>- `"json"`: Returns Vega-Lite JSON specification<br>- `"png"`: Returns PNG image bytes (requires `pip install altair[all]`)<br>- `"svg"`: Returns SVG string (requires `pip install altair[all]`) |

**Returns:** Chart in the requested format (Altair Chart object, dict, bytes, or str depending on format)

For more examples, see `examples-new-api/` in the repository.
