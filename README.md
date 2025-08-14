# Boring Semantic Layer (BSL)

The Boring Semantic Layer (BSL) is a lightweight semantic layer based on [Ibis](https://ibis-project.org/). 

**Key Features:**
- **Lightweight**: `pip install boring-semantic-layer`
- **Ibis-powered**: Built on top of [Ibis](https://ibis-project.org/), supporting any database engine that Ibis integrates with (DuckDB, Snowflake, BigQuery, PostgreSQL, and more)
- **Smart Filtering**: Automatically applies filters at the right time - dimensions/columns before aggregation, measures after aggregation
- **MCP-friendly**: Perfect for connecting Large Language Models to structured data sources


*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs/xorq) and [boringdata](https://www.boringdata.io/).*

We welcome feedback and contributions!

# Quick Example

**1. Define your ibis input table**

```python
import ibis

flights_tbl = ibis.table(
    name="flights",
    schema={"origin": "string", "carrier": "string", "distance": "int64"}
)
```

**2. Define a semantic model**

```python
from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={"origin": lambda t: t.origin},
    measures={"flight_count": lambda t: t.count(), "avg_distance": lambda t: t.distance.mean()}
)
```

**3. Query with smart filtering**

```python
# Filter by raw column (pre-aggregation) and measure (post-aggregation)
flights_sm.query(
    dimensions=["origin"],
    measures=["flight_count", "avg_distance"],
    filters=[
        {"field": "distance", "operator": ">", "value": 1000},      # Raw column filter
        {"field": "flight_count", "operator": ">", "value": 100}   # Measure filter
    ]
).execute()
```

**Example output (dataframe):**

| origin | flight_count | avg_distance |
|--------|--------------|--------------|
| JFK    | 1247         | 1456.2       |
| LAX    | 892          | 1789.5       |

*Only origins with long-distance flights (>1000 miles) AND high traffic (>100 flights)*


-----

## Table of Contents

- [Installation](#installation)
- [Get Started](#get-started)
  1. [Get Sample Data](#1-get-sample-data)
  2. [Build a Semantic Model](#2-build-a-semantic-model)
  3. [Query a Semantic Model](#3-query-a-semantic-model)
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

### 2. Build a Semantic Model

Define your data source and create a semantic model that describes your data in terms of dimensions and measures.

```python
import ibis
from boring_semantic_layer import SemanticModel

# Connect to your database (here, DuckDB in-memory for demo)
con = ibis.duckdb.connect(":memory:")
flights_tbl = con.read_parquet("flights.parquet")
carriers_tbl = con.read_parquet("carriers.parquet")

# Define the semantic model
flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'destination': lambda t: t.dest,
        'year': lambda t: t.year
    },
    measures={
        'total_flights': lambda t: t.count(),
        'total_distance': lambda t: t.distance.sum(),
        'avg_distance': lambda t: t.distance.mean(),
    }
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

---

### 2. Query a Semantic Model

Use your semantic model to run queries—selecting dimensions, measures, and applying filters or limits.

```python
flights_sm.query(
    dimensions=['origin'],
    measures=['total_flights', 'avg_distance'],
    limit=10
).execute()
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

The `query` method can filter data using raw Ibis expressions for full flexibility.
  
```python
flights_sm.query(
    dimensions=['origin'],
    measures=['total_flights'],
    filters=[
        lambda t: t.origin == 'JFK'
    ]
)
```


| origin | total_flights |
|--------|---------------|
| JFK    | 3689          |

#### JSON-based (MCP & LLM friendly)

A format that's easy to serialize, good for dynamic queries or LLM integration.
```python
flights_sm.query(
    dimensions=['origin'],
    measures=['total_flights'],
    filters=[
        {
            'operator': 'AND',
            'conditions': [
                {'field': 'origin', 'operator': 'in', 'values': ['JFK', 'LGA', 'PHL']}
            ]
        }
    ]
).execute()
```
**Example output (dataframe):**

| origin | total_flights |
|--------|---------------|
| LGA    | 7000          |
| PHL    | 7708          |

BSL supports the following operators: `=`, `!=`, `>`, `>=`, `in`, `not in`, `like`, `not like`, `is null`, `is not null`, `AND`, `OR`

**Smart Filtering:** BSL intelligently applies filters at the optimal time for accurate results:

- **Dimension & Raw Column Filters** → Applied before aggregation (WHERE clause)
- **Measure Filters** → Applied after aggregation (HAVING clause equivalent)

```python
# Example: Realistic business filtering
transactions_sm.query(
    dimensions=['company_name'],
    measures=['total_amount_eur', 'transaction_count'],
    filters=[
        {'field': 'transaction_type', 'operator': '=', 'value': 'purchase'},    # Raw column (pre-aggregation)
        {'field': 'total_amount_eur', 'operator': '>', 'value': 100000}        # Measure (post-aggregation)
    ]
)
```

This query:
1. **First** filters raw data to only purchase transactions  
2. **Then** aggregates by company to calculate `total_amount_eur`
3. **Finally** keeps only companies with total purchases > €100,000


### Time-Based Dimensions and Queries

BSL has built-in support for flexible time-based analysis. 

To use it, define a `time_dimension` in your `SemanticModel` that points to a timestamp column. 

You can also set `smallest_time_grain` to prevent incorrect time aggregations.

```python
flights_sm_with_time = SemanticModel(
    name="flights_timed",
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'destination': lambda t: t.dest,
        'year': lambda t: t.year,
    },
    measures={
        'total_flights': lambda t: t.count(),
    },
    time_dimension='dep_time', # The column containing timestamps. Crucial for time-based queries.
    smallest_time_grain='TIME_GRAIN_SECOND' # Optional: sets the lowest granularity (e.g., DAY, MONTH).
)

# With the time dimension defined, you can query using a specific time range and grain.
query_time_based_df = flights_sm_with_time.query(
    dimensions=['origin'],
    measures=['total_flights'],
    time_range={'start': '2013-01-01', 'end': '2013-01-31'},
    time_grain='TIME_GRAIN_DAY' # Use specific TIME_GRAIN constants
).execute()

print(query_time_based_df)
```
Example output:

| origin | arr_time   | flight_count |
|--------|------------|--------------|
| PHL    | 2013-01-01 | 5            |
| CLE    | 2013-01-01 | 5            |
| DFW    | 2013-01-01 | 7            |
| DFW    | 2013-01-02 | 9            |
| DFW    | 2013-01-03 | 13           |

### Joins Across Semantic Models

BSL allows you to join multiple `SemanticModel` instances to enrich your data. Joins are defined in the `joins` parameter of a `SemanticModel`.

There are four main ways to define joins:

#### Classic SQL Joins

For full control, you can create a `Join` object directly, specifying the join condition with an `on` lambda function and the join type with `how` (e.g., `'inner'`, `'left'`).

First, let's define two semantic models: one for flights and one for carriers.

The flight model resulting from a join with the carriers model:

```python
from boring_semantic_layer import  Join, SemanticModel
import ibis
import os 

# Assume `con` is an existing Ibis connection from the Quickstart example.
con = ibis.duckdb.connect(":memory:")

# Load the required tables from the sample data
flights_tbl = con.read_parquet("malloy-samples/data/flights.parquet")
carriers_tbl = con.read_parquet("malloy-samples/data/carriers.parquet")

# First, define the 'carriers' semantic model to join with.
carriers_sm = SemanticModel(
    name="carriers",
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "nickname": lambda t: t.nickname,
    },
    measures={
        "carrier_count": lambda t: t.count(),
    }
)

# Now, define the 'flights' semantic model with a join to 'carriers'
flight_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier, # This is the join key
    },
    measures={
        "flight_count": lambda t: t.count(),
    },
    joins={
        "carriers": Join(
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code,
        ),
    }
)

# Querying across the joined models to get flight counts by carrier name
query_joined_df = flight_sm.query(
    dimensions=['carriers.name', 'origin'],
    measures=['flight_count'],
    limit=10
).execute()
```
| carriers_name              | origin | flight_count |
|---------------------------|--------|--------------|
| Delta Air Lines           | MDT    | 235          |
| Delta Air Lines           | ATL    | 8419         |
| Comair (Delta Connections)| ATL    | 239          |
| American Airlines         | DFW    | 8742         |
| American Eagle Airlines   | JFK    | 418          |

#### join_one

For common join patterns, BSL provides helper class methods inspired by [Malloy](https://docs.malloydata.dev/documentation/language/join): `Join.one`, `Join.many`, and `Join.cross`.

These simplify joins based on primary/foreign key relationships.

To use them, first define a `primary_key` on the model you are joining to. The primary key should be one of the model's dimensions.

```python
carriers_pk_sm = SemanticModel(
    name="carriers",
    table=con.read_parquet("malloy-samples/data/carriers.parquet"),
    primary_key="code",
    dimensions={
        'code': lambda t: t.code,
        'name': lambda t: t.name
    },
    measures={'carrier_count': lambda t: t.count()}
)
```

Now, you can use `Join.one` in the `flights` model to link to `carriers_pk_sm`. The `with_` parameter specifies the foreign key on the `flights` model.

```python
from boring_semantic_layer import Join

flights_with_join_one_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={'origin': lambda t: t.origin},
    measures={'flight_count': lambda t: t.count()},
    joins={
        "carriers": Join.one(
            alias="carriers",
            model=carriers_pk_sm,
            with_=lambda t: t.carrier
        )
    }
)
```

- **`Join.one(alias, model, with_)`**: Use for one-to-one or many-to-one relationships. It joins where the foreign key specified in `with_` matches the `primary_key` of the joined `model`.

#### join_many

- **`Join.many(alias, model, with_)`**: Similar to `Join.one`, but semantically represents a one-to-many relationship.

#### join_cross

- **`Join.cross(alias, model)`**: Creates a cross product, joining every row from the left model with every row of the right `model`.

Querying remains the same—just reference the joined fields using the alias.

```python
flights_with_join_one_sm.query(
    dimensions=["carriers.name"],
    measures=["flight_count"],
    limit=5
).execute()
```

Example output:

| carriers_name            | flight_count |
|-------------------------|--------------|
| Delta Air Lines         | 10000        |
| American Airlines       | 9000         |
| United Airlines         | 8500         |
| Southwest Airlines      | 8000         |
| JetBlue Airways         | 7500         |

## Model Context Protocol (MCP) Integration

BSL includes built-in support for the [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol/python-sdk), allowing you to expose your semantic models to LLMs like Claude.

### Installation

To use MCP functionality, install with the `mcp` extra:

```bash
pip install 'boring-semantic-layer[mcp]'
```

### Setting up an MCP Server

Create an MCP server script that exposes your semantic models:

```python
# example_mcp.py
import ibis
from boring_semantic_layer import SemanticModel, MCPSemanticModel

# Connect to your database
con = ibis.duckdb.connect(":memory:")
flights_tbl = con.read_parquet("path/to/flights.parquet")

# Define your semantic model
flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'destination': lambda t: t.dest,
        'carrier': lambda t: t.carrier,
    },
    measures={
        'total_flights': lambda t: t.count(),
        'avg_distance': lambda t: t.distance.mean(),
    }
)

# Create and run the MCP server
mcp_server = MCPSemanticModel(
    models={"flights": flights_sm},
    name="Flight Data Server"
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
    "flight_sm": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/your/project/examples/",
        "run",
        "example_mcp.py"
      ]
    }
  }
}
```

Replace `/path/to/your/project/` with the actual path to your project directory.

### Available MCP Tools

Once configured, Claude will have access to these tools:

- `list_models`: List all available semantic model names
- `get_model`: Get details about a specific model including dimensions and measures
- `get_time_range`: Get the available time range for time-series data
- `query_model`: Execute queries with dimensions, measures, and filters
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
from examples.example_basic import flights_sm

# Query with custom styling
chart = flights_sm.query(
    dimensions=["origin"], 
    measures=["flight_count"],
    limit=5
).chart(spec={
    "mark": {"type": "bar", "color": "steelblue"},
    "title": "Flights by Origin"
})
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
from examples.example_basic import flights_sm

# Query top destinations by flight count
query = flights_sm.query(
    dimensions=["destination"],
    measures=["flight_count"],
    order_by=[("flight_count", "desc")],
    limit=10
)

# Auto-detects bar chart
chart = query.chart()
```

![Bar Chart](docs/chart_bar.png)

#### 2. Auto-detected Time Series Chart

For time-based queries, BSL automatically creates line charts with proper time formatting:

```python
# Time series query
time_query = flights_sm.query(
    dimensions=["arr_time"],
    measures=["flight_count"],
    time_range={"start": "2003-01-01", "end": "2003-03-31"},
    time_grain="TIME_GRAIN_WEEK"
)

# Auto-detects time series line chart
chart = time_query.chart()
```

![Time Series Chart](docs/chart_timeseries.png)

#### 3. Auto-detected Heatmap

When querying two categorical dimensions with a measure, BSL creates a heatmap:

```python
# Two dimensions create a heatmap
heatmap_query = flights_sm.query(
    dimensions=["destination", "origin"],
    measures=["flight_count"],
    limit=50
)

# Auto-detects heatmap with custom sizing
chart = heatmap_query.chart(spec={
    "height": 300,
    "width": 400
})
```

![Heatmap Chart](docs/chart_heatmap.png)

#### 4. Custom Mark with Auto-detection

Mix your preferences with BSL's auto-detection by specifying only what you want to change:

```python
# Change only the mark type, keep auto-detected encoding
line_query = flights_sm.query(
    dimensions=["destination"],
    measures=["avg_distance"],
    order_by=[("avg_distance", "desc")],
    limit=15
)

# Just change to line chart, encoding auto-detected
chart = line_query.chart(spec={"mark": "line"})
```

![Line Chart](docs/chart_line.png)

#### 5. Full Custom Specification

For complete control, specify everything you need:

```python
# Full custom specification
custom_query = flights_sm.query(
    dimensions=["carriers.name"],
    measures=["flight_count"],
    order_by=[("flight_count", "desc")],
    limit=8
)

# Complete custom chart specification
chart = custom_query.chart(spec={
    "title": "Top Airlines by Flight Count",
    "mark": {"type": "bar", "color": "steelblue"},
    "encoding": {
        "x": {"field": "carriers_name", "type": "nominal", "sort": "-y"},
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
altair_chart = query.chart()                # Altair Chart object (default)
interactive = query.chart(format="interactive")  # With interactive tooltips
json_spec = query.chart(format="json")      # Vega-Lite specification
png_bytes = query.chart(format="png")       # PNG image (requires altair[all])
svg_str = query.chart(format="svg")         # SVG markup (requires altair[all])

# Save as file
with open("my_chart.png", "wb") as f:
    f.write(png_bytes)
```

## Reference

### SemanticModel

| Field                | Type                                      | Required | Allowed Values / Notes                                                                                      |
|----------------------|-------------------------------------------|----------|------------------------------------------------------------------------------------------------------------|
| `table`              | Ibis table expression                     | Yes      | Any Ibis table or view                                                                                     |
| `dimensions`         | dict[str, callable]                       | Yes      | Keys: dimension names; Values: functions mapping table → column                                             |
| `measures`           | dict[str, callable]                       | Yes      | Keys: measure names; Values: functions mapping table → aggregation                                          |
| `joins`              | dict[str, Join]                           | No       | Keys: join alias; Values: `Join` object (see below)                                                         |
| `primary_key`        | str                                       | No       | Name of the primary key dimension (required for certain join types)                                         |
| `name`               | str                                       | No       | Optional model name (inferred from table if omitted)                                                        |
| `time_dimension`     | str                                       | No       | Name of the column to use as the time dimension                                                             |
| `smallest_time_grain`| str                                       | No       | One of:<br>`TIME_GRAIN_SECOND`, `TIME_GRAIN_MINUTE`, `TIME_GRAIN_HOUR`, `TIME_GRAIN_DAY`,<br>`TIME_GRAIN_WEEK`, `TIME_GRAIN_MONTH`, `TIME_GRAIN_QUARTER`, `TIME_GRAIN_YEAR` |

#### Join object (for `joins`)
- Use `Join.one(alias, model, with_)` for one-to-one/many-to-one
- Use `Join.many(alias, model, with_)` for one-to-many
- Use `Join.cross(alias, model)` for cross join

---

### Query (SemanticModel.query / QueryExpr)

| Parameter      | Type                                              | Required | Allowed Values / Notes                                                                                      |
|----------------|---------------------------------------------------|----------|------------------------------------------------------------------------------------------------------------|
| `dimensions`   | list[str]                                         | No       | List of dimension names (can include joined fields, e.g. `"carriers.name"`)                                 |
| `measures`     | list[str]                                         | No       | List of measure names (can include joined fields)                                                           |
| `filters`      | list[dict/str/callable] or dict/str/callable      | No       | See below for filter formats and operators                                                                  |
| `order_by`     | list[tuple[str, str]]                             | No       | List of (field, direction) tuples, e.g. `[("avg_delay", "desc")]`                                           |
| `limit`        | int                                               | No       | Maximum number of rows to return                                                                            |
| `time_range`   | dict with `start` and `end` (ISO 8601 strings)    | No       | Example: `{'start': '2024-01-01', 'end': '2024-12-31'}`                                                     |
| `time_grain`   | str                                               | No       | One of:<br>`TIME_GRAIN_SECOND`, `TIME_GRAIN_MINUTE`, `TIME_GRAIN_HOUR`, `TIME_GRAIN_DAY`,<br>`TIME_GRAIN_WEEK`, `TIME_GRAIN_MONTH`, `TIME_GRAIN_QUARTER`, `TIME_GRAIN_YEAR` |

#### Filters

- **Simple filter (dict):**
  ```python
  {"field": "origin", "operator": "=", "value": "JFK"}
  ```
- **Compound filter (dict):**
  ```python
  {
    "operator": "AND",
    "conditions": [
      {"field": "origin", "operator": "in", "values": ["JFK", "LGA"]},
      {"field": "year", "operator": ">", "value": 2010}
    ]
  }
  ```
- **Callable:** `lambda t: t.origin == 'JFK'`
- **String:** `"_.origin == 'JFK'"`

**Supported operators:** `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`, `like`, `not like`, `is null`, `is not null`, `AND`, `OR`

**Smart Filter Types:** BSL automatically determines the correct timing for each filter:

- **Dimension & raw column filters** → Pre-aggregation (WHERE clause)
- **Measure filters** → Post-aggregation (HAVING clause equivalent)

This ensures accurate results for complex business queries.

#### Example

```python
# Complex business query with mixed filter types
sales_sm.query(
    dimensions=['region', 'product_category'],
    measures=['total_revenue', 'order_count'],
    filters=[
        {"field": "region", "operator": "in", "values": ["North", "South"]},     # Dimension (pre-aggregation)
        {"field": "order_date", "operator": ">=", "value": "2024-01-01"},       # Raw column (pre-aggregation)
        {"field": "order_amount", "operator": ">", "value": 50},                # Raw column (pre-aggregation)
        {"field": "total_revenue", "operator": ">", "value": 10000}             # Measure (post-aggregation)
    ],
    order_by=[('total_revenue', 'desc')],
    limit=10
)
```

**Query execution:**
1. Filter raw data: North/South regions, orders since 2024, amounts > $50
2. Aggregate by region/category: calculate total_revenue, order_count  
3. Filter results: keep only combinations with revenue > $10,000

Example output:

| region | product_category | total_revenue | order_count |
|--------|-----------------|---------------|-------------|
| North  | Electronics     | 25,450        | 142         |
| South  | Home & Garden   | 18,200        | 89          |

### Chart API Reference

The `QueryExpr` object provides the `chart()` method for visualization:

| Parameter | Type                     | Required | Allowed Values / Notes                                                                                      |
|-----------|--------------------------|----------|------------------------------------------------------------------------------------------------------------|
| `spec`    | dict or None             | No       | Vega-Lite specification dict. If not provided, will auto-detect chart type.<br>If partial spec is provided (e.g., only encoding or only mark), missing parts will be auto-detected and merged. |
| `format`  | str                      | No       | Output format of the chart:<br>- `"altair"` (default): Returns Altair Chart object<br>- `"interactive"`: Returns interactive Altair Chart with tooltip<br>- `"json"`: Returns Vega-Lite JSON specification<br>- `"png"`: Returns PNG image bytes (requires `pip install altair[all]`)<br>- `"svg"`: Returns SVG string (requires `pip install altair[all]`) |

**Returns:** Chart in the requested format (Altair Chart object, dict, bytes, or str depending on format)

For more examples, see `examples/example_chart.py` in the repository.
