# Boring Semantic Layer (BSL)

The Boring Semantic Layer (BSL) is a lightweight semantic layer based on [Ibis](https://ibis-project.org/).

**Key Features:**
- **Lightweight**: `pip install boring-semantic-layer`
- **Ibis-powered**: Built on top of [Ibis](https://ibis-project.org/), supporting any database engine that Ibis integrates with (DuckDB, Snowflake, BigQuery, PostgreSQL, and more)
- **MCP-friendly**: Perfect for connecting Large Language Models to structured data sources


*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs/xorq) and [boringdata](https://www.boringdata.io/).*

We welcome feedback and contributions!

# Quick Example

```
pip install 'boring-semantic-layer[examples]'
```

**1. Define your ibis input table**

```python
import ibis

flights_tbl = ibis.table(
    name="flights",
    schema={"origin": "string", "carrier": "string"}
)
```

**2. Define a semantic model**

```python
from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={"origin": lambda t: t.origin},
    measures={"flight_count": lambda t: t.count()}
)
```

**3. Query it**

```python
flights_sm.query(
    dimensions=["origin"],
    measures=["flight_count"]
).execute()
```

**Example output (dataframe):**

| origin | flight_count |
| ------ | ------------ |
| JFK    | 3689         |
| LGA    | 2941         |
| ...    | ...          |


-----

## Table of Contents

- [Boring Semantic Layer (BSL)](#boring-semantic-layer-bsl)
- [Quick Example](#quick-example)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Get Started](#get-started)
    - [1. Get Sample Data](#1-get-sample-data)
    - [2. Build a Semantic Model](#2-build-a-semantic-model)
    - [Adding Descriptions to Semantic Models, Dimensions, and Measures](#adding-descriptions-to-semantic-models-dimensions-and-measures)
    - [3. Query a Semantic Model](#3-query-a-semantic-model)
  - [Features](#features)
    - [Filters](#filters)
      - [Ibis Expression](#ibis-expression)
      - [JSON-based (MCP \& LLM friendly)](#json-based-mcp--llm-friendly)
    - [Joins Across Semantic Models](#joins-across-semantic-models)
      - [Classic SQL Joins](#classic-sql-joins)
      - [join\_one](#join_one)
      - [join\_many](#join_many)
      - [join\_cross](#join_cross)
  - [Model Context Protocol (MCP) Integration](#model-context-protocol-mcp-integration)
    - [Installation](#installation-1)
    - [Setting up an MCP Server](#setting-up-an-mcp-server)
    - [Configuring Claude Desktop](#configuring-claude-desktop)
    - [Available MCP Tools](#available-mcp-tools)
  - [Chart Visualization](#chart-visualization)
    - [Installation](#installation-2)
    - [How BSL Charting Works](#how-bsl-charting-works)
      - [Quick Start Example](#quick-start-example)
      - [How It Works](#how-it-works)
    - [Smart Chart Creation](#smart-chart-creation)
      - [1. Auto-detected Bar Chart](#1-auto-detected-bar-chart)
      - [2. Auto-detected Time Series Chart](#2-auto-detected-time-series-chart)
      - [3. Auto-detected Heatmap](#3-auto-detected-heatmap)
      - [4. Custom Mark with Auto-detection](#4-custom-mark-with-auto-detection)
      - [5. Full Custom Specification](#5-full-custom-specification)
      - [Export Formats](#export-formats)
  - [Reference](#reference)
    - [SemanticModel](#semanticmodel)
      - [Spec Classes (for dimensions and measures with descriptions)](#spec-classes-for-dimensions-and-measures-with-descriptions)
      - [Join object (for `joins`)](#join-object-for-joins)
    - [Query (SemanticModel.query / QueryExpr)](#query-semanticmodelquery--queryexpr)
      - [Filters](#filters-1)
      - [Example](#example)
    - [Chart API Reference](#chart-api-reference)

-----

## Installation

```bash
# Basic installation
pip install boring-semantic-layer

# For DuckDB support (used in examples)
pip install 'boring-semantic-layer[examples]'

# For MCP integration
pip install 'boring-semantic-layer[fastmcp]'

# For visualization with Altair
pip install 'boring-semantic-layer[viz-altair]'

# For visualization with Plotly
pip install 'boring-semantic-layer[viz-plotly]'
```

-----

## Get Started

### 1. Get Sample Data

We expose some test data in a public bucket. You can download it with:

```bash
curl -L https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev/flights.parquet -o flights.parquet
curl -L https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev/carriers.parquet -o carriers.parquet
```

**Note:** Examples use DuckDB, so install with: `pip install 'boring-semantic-layer[examples]'`

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

### Adding Descriptions to Semantic Models, Dimensions, and Measures

BSL supports adding human-readable descriptions to dimensions, measures, and semantic models. This helps in documenting your data model and making it easier for others to understand and AI agents to interact with.

Semantic model descriptions are an optional parameter than can be used to provide a summary of what the semantic model contains and what it should be used for.
```python
from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    description="Flight data with departure and flight count information",
    dimensions={"origin": lambda t: t.origin},
    measures={"flight_count": lambda t: t.count()}
)
```

You can define dimensions and measures in two ways:

**Classic format (still fully supported):**
```python
from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={"origin": lambda t: t.origin},
    measures={"flight_count": lambda t: t.count()}
)
```
**New format with descriptions:**
```python
from boring_semantic_layer import SemanticModel, DimensionSpec, MeasureSpec

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={
        "origin": DimensionSpec(
            expr=lambda t: t.origin,
            description="Origin Airport where the flight departed from"
        )
    },
    measures={
        "flight_count": MeasureSpec(
            expr=lambda t: t.count(),
            description="Total number of flights"
        )
    }
)
```
**Why use descriptions?**
- **Human-readable**: Makes your models self documenting for team members.
- **AI friendly**: Perfect for MCP agents and LLM's that need to understand your models in more detail and nuances between similar dimensions and measures.
- **Flexible**: You can mix classic and descriptive formats seamlessly.
- **Backwards compatible**: All existing models will continue to work without changes.

**YAML Configuration Support:**

You can also define models with descriptions using YAML configuration files:

```yaml

flights:
  table: flights_table
  description: "Flight data with departure and arrival information"

  dimensions:
    # Classic format
    origin: _.origin

    # New format
    destination:
      expr: _.destination
      description: "Destination airport code where the flight arrived at"

  measures:
    # Classic format
    flight_count: _.count()

    # New format
    avg_distance:
      expr: _.distance.mean()
      description: "Average distance of flights in miles"
```

Load the YAML model:
```python
models = SemanticModel.from_yaml("flights_model.yml", tables={"flights_table": flights_table})
flights_sm = models["flights"]
```

---

### 3. Query a Semantic Model

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
| ------ | ------------- | ------------ |
| JFK    | 3689          | 1047.71      |
| PHL    | 7708          | 1044.97      |
| ...    | ...           | ...          |

**Getting the SQL:**

To inspect the generated SQL without executing the query, use `.sql()`:

```python
query = flights_sm.query(
    dimensions=['origin'],
    measures=['total_flights', 'avg_distance'],
    limit=10
)

print(query.sql())  # Prints the SQL statement
```

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
| ------ | ------------- |
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
| ------ | ------------- |
| LGA    | 7000          |
| PHL    | 7708          |

BSL supports the following operators: `=`, `!=`, `>`, `>=`, `in`, `not in`, `like`, `not like`, `is null`, `is not null`, `AND`, `OR`

**Note on filtering measures:** filters only work with dimensions.
```

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
| ------ | ---------- | ------------ |
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
| -------------------------- | ------ | ------------ |
| Delta Air Lines            | MDT    | 235          |
| Delta Air Lines            | ATL    | 8419         |
| Comair (Delta Connections) | ATL    | 239          |
| American Airlines          | DFW    | 8742         |
| American Eagle Airlines    | JFK    | 418          |

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

| carriers_name      | flight_count |
| ------------------ | ------------ |
| Delta Air Lines    | 10000        |
| American Airlines  | 9000         |
| United Airlines    | 8500         |
| Southwest Airlines | 8000         |
| JetBlue Airways    | 7500         |

## Model Context Protocol (MCP) Integration

BSL includes built-in support for the [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol/python-sdk), allowing you to expose your semantic models to LLMs like Claude.

**💡 Pro tip:** Use [descriptions in semantic models, dimensions and measures](#adding-descriptions-to-semantic-models-dimensions-and-measures) to make your models more AI-friendly. Descriptions help provide context to LLM's, enabling them to understand what each field represents and when to use them.

### Installation

To use MCP functionality, install with the `mcp` extra:

```bash
pip install 'boring-semantic-layer[fastmcp]'
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

To use `altair` backend:
```bash
pip install 'boring-semantic-layer[viz-altair]'
```

To use `plotly` backend:
```bash
pip install 'boring-semantic-layer[viz-plotly]'
```
### How BSL Charting Works

BSL's charting system features **dual backend support**, allowing you to choose between two powerful visualization libraries:

- **[Altair](https://altair-viz.github.io/)** (default): Built on **[Vega-Lite](https://vega.github.io/vega-lite/)**, a JSON-based grammar for creating interactive web-native visualizations with a declarative approach
- **[Plotly](https://plotly.com/python/)**: Rich interactive plotting library with extensive chart types and dashboard integration capabilities

You can switch backends using the `backend` parameter: `chart(backend="altair")` or `chart(backend="plotly")`.

BSL supports multiple output formats including interactive charts, static images (PNG/SVG), and JSON specifications for web embedding across both backends.

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

### Backend Selection

BSL supports two charting backends:

- **Altair** (default): `chart(backend="altair")`
- **Plotly**: `chart(backend="plotly")`

```python
# Altair backend (default) - uses Vega-Lite spec format
altair_chart = query.chart()  # or chart(backend="altair")
altair_custom = query.chart(spec={"mark": "bar", "title": "My Chart"})

# Plotly backend - uses BSL custom spec format  
plotly_chart = query.chart(backend="plotly")
plotly_custom = query.chart(backend="plotly", spec={
    "chart_type": "scatter",  # Maps to px.scatter() function
    "layout": {"title": "My Chart"},  # Plotly layout options
    "color": "category"  # Plotly Express parameters
})
```

**Spec Format Differences:**
- **Altair**: Uses standard [Vega-Lite specification](https://vega.github.io/vega-lite/docs/spec.html) format
- **Plotly**: Uses BSL's custom format combining:
  - `chart_type`: Maps to Plotly Express functions (`px.bar`, `px.line`, `px.scatter`, etc.)  
  - `layout`: Standard [Plotly layout](https://plotly.com/python/reference/layout/) options
  - Other keys: [Plotly Express parameters](https://plotly.com/python-api-reference/plotly.express.html)

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

# Auto-detects bar chart (Altair)
altair_chart = query.chart()

# Auto-detects bar chart (Plotly)
plotly_chart = query.chart(backend="plotly")
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

# Auto-detects time series line chart (Altair)
altair_chart = time_query.chart()

# Auto-detects time series line chart (Plotly)
plotly_chart = time_query.chart(backend="plotly")
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

# Auto-detects heatmap with custom sizing (Altair)
altair_chart = heatmap_query.chart(spec={
    "height": 300,
    "width": 400
})

# Auto-detects heatmap (Plotly)
plotly_chart = heatmap_query.chart(backend="plotly")
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

| Field                 | Type                                 | Required | Allowed Values / Notes                                                                                                                                                      |
| --------------------- | ------------------------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `table`               | Ibis table expression                | Yes      | Any Ibis table or view                                                                                                                                                      |
| `dimensions`          | dict[str, callable or DimensionSpec] | Yes      | Keys: dimension names; Values: functions mapping table → column OR DimensionSpec objects with descriptions                                                                  |
| `measures`            | dict[str, callable or MeasureSpec]   | Yes      | Keys: measure names; Values: functions mapping table → aggregation OR MeasureSpec objects with descriptions                                                                 |
| `description`         | str                                  | No       | Optional description of the model                                                                                                                                           |
| `joins`               | dict[str, Join]                      | No       | Keys: join alias; Values: `Join` object (see below)                                                                                                                         |
| `primary_key`         | str                                  | No       | Name of the primary key dimension (required for certain join types)                                                                                                         |
| `name`                | str                                  | No       | Optional model name (inferred from table if omitted)                                                                                                                        |
| `time_dimension`      | str                                  | No       | Name of the column to use as the time dimension                                                                                                                             |
| `smallest_time_grain` | str                                  | No       | One of:<br>`TIME_GRAIN_SECOND`, `TIME_GRAIN_MINUTE`, `TIME_GRAIN_HOUR`, `TIME_GRAIN_DAY`,<br>`TIME_GRAIN_WEEK`, `TIME_GRAIN_MONTH`, `TIME_GRAIN_QUARTER`, `TIME_GRAIN_YEAR` |

#### Spec Classes (for dimensions and measures with descriptions)

**DimensionSpec:**
| Field         | Type     | Required | Notes                                                 |
| ------------- | -------- | -------- | ----------------------------------------------------- |
| `expr`        | callable | Yes      | Function mapping table -> column expression           |
| `description` | str      | No       | Human readable description (defaults to empty string) |

**MeasureSpec:**
| Field         | Type     | Required | Notes                                                 |
| ------------- | -------- | -------- | ----------------------------------------------------- |
| `expr`        | callable | Yes      | Function mapping table -> column expression           |
| `description` | str      | No       | Human readable description (defaults to empty string) |

**Example:**
```python
from boring_semantic_layer import DimensionSpec, MeasureSpec

# Define a dimension spec
origin_dimension = DimensionSpec(
    expr=lambda t: t.origin.upper(),
    description='The airport origin code in upper case'
)

# Define a measure spec
avg_distance_measure = MeasureSpec(
    expr=lambda t: t.distance.mean(),
    description='Average flight distance in miles'
)
```

#### Join object (for `joins`)
- Use `Join.one(alias, model, with_)` for one-to-one/many-to-one
- Use `Join.many(alias, model, with_)` for one-to-many
- Use `Join.cross(alias, model)` for cross join

---

### Query (SemanticModel.query / QueryExpr)

| Parameter    | Type                                           | Required | Allowed Values / Notes                                                                                                                                                      |
| ------------ | ---------------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dimensions` | list[str]                                      | No       | List of dimension names (can include joined fields, e.g. `"carriers.name"`)                                                                                                 |
| `measures`   | list[str]                                      | No       | List of measure names (can include joined fields)                                                                                                                           |
| `filters`    | list[dict/str/callable] or dict/str/callable   | No       | See below for filter formats and operators                                                                                                                                  |
| `order_by`   | list[tuple[str, str]]                          | No       | List of (field, direction) tuples, e.g. `[("avg_delay", "desc")]`                                                                                                           |
| `limit`      | int                                            | No       | Maximum number of rows to return                                                                                                                                            |
| `time_range` | dict with `start` and `end` (ISO 8601 strings) | No       | Example: `{'start': '2024-01-01', 'end': '2024-12-31'}`                                                                                                                     |
| `time_grain` | str                                            | No       | One of:<br>`TIME_GRAIN_SECOND`, `TIME_GRAIN_MINUTE`, `TIME_GRAIN_HOUR`, `TIME_GRAIN_DAY`,<br>`TIME_GRAIN_WEEK`, `TIME_GRAIN_MONTH`, `TIME_GRAIN_QUARTER`, `TIME_GRAIN_YEAR` |

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

#### Example

```python
flights_sm.query(
    dimensions=['origin', 'year'],
    measures=['total_flights'],
    filters=[
        {"field": "origin", "operator": "in", "values": ["JFK", "LGA"]},
        {"field": "year", "operator": ">", "value": 2010}
    ],
    order_by=[('total_flights', 'desc')],
    limit=10,
    time_range={'start': '2015-01-01', 'end': '2015-12-31'},
    time_grain='TIME_GRAIN_MONTH'
)
```

Example output:

| origin | year | total_flights |
| ------ | ---- | ------------- |
| JFK    | 2015 | 350           |
| LGA    | 2015 | 300           |

### Chart API Reference

The `QueryExpr` object provides the `chart()` method for visualization:

| Parameter | Type         | Required | Allowed Values / Notes                                                                                                                                                                                                                                                                                                                                      |
| --------- | ------------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spec`    | dict or None | No       | Chart specification dict. Format depends on backend:<br>- **Altair**: [Vega-Lite specification](https://vega.github.io/vega-lite/docs/spec.html)<br>- **Plotly**: BSL custom format (see Backend Selection section)<br>If not provided, will auto-detect chart type. If partial spec provided, missing parts will be auto-detected and merged. |
| `backend` | str          | No       | Charting backend to use:<br>- `"altair"` (default): Use Altair/Vega-Lite backend<br>- `"plotly"`: Use Plotly backend |
| `format`  | str          | No       | Output format of the chart:<br>- `"static"` (default): Returns chart object (Chart/Figure)<br>- `"interactive"`: Returns interactive chart with tooltip<br>- `"json"`: Returns JSON specification<br>- `"png"`: Returns PNG image bytes (requires additional dependencies)<br>- `"svg"`: Returns SVG string (requires additional dependencies) |

**Returns:** Chart in the requested format (Altair Chart object, dict, bytes, or str depending on format)

For more examples, see `examples/example_chart.py` in the repository.
