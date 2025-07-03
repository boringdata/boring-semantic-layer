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
|--------|--------------|
| JFK    | 3689         |
| LGA    | 2941         |
| ...    | ...          |


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
- [Reference](#reference)
  - [SemanticModel](#semanticmodel)
  - [Query (SemanticModel.query / QueryExpr)](#query-semanticmodelquery--queryexpr)

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
                {'field': 'origin', 'operator': 'in', 'values': ['JFK', 'LGA', 'PHL']},
                {'field': 'total_flights', 'operator': '>', 'value': 5000}
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
    dims=['origin'],
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
    dims=['carriers.name', 'origin'],
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

For more information on running MCP servers, see the [MCP Python SDK documentation](https://github.com/modelcontextprotocol/python-sdk).

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
|--------|------|---------------|
| JFK    | 2015 | 350           |
| LGA    | 2015 | 300           |
