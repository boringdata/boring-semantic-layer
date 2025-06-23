# Boring Semantic Layer (BSL)

The Boring Semantic Layer (BSL) is a lightweight semantic layer based on [Ibis](https://ibis-project.org/). 

**Key Features:**
- **Lightweight**: Just `pip install boring-semantic-layer`
- **Ibis-powered**: Built on top of [Ibis](https://ibis-project.org/), supporting any database engine that Ibis integrates with (DuckDB, Snowflake, BigQuery, PostgreSQL, and more)
- **MCP-friendly**: Perfect for connecting Large Language Models to structured data sources

*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs) and [boringdata](https://www.boringdata.io/). We welcome feedback and contributions!*

-----

## Table of Contents

- [Why Choose Boring Semantic Layer?](#why-choose-boring-semantic-layer)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Get Started](#get-started)
  0. [Get Sample Data](#0-get-sample-data)
  2. [Query a Semantic Model](#2-query-a-semantic-model)
  3. [Advanced Usage](#3-advanced-usage)
- [Features](#features)
  - [Filters](#filters)
    - [Ibis Expression](#ibis-expression)
    - [JSON-based](#json-based)
  - [Time-Based Dimensions and Queries](#time-based-dimensions-and-queries)
  - [Joins Across Semantic Models](#joins-across-semantic-models)
    - [Classic SQL Joins](#classic-sql-joins)
    - [join_one](#join_one)
    - [join_many](#join_many)
    - [join_cross](#join_cross)
- [Reference](#reference)
  - [SemanticModel](#semanticmodel)
  - [Query (SemanticModel.query / QueryExpr)](#query-semanticmodelquery--queryexpr)

-----

## Why Choose Boring Semantic Layer?

Semantic layers are becoming essential for connecting Large Language Models (LLMs) with data, making data interaction more intuitive and powerful.

Our Ibis-based Semantic Layer offers:

  * **Embeddable in any Python environment**: Use BSL in any Python tool, be it a BI platform, an ML pipeline, or a simple Jupyter notebook.
  * **Wide Backend Support**: Powered by Ibis, BSL connects to numerous data warehouses and databases like **DuckDB**, **Snowflake**, **BigQuery**, and **PostgreSQL**.
  * **Simple Pythonic API**: Define dimensions and measures with an easy-to-use Python API, simplifying the creation and maintenance of your semantic definitions.

-----

## How It Works

BSL lets you define a `SemanticModel` on your data tables. This gives you a structured and consistent way to view your data for analysis.

BSL's core is the `SemanticModel` class, where you define your dimensions and measures:

```python
flights_sm = SemanticModel(
    name="flights", # Optional: A human-readable name for your model
    table=flights_tbl, # Your Ibis table expression
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
    },
    measures={
        "flight_count": lambda t: t.count(),
    }
)
```

**Understanding Dimensions and Measures:**

  * **Dimensions**: These are attributes you use to group, segment, and filter your data (e.g., `country`, `user_cohort`, `time_of_day`). Think of them as the "by" clauses in your analysis – for example, analyzing flights "by origin" or "by destination."
  * **Measures**: These are numerical values that you aggregate or calculate (e.g., `total_revenue`, `unique_users`, `average_order_value`). They represent the "what" you're measuring.

**Ibis Under the Hood**: BSL uses Ibis expressions for all queries and data transformations. When you query a `SemanticModel`, your request becomes an Ibis expression. Ibis then translates it into optimized SQL for your data backend. This means your semantic models work across different backends without changes.

**Example Query**:

```python
# Querying for the flight count by origin
result_df = flights_sm.query(
    dims=["origin"],
    measures=["flight_count"]
).execute() # .execute() materializes the query into a pandas DataFrame (by default)
print(result_df)
```

This simple query gives you a DataFrame, hiding the complex SQL:

| origin | flight\_count |
| :----- | :----------- |
| PHL | 7708 |
| JFK | 3689 |
| JAX | 1599 |
| FNT | 83 |
| MLB | 10 |
| ... | ... |

-----

## Installation

```bash
pip install boring-semantic-layer
```

-----

## Get Started

### 0. Get Sample Data

We'll use a public flight dataset from the [Malloy Samples repository](https://github.com/malloydata/malloy-samples/tree/main/data).

```bash
git clone https://github.com/malloydata/malloy-samples
```

### 2. Build a Semantic Model

Define your data source and create a semantic model that describes your data in terms of dimensions and measures.

```python
import ibis
from boring_semantic_layer import SemanticModel

# Connect to your database (here, DuckDB in-memory for demo)
con = ibis.duckdb.connect(":memory:")
flights_tbl = con.read_parquet("malloy-samples/data/flights.parquet")

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

---

### 3. Query a Semantic Model

Use your semantic model to run queries—selecting dimensions, measures, and applying filters or limits.

```python
# Query: total flights and average distance by origin, limit 10
flights_sm.query(
    dimensions=['origin'],
    measures=['total_flights', 'avg_distance'],
    filters=[{'field': 'origin', 'operator': '=', 'value': 'JFK'}],
    limit=10
).execute() # Execute the query to get a pandas DataFrame

print(query_result_df)
```

This returns a DataFrame like:

| origin | total_flights | avg_distance |
|--------|---------------|--------------|
| JFK    | 3689          | 1047.71      |
| PHL    | 7708          | 1044.97      |
| ...    | ...           | ...          |

-----

## Features

Explore more features for advanced data analysis.

- [Filters](#filters): Filter data using Ibis expressions or a flexible JSON format, ideal for LLM integration.
- [Time-Based Dimensions and Queries](#time-based-dimensions-and-queries): Easily aggregate data over specific time ranges and granularities.
- [Joins Across Semantic Models](#joins-across-semantic-models): Enrich your data by defining seamless joins between different `SemanticModel` instances.

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

#### JSON-based

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
BSL supports the following operators: `=`, `!=`, `>`, `>=`, `in`, `not in`, `like`, `not like`, `is null`, `is not null`, `AND`, `OR`

### Time-Based Dimensions and Queries

BSL has built-in support for flexible time-based analysis. 

To use it, define a `timeDimension` in your `SemanticModel` that points to a timestamp column. 

You can also set `smallestTimeGrain` to prevent incorrect time aggregations.

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
    timeDimension='dep_time', # The column containing timestamps. Crucial for time-based queries.
    smallestTimeGrain='TIME_GRAIN_SECOND' # Optional: sets the lowest granularity (e.g., DAY, MONTH).
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
The query aggregates the number of flights by origin and day:
| origin | arr_time   | flight_count |
|--------|------------|--------------|
| PHL    | 2004-07-27 | 5            |
| CLE    | 2004-05-19 | 5            |
| DFW    | 2004-04-27 | 7            |
| DFW    | 2004-03-18 | 9            |
| DFW    | 2004-01-08 | 13           |

### Joins Across Semantic Models

BSL allows you to join multiple `SemanticModel` instances to enrich your data. Joins are defined in the `joins` parameter of a `SemanticModel`.

There are four main ways to define joins:

#### Classic SQL Joins

For full control, you can create a `Join` object directly, specifying the join condition with an `on` lambda function and the join type with `how` (e.g., `'inner'`, `'left'`).

First, let's define two semantic models: one for flights and one for carriers.

The flight model resulting from a join with the carriers model:

```python
from boring_semantic_layer.semantic_model import Join, SemanticModel
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
    },
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

print(query_joined_df)
```
| carriers_name | origin | flight_count |
|---------------|--------|--------------|
| Delta Air Lines | MDT | 235 |
| Delta Air Lines | ATL | 8419 |
| Comair (Delta Connections) | ATL | 239 |
| American Airlines | DFW | 8742 |
| American Eagle Airlines | JFK | 418 |

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
from boring_semantic_layer.semantic_model import Join

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
