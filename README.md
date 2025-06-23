# Boring Semantic Layer (BSL)

The Boring Semantic Layer (BSL) is a lightweight semantic layer that provides a simple, Pythonic interface for defining and querying data models.

**Key Features:**
- **Lightweight**: Just `pip install boring-semantic-layer`
- **Ibis-powered**: Built on top of [Ibis](https://ibis-project.org/), supporting any database engine that Ibis integrates with (DuckDB, Snowflake, BigQuery, PostgreSQL, and more)
- **Python-native**: Define dimensions and measures using familiar Python syntax
- **LLM-friendly**: Perfect for connecting Large Language Models to structured data sources

*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs) and [boringdata](https://www.boringdata.io/). 

We welcome feedback and contributions!*

-----

## Table of Contents

- [Installation](#installation)
- [How It Works & Quickstart](#how-it-works--quickstart)
- [Advanced Usage](#advanced-usage)
  - [Filters](#filters)
  - [Time-Based Dimensions and Queries](#time-based-dimensions-and-queries)
  - [Joins Across Semantic Models](#joins-across-semantic-models)

-----

## Installation

Getting started with Boring Semantic Layer is straightforward:

```bash
pip install boring-semantic-layer
```

-----

## How It Works & Quickstart

*Note: All documentation examples in this guide are based on the [flight dataset](https://github.com/malloydata/malloy-samples/tree/main/data) to demonstrate real-world usage patterns.*

### 1. Get Sample Data

We'll use a public flight dataset from the [Malloy Samples repository](https://github.com/malloydata/malloy-samples/tree/main/data).

First, clone the data directory:

```bash
git clone https://github.com/malloydata/malloy-samples/tree/main/data
```

### 2. Connect to Your Data Source with Ibis

Define your data tables with Ibis. We'll use DuckDB with an in-memory connection and load a Parquet file.

```python
import ibis
import os
from boring_semantic_layer.semantic_model import SemanticModel

# Connect to an in-memory DuckDB database
con = ibis.duckdb.connect(":memory:")

# Load the flights.parquet file into an Ibis table expression
flights_tbl = con.read_parquet("malloy-samples/data/flights.parquet")
```

### 3. Defining a Semantic Model

BSL's core is the `SemanticModel` class, where you define your dimensions and measures using familiar Python syntax:

Let's have a look at an example of a `SemanticModel` for the flight dataset:

```python
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

The semantic model defines the following:

* **Table**: The Ibis table that the semantic model is based on.

* **Dimensions**: These are attributes you use to group, segment, and filter your data (e.g., `origin`, `destination`). Think of them as the "by" clauses in your analysis – for example, analyzing flights "by origin" or "by destination."

* **Measures**: These are numerical values that you aggregate or calculate (e.g., `flight_count`). They represent the "what" you're measuring.

All dimensions and measures are defined as Ibis expressions.

Ibis expressions are Python functions that represent database operations.

They allow you to write database queries using familiar Python syntax while Ibis handles the translation to optimized SQL for your specific database backend (like DuckDB, PostgreSQL, BigQuery, etc.).

For example, in our semantic model:
- `lambda t: t.origin` is an Ibis expression that references the "origin" column
- `lambda t: t.count()` is an Ibis expression that counts rows
- `lambda t: t.distance.mean()` is an Ibis expression that calculates the average distance

The `t` parameter represents the table, and you can chain operations like `t.origin.upper()` or `t.dep_delay > 0` to create complex expressions. Ibis ensures these expressions are translated to efficient SQL queries.

### 4. Querying a Semantic Model

When you query a `SemanticModel`, your request becomes an Ibis expression.

Ibis then translates it into optimized SQL for your data backend. 

This means your semantic models work across different backends without changes.

Let's query the semantic model to get:
- the total flights and average distance 
- group by origin
- with a limit of 10

```python
flights_sm.query(
    dimensions=['origin'],
    measures=['total_flights', 'avg_distance'],
    limit=10
).execute()
```

The result is a dataframe:

| origin | total\_flights | avg\_distance |
| :----- | :------------ | :----------- |
| JFK | 3689 | 1047.71 |
| PHL | 7708 | 1044.97 |
| JAX | 1599 | 1044.00 |
| FNT | 83 | 1044.00 |
| MLB | 10 | 1044.00 |
| ... | ... | ... |

-----

## Advanced Usage

Explore more features for advanced data analysis.

- [Filters](#filters): Filter data using Ibis expressions or a flexible JSON format, ideal for LLM integration.
- [Time-Based Dimensions and Queries](#time-based-dimensions-and-queries): Easily aggregate data over specific time ranges and granularities.
- [Joins Across Semantic Models](#joins-across-semantic-models): Enrich your data by defining seamless joins between different `SemanticModel` instances.

### Filters

The `query` method can filter data.

Filters can be of 2 types:
- **Ibis expression**: Use raw Ibis expressions for full flexibility.
  
  ```python
   flights_sm.query(
      dimensions=['origin'],
      measures=['total_flights'],
      filters=[
          lambda t: t.origin == 'JFK'
      ]
  )
  ```

- **JSON-based** (for easy LLM integration): A format that's easy to serialize, good for dynamic queries or LLM integration.
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
    time_dimension='dep_time',
    smallest_time_grain='TIME_GRAIN_SECOND'
)
```
With the time dimension defined, you can query using a specific time range and grain.

```python
flights_sm_with_time.query(
    dimensions=['origin'],
    measures=['total_flights'],
    time_range={'start': '2013-01-01', 'end': '2013-01-31'},
    time_grain='TIME_GRAIN_DAY' # Use specific TIME_GRAIN constants
)
```
The query aggregates the number of flights by origin and day:
| origin | arr_time   | flight_count |
|--------|------------|--------------|
| PHL    | 2004-07-27 | 5            |
| CLE    | 2004-05-19 | 5            |
| DFW    | 2004-04-27 | 7            |
| DFW    | 2004-03-18 | 9            |
| DFW    | 2004-01-08 | 13           |

Available time grains are: `TIME_GRAIN_SECOND`, `TIME_GRAIN_MINUTE`, `TIME_GRAIN_HOUR`, `TIME_GRAIN_DAY`, `TIME_GRAIN_WEEK`, `TIME_GRAIN_MONTH`, `TIME_GRAIN_QUARTER`, `TIME_GRAIN_YEAR`.

### Joins Across Semantic Models

BSL allows you to join multiple `SemanticModel` instances to enrich your data. Joins are defined in the `joins` parameter of a `SemanticModel`.

There are two main ways to define joins:

#### 1. Regular Joins

For full control, you can create a `Join` object directly, specifying the join condition with an `on` lambda function and the join type with `how` (e.g., `'inner'`, `'left'`).

First, let's define two semantic models: one for flights and one for carriers.

The flight model resulting from a join with the carriers model:

```python
from boring_semantic_layer.semantic_model import Join

# Carriers model
carriers_sm = SemanticModel(
    name="carriers",
    table=con.read_parquet("malloy-samples/data/carriers.parquet"),
    dimensions={'name': lambda t: t.name},
    measures={'carrier_count': lambda t: t.count()}
)

# Flights model with a regular join to carriers
flights_with_join_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={'origin': lambda t: t.origin},
    measures={'flight_count': lambda t: t.count()},
    joins={
        "carriers": Join(
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code,
            how="left"
        )
    }
)
```

Now, you can query across both models by prefixing dimensions and measures with the join alias:

```python
flights_with_join_sm.query(
    dimensions=["carriers.name"],
    measures=["flight_count"],
    limit=5
).execute()
```

#### 2. Malloy-style Joins (with `primary_key`)

For common join patterns, BSL provides helper functions inspired by [Malloy](https://docs.malloydata.dev/documentation/language/join): `join_one`, `join_many`, and `join_cross`. 

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

Now, you can use `join_one` in the `flights` model to link to `carriers_pk_sm`. The `with_` parameter specifies the foreign key on the `flights` model.

```python
from boring_semantic_layer.semantic_model import join_one

flights_with_join_one_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={'origin': lambda t: t.origin},
    measures={'flight_count': lambda t: t.count()},
    joins={
        "carriers": join_one(
            model=carriers_pk_sm,
            with_=lambda t: t.carrier 
        )
    }
)
```

- **`join_one(model, with_)`**: Use for one-to-one or many-to-one relationships. It joins where the foreign key specified in `with_` matches the `primary_key` of the joined `model`.
- **`join_many(model, with_)`**: Similar to `join_one`, but semantically represents a one-to-many relationship.
- **`join_cross(model)`**: Creates a cross product, joining every row from the left model with every row of the right `model`.

Querying remains the same—just reference the joined fields using the alias.

```python
flights_with_join_one_sm.query(
    dimensions=["carriers.name"],
    measures=["flight_count"],
    limit=5
).execute()
```
