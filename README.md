# Boring Semantic Layer (BSL)

The Boring Semantic Layer (BSL) is a lightweight semantic layer based on [Ibis](https://ibis-project.org/). 

It's designed to integrate easily into your Python environment and be compatible with any backend where your data is stored.

*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs) and [boringdata](https://www.boringdata.io/). We welcome feedback and contributions!*

-----

## Table of Contents

- [Why Choose Boring Semantic Layer?](#why-choose-boring-semantic-layer)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Quickstart: Your First Semantic Model](#quickstart-your-first-semantic-model)
- [Advanced Usage](#advanced-usage)
  - [Filters](#filters)
  - [Time-Based Dimensions and Queries](#time-based-dimensions-and-queries)
  - [Joins Across Semantic Models](#joins-across-semantic-models)

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

Getting started with Boring Semantic Layer is straightforward:

```bash
pip install boring-semantic-layer
```

-----

## Quickstart: Your First Semantic Model

Let's create a `SemanticModel` and run a query.

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

### 3. Define Your Semantic Model

Now, define your `SemanticModel`. We'll add dimensions for origin and destination, and measures for total flights, total distance, and average distance.

```python
flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'destination': lambda t: t.dest, # Note: using 'dest' as per actual column in the dataset
        'year': lambda t: t.year, # Add year as a dimension for richer analysis
    },
    measures={
        'total_flights': lambda t: t.count(),
        'total_distance': lambda t: t.distance.sum(),
        'avg_distance': lambda t: t.distance.mean(),
    }
)
```


### 4. Query Your Semantic Model

Finally, use the `query` method to get data. You can specify dimensions, measures, filters, and a limit.

```python
# Query total flights and average distance for flights originating from JFK,
# grouped by origin, with a limit of 10 results.
query_result_df = flights_sm.query(
    dims=['origin'],
    measures=['total_flights', 'avg_distance'],
    filters=[{'field': 'origin', 'operator': '=', 'value': 'JFK'}],
    limit=10
).execute() # Execute the query to get a pandas DataFrame

print(query_result_df)
```

This returns a DataFrame. BSL makes complex queries simple:

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
  # Example of an Ibis expression filter to find flights from 'JFK'
  ibis_filter_df = flights_sm.query(
      dims=['origin'],
      measures=['total_flights'],
      filters=[
          lambda t: t.origin == 'JFK'
      ]
  ).execute()
  print(ibis_filter_df)
  ```

- **JSON-based** (for easy LLM integration): A format that's easy to serialize, good for dynamic queries or LLM integration.
  ```python
  # Example of a compound filter using 'AND' logic
  compound_filter_df = flights_sm.query(
      dims=['origin'],
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

Join different `SemanticModel` instances to query across related datasets.

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


-----