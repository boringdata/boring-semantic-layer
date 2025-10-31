# Building a Semantic Table

Define your data model with dimensions and measures using Ibis expressions.

## Overview

A Semantic Table is the core building block of BSL. It transforms a raw Ibis table into a reusable, self-documenting data model by defining:
- **Dimensions**: Attributes to group by (e.g., origin, carrier, year)
- **Measures**: Aggregations and calculations (e.g., flight count, total distance)

## to_semantic_table()

```setup_flights
import ibis
from boring_semantic_layer import to_semantic_table

# 1. Start with an Ibis table
con = ibis.duckdb.connect(":memory:")
flights_data = ibis.memtable({
    "origin": ["JFK", "LAX", "SFO"],
    "dest": ["LAX", "SFO", "JFK"],
    "carrier": ["AA", "UA", "DL"],
    "year": [2023, 2023, 2024],
    "distance": [2475, 337, 382],
    "dep_delay": [10, 5, 0]
})
flights_tbl = con.create_table("flights", flights_data)

# 2. Convert to a Semantic Table
flights_st = to_semantic_table(flights_tbl, name="flights")
```

## with_dimensions()

Dimensions define the attributes you can group by in your queries. They represent the categorical or descriptive aspects of your data that you want to analyze.

You can define dimensions using lambda expressions, unbound syntax (`_.`), or the `Dimension` class with descriptions:

```dimensions_demo
from ibis import _
from boring_semantic_layer import Dimension

flights_st = flights_st.with_dimensions(
    # Lambda expressions - simple and explicit
    origin=lambda t: t.origin,

    # Unbound syntax - cleaner and more concise
    destination=_.dest,
    year=_.year,

    # Dimension - self-documenting and AI-friendly
    carrier=Dimension(
        expr=lambda t: t.carrier,
        description="Airline carrier code"
    )
)

flights_st.dimensions
```
<regularoutput code-block="dimensions_demo"></regularoutput>

## with_measures()

Measures define the aggregations and calculations you can query. They represent the quantitative aspects of your data that you want to analyze (counts, sums, averages, etc.).

You can define measures using lambda expressions, reference other measures for composition, or use the `Measure` class with descriptions:

```measures_demo
from boring_semantic_layer import Measure

flights_st = flights_st.with_measures(
    # Lambda expressions - simple and concise
    total_flights=lambda t: t.count(),
    total_distance=lambda t: t.distance.sum(),
    max_delay=lambda t: t.dep_delay.max(),

    # Reference other measures for composition
    avg_distance_per_flight=lambda t: t.total_distance / t.total_flights,

    # Measure - self-documenting and AI-friendly
    avg_distance=Measure(
        expr=lambda t: t.distance.mean(),
        description="Average flight distance in miles"
    )
)

flights_st.measures
```

<regularoutput code-block="measures_demo"></regularoutput>

### all()

The `all()` function references the entire dataset within measure definitions, enabling percent-of-total and comparison calculations.

**Example:** Calculate market share as a percentage

```measure_all_demo
flights_with_pct = flights_st.with_measures(
        flight_count=lambda t: t.count(),
        market_share=lambda t: t.flight_count / t.all(t.flight_count) * 100  # Percent of total
    )

# Query by carrier
result = (
    flights_with_pct
    .group_by("carrier")
    .aggregate("flight_count", "market_share")
)
```

<bslquery code-block="measure_all_demo"></bslquery>

<note type="info">
`t.all()` is a method available on the table parameter `t` in measure definitions. It references the entire dataset regardless of grouping, making it perfect for calculating percentages, or comparing groups to the total.
</note>

For more examples, see the [Percent of Total pattern](/advanced/percentage-total).

## join_one() / join_many() / join_cross()

Join semantic tables together to query across relationships. Joins allow you to combine data from multiple semantic tables and access dimensions and measures across all joined tables.

Why Semantic Joins?

Instead of just using SQL join types (`LEFT`, `INNER`, etc.), BSL offers relationship-based joins that capture the **meaning** of your data connections:

**Traditional SQL Approach:**
```python
# Which join type? LEFT or INNER? Why?
flights.join(carriers, condition, how="left")  # unclear intent
```

**BSL Semantic Approach:**
```python
# Clear intent: one carrier has many flights
flights.join_many(carriers, left_on="carrier", right_on="code")
```

**Benefits:**
- **Self-documenting**: `join_many()` tells you it's a one-to-many relationship
- **Clearer intent**: The method name describes the data relationship, not just SQL mechanics
- **Safer defaults**: Each method uses the appropriate join type for that relationship
- **Better tooling**: IDEs and AI assistants understand your data model semantics

<note type="info">
After joining, dimensions and measures are prefixed with table names (e.g., `flights.origin`, `carriers.name`) to avoid naming conflicts.
</note>


Let's get some additional data:

```setup_carriers
import ibis
from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect(":memory:")

# Create carriers data
carriers_data = ibis.memtable({
    "code": ["AA", "UA", "DL"],
    "name": ["American Airlines", "United Airlines", "Delta Air Lines"]
})
carriers_tbl = con.create_table("carriers", carriers_data)
```
<collapsedcodeblock code-block="setup_carriers" title="Create carriers Ibis table"></collapsedcodeblock>

And create a carriers semantic table:

```carriers_st
carriers = (
    to_semantic_table(carriers_tbl, name="carriers")
    .with_dimensions(
        code=lambda t: t.code,
        name=lambda t: t.name
    )
    .with_measures(
        carrier_count=lambda t: t.count()
    )
)
```

### join_many() - One-to-Many Relationships

Use `join_many()` when one row in the left table can match multiple rows in the right table (LEFT JOIN).

```join_demo
# Join carriers to flights - one carrier has many flights
flights_with_carriers = flights_st.join_many(
    carriers,
    left_on="carrier",
    right_on="code"
)

# Inspect available dimensions and measures
flights_with_carriers.dimensions
```
<regularoutput code-block="join_demo"></regularoutput>

After joining, all dimensions and measures from both tables are available. Each is prefixed with its table name to avoid conflicts:


### join_one() - One-to-One Relationships

Use `join_one()` when rows have a unique matching relationship (INNER JOIN).

```python
# Many flights → one carrier (each flight has exactly one carrier)
flights_with_carrier = flights_st.join_one(
    carriers,
    left_on="carrier",
    right_on="code"
)
```

### join_cross() - Cross Join

Use `join_cross()` to create every possible combination of rows from both tables (CARTESIAN PRODUCT).

```python
# Every flight × every carrier combination
all_combinations = flights_st.join_cross(carriers)
```

### join() - Custom Join Conditions

Use `join()` for complex join conditions or specific SQL join types.

```python
# LEFT JOIN with custom condition
flights_with_carriers = flights_st.join(
    carriers,
    lambda f, c: f.carrier == c.code,
    how="left"
)

# INNER JOIN
flights_matched = flights_st.join(
    carriers,
    lambda f, c: f.carrier == c.code,
    how="inner"
)

# Complex conditions
date_range_join = flights_st.join(
    promotions,
    lambda f, p: (f.date >= p.start_date) & (f.date <= p.end_date),
    how="left"
)
```

**Supported join types:** `"inner"`, `"left"`, `"right"`, `"outer"`, `"cross"`

## Next Steps

- Learn about [Composing Models](/examples/compose)
- Explore [YAML Configuration](/examples/yaml-config)
- Start [Querying Semantic Tables](/examples/query-methods)
