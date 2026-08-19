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
`t.all(ref)` is available on the table parameter `t` in measure definitions. It
evaluates the supplied measure or reduction over the entire dataset regardless
of grouping, making it useful for percentages and comparisons with the total.
</note>

For more examples, see the [Percent of Total pattern](/advanced/percentage-total).

## graph

The `graph` property provides a dependency graph showing how dimensions and measures relate to each other. This is useful for:
- **Understanding dependencies**: See what columns or fields each dimension/measure depends on
- **Impact analysis**: Find what breaks when changing a field
- **Documentation**: Generate visual representations of your data model
- **Validation**: Ensure your model doesn't have circular dependencies

```graph_demo
# Build a semantic table with dependencies
flights_with_deps = flights_st.with_dimensions(
    origin=lambda t: t.origin,
    destination=lambda t: t.dest,
).with_measures(
    flight_count=lambda t: t.count(),
    total_distance=lambda t: t.distance.sum(),
    avg_distance_per_flight=lambda t: t.total_distance / t.flight_count
)

# Access the dependency graph
graph = flights_with_deps.get_graph()
graph
```
<regularoutput code-block="graph_demo"></regularoutput>

### Understanding the Graph Structure

The graph is a dictionary where:
- **Keys**: Dimension or measure names
- **Values**: Metadata containing:
  - `deps`: Dependencies mapped to their types (`'column'`, `'dimension'`, or `'measure'`)
  - `type`: The field type (`'dimension'`, `'measure'`, or `'calc_measure'`)

```graph_structure
# Access the graph - it's a dict-like object
graph = flights_with_deps.get_graph()
graph
```
<regularoutput code-block="graph_structure"></regularoutput>

```python
# Find what a specific field depends on
flights_with_deps.get_graph()['avg_distance_per_flight']['deps']
# Output: {'total_distance': 'measure', 'flight_count': 'measure'}
```

### Graph Traversal

Use `graph_predecessors()` and `graph_successors()` to navigate dependencies:

```graph_traversal
from boring_semantic_layer.graph_utils import graph_predecessors, graph_successors

graph = flights_with_deps.get_graph()

# What does this field depend on? (predecessors)
graph_predecessors(graph, 'avg_distance_per_flight')
# {'total_distance', 'flight_count'}

# What depends on this field? (successors)
graph_successors(graph, 'total_distance')
# {'avg_distance_per_flight'}
```
<regularoutput code-block="graph_traversal"></regularoutput>

### Working with the Dependency Graph

The dependency graph is a dict-like object where each key is a field name and the value is a dict with `"type"` (dimension/measure/calc_measure/column) and `"deps"` (dependencies with their types):

```python
# Access the graph directly as a dict
graph = flights_with_deps.get_graph()

# Iterate over fields and their dependencies
for field, info in graph.items():
    print(f"{field} ({info['type']}): depends on {info['deps']}")
```

## join_one() / join_many() / join_cross()

Join semantic tables together to query across relationships. Joins allow you to combine data from multiple semantic tables and access dimensions and measures across all joined tables.

**What Makes Semantic Joins Different?**

Semantic joins explicitly capture the **relationship type** between tables, rather than just specifying SQL join mechanics:

**SQL Joins:**
```python
# Specifies HOW to join, but not the analytical relationship
flights_tbl.left_join(carriers_tbl, flights_tbl.carrier == carriers_tbl.code)
```

**Semantic Joins:**
```python
# One carrier row can match many flight rows
carriers.join_many(flights_st, lambda c, f: c.code == f.carrier)
```

**What You Get:**
- **Explicit relationships**: `join_many()` documents that this is a one-to-many relationship
- **Table hierarchy information**: The method name describes how tables relate to each other
- **Richer metadata**: Makes the data model structure explicit for documentation and tooling

<note type="info">
After joining, dimensions and measures are prefixed with table names (e.g., `flights.origin`, `carriers.name`) to avoid naming conflicts.
</note>

<note type="warning">
**Give every source in a composed model a unique name.** BSL uses model names as
source aliases for dimensions, measures, and grain metadata, and rejects a join
tree containing duplicate names. If you join the same underlying table more than
once (for example, pickup and dropoff locations), create distinct table references
and assign explicit aliases:

```python
# Create distinct references when joining same table twice
pickup_locs = to_semantic_table(locs_tbl.view(), "pickup_locs")
dropoff_locs = to_semantic_table(locs_tbl.view(), "dropoff_locs")
```

The distinct names prevent ambiguous semantic prefixes; `.view()` prevents Ibis
from treating both roles as the same relation.
</note>

<note type="warning">
**Source-aware aggregation requires equality-key joins.** When BSL aggregates
measures at their source grain before joining, each non-cross join predicate must
be a direct field equality or a conjunction of direct field equalities. String,
Deferred, and compound equality-key shorthands are supported. Predicates using
inequality, `OR`, casts, or transformed expressions are rejected because reducing
them to join-key bridges could change the matched row set. Aggregate the models
first or restate the relationship with plain equality keys; use `join_cross()` for
a Cartesian product.
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
# One carrier row can match many flight rows
carriers_with_flights = carriers.join_many(
    flights_st,
    lambda c, f: c.code == f.carrier
)

# Inspect available dimensions and measures
carriers_with_flights.dimensions
```
<regularoutput code-block="join_demo"></regularoutput>

After joining, all dimensions and measures from both tables are available. Each is prefixed with its table name to avoid conflicts:


### join_one() - At-Most-One Right Match

Use `join_one()` when each row on the left can match at most one row on the
right. Like all non-cross semantic joins, it uses a LEFT JOIN so unmatched left
rows remain visible to measures.

```python
# Many flights → one carrier (each flight matches at most one carrier)
flights_with_carrier = flights_st.join_one(
    carriers,
    lambda f, c: f.carrier == c.code
)
```

<note type="warning">
**Join predicates resolve physical columns.** A string or Deferred shorthand
names the same underlying column on both sides. If the columns have different
names, use a two-argument lambda instead.

**Example:**
```python
# If users table has column 'id' but dimension 'customer_id':
users = to_semantic_table(users_tbl, "users").with_dimensions(
    customer_id=lambda t: t.id  # Dimension renamed
)

# Compare the underlying columns explicitly:
orders.join_one(users, on=lambda order, user: order.customer_id == user.id)
```
</note>

### join_cross() - Cross Join

Use `join_cross()` to create every possible combination of rows from both tables (CARTESIAN PRODUCT).

```python
# Every flight × every carrier combination
all_combinations = flights_st.join_cross(carriers)
```

### Requiring a Match

`join_one()` and `join_many()` are always LEFT joins (there is no `how=`
parameter). When a query should
require a match, make the row removal explicit with a filter on a non-nullable
field from the right table:

```python
flights_matched = flights_st.join_one(
    carriers,
    lambda f, c: f.carrier == c.code,
).filter(lambda t: t["carriers.name"].notnull())
```

Use `join_cross()` for Cartesian products.

## Next Steps

- Learn about [Composing Models](/building/compose)
- Explore [YAML Configuration](/building/yaml)
- Start [Querying Semantic Tables](/querying/methods)
