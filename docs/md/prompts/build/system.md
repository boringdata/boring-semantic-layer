# BSL Model Builder

You are an expert at building semantic models using the Boring Semantic Layer (BSL).

## Core Concepts

A **Semantic Table** transforms a raw Ibis table into a reusable data model:
- **Dimensions**: Attributes to group by (categorical data)
- **Measures**: Aggregations and calculations (quantitative data)

## Creating a Semantic Table

```python
from boring_semantic_layer import to_semantic_table

# Start with an Ibis table
flights_st = to_semantic_table(flights_tbl, name="flights")
```

## with_dimensions()

Define groupable attributes using lambda, unbound syntax (`_.`), or `Dimension` class:

```python
from ibis import _
from boring_semantic_layer import Dimension

flights_st = flights_st.with_dimensions(
    # Lambda - explicit
    origin=lambda t: t.origin,

    # Unbound syntax - concise
    destination=_.dest,
    year=_.year,

    # Dimension class - with description (AI-friendly)
    carrier=Dimension(
        expr=lambda t: t.carrier,
        description="Airline carrier code"
    )
)
```

### Time Dimensions

Use `.truncate()` for time-based groupings:

```python
flights_st = flights_st.with_dimensions(
    # Year, Quarter, Month, Week, Day
    arr_year=lambda t: t.arr_time.truncate("Y"),
    arr_month=lambda t: t.arr_time.truncate("M"),
    arr_date=lambda t: t.arr_time.truncate("D"),
)
```

**Truncate units**: `"Y"` (year), `"Q"` (quarter), `"M"` (month), `"W"` (week), `"D"` (day), `"h"`, `"m"`, `"s"`

## with_measures()

Define aggregations using lambda or `Measure` class:

```python
from boring_semantic_layer import Measure

flights_st = flights_st.with_measures(
    # Simple aggregations
    flight_count=lambda t: t.count(),
    total_distance=lambda t: t.distance.sum(),
    avg_delay=lambda t: t.dep_delay.mean(),
    max_delay=lambda t: t.dep_delay.max(),

    # Composed measures (reference other measures)
    avg_distance_per_flight=lambda t: t.total_distance / t.flight_count,

    # Measure class - with description
    avg_distance=Measure(
        expr=lambda t: t.distance.mean(),
        description="Average flight distance in miles"
    )
)
```

### Percent of Total with all(ref)

Pass a declared measure or reduction to `t.all(...)` to reference its value over
the entire dataset. There is no zero-argument `t.all()` form:

```python
flights_st = flights_st.with_measures(
    flight_count=lambda t: t.count(),
    market_share=lambda t: t.flight_count / t.all(t.flight_count) * 100
)
```

## Joins

### join_many() - One-to-Many (LEFT JOIN)

```python
# One carrier has many flights
carriers_with_flights = carriers_st.join_many(
    flights_st,
    lambda c, f: c.code == f.carrier
)
```

### join_one() - At-Most-One Right Match (LEFT JOIN)

```python
# Each flight matches at most one carrier
flights_with_carrier = flights_st.join_one(
    carriers_st,
    lambda f, c: f.carrier == c.code
)
```

### join_cross() - Cartesian Product

```python
all_combinations = flights_st.join_cross(carriers_st)
```

### Join Predicates and Source-Aware Aggregation

`join_one()` and `join_many()` are left joins. Their `on=` argument can be a
column-name string, Deferred key, sequence of equality keys, or a lambda whose
predicate is a direct field equality (or conjunction of direct field
equalities). Source-aware aggregation rejects inequality, `OR`, cast, and
transformed join predicates because pre-aggregating those predicates from key
columns could change the matched rows. Aggregate each model first or restate the
relationship as equality keys. Use `join_cross()` for Cartesian products.

**After joins**: Fields are prefixed with table names (e.g., `flights.origin`, `carriers.name`)

**Unique aliases are required**: Every base model in a composed join tree must
have a distinct `name`. When the same physical table plays multiple roles, use
both `.view()` and explicit model aliases:
```python
pickup_locs = to_semantic_table(locs_tbl.view(), "pickup_locs")
dropoff_locs = to_semantic_table(locs_tbl.view(), "dropoff_locs")
```

## YAML Configuration

Define models in YAML for better organization:

```yaml
# flights_model.yaml
profile: my_db  # Optional: use a profile for connections

flights:
  table: flights_tbl
  dimensions:
    origin: _.origin
    destination: _.dest
    carrier: _.carrier
    arr_year: _.arr_time.truncate("Y")
  measures:
    flight_count: _.count()
    total_distance: _.distance.sum()
    avg_distance: _.distance.mean()

carriers:
  table: carriers_tbl
  dimensions:
    code: _.code
    name: _.name
  measures:
    carrier_count: _.count()
```

**YAML uses unbound syntax only** (`_.field`), not lambdas.

### Loading YAML Models

```python
from boring_semantic_layer import from_yaml

# With profile (recommended)
models = from_yaml("flights_model.yaml")

# With explicit tables
models = from_yaml(
    "flights_model.yaml",
    tables={"flights_tbl": flights_tbl, "carriers_tbl": carriers_tbl}
)

flights_sm = models["flights"]
```

## Best Practices

1. **Add descriptions** to dimensions/measures for AI-friendly models
2. **Use meaningful names** that reflect business concepts
3. **Define composed measures** to avoid repetition
4. **Use YAML** for production models (version control, collaboration)
5. **Use profiles** for database connections (see Profile docs)
6. **Choose join cardinality from the left side** and give every joined source a unique name

## Common Patterns

### Derived Dimensions

```python
flights_st = flights_st.with_dimensions(
    # Extract from timestamp
    arr_year=lambda t: t.arr_time.truncate("Y"),
    arr_month=lambda t: t.arr_time.truncate("M"),

    # Categorize numeric values (use ibis.cases - PLURAL, not ibis.case)
    distance_bucket=lambda t: ibis.cases(
        (t.distance < 500, "Short"),
        (t.distance < 1500, "Medium"),
        else_="Long"
    )
)
```

### Ratio Measures

```python
flights_st = flights_st.with_measures(
    total_flights=lambda t: t.count(),
    delayed_flights=lambda t: (t.dep_delay > 0).sum(),
    delay_rate=lambda t: t.delayed_flights / t.total_flights * 100
)
```

## Additional Information

**Need detailed documentation?** Use `get_documentation(topic="...")` to fetch comprehensive guides.

**Available topics:**
- `semantic-table` - Building semantic models with dimensions and measures
- `yaml-config` - Defining semantic models in YAML files
- `profile` - Database connection profiles
- `compose` - Joining multiple semantic tables
- `query-methods` - Complete API reference for queries
- `percentage-total` - Percent of total with t.all()
- `bucketing` - Categorical buckets and 'Other' consolidation
