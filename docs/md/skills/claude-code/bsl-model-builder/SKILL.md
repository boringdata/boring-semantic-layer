---
name: bsl-model-builder
description: Build BSL semantic models with dimensions, measures, joins, and YAML config. Use for creating/modifying data models.
---

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

### Percent of Total with all()

Use `t.all()` to reference the entire dataset:

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
flights_with_carriers = flights_st.join_many(
    carriers_st,
    lambda f, c: f.carrier == c.code
)
```

### join_one() - One-to-One (INNER JOIN)

```python
# Each flight has exactly one carrier
flights_with_carrier = flights_st.join_one(
    carriers_st,
    lambda f, c: f.carrier == c.code
)
```

### join_cross() - Cartesian Product

```python
all_combinations = flights_st.join_cross(carriers_st)
```

### Custom Joins

```python
flights_st.join(
    carriers_st,
    lambda f, c: f.carrier == c.code,
    how="left"  # "inner", "left", "right", "outer", "cross"
)
```

**After joins**: Fields are prefixed with table names (e.g., `flights.origin`, `carriers.name`)

**Multiple joins to same table**: Use `.view()` to create distinct references:
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

**Available documentation:**

- **Getting Started**: Introduction to BSL, installation, and basic usage with semantic tables
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/getting-started.md
- **Semantic Tables**: Building semantic models with dimensions, measures, and expressions
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/semantic-table.md
- **YAML Configuration**: Defining semantic models in YAML files for better organization
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/yaml-config.md
- **Profiles**: Database connection profiles for connecting to data sources
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/profile.md
- **Composing Models**: Joining multiple semantic tables together
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/compose.md
- **Query Methods**: Complete API reference for group_by, aggregate, filter, order_by, limit, mutate
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/query-methods.md
- **Window Functions**: Running totals, moving averages, rankings, lag/lead, and cumulative calculations
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/windowing.md
- **Bucketing with Other**: Create categorical buckets and consolidate long-tail into 'Other' category
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/bucketing.md
- **Nested Subtotals**: Rollup calculations with subtotals at each grouping level
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/nested-subtotals.md
- **Percent of Total**: Calculate percentages using t.all() for market share and distribution analysis
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/percentage-total.md
- **Dimensional Indexing**: Compare values to baselines and calculate indexed metrics
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/indexing.md
- **Charting Overview**: Data visualization basics with automatic chart type detection
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/charting.md
- **Altair Charts**: Interactive web charts with Vega-Lite via Altair backend
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/prompts/chart/altair.md
- **Plotly Charts**: Interactive charts with Plotly backend for dashboards
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/prompts/chart/plotly.md
- **Terminal Charts**: ASCII charts for terminal/CLI with Plotext backend
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/prompts/chart/plotext.md
- **Sessionized Data**: Working with session-based data and user journey analysis
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/sessionized.md
- **Comparison Queries**: Period-over-period comparisons and trend analysis
  - URL: https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md/doc/comparison.md