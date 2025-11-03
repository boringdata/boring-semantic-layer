# Boring Semantic Layer - Examples

This directory contains focused examples demonstrating the core features of the Boring Semantic Layer using the new Ibis Relation-based fluent API.

## Quick Start

Run the examples in order to learn the key features:

```bash
# Example 1: Basic semantic tables and queries
python examples/01_basic_flights.py

# Example 2: Market share and percent of total
python examples/02_percent_of_total.py

# Example 3: Window functions (rolling averages, rankings)
python examples/03_window_functions.py

# Example 4: Joins and foreign sums/averages
python examples/04_joins.py

# Example 5: Bucketing with 'Other' (Top N with rollup)
python examples/05_bucketing_with_other.py
```

## Examples Overview

### 01_basic_flights.py - Getting Started

Learn the fundamentals of the Boring Semantic Layer:
- Creating semantic tables with dimensions and measures
- Using the fluent API for queries (`.group_by()` → `.aggregate()`)
- Mixing semantic measures with ad-hoc aggregations
- Post-aggregation calculations with `.mutate()`
- Both lambda and Ibis deferred expression syntax (`_.col`)

**Key Concepts**: dimensions, measures, fluent API, method chaining

### 02_percent_of_total.py - Market Share Analysis

Master the `t.all()` functionality for percentage calculations:
- Computing market share: `measure / t.all(measure)`
- Contribution analysis and relative metrics
- Comparing group-level vs grand total percentages
- Using window functions vs t.all() for different aggregation levels

**Key Concepts**: t.all(), percent of total, market share, contribution analysis

### 03_window_functions.py - Advanced Analytics

Explore powerful window functions for time-series and comparative analysis:
- Rolling/moving averages with configurable windows
- Running totals (cumulative sums)
- Rankings within groups
- Lead/lag for day-over-day changes
- Statistical measures (min, max, stddev) over windows

**Key Concepts**: ibis.window(), rolling averages, rankings, cumulative sums

### 04_joins.py - Foreign Sums and Averages

Understand how joins work correctly with aggregations (Malloy-style "foreign sums"):
- Joining semantic tables with proper relationships
- Automatic prefixing of measures with table names
- Computing aggregations at different levels of the join tree
- Three-way joins (flights → aircraft → models)
- Cross-team composability example

**Key Concepts**: join_one(), foreign sums, join tree aggregations, composability

### 05_bucketing_with_other.py - Bucketing with 'Other' (Top N Analysis)

Master the "bucketing with OTHER" pattern for clean reports and visualizations:
- Show top N items individually, group rest as 'OTHER'
- Use `ibis.rank()` with window functions for rankings
- Use `ibis.cases()` for bucketing logic
- Drop to ibis level with `.to_ibis()` for second aggregation
- Top N per group (e.g., top 3 states per facility type)
- Dynamic thresholds (e.g., states covering 80% of total)
- Pie-chart-ready aggregations with limited slices

**Key Concepts**: window functions, rankings, case expressions, multi-level aggregation, Malloy bucketing pattern

## Additional Resources

### Tests

For more advanced examples and patterns, see the test suite:
- `src/boring_semantic_layer/api/tests/test_real_world_scenarios.py`
- `src/boring_semantic_layer/api/tests/malloy_equivalence/`

## Data Sources

All examples use simple in-memory data created with pandas DataFrames and loaded
into DuckDB. The examples are self-contained and don't require external data files.

The flights data is inspired by real aviation datasets and demonstrates realistic
analytical patterns.

## Common Patterns

### Basic Query Pattern

```python
from boring_semantic_layer.api import to_semantic_table
import ibis
from ibis import _

# Create semantic table
flights = (
    to_semantic_table(raw_table, name="flights")
    .with_dimensions(
        origin=lambda t: t.origin,
        carrier=lambda t: t.carrier,
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: t.distance.mean(),
    )
)

# Query it
result = (
    flights
    .group_by("origin")
    .aggregate("flight_count", "avg_distance")
    .order_by(_.flight_count.desc())
    .execute()
)
```

### Percent of Total Pattern

```python
result = (
    flights
    .group_by("carrier")
    .aggregate("flight_count")
    .mutate(
        market_share=lambda t: t.flight_count / t.all(t.flight_count)
    )
    .execute()
)
```

### Join Pattern

```python
flights_with_aircraft = flights.join_one(
    aircraft,
    left_on="tail_num",
    right_on="tail_num"
)

# Measures are prefixed: flights__flight_count, aircraft__aircraft_count
```

### Window Function Pattern

```python
rolling_window = ibis.window(order_by="date", preceding=6, following=0)

result = (
    flights
    .group_by("date")
    .aggregate("daily_flights")
    .mutate(
        rolling_7d_avg=lambda t: t.daily_flights.mean().over(rolling_window)
    )
    .execute()
)
```

## Syntax Notes

### Lambda vs Deferred Syntax

Both syntaxes work throughout the API:

```python
# Lambda syntax
.with_measures(total=lambda t: t.amount.sum())
.mutate(pct=lambda t: t.value / t.all(t.value))

# Deferred syntax (using _)
from ibis import _

.with_measures(total=_.amount.sum())
.mutate(pct=_.value / _.all(_.value))
```

### Dot Notation vs Bracket Notation

Both work everywhere, use whichever you prefer:

```python
# Dot notation (cleaner)
.mutate(pct=lambda t: t.total_sales / t.all(t.total_sales))

# Bracket notation (explicit)
.mutate(pct=lambda t: t["total_sales"] / t.all(t["total_sales"]))
```

**Recommendation**: Use dot notation for cleaner code, unless you have column names with
special characters or spaces.

## Contributing

To add new examples:
1. Follow the naming convention: `NN_descriptive_name.py`
2. Include clear docstrings explaining the example
3. Use print statements with section headers for readability
4. Include "Key Takeaways" at the end
5. Reference the next example in sequence

## Questions?

See the main README or check the test suite for more complex examples.
