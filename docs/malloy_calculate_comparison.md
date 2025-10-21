# Malloy `calculate:` vs BSL v2 `.mutate()` Comparison

## Summary

**Yes, BSL v2 with `.mutate()` can replicate all Malloy `calculate:` patterns using ibis window functions!**

Our existing `.mutate()` method provides equivalent functionality to Malloy's `calculate:` for post-aggregation calculations and window functions.

## Feature Comparison

| Feature | Malloy `calculate:` | BSL v2 `.mutate()` | Status |
|---------|---------------------|-------------------|--------|
| Window functions (lag, lead) | `lag(field)`, `lead(field)` | `t.field.lag()`, `t.field.lead()` | ✅ Works |
| Ranking functions | `rank()`, `row_number()` | `ibis.rank()`, `ibis.row_number()` | ✅ Works |
| Custom ordering | `{ order_by: field }` | `ibis.window(order_by=...)` | ✅ Works |
| Partitioning | `{ partition_by: field }` | `ibis.window(group_by=...)` | ✅ Works |
| Moving averages | Window frames | `ibis.window(preceding=..., following=...)` | ✅ Works |
| Cumulative sums | `running_total` | `sum().over(ibis.window(preceding=None))` | ✅ Works |
| Percent of total | `field / all(field)` | `t.field / t.all(t.field)` | ✅ Works (with helper) |

## Pattern Mappings

### 1. Basic Window Functions

**Malloy:**
```malloy
calculate: year_change is flight_count - lag(flight_count)
```

**BSL v2:**
```python
.mutate(
    year_change=lambda t: t.flight_count - t.flight_count.lag()
)
```

### 2. Custom Ordering

**Malloy:**
```malloy
calculate: year_change is flight_count - lag(flight_count) {
  order_by: dep_year asc
}
```

**BSL v2:**
```python
.mutate(
    year_change=lambda t: t.flight_count - t.flight_count.lag().over(
        ibis.window(order_by=t.dep_year.asc())
    )
)
```

### 3. Partitioned Windows

**Malloy:**
```malloy
calculate: year_change is flight_count - lag(flight_count) {
  partition_by: dep_month
  order_by: dep_year
}
```

**BSL v2:**
```python
.mutate(
    yoy_change=lambda t: t.flight_count - t.flight_count.lag().over(
        ibis.window(group_by=t.dep_month, order_by=t.dep_year)
    )
)
```

**Note:** ibis uses `group_by` instead of `partition_by` for window partitioning.

### 4. Moving Averages

**Malloy:**
```malloy
calculate: ma_3 is avg(flight_count) {
  window: {
    rows: between -1 and 1
  }
}
```

**BSL v2:**
```python
.mutate(
    ma_3=lambda t: t.flight_count.mean().over(
        ibis.window(
            order_by=t.dep_year,
            preceding=1,
            following=1
        )
    )
)
```

### 5. Cumulative Calculations

**Malloy:**
```malloy
calculate: running_total is sum(flight_count) {
  window: {
    rows: unbounded preceding to current row
  }
}
```

**BSL v2:**
```python
.mutate(
    cumulative_flights=lambda t: t.flight_count.sum().over(
        ibis.window(order_by=t.dep_year, preceding=None, following=0)
    )
)
```

### 6. Percent of Total

**Malloy:**
```malloy
calculate: pct_of_total is flight_count / all(flight_count)
```

**BSL v2:**
```python
.mutate(
    pct_of_total=lambda t: t.flight_count / t.all(t.flight_count)
)
```

**Note:** We have a `t.all()` helper that creates the appropriate window function.

### 7. Ranking with Partitions

**Malloy:**
```malloy
calculate: carrier_rank is rank() {
  partition_by: carrier
  order_by: flight_count desc
}
```

**BSL v2:**
```python
.mutate(
    carrier_rank=lambda t: ibis.rank().over(
        ibis.window(group_by=t.carrier, order_by=t.flight_count.desc())
    )
)
```

## Key Differences

### API Syntax
- **Malloy** uses a declarative syntax with `calculate:` block
- **BSL v2** uses lambda functions in `.mutate()` method

### Window Function Specification
- **Malloy** uses `partition_by` keyword
- **ibis** (and therefore BSL v2) uses `group_by` for window partitions

### Ordering Behavior
- **Malloy** automatically uses query ordering for window functions
- **BSL v2** requires explicit `ibis.window(order_by=...)` for custom ordering

### Function Namespacing
- **Malloy** has functions like `lag()`, `rank()` in global scope
- **BSL v2** uses `ibis.rank()`, `ibis.row_number()` for analytic functions, and `.lag()`, `.lead()` as column methods

## Advantages of BSL v2 Approach

1. **Full Python Ecosystem** - Access to all Python libraries
2. **Explicit Window Specifications** - More control over window behavior
3. **Type Safety** - Python type hints and IDE support
4. **Flexible Composition** - Can combine with any ibis expressions
5. **No New Syntax** - Uses standard Python lambda functions

## Example: Complete Workflow

```python
from boring_semantic_layer.semantic_api import to_semantic_table
import ibis

# Define semantic table
flights_st = (
    to_semantic_table(flights_tbl, "flights")
    .with_dimensions(
        dep_year=lambda t: t.dep_time.year(),
        dep_month=lambda t: t.dep_time.month()
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: t.distance.mean()
    )
)

# Aggregate and apply window functions
result = (
    flights_st
    .group_by("dep_year", "dep_month")
    .aggregate("flight_count", "avg_distance")
    .mutate(
        # Year-over-year change (partitioned by month)
        yoy_change=lambda t: t.flight_count - t.flight_count.lag().over(
            ibis.window(group_by=t.dep_month, order_by=t.dep_year)
        ),
        # Percent of total
        pct_of_total=lambda t: t.flight_count / t.all(t.flight_count),
        # Moving average
        ma_3=lambda t: t.flight_count.mean().over(
            ibis.window(order_by=t.dep_year, preceding=1, following=1)
        ),
        # Cumulative sum
        cumulative=lambda t: t.flight_count.sum().over(
            ibis.window(order_by=t.dep_year, preceding=None, following=0)
        )
    )
    .order_by("dep_year", "dep_month")
)

df = result.execute()
```

## Conclusion

**BSL v2's `.mutate()` method successfully replicates all Malloy `calculate:` functionality.**

The approach is:
- ✅ **Complete** - All window function patterns are supported
- ✅ **Flexible** - Full control over window specifications
- ✅ **Pythonic** - Uses familiar lambda syntax
- ✅ **Powerful** - Access to full ibis window function API

We can confidently use `.mutate()` as BSL v2's equivalent to Malloy's `calculate:` block!
