Execute BSL queries and visualize results. Returns query results with optional charts.

## Core Query Pattern

```python
model_name.group_by(<dimensions>).aggregate(<measures>)
```

- **model_name**: Semantic model name from `list_models()`
- **group_by()**: Dimension names as **strings only**
- **aggregate()**: Measure names as **strings**

**CRITICAL**: `aggregate()` takes **measure names as strings**, NOT expressions!

```python
# CORRECT
model.group_by("category").aggregate("count", "total_amount")

# WRONG - do NOT use kwargs
model.aggregate(total=lambda t: t.sum())  # ERROR!
```

## Method Order

**CRITICAL**: Always follow this order:

```
model -> with_dimensions -> filter -> with_measures -> group_by -> aggregate -> order_by -> mutate -> limit
```

## Filtering

Use `.filter()` with lambda expressions:

```python
# Simple filter
model.filter(lambda t: t.status == "active").group_by("category").aggregate("count")

# Multiple conditions - use ibis.and_() and ibis.or_()
model.filter(lambda t: ibis.and_(t.amount > 1000, t.year >= 2023)).group_by("category").aggregate("count")

# IN operator - MUST use .isin() method
model.filter(lambda t: t.region.isin(["US", "EU", "APAC"])).group_by("region").aggregate("count")

# WRONG - Python's "in" operator does NOT work with Ibis columns!
model.filter(lambda t: t.region in ["US", "EU"])  # ERROR: truth value of Ibis expression is not defined
```

**CRITICAL**: In lambda expressions, `t` refers directly to columns - use `t.column_name`:

```python
# CORRECT
model.filter(lambda t: t.status == "active")

# WRONG
model.filter(lambda t: t.model.status == "active")  # ERROR!
```

## Filtering with Joined Columns

Models with joins expose columns from related tables using **prefixed names** (e.g., `related_model.column`). These prefixes indicate which joined model the column comes from.

**CRITICAL**: Use the EXACT column name from `get_model()` output:
- Prefixed columns like `customers.country` -> use `t.customers.country` in lambdas
- Non-prefixed columns like `region` -> use `t.region` in lambdas
- **NEVER call methods on columns** like `.state()` or `.country()` - these don't exist!

```python
# CORRECT - use prefixed column from joined model
model.filter(lambda t: t.customers.country.isin(["US", "CA"])).group_by("customers.country").aggregate("count")

# WRONG - columns don't have lookup methods!
model.filter(lambda t: t.customer_id.country())  # ERROR: 'StringColumn' has no attribute 'country'
```

**Key insight**: When you need related data, look for **prefixed columns** in `get_model()` output - they come from joined models. Don't try to call methods on ID/code columns.

## Time Transformations

**CRITICAL**: `group_by()` only accepts dimension names as strings. Use `.with_dimensions()` first:

```python
model.with_dimensions(
    year=lambda t: t.created_at.truncate("Y")
).group_by("year").aggregate("count")
```

**Truncate units**: `"Y"` (year), `"Q"` (quarter), `"M"` (month), `"W"` (week), `"D"` (day), `"h"`, `"m"`, `"s"`

## Filtering Timestamps

Match types correctly when filtering timestamp fields:

```python
# .year() returns integer -> compare with integer
model.filter(lambda t: t.created_at.year() >= 2023)

# .truncate() returns timestamp -> compare with ISO date string
model.with_dimensions(
    created_year=lambda t: t.created_at.truncate("Y")
).filter(lambda t: t.created_year >= '2023-01-01')
```

## Percentage of Total with t.all()

**CRITICAL**: Use `.with_measures()` + `t.all(t.measure)` to define percentage measures **before** grouping:

```python
# Percentage per category
sales.with_measures(
    pct=lambda t: t.revenue / t.all(t.revenue) * 100
).group_by("category").aggregate("revenue", "pct")

# Percentage per day with filter (using joined column)
orders.filter(lambda t: t.customers.country.isin(["US", "CA"])).with_dimensions(
    order_date=lambda t: t.created_at.date()
).with_measures(
    pct=lambda t: t.order_count / t.all(t.order_count) * 100
).group_by("order_date").aggregate("order_count", "pct").order_by("order_date")
```

**Key pattern**: `t.all(t.measure)` returns the grand total. Define in `.with_measures()`, then aggregate by name.

For detailed examples: `get_documentation(topic="percentage-total")`

## Sorting and Limiting

Use `.order_by()` and `.limit()` to sort and restrict results.

**CRITICAL DISTINCTION:**
- `.limit()` in query -> Limits data **before** calculations (breaks window functions!)
- `limit` parameter -> Only limits **table display**, full dataset processed

**For descending order, use `ibis.desc("column")`:**
```python
model.group_by("category").aggregate("revenue").order_by(ibis.desc("revenue")).limit(10)
```

## Window Functions (.mutate)

Post-aggregation transformations using `.mutate()` - **MUST** come after `.order_by()`.

**Quick example - rolling average:**
```python
model.group_by("week").aggregate("count").order_by("week").mutate(
    rolling_avg=lambda t: t.count.mean().over(ibis.window(rows=(-9, 0), order_by="week"))
)
```

**For complete documentation:** `get_documentation(topic="windowing")`

## Chart Specifications

Override auto-detection with `chart_spec`:
```json
{"chart_type": "bar"}  // or "line", "scatter"
```

**Backends:** `"plotext"` (terminal), `"altair"` (web/Vega-Lite), `"plotly"` (web/interactive)
