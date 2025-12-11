Execute BSL queries and visualize results. Returns query results with optional charts.

## Core Pattern
```python
model.group_by(<dimensions>).aggregate(<measures>)  # Both take STRING names only
```
**CRITICAL**: `aggregate()` takes measure **names as strings**, NOT expressions or lambdas!

## Method Order
```
model -> with_dimensions -> filter -> with_measures -> group_by -> aggregate -> order_by -> mutate -> limit
```

## Lambda Column Access
**CRITICAL**: In `with_dimensions` and `with_measures` lambdas, access columns directly - NO model prefix!
```python
# ✅ CORRECT - access columns directly via t
flights.with_dimensions(x=lambda t: ibis.cases((t.carrier == "WN", "Southwest"), else_="Other"))
flights.with_measures(pct=lambda t: t.flight_count / t.all(t.flight_count) * 100)

# ❌ WRONG - model prefix fails in with_dimensions/with_measures
flights.with_dimensions(x=lambda t: t.flights.carrier)  # ERROR: 'Table' has no attribute 'flights'
flights.with_measures(x=lambda t: t.flights.flight_count)  # ERROR!
```
Note: Model prefix (e.g., `t.flights.carrier`) works in `.filter()` but NOT in `with_dimensions`/`with_measures`.

## Filtering
```python
# Simple filter
model.filter(lambda t: t.status == "active").group_by("category").aggregate("count")

# Multiple conditions - use ibis.and_() / ibis.or_()
model.filter(lambda t: ibis.and_(t.amount > 1000, t.year >= 2023))

# IN operator - MUST use .isin() (Python "in" does NOT work!)
model.filter(lambda t: t.region.isin(["US", "EU"]))  # ✅
model.filter(lambda t: t.region in ["US", "EU"])    # ❌ ERROR!

# Post-aggregate filter (SQL HAVING) - filter AFTER aggregate
model.group_by("carrier").aggregate("count").filter(lambda t: t.count > 1000)
```

## Joined Columns
Models with joins expose prefixed columns (e.g., `customers.country`). Use EXACT names from `get_model()`:
```python
# ✅ CORRECT - use prefixed column name
model.filter(lambda t: t.customers.country.isin(["US", "CA"])).group_by("customers.country").aggregate("count")

# ❌ WRONG - columns don't have lookup methods!
model.filter(lambda t: t.customer_id.country())  # ERROR: no 'country' attribute
```
**Key**: Look for prefixed columns in `get_model()` output - don't call methods on ID columns.

## Time Transformations
`group_by()` only accepts strings. Use `.with_dimensions()` first:
```python
model.with_dimensions(year=lambda t: t.created_at.truncate("Y")).group_by("year").aggregate("count")
```
**Truncate units**: `"Y"`, `"Q"`, `"M"`, `"W"`, `"D"`, `"h"`, `"m"`, `"s"`

## Filtering Timestamps - Match Types!
```python
# .year() returns int -> compare with int
model.filter(lambda t: t.created_at.year() >= 2023)

# .truncate() returns timestamp -> compare with ISO string
model.with_dimensions(yr=lambda t: t.created_at.truncate("Y")).filter(lambda t: t.yr >= '2023-01-01')
```

## Percentage of Total
Use `t.all(t.measure)` in `.with_measures()` for grand total:
```python
# Simple percentage by category
sales.with_measures(pct=lambda t: t.revenue / t.all(t.revenue) * 100).group_by("category").aggregate("revenue", "pct")

# Complex: filter + joined column + time dimension + percentage
orders.filter(lambda t: t.customers.country.isin(["US", "CA"])).with_dimensions(
    order_date=lambda t: t.created_at.date()
).with_measures(
    pct=lambda t: t.order_count / t.all(t.order_count) * 100
).group_by("order_date").aggregate("order_count", "pct").order_by("order_date")
```
**More**: `get_documentation(topic="percentage-total")`

## Sorting & Limiting
```python
model.group_by("category").aggregate("revenue").order_by(ibis.desc("revenue")).limit(10)
```
**CRITICAL**: `.limit()` in query limits data **before** calculations. Use `limit` parameter for display-only limiting.

## Window Functions
`.mutate()` for post-aggregation transforms - **MUST** come after `.order_by()`:
```python
model.group_by("week").aggregate("count").order_by("week").mutate(
    rolling_avg=lambda t: t.count.mean().over(ibis.window(rows=(-9, 0), order_by="week"))
)
```
**More**: `get_documentation(topic="windowing")`

## Chart
```python
chart_spec={"chart_type": "bar"}  # or "line", "scatter" - omit for auto-detect
```
