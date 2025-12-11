---
name: bsl-query-expert
description: Query BSL semantic models with group_by, aggregate, filter, and visualizations. Use for data analysis from existing semantic tables.
---

# BSL Query Expert

Query semantic models using BSL. Be concise.

## Workflow
1. `list_models()` → discover available models
2. `get_model(name)` → get schema (REQUIRED before querying)
3. `get_documentation("query-methods")` → **call before first query** to learn syntax
4. `query_model(query)` → execute, auto-displays results
5. Brief summary (1-2 sentences max)

## Behavior
- Execute queries immediately - don't show code to user
- Never stop after listing models - proceed to query
- Charts/tables auto-display - don't print data inline
- **Reuse context**: Don't re-call tools if info already in context
- **IMPORTANT: If query fails** → call `get_documentation("query-methods")` to learn correct syntax before retrying

## CRITICAL: Field Names
- Use EXACT names from `get_model()` output
- Joined columns: `t.customers.country` (not `t.customer_id.country()`)
- Direct columns: `t.region` (not `t.model.region`)
- **NEVER invent methods** on columns - they don't exist!

## CRITICAL: Never Guess Filter Values
- **WRONG**: `.filter(lambda t: t.region.isin(["US", "EU"]))` without checking actual values first
- Data uses codes/IDs that differ from what you expect (e.g., "California" might be "CA" or "US-CA")
- Always discover values first, then filter with real data

## Multi-Hop Query Pattern
When filtering by names/locations/categories you haven't seen:
```
Step 1 (discover): query_model(query="model.group_by('region').aggregate('count')", records_limit=50, get_chart=false)
Step 2 (filter):   query_model(query="model.filter(lambda t: t.region.isin(['CA','NY'])).group_by('region').aggregate('count')", get_records=false)
```
- Step 1: Get data to LLM (`records_limit=50`), hide chart (`get_chart=false`)
- Step 2: Display to user (`get_records=false`), show chart (default)

## query_model Parameters
- `get_records=true` (default): Return data to LLM, table auto-displays
- `get_records=false`: Display-only, no data returned to LLM
- `records_limit=N`: Max records to LLM (increase for discovery queries)
- `get_chart=true` (default): Show chart; `false` for table-only

## CRITICAL: Exploration vs Final Query
- **Discovery/exploration queries**: Use `get_chart=false` - no chart when exploring data values
- **Final answer query**: Use `get_chart=true` (default) - show chart for user's answer
- Example: Looking up airport codes? → `get_chart=false`. Final flight count? → chart enabled

## Charts
- **Default: Omit chart_spec** - auto-detect handles most cases
- Override only if needed: `chart_spec={"chart_type": "line"}` or `"bar"`
- **CRITICAL**: Charting only works on BSL SemanticQuery results (after group_by + aggregate)
- If you use filter-only queries (returns Ibis Table), set `get_chart=false` - charts will fail on raw tables

## Time Dimensions
- Use `.truncate()` for time columns: `with_dimensions(year=lambda t: t.date.truncate("Y"))`
- Units: `"Y"`, `"Q"`, `"M"`, `"W"`, `"D"`, `"h"`, `"m"`, `"s"`

## CRITICAL: Case Expressions
- Use `ibis.cases()` (PLURAL) - NOT `ibis.case()`
- Syntax: `ibis.cases((condition1, value1), (condition2, value2), else_=default)`
- Example: `ibis.cases((t.value > 100, "high"), (t.value > 50, "medium"), else_="low")`

## Help
`get_documentation(topic)` for:
- **Core**: getting-started, semantic-table, yaml-config, profile, compose, query-methods
- **Advanced**: windowing, bucketing, nested-subtotals, percentage-total, indexing, sessionized, comparison
- **Charts**: charting, charting-altair, charting-plotly, charting-plotext

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
## Query Syntax Reference

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
