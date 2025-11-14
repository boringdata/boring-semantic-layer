---
name: bsl-query-expert
description: Query BSL semantic models with group_by, aggregate, filter, and visualizations. Use for data analysis from existing semantic tables.
---

# BSL Query Expert

Complete guide for querying semantic models using the Boring Semantic Layer (BSL).

## Basic Query Syntax

All BSL queries follow this pattern:

```python
model_name.group_by(<dimensions>).aggregate(<measures>)
```

### Components

1. **model_name**: The semantic model (e.g., `flights`, `carriers`)
2. **group_by()**: Dimensions to group by - accepts **strings only**
3. **aggregate()**: Measures to calculate (metrics like counts, averages)

## Simple Examples

### Single Dimension, Single Measure
```python
flights.group_by("origin").aggregate("flight_count")
```

### Single Dimension, Multiple Measures
```python
flights.group_by("carrier").aggregate("flight_count", "avg_distance", "total_distance")
```

### Multiple Dimensions
```python
flights.group_by("origin", "destination").aggregate("flight_count")
```

### No Grouping (Total Aggregation)
```python
flights.aggregate("flight_count", "avg_distance")
```

## Filtering Data

Use `.filter()` to narrow down your data before aggregation. **IMPORTANT**: Filters must come AFTER `.with_dimensions()` if you use both.

### Basic Filters

```python
# Filter by string equality
flights.filter(lambda t: t.carrier == "AA").group_by("origin").aggregate("flight_count")

# Filter by numeric comparison
flights.filter(lambda t: t.distance > 1000).aggregate("flight_count", "avg_distance")

# Filter by date/time
flights.filter(lambda t: t.arr_time.year() == 2004).group_by("carrier").aggregate("flight_count")
```

### Multiple Conditions

```python
# AND conditions (chain filters or use &)
flights.filter(lambda t: t.carrier == "AA").filter(lambda t: t.distance > 500).aggregate("flight_count")

# OR using the | operator
flights.filter(lambda t: (t.carrier == "AA") | (t.carrier == "DL")).aggregate("flight_count")

# Complex conditions
flights.filter(lambda t: (t.distance > 1000) & (t.arr_time.year() >= 2003)).group_by("carrier").aggregate("flight_count")
```

### Combining with_dimensions and filter

**CRITICAL**: When using both `.with_dimensions()` and `.filter()`, you MUST use `.with_dimensions()` **first**.

```python
# ✓ CORRECT - with_dimensions first, then filter
flights.with_dimensions(arr_date=lambda t: t.arr_time.date()).filter(lambda t: t.carrier == "AA").group_by("arr_date").aggregate("flight_count")

# ❌ WRONG - Can't use with_dimensions after filter
# flights.filter(lambda t: t.carrier == "AA").with_dimensions(...)  # This will error!
```

The correct order is always: **Model → with_dimensions → filter → group_by → aggregate**

## Time Transformations (CRITICAL)

**CRITICAL**: You CANNOT use `.year()`, `.month()`, etc. directly in `group_by()`.

The `group_by()` method **only accepts dimension names as strings**.

### Correct Pattern for Time Aggregations

To group by time periods:
1. First add time dimensions using `.with_dimensions()`
2. Then reference the new dimension by name in `.group_by()`

```python
# ❌ WRONG - This will fail
flights.group_by(flights.arr_time.year()).aggregate("flight_count")

# ✓ CORRECT - Define dimension first, then use by name
flights_with_year = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year()
)
flights_with_year.group_by("arr_year").aggregate("flight_count")

# ✓ COMPOSITE TIME DIMENSION (Year + Quarter as single dimension)
# Use string concatenation to create "2002-Q1" format
flights_with_quarter = flights.with_dimensions(
    year_quarter=lambda t: t.arr_time.year().cast(str) + "-Q" + (((t.arr_time.month() - 1) // 3) + 1).cast(str)
)
flights_with_quarter.group_by("year_quarter").aggregate("flight_count")

# ✓ COMPOSITE TIME DIMENSION (Year + Month as "2002-01" format)
flights_with_month = flights.with_dimensions(
    year_month=lambda t: t.arr_time.year().cast(str) + "-" + t.arr_time.month().cast(str).lpad(2, "0")
)
flights_with_month.group_by("year_month").aggregate("flight_count")
```

### Using .truncate() for Time Dimensions

**RECOMMENDED**: The `.truncate()` method is the preferred way to group timestamps by time periods. It's cleaner and more efficient than manual extraction methods.

```python
# ✓ BEST - Truncate to year
flights_by_year = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.truncate("Y")
)
flights_by_year.group_by("arr_year").aggregate("flight_count")

# ✓ Truncate to quarter
flights_by_quarter = flights.with_dimensions(
    arr_quarter=lambda t: t.arr_time.truncate("Q")
)
flights_by_quarter.group_by("arr_quarter").aggregate("flight_count")

# ✓ Truncate to month
flights_by_month = flights.with_dimensions(
    arr_month=lambda t: t.arr_time.truncate("M")
)
flights_by_month.group_by("arr_month").aggregate("flight_count")

# ✓ Truncate to day
flights_by_day = flights.with_dimensions(
    arr_day=lambda t: t.arr_time.truncate("D")
)
flights_by_day.group_by("arr_day").aggregate("flight_count")
```

**Available truncate units:**
- `"Y"` - Year
- `"Q"` - Quarter
- `"M"` - Month
- `"W"` - Week
- `"D"` - Day
- `"h"` - Hour
- `"m"` - Minute
- `"s"` - Second

**When to use `.truncate()` vs manual extraction:**
- **Use `.truncate()`** when you want grouped timestamps (e.g., "2002-01-01 00:00:00" for January 2002)
- **Use manual extraction** (`.year()`, `.month()`, etc.) when you want integer values or custom formatting

### Available Time Transformations

- `.year()` - Extract year from timestamp
- `.month()` - Extract month (1-12) from timestamp
- `.day()` - Extract day of month from timestamp
- `.quarter()` - Extract quarter (1-4) from timestamp
- `.date()` - Extract date part from timestamp
- `.cast(str)` - Convert to string for concatenation
- `.lpad(n, "0")` - Left-pad with zeros (e.g., "01" instead of "1")

## For Agent Integration

If you are an LLM agent (aichat, langchain, etc.), follow these instructions:

### Available Tools

- `list_bsl()` - Discover available models, dimensions, and measures
- `query_bsl(query, show_chart, chart_spec)` - Execute queries and optionally display charts

### Query Construction Rules

1. **Always quote dimension and measure names** as strings: `"carrier"` not `carrier`
2. **Use commas** to separate multiple dimensions or measures
3. **Time transformations**: Use `.with_dimensions()` first, then reference by name
4. **group_by() only accepts strings** - no expressions or lambdas
5. **Method order**: Model → with_dimensions → filter → group_by → aggregate
6. **Call `list_bsl()` only when needed** to discover available fields, then proceed with the actual query
7. **Set `show_chart=true`** by default for better UX (visualizations help users)
8. **No `.execute()`** - the tool handles execution automatically

### Important Agent Behavior

**CRITICAL**: When a user asks for data analysis:
1. If you need to know what dimensions/measures exist, call `list_bsl()` first
2. **Then immediately proceed** to call `query_bsl()` with the actual query
3. **Do NOT stop** after listing - the user wants the actual data/visualization

For example, if the user asks "flight per year":
1. Call `list_bsl()` to see available fields (if needed)
2. **Then immediately** call `query_bsl()` with a query like:
   ```
   flights.with_dimensions(arr_year=lambda t: t.arr_time.truncate("Y")).group_by("arr_year").aggregate("flight_count")
   ```
   with `show_chart=true`

### Example Tool Usage

```json
{
  "query": "flights.group_by('carrier').aggregate('flight_count')",
  "show_chart": true
}
```

### Custom Chart Specifications

You can override the auto-detected chart type by passing a `chart_spec`:

```json
{
  "query": "flights.group_by('carrier').aggregate('flight_count')",
  "show_chart": true,
  "chart_spec": {"chart_type": "bar"}
}
```

**Available chart types for plotext backend:**
- `"bar"` - Bar chart (good for categorical comparisons)
- `"line"` - Line chart (good for trends and time series)
- `"scatter"` - Scatter plot (good for two-dimensional relationships)

### Quick Tips

- **Start Simple**: Begin with single dimension and measure queries
- **Test incrementally**: Build complex queries step by step
- **Remember ordering**: with_dimensions → filter → group_by → aggregate
- **Use list_bsl**: Always check available data first

## Further Reading

For advanced features and detailed documentation:

### Advanced Features

- **Advanced patterns** - Nested subtotals, percentage of total, windowing (ranking/cumulative), bucketing, and sessionization. See docs for details.

### Query Methods

- **[Query Methods](../../../../../docs/content/query-methods.md)** - Comprehensive query API reference covering:
  - `group_by()` - Group data by dimensions
  - `aggregate()` - Calculate measures and on-the-fly transformations
  - `filter()` - Apply conditions to filter data
  - `order_by()` - Sort results
  - `limit()` - Restrict number of rows
  - `nest()` - Create nested data structures (arrays of structs)
  - `mutate()` - Transform aggregated results with computed columns
  - `over()` - Window functions (lag/lead, cumsum, rank)
  - `as_table()` - Convert results back to SemanticModel
- **[Compose](../../../../../docs/content/compose.md)** - Reusable query composition patterns
- **[Indexing](../../../../../docs/content/indexing.md)** - Direct dimension/measure access

### Visualization

- **[Charting](../../../../../docs/content/charting.md)** - Chart backends, auto-detection, custom specs
