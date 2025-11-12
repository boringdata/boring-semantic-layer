# BSL Agent Guide

Complete guide for LLM agents using the Boring Semantic Layer (BSL).

## Overview

The Boring Semantic Layer provides a simple, intuitive API for querying data using semantic models. This guide explains the BSL query syntax and patterns.

## Basic Query Syntax

All BSL queries follow this pattern:

```python
model_name.group_by(<dimensions>).aggregate(<measures>)
```

### Components

1. **model_name**: The semantic model (e.g., `flights`, `carriers`)
2. **group_by()**: Dimensions to group by (categorical data) - accepts **strings only**
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

## Working with Joins

When models have defined join relationships, use dot notation with model prefixes:

```python
flights.group_by("flights.origin", "carriers.name").aggregate("flight_count")
```

**Important**:
- `flights.origin` - dimension from flights model
- `carriers.name` - dimension from carriers model (via join)

## Time Transformations (CRITICAL)

**CRITICAL**: You CANNOT use `.year()`, `.month()`, etc. directly in `group_by()`.

The `group_by()` method **only accepts dimension names as strings** (signature: `tuple[str, ...]`).

### Correct Pattern for Time Aggregations

To group by time periods, you MUST:
1. First add time dimensions using `.with_dimensions()`
2. Then reference the new dimension by name in `.group_by()`

### Time Transformation Examples

```python
# ❌ WRONG - This will fail with SignatureValidationError
flights.group_by(flights.arr_time.year()).aggregate("flight_count")

# ✓ CORRECT - Define dimension first, then use by name
flights_with_year = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year()
)
flights_with_year.group_by("arr_year").aggregate("flight_count")

# ✓ Group by year and month
flights_with_time = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year(),
    arr_month=lambda t: t.arr_time.month()
)
flights_with_time.group_by("arr_year", "arr_month").aggregate("flight_count")

# ✓ Combine with regular dimensions
flights_with_time.group_by("carrier", "arr_year").aggregate("flight_count", "avg_distance")
```

### Available Time Transformations

- `.year()` - Extract year from timestamp
- `.month()` - Extract month (1-12) from timestamp
- `.day()` - Extract day of month from timestamp
- `.quarter()` - Extract quarter (1-4) from timestamp
- `.dayofweek()` - Extract day of week from timestamp
- `.date()` - Extract date part from timestamp

### Why This Pattern?

- Ensures type safety (group_by expects `tuple[str, ...]`)
- Makes queries more readable and maintainable
- Allows dimension reuse across multiple queries
- Enables proper schema introspection

## Common Query Patterns

### Ranking
```python
# Top origins by flight count
flights.group_by("origin").aggregate("flight_count")
```

### Comparative Analysis
```python
# Compare carriers on multiple metrics
flights.group_by("carrier").aggregate("flight_count", "avg_distance", "avg_delay")
```

### Relationship Analysis
```python
# Analyze origin-destination pairs
flights.group_by("origin", "destination").aggregate("flight_count", "avg_distance")
```

### Time Series (Raw Timestamp)
```python
# Flights over time using raw timestamp
flights.group_by("arr_time").aggregate("flight_count")
```

### Time Series (Year/Month/Day)
```python
# Group flights by year
flights_with_year = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year()
)
flights_with_year.group_by("arr_year").aggregate("flight_count")
```

### Using Joined Data
```python
# Get full airline names instead of codes
flights.group_by("carriers.name").aggregate("flight_count", "avg_distance")
```

## Sorting and Ordering Results

BSL queries return results in the order they appear in the underlying data. To sort results, you need to work with the executed DataFrame.

### Sorting Patterns

#### Sort by Measure (Most Common)
```python
# Get top carriers by flight count
result = flights.group_by("carrier").aggregate("flight_count")
df = result.execute()
df_sorted = df.sort_values("flight_count", ascending=False)

# Top 10 carriers
df_sorted.head(10)
```

#### Sort by Dimension
```python
# Alphabetically by carrier code
result = flights.group_by("carrier").aggregate("flight_count")
df = result.execute()
df_sorted = df.sort_values("carrier")
```

#### Multi-Column Sorting
```python
# Sort by year, then by flight count descending
flights_with_year = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year()
)
result = flights_with_year.group_by("carrier", "arr_year").aggregate("flight_count")
df = result.execute()
df_sorted = df.sort_values(["arr_year", "flight_count"], ascending=[True, False])
```

### Important Notes on Sorting

1. **Execute first, then sort**: Always call `.execute()` before sorting
2. **Use pandas methods**: BSL returns pandas DataFrames, so use `.sort_values()`
3. **ascending parameter**:
   - `ascending=False` for descending (largest first)
   - `ascending=True` for ascending (smallest first, default)
4. **Top N results**: Use `.head(n)` after sorting
5. **Bottom N results**: Use `.tail(n)` after sorting

### Common Sorting Use Cases

```python
# Top 10 busiest routes
result = flights.group_by("origin", "destination").aggregate("flight_count")
df = result.execute().sort_values("flight_count", ascending=False).head(10)

# Carriers ranked by average delay (best to worst)
result = flights.group_by("carrier").aggregate("avg_delay")
df = result.execute().sort_values("avg_delay").head(10)

# Time series ordered by date
flights_with_date = flights.with_dimensions(
    arr_date=lambda t: t.arr_time.date()
)
result = flights_with_date.group_by("arr_date").aggregate("flight_count")
df = result.execute().sort_values("arr_date")
```

## Query Guidelines

1. **Always quote dimension and measure names** as strings
2. **Use commas** to separate multiple dimensions or measures
3. **Prefix with model name** when using joins (e.g., `"flights.origin"`)
4. **Time transformations**: Use `.with_dimensions()` first, then reference by name
5. **group_by() only accepts strings** - no expressions or lambdas

## Integration-Specific Instructions

### For LangChain Agents

- Use the `list_bsl()` tool to discover dimensions and measures
- Use the `query_bsl(query)` tool to execute queries
- No need to call `.execute()` - the tool handles it

### For OpenAI Function Calling

- Define functions for `list_bsl` and `query_bsl`
- Parse user intent to construct BSL query strings
- Return results in structured format

### For AiChat

- BSL functions are available as aichat tools
- Use standard BSL query syntax in function parameters
- **Always set `show_chart=true`** when calling `query_bsl` to show visualizations
- Results include both data and charts when `show_chart=true`
- **Important**: Set `show_chart=true` by default for better user experience

Example function call:
```json
{
  "query": "model.group_by('dimension').aggregate('measure')",
  "show_chart": true
}
```

## Error Handling

Common errors and solutions:

1. **SignatureValidationError: keys is not tuple of strs**
   - You tried to use an expression in `group_by()`
   - Solution: Use `.with_dimensions()` first, then group by dimension name

2. **Model not found**
   - Check model name matches exactly (case-sensitive)
   - Use `list_bsl()` to see available models

3. **Dimension not found**
   - Use `list_bsl()` to see available dimensions
   - Check for typos in dimension names

4. **Join dimension error**
   - Remember to prefix with model name for joined dimensions
   - Example: `"carriers.name"` not just `"name"`

## Tips

1. **Start Simple**: Begin with single dimension and measure queries
2. **Use list_bsl**: Always check available dimensions and measures first
3. **Quote Strings**: Always use quotes around dimension and measure names
4. **Test Incrementally**: Build complex queries step by step
5. **Time Dimensions**: Always use `.with_dimensions()` for time transformations

## Example Workflow

```python
# 1. Discover available data
list_bsl()

# 2. Simple query
flights.group_by("carrier").aggregate("flight_count")

# 3. Add time dimension for temporal analysis
flights_with_time = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year(),
    arr_month=lambda t: t.arr_time.month()
)

# 4. Query with time dimensions
flights_with_time.group_by("carrier", "arr_year").aggregate("flight_count", "avg_distance")

# 5. Use joins for richer data
flights.group_by("carriers.name", "origin").aggregate("flight_count", "avg_distance")
```

## Reference: Available Models

### flights Model

**Dimensions:**
- `origin`: Origin airport code
- `destination`: Destination airport code
- `carrier`: Carrier code
- `arr_time`: Arrival timestamp (time dimension)

**Measures:**
- `flight_count`: Total number of flights
- `total_distance`: Total distance flown
- `avg_distance`: Average flight distance
- `max_distance`: Maximum flight distance
- `avg_delay`: Average departure delay

**Joins:**
- `carriers`: Join to carriers model on carrier code

### carriers Model

**Dimensions:**
- `code`: Carrier code
- `name`: Full airline name
- `nickname`: Short airline name

**Measures:**
- `carrier_count`: Number of carriers

---

## Further Reading

For detailed documentation on specific features, see:

### Core Concepts
- **[Getting Started](../../../docs/content/getting-started.md)** - Quick start guide
- **[Reference](../../../docs/content/reference.md)** - Complete API reference
- **[Semantic Table](../../../docs/content/semantic-table.md)** - Understanding semantic tables
- **[YAML Config](../../../docs/content/yaml-config.md)** - YAML configuration guide

### Query Methods
- **[Query Methods](../../../docs/content/query-methods.md)** - All available query operations
- **[Compose](../../../docs/content/compose.md)** - Query composition patterns
- **[Indexing](../../../docs/content/indexing.md)** - Accessing dimensions and measures

### Advanced Features
- **[Nested Subtotals](../../../docs/content/nested-subtotals.md)** - Hierarchical aggregations
- **[Percentage of Total](../../../docs/content/percentage-total.md)** - Percentage calculations
- **[Windowing](../../../docs/content/windowing.md)** - Window functions
- **[Bucketing](../../../docs/content/bucketing.md)** - Binning and bucketing
- **[Sessionized](../../../docs/content/sessionized.md)** - Session analytics

### Visualization & Integration
- **[Charting](../../../docs/content/charting.md)** - Chart generation
- **[MCP](../../../docs/content/mcp.md)** - Model Context Protocol integration

### Examples
- `test_time_groupby_year.py` - Time dimension examples
- `BSL_TIME_GROUPING_SUMMARY.md` - Detailed time grouping reference
