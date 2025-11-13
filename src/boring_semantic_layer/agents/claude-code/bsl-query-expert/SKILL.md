---
name: bsl-query-expert
description: Query BSL semantic models with group_by, aggregate, filter, and visualizations. Use for data analysis from existing semantic tables.
---

# BSL Query Expert

Use this skill when querying existing semantic models for data analysis, aggregations, filtering, and visualizations.

## When to Use This Skill

- Querying data from existing semantic models
- Creating aggregations and group-bys
- Filtering data by dimensions or time ranges
- Generating charts and visualizations
- Working with time dimensions and time grains

## Quick Reference

### Basic Query Pattern

```python
# Load semantic models
from boring_semantic_layer import from_yaml
models = from_yaml("path/to/model.yml")

# Basic query
result = models["flights"] \
    .group_by("carrier") \
    .aggregate("flight_count", "avg_distance") \
    .execute()
```

### Query with Filters (CRITICAL ORDERING!)

```python
# ✅ CORRECT ORDER: with_dimensions → filter → group_by → aggregate
result = model \
    .with_dimensions(arr_date=lambda t: t.arr_time.date()) \
    .filter(lambda t: t.arr_time.year() > 2000) \
    .group_by("arr_date") \
    .aggregate("flight_count") \
    .execute()

# ❌ WRONG: filter → with_dimensions (will fail!)
```

### Charts and Visualizations

```python
# Generate chart
chart = model \
    .group_by("carrier") \
    .aggregate("flight_count") \
    .chart(backend="plotext")  # or "altair", "plotly"
```

## Key Capabilities

### 1. Group By and Aggregate

**File**: [docs/content/query-methods.md](../../../../../docs/content/query-methods.md)

```python
# Single dimension, single measure
model.group_by("carrier").aggregate("flight_count")

# Multiple dimensions and measures
model.group_by("carrier", "origin").aggregate("flight_count", "avg_distance")
```

### 2. Filtering (⚠️ ORDER MATTERS!)

**File**: [docs/content/query-methods.md](../../../../../docs/content/query-methods.md)

**Critical constraint**: `.with_dimensions()` MUST come BEFORE `.filter()` (GitHub issue #98)

```python
# Simple filter
model.filter(lambda t: t.carrier == "AA").group_by("origin").aggregate("flight_count")

# Time filter
model.filter(lambda t: t.arr_time.year() > 2000).group_by("carrier").aggregate("flight_count")

# With dimension transformation (use correct order!)
model \
    .with_dimensions(arr_date=lambda t: t.arr_time.date()) \
    .filter(lambda t: t.arr_time.year() > 2000) \
    .group_by("arr_date") \
    .aggregate("flight_count")
```

### 3. Time Dimensions and Grains

**File**: [docs/content/time-dimensions.md](../../../../../docs/content/time-dimensions.md)

```python
# Create time dimension
model.with_dimensions(
    arr_date=lambda t: t.arr_time.date()
).group_by("arr_date").aggregate("flight_count")

# Use time grain (with .query() interface)
result = model.query(
    dimensions=["arr_time"],
    measures=["flight_count"],
    time_grain="TIME_GRAIN_MONTH"
)
```

### 4. Visualizations

**File**: [docs/content/charting.md](../../../../../docs/content/charting.md)

```python
# Terminal charts (plotext)
model.group_by("carrier").aggregate("flight_count").chart(backend="plotext")

# Web charts (altair/plotly)
model.group_by("carrier").aggregate("flight_count").chart(backend="altair")

# Custom chart spec
model.group_by("carrier").aggregate("flight_count").chart(
    backend="altair",
    spec={"mark": "bar", "encoding": {...}}
)
```

## Critical Gotchas

### 1. Filter Ordering Constraint

**Problem**: `.filter()` returns `SemanticFilter`, which lacks `.with_dimensions()` method

**Solution**: Always use `.with_dimensions()` BEFORE `.filter()`

```python
# ✅ Works
model.with_dimensions(...).filter(...).group_by(...).aggregate(...)

# ❌ Fails with AttributeError
model.filter(...).with_dimensions(...).group_by(...).aggregate(...)
```

**Reference**: GitHub issue #98

### 2. Time Dimension Types

Different backends handle datetime types differently. Use `.date()` for date-only dimensions:

```python
# For date grouping
.with_dimensions(arr_date=lambda t: t.arr_time.date())

# For year extraction
.filter(lambda t: t.arr_time.year() > 2000)
```

## Documentation Navigation

- **Getting Started**: [docs/content/getting-started.md](../../../../../docs/content/getting-started.md)
- **Query Methods**: [docs/content/query-methods.md](../../../../../docs/content/query-methods.md) ⭐
- **Time Dimensions**: [docs/content/time-dimensions.md](../../../../../docs/content/time-dimensions.md)
- **Charting**: [docs/content/charting.md](../../../../../docs/content/charting.md)
- **Agent Guide**: [../BSL_AGENT_GUIDE.md](../../BSL_AGENT_GUIDE.md) (comprehensive LLM guide)

## CLI Tools

```bash
# Interactive chat with aichat
bsl chat aichat --sm model.yml

# Interactive chat with LangChain
bsl chat langchain --sm model.yml

# MCP server
bsl serve --sm model.yml
```
