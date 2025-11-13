---
name: bsl-model-building
description: Build semantic models with dimensions, measures, and joins. Use for creating and configuring SemanticTable definitions.
---

# BSL Model Building

Use this skill when creating or modifying semantic table definitions with dimensions, measures, calculated measures, and joins.

## When to Use This Skill

- Creating new semantic models from data sources
- Defining dimensions and measures
- Adding calculated measures
- Joining semantic tables
- Configuring YAML model definitions
- Setting up time dimensions

## Quick Reference

### Basic Model Creation (Python)

```python
from boring_semantic_layer import SemanticTable
import ibis

# Connect to data source
con = ibis.duckdb.connect("data.db")
flights_table = con.table("flights")

# Create semantic model
flights = SemanticTable(
    table=flights_table,
    name="flights",
    description="Flight data with carriers and airports",
    dimensions={
        "carrier": (lambda t: t.carrier, "Airline carrier code"),
        "origin": (lambda t: t.origin, "Origin airport"),
        "dest": (lambda t: t.dest, "Destination airport"),
    },
    measures={
        "flight_count": (lambda t: t.count(), "Total number of flights"),
        "avg_distance": (lambda t: t.distance.mean(), "Average flight distance"),
    }
)
```

### YAML Model Definition

```yaml
models:
  flights:
    table: flights
    description: "Flight data with carriers and airports"

    dimensions:
      carrier:
        expr: carrier
        description: "Airline carrier code"

      origin:
        expr: origin
        description: "Origin airport"

    measures:
      flight_count:
        expr: count()
        description: "Total number of flights"

      avg_distance:
        expr: distance.mean()
        description: "Average flight distance"
```

## Key Capabilities

### 1. Defining Dimensions

**File**: [docs/content/semantic-table.md](../../../../../docs/content/semantic-table.md)

```python
dimensions={
    # Simple column reference
    "carrier": (lambda t: t.carrier, "Airline carrier code"),

    # Transformed dimension
    "arr_date": (lambda t: t.arr_time.date(), "Arrival date"),

    # Time dimension with grain
    "arr_time": (lambda t: t.arr_time, "Arrival timestamp", "day"),
}
```

### 2. Defining Measures

**File**: [docs/content/semantic-table.md](../../../../../docs/content/semantic-table.md)

```python
measures={
    # Count measure
    "flight_count": (lambda t: t.count(), "Total flights"),

    # Aggregation measure
    "avg_distance": (lambda t: t.distance.mean(), "Average distance"),
    "total_distance": (lambda t: t.distance.sum(), "Total distance"),
    "max_delay": (lambda t: t.arr_delay.max(), "Maximum delay"),
}
```

### 3. Calculated Measures

**File**: [docs/content/calculated-measures.md](../../../../../docs/content/calculated-measures.md)

```python
# Python API
flights = flights.with_calculated_measures({
    "avg_distance_per_flight": lambda m: m.total_distance / m.flight_count,
})

# YAML
calculated_measures:
  avg_distance_per_flight:
    expr: total_distance / flight_count
    description: "Average distance per flight"
```

### 4. Joining Models

**File**: [docs/content/joining-models.md](../../../../../docs/content/joining-models.md)

```python
# Join two semantic tables
flights_with_weather = flights.join(
    weather,
    left_on="origin",
    right_on="airport_code",
    how="left"
)

# YAML
joins:
  - model: weather
    left_on: origin
    right_on: airport_code
    how: left
```

### 5. Time Dimensions

**File**: [docs/content/time-dimensions.md](../../../../../docs/content/time-dimensions.md)

```python
# Define time dimension with smallest grain
dimensions={
    "arr_time": (
        lambda t: t.arr_time,
        "Arrival timestamp",
        "day"  # smallest_time_grain
    ),
}

# Or explicitly mark as time dimension
from boring_semantic_layer import Dimension

dimensions={
    "arr_time": Dimension(
        expr=lambda t: t.arr_time,
        description="Arrival timestamp",
        is_time_dimension=True,
        smallest_time_grain="day"
    ),
}
```

## Critical Gotchas

### 1. Lambda Expression Format

Python API requires `lambda t:` format, YAML uses direct expressions:

```python
# ✅ Python: lambda required
dimensions={"carrier": (lambda t: t.carrier, "description")}

# ✅ YAML: no lambda
dimensions:
  carrier:
    expr: carrier
```

### 2. Measure Aggregations

All measures must use aggregation functions:

```python
# ✅ Correct
"flight_count": (lambda t: t.count(), "description")
"avg_distance": (lambda t: t.distance.mean(), "description")

# ❌ Wrong: missing aggregation
"distance": (lambda t: t.distance, "description")  # Not aggregated!
```

### 3. Join Key Matching

Join keys must reference dimension names, not underlying column names:

```python
# ✅ Correct: uses dimension name
flights.join(weather, left_on="origin", right_on="airport_code")

# ❌ Wrong: if "origin" is not defined as dimension
```

## Model Definition Patterns

### Pattern 1: Base Model with Time

```python
from boring_semantic_layer import SemanticTable

model = SemanticTable(
    table=con.table("data"),
    name="my_model",
    description="Description of the model",
    dimensions={
        "date": (lambda t: t.timestamp.date(), "Date dimension", "day"),
        "category": (lambda t: t.category, "Category dimension"),
    },
    measures={
        "count": (lambda t: t.count(), "Total count"),
        "avg_value": (lambda t: t.value.mean(), "Average value"),
    }
)
```

### Pattern 2: Model with Calculated Measures

```python
model = base_model.with_calculated_measures({
    "ratio": lambda m: m.numerator / m.denominator,
    "percentage": lambda m: (m.part / m.total) * 100,
})
```

### Pattern 3: Joined Model

```python
# Join two semantic models
combined = left_model.join(
    right_model,
    left_on="left_key",
    right_on="right_key",
    how="left"
)
```

## YAML Structure

**File**: [docs/content/yaml-configuration.md](../../../../../docs/content/yaml-configuration.md)

```yaml
connection:
  backend: duckdb
  database: data.db

models:
  model_name:
    table: table_name
    description: "Model description"

    dimensions:
      dim_name:
        expr: column_name
        description: "Dimension description"
        is_time_dimension: false

    measures:
      measure_name:
        expr: aggregation_function()
        description: "Measure description"

    calculated_measures:
      calc_measure_name:
        expr: measure1 / measure2
        description: "Calculated measure description"

    joins:
      - model: other_model
        left_on: left_key
        right_on: right_key
        how: left
```

## Documentation Navigation

- **Getting Started**: [docs/content/getting-started.md](../../../../../docs/content/getting-started.md)
- **Semantic Table**: [docs/content/semantic-table.md](../../../../../docs/content/semantic-table.md) ⭐
- **Time Dimensions**: [docs/content/time-dimensions.md](../../../../../docs/content/time-dimensions.md)
- **Calculated Measures**: [docs/content/calculated-measures.md](../../../../../docs/content/calculated-measures.md)
- **Joining Models**: [docs/content/joining-models.md](../../../../../docs/content/joining-models.md)
- **YAML Configuration**: [docs/content/yaml-configuration.md](../../../../../docs/content/yaml-configuration.md)

## CLI Tools

```bash
# Validate YAML model
bsl validate model.yml

# Interactive query (loads model)
bsl chat aichat --sm model.yml
```
