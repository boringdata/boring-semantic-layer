# BSL ↔ OSI YAML: Quick Reference

## Status: ⚠️ INCOMPATIBLE BUT FIXABLE

---

## 1. Top-Level Structure

### ❌ Current BSL
```yaml
profile: duckdb
carriers:
  table: carriers_tbl
flights:
  table: flights_tbl
```
**Problem**: No model wrapper, datasets at root level

### ✅ OSI Standard
```yaml
name: "flights_model"
description: "..."
ai_context: {...}
datasets:
  - name: carriers
    source: carriers_tbl
    primary_key: [code]
  - name: flights
    source: flights_tbl
    primary_key: [flight_id]
```
**Better**: Explicit model, dataset wrapper

---

## 2. Fields/Dimensions

### ❌ Current BSL
```yaml
dimensions:
  origin: _.origin
  city:
    expr: _.city
    description: "City name"
  timestamp:
    expr: _.created_at
    is_time_dimension: true
    smallest_time_grain: "TIME_GRAIN_DAY"
```

### ✅ OSI Standard
```yaml
fields:
  - name: origin
    expression: origin
    dimension:
      is_time: false

  - name: city
    expression: city
    description: "City name"
    dimension:
      is_time: false

  - name: timestamp
    expression: created_at
    description: "Creation timestamp"
    dimension:
      is_time: true
      time_grain: day
    ai_context:
      synonyms: [created_at, creation_date]
```

**Differences**:
- `dimensions` → `fields` (and array format)
- `expr` → `expression`
- Top-level `is_time_dimension` → nested `dimension.is_time`
- `smallest_time_grain: "TIME_GRAIN_DAY"` → `time_grain: day`
- **Missing**: `ai_context` per field

---

## 3. Metrics/Measures

### ❌ Current BSL
```yaml
measures:
  flight_count:
    expr: _.count()
    description: "Total flights"

  total_distance:
    expr: _.distance.sum()
    description: "Total distance"
```

### ✅ OSI Standard
```yaml
metrics:
  - name: flight_count
    expression: "COUNT(*)"
    description: "Total flights"
    ai_context:
      synonyms: [num_flights, count]

  - name: total_distance
    expressions:
      ansi_sql: "SUM(distance)"
      snowflake: "SUM(distance)"
      databricks: "SUM(distance)"
    description: "Total distance flown"
    ai_context:
      synonyms: [total_miles, cumulative_distance]
```

**Differences**:
- `measures` → `metrics`
- `expr: _.count()` → `expression: "COUNT(*)"`
- Single expression → Multi-dialect `expressions` object
- **Missing**: `ai_context` per metric

---

## 4. Relationships

### ❌ Current BSL
```yaml
joins:
  carriers:
    model: carriers
    type: one
    left_on: carrier
    right_on: code

  airports:
    model: airports
    type: one
    left_on: origin
    right_on: code
```
**Problem**: Implicit relationships, join-centric, no documentation

### ✅ OSI Standard
```yaml
relationships:
  - name: "flights_to_carriers"
    from_dataset: flights
    to_dataset: carriers
    type: many_to_one
    join_keys:
      - from_column: carrier
        to_column: code
    description: "Each flight belongs to one carrier"

  - name: "flights_to_origin_airport"
    from_dataset: flights
    to_dataset: airports
    type: many_to_one
    join_keys:
      - from_column: origin
        to_column: code
    description: "Flight originates from one airport"
```

**Differences**:
- `joins` → `relationships` (array format)
- `model` → `to_dataset` (and explicit `from_dataset`)
- `type: one` → `type: many_to_one` (OSI cardinality)
- `left_on`/`right_on` → `join_keys` (array of objects)
- **Missing**: Relationship names and descriptions

---

## 5. Expression Format

### Problem: Ibis vs SQL

**❌ Current BSL** (Ibis deferred syntax):
```yaml
expr: _.distance.sum()
expr: _.count()
expr: _.created_at.year()
expr: _.amount > 100
```
- **Pros**: Type-safe, Ibis handles dialect translation
- **Cons**: Not portable, requires Python runtime, not standard SQL

**✅ OSI Standard** (SQL):
```yaml
# Single SQL
expression: "SUM(distance)"
expression: "COUNT(*)"
expression: "YEAR(created_at)"
expression: "amount > 100"

# Multi-dialect
expressions:
  ansi_sql: "SUM(amount)"
  snowflake: "SUM(amount) IGNORE NULLS"
  databricks: "CAST(SUM(amount) AS DECIMAL(18,2))"
```
- **Pros**: Portable, standard, readable
- **Cons**: No type safety, dialect-specific syntax needed

---

## 6. AI Context (MISSING IN BSL)

### ❌ Current BSL
```yaml
# No AI context at all!
dimensions:
  origin: _.origin

measures:
  flight_count:
    expr: _.count()
    description: "Total flights"
```

### ✅ OSI Standard + Proposal for BSL
```yaml
fields:
  - name: origin
    expression: origin
    description: "Origin airport code"
    ai_context:
      business_definition: "The departure airport for this flight"
      synonyms: [departure_airport, dep_airport, from_airport]
      examples: ["LAX", "JFK", "ORD"]
      query_hints: "Use for geographic filtering; join with airports table for details"

metrics:
  - name: flight_count
    expression: "COUNT(*)"
    description: "Total number of flights"
    ai_context:
      business_definition: "The number of flight records in the dataset"
      synonyms: [num_flights, flight_volume, count]
      query_hints: "Aggregates naturally; watch for one-to-many joins with detailed tables"
```

**Why it matters for Claude/LLMs**:
- Understands that `origin` = `departure_airport` = `from_airport`
- Knows not to use `origin` in aggregations (it's a dimension)
- Understands cardinality and join implications
- Better natural language query generation

---

## 7. Summary Table

| Feature | BSL Current | OSI Standard | Priority | Effort |
|---------|---------|------|----------|--------|
| **Structure** | | | | |
| Model wrapper | ❌ | ✅ | CRITICAL | Low |
| Dataset array | ❌ | ✅ | CRITICAL | Medium |
| Primary keys | ❌ | ✅ | High | Low |
| **Fields** | | | | |
| Named explicitly | ❌ | ✅ | CRITICAL | Medium |
| Field array | ❌ | ✅ | CRITICAL | Medium |
| `expr` → `expression` | ⚠️ | ✅ | CRITICAL | Medium |
| Ibis → SQL conversion | ❌ | ✅ | CRITICAL | **HIGH** |
| `is_time_dimension` structure | ⚠️ | ✅ | High | Low |
| AI context | ❌ | ✅ | High | Medium |
| **Metrics** | | | | |
| Named explicitly | ❌ | ✅ | CRITICAL | Medium |
| Metric array | ❌ | ✅ | CRITICAL | Medium |
| Multi-dialect | ❌ | ✅ | High | **HIGH** |
| AI context | ❌ | ✅ | High | Medium |
| **Relationships** | | | | |
| Explicit relationships | ❌ | ✅ | High | Medium |
| Cardinality (type) | ⚠️ | ✅ | High | Low |
| Relationship names | ❌ | ✅ | High | Low |
| Relationship docs | ❌ | ✅ | High | Low |

---

## 8. Conversion Roadmap

### Phase 1: Add Wrappers & Primary Keys (Story #1-2)
```python
# Old format still works
old_yaml = load("flights.yml")

# New format also works
new_yaml = load("flights.osi.yaml")

# Auto-detect and handle both
model = from_yaml("flights.yml", format="auto")
```

### Phase 2: Rename Keys & Structure (Story #2)
```python
# Support both naming conventions
model.dimensions  # Still works
model.fields      # Also works

model.measures    # Still works
model.metrics     # Also works
```

### Phase 3: Add Multi-Dialect (Story #9)
```python
# Old single expression
expr: _.count()

# New multi-dialect
expressions:
  ansi_sql: "COUNT(*)"
  snowflake: "COUNT(*) FILTER (WHERE ...)"
  # Ibis still works as fallback
  ibis: "_.count()"
```

### Phase 4: Full OSI Conversion (Story #3)
```python
# Export to OSI format
osi_json = model.to_osi()

# Import from OSI
model = from_osi(osi_json)
```

---

## 9. Minimal OSI-Compliant YAML

Here's the **smallest** valid OSI YAML that covers BSL's current features:

```yaml
name: "my_model"
description: "Semantic model"

datasets:
  - name: flights
    source: flights_tbl
    primary_key: [id]
    description: "Flight records"

    fields:
      - name: origin
        expression: origin
      - name: date
        expression: date
        dimension:
          is_time: true

    metrics:
      - name: count
        expression: "COUNT(*)"
      - name: distance
        expression: "SUM(distance)"

relationships:
  - name: "flights_to_carriers"
    from_dataset: flights
    to_dataset: carriers
    type: many_to_one
    join_keys:
      - from_column: carrier
        to_column: code
```

---

## 10. Recommended Approach

### Option A: Full OSI Alignment (Better long-term)
✅ Pros:
- Portable across OSI tools
- Standard format
- Future-proof

❌ Cons:
- Major refactoring
- Breaking changes (with migration path)

### Option B: Dual Format Support (Practical)
✅ Pros:
- Backward compatible
- Gradual migration
- Low-risk

❌ Cons:
- More complex code
- Need format detection/conversion

### 🎯 Recommendation: **Option B** (Dual Format)
1. Keep current BSL YAML working
2. Add OSI YAML support alongside
3. Auto-convert between formats
4. Deprecate old format gradually

---

## Implementation Checklist

For Story #1 (Define OSI Compliance):
- [ ] Document mapping: BSL ↔ OSI
- [ ] Define conversion rules
- [ ] Plan dual-format support
- [ ] Create validation schemas

For Story #2-4 (Phase 1):
- [ ] Update YAML parser to handle both formats
- [ ] Implement auto-conversion
- [ ] Write round-trip tests
- [ ] Update docs with new format examples

For Story #9 (Multi-dialect):
- [ ] Add `expressions` field to Measure
- [ ] Implement dialect selection logic
- [ ] Add SQL expression parser
- [ ] Create transpiler for common patterns

For Story #3 (OSI Export):
- [ ] Generate OSI-compliant YAML
- [ ] Validate against OSI schema
- [ ] Test round-trip conversion
- [ ] Document examples

