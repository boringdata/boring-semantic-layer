# YAML Transformation Examples: BSL → OSI

**Visual side-by-side transformations showing exact conversions needed**

---

## Example 1: Minimal Model

### BSL Format
```yaml
profile: duckdb

users:
  table: users_table
```

### Transformation Steps
```
Step 1: Add model wrapper
Step 2: Rename 'table' → 'source'
Step 3: Wrap in 'datasets' array
Step 4: Add primary_key
```

### OSI Format
```yaml
name: "users_model"

datasets:
  - name: users
    source: users_table
    primary_key: [user_id]
```

---

## Example 2: With Dimensions

### BSL Format
```yaml
users:
  table: users_table

  dimensions:
    user_id: _.user_id
    name: _.name
    created_date:
      expr: _.created_at
      description: "Account creation date"
```

### Step-by-Step Transformation

**Step 1**: Wrap dataset
```yaml
datasets:
  - name: users
    source: users_table
```

**Step 2**: Convert dimensions → fields (array)
```yaml
    fields:
      - name: user_id
        expression: user_id
      - name: name
        expression: name
      - name: created_date
        expression: created_at
        description: "Account creation date"
```

**Step 3**: Add model wrapper
```yaml
name: "users_model"

datasets:
  - name: users
    source: users_table
    primary_key: [user_id]

    fields:
      - name: user_id
        expression: user_id
      - name: name
        expression: name
      - name: created_date
        expression: created_at
        description: "Account creation date"
```

---

## Example 3: With Measures

### BSL Format
```yaml
orders:
  table: orders

  dimensions:
    order_id: _.order_id
    customer_id: _.customer_id

  measures:
    order_count: _.count()
    total_amount:
      expr: _.amount.sum()
      description: "Sum of all amounts"
```

### OSI Format
```yaml
name: "orders_model"

datasets:
  - name: orders
    source: orders
    primary_key: [order_id]

    fields:
      - name: order_id
        expression: order_id
        dimension:
          is_time: false

      - name: customer_id
        expression: customer_id
        dimension:
          is_time: false

    metrics:
      - name: order_count
        expression: "COUNT(*)"

      - name: total_amount
        expression: "SUM(amount)"
        description: "Sum of all amounts"
```

### Expression Conversion Details
```
BSL: _.count()              →  OSI: "COUNT(*)"
BSL: _.amount.sum()         →  OSI: "SUM(amount)"
BSL: _.customer_id.nunique()  →  OSI: "COUNT(DISTINCT customer_id)"
```

---

## Example 4: With Time Dimensions

### BSL Format
```yaml
events:
  table: events

  dimensions:
    event_date:
      expr: _.created_at
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_MONTH"
```

### OSI Format
```yaml
datasets:
  - name: events
    source: events

    fields:
      - name: event_date
        expression: created_at
        dimension:
          is_time: true
          time_grain: month
```

### Time Grain Mapping
```
BSL: TIME_GRAIN_DAY      →  OSI: day
BSL: TIME_GRAIN_WEEK     →  OSI: week
BSL: TIME_GRAIN_MONTH    →  OSI: month
BSL: TIME_GRAIN_QUARTER  →  OSI: quarter
BSL: TIME_GRAIN_YEAR     →  OSI: year
```

---

## Example 5: With Relationships (Joins)

### BSL Format
```yaml
customers:
  table: customers

  dimensions:
    customer_id: _.customer_id
    name: _.name

orders:
  table: orders

  dimensions:
    order_id: _.order_id
    customer_id: _.customer_id

  measures:
    order_count: _.count()

  joins:
    customer:
      model: customers
      type: one
      left_on: customer_id
      right_on: customer_id
```

### OSI Format
```yaml
name: "orders_with_customers"

datasets:
  - name: customers
    source: customers
    primary_key: [customer_id]

    fields:
      - name: customer_id
        expression: customer_id
      - name: name
        expression: name

  - name: orders
    source: orders
    primary_key: [order_id]

    fields:
      - name: order_id
        expression: order_id
      - name: customer_id
        expression: customer_id

    metrics:
      - name: order_count
        expression: "COUNT(*)"

relationships:
  - name: "orders_to_customers"
    from_dataset: orders
    to_dataset: customers
    type: many_to_one
    join_keys:
      - from_column: customer_id
        to_column: customer_id
    description: "Each order belongs to one customer"
```

### Join Type Mapping
```
BSL: type: one           →  OSI: type: many_to_one
BSL: type: many          →  OSI: type: one_to_many
BSL: type: cross         →  OSI: type: many_to_many
```

---

## Example 6: With AI Context

### BSL Format (Current - No AI Context)
```yaml
flights:
  table: flights

  dimensions:
    origin:
      expr: _.origin
      description: "Origin airport code"

  measures:
    flight_count: _.count()
```

### OSI Format (Enhanced with AI Context)
```yaml
datasets:
  - name: flights
    source: flights

    fields:
      - name: origin
        expression: origin
        description: "Origin airport code"
        ai_context:
          synonyms: [departure_airport, from_airport, dep_airport]
          business_definition: "The airport where the flight departs from"
          examples: [LAX, JFK, ORD]
          query_hints: "Use for geographic filtering; join with airports for details"

    metrics:
      - name: flight_count
        expression: "COUNT(*)"
        ai_context:
          synonyms: [num_flights, flight_volume]
          business_definition: "The total number of flight records"
          query_hints: "Aggregates naturally by day/week/month"
```

---

## Example 7: Real-World Complex Model

### BSL Format (flights.yml from examples)

```yaml
profile:
  type: duckdb
  database: ":memory:"

carriers:
  table: carriers_tbl
  description: "Airline carrier information"

  dimensions:
    code: _.code
    name:
      expr: _.name
      description: "Full airline name"

  measures:
    carrier_count:
      expr: _.count()
      description: "Number of carriers"

aircraft:
  table: aircraft_tbl

  dimensions:
    tail_num: _.tail_num
    aircraft_model_code: _.aircraft_model_code

  measures:
    aircraft_count: _.count()

flights:
  table: flights_tbl
  description: "Flight data with origin, destination, and metrics"

  dimensions:
    origin:
      expr: _.origin
      description: "Origin airport code"
    destination:
      expr: _.destination
      description: "Destination airport code"
    carrier: _.carrier
    arr_time:
      expr: _.arr_time
      description: "Arrival timestamp"
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_DAY"
    distance:
      expr: _.distance
      description: "Flight distance in miles"

  measures:
    flight_count:
      expr: _.count()
      description: "Total number of flights"
    total_distance:
      expr: _.distance.sum()
      description: "Total distance flown"
    avg_distance:
      expr: _.distance.mean()
      description: "Average flight distance"
    avg_delay:
      expr: _.dep_delay.mean()
      description: "Average departure delay"

  joins:
    carriers:
      model: carriers
      type: one
      left_on: carrier
      right_on: code
    aircraft:
      model: aircraft
      type: one
      left_on: tail_num
      right_on: tail_num
```

### OSI Format (Transformed)

```yaml
name: "flight_analytics"
description: "Comprehensive flight analytics model"

datasets:
  - name: carriers
    source: carriers_tbl
    primary_key: [code]
    description: "Airline carrier information"

    fields:
      - name: code
        expression: code
        dimension:
          is_time: false

      - name: name
        expression: name
        description: "Full airline name"
        dimension:
          is_time: false

    metrics:
      - name: carrier_count
        expression: "COUNT(*)"
        description: "Number of carriers"

  - name: aircraft
    source: aircraft_tbl
    primary_key: [tail_num]

    fields:
      - name: tail_num
        expression: tail_num
        dimension:
          is_time: false

      - name: aircraft_model_code
        expression: aircraft_model_code
        dimension:
          is_time: false

    metrics:
      - name: aircraft_count
        expression: "COUNT(*)"

  - name: flights
    source: flights_tbl
    primary_key: [flight_id]
    description: "Flight data with origin, destination, and metrics"

    fields:
      - name: origin
        expression: origin
        description: "Origin airport code"
        dimension:
          is_time: false
        ai_context:
          synonyms: [departure_airport]

      - name: destination
        expression: destination
        description: "Destination airport code"
        dimension:
          is_time: false

      - name: carrier
        expression: carrier
        dimension:
          is_time: false

      - name: arr_time
        expression: arr_time
        description: "Arrival timestamp"
        dimension:
          is_time: true
          time_grain: day

      - name: distance
        expression: distance
        description: "Flight distance in miles"
        dimension:
          is_time: false

    metrics:
      - name: flight_count
        expression: "COUNT(*)"
        description: "Total number of flights"
        ai_context:
          synonyms: [num_flights]

      - name: total_distance
        expression: "SUM(distance)"
        description: "Total distance flown"
        ai_context:
          synonyms: [distance_sum]

      - name: avg_distance
        expression: "AVG(distance)"
        description: "Average flight distance"

      - name: avg_delay
        expression: "AVG(dep_delay)"
        description: "Average departure delay"

relationships:
  - name: "flights_to_carriers"
    from_dataset: flights
    to_dataset: carriers
    type: many_to_one
    join_keys:
      - from_column: carrier
        to_column: code
    description: "Each flight is operated by one carrier"

  - name: "flights_to_aircraft"
    from_dataset: flights
    to_dataset: aircraft
    type: many_to_one
    join_keys:
      - from_column: tail_num
        to_column: tail_num
    description: "Each flight uses one aircraft"
```

---

## Example 8: With Multi-Dialect Expressions (OSI Advanced)

### OSI Format with Multi-Dialect Expressions

```yaml
datasets:
  - name: orders
    source: orders

    metrics:
      - name: total_revenue
        # Default expression (ANSI SQL)
        expression: "SUM(amount)"

        # Dialect-specific expressions (optional, for Phase 3)
        expressions:
          ansi_sql: "SUM(amount)"
          snowflake: "SUM(amount) IGNORE NULLS"
          databricks: "SUM(CAST(amount AS DECIMAL(18,2)))"
          bigquery: "SUM(CAST(amount AS NUMERIC))"

        description: "Total order revenue"
```

---

## Expression Conversion Table (Complete)

| Context | BSL | OSI | Notes |
|---------|-----|-----|-------|
| **Direct Column** | `_.user_id` | `user_id` | Simple reference |
| **Aggregates** | `_.count()` | `COUNT(*)` | Row count |
| | `_.column.count()` | `COUNT(column)` | Count values |
| | `_.column.nunique()` | `COUNT(DISTINCT column)` | Distinct count |
| | `_.column.sum()` | `SUM(column)` | Sum |
| | `_.column.mean()` | `AVG(column)` | Average |
| | `_.column.min()` | `MIN(column)` | Minimum |
| | `_.column.max()` | `MAX(column)` | Maximum |
| **Date** | `_.date.year()` | `YEAR(date)` | Extract year |
| | `_.date.month()` | `MONTH(date)` | Extract month |
| | `_.date.day()` | `DAY(date)` | Extract day |
| **Nested** | `_.struct.field` | `struct.field` | Nested access |
| | `_.array.element()` | `array[OFFSET(0)]` | Array access (DB specific) |
| **Comparison** | `_.amount > 100` | `amount > 100` | Greater than |
| | `_.status = 'active'` | `status = 'active'` | Equality |
| **Membership** | `_.id.isin([1,2,3])` | `id IN (1, 2, 3)` | In list |
| | `_.name.like('%john%')` | `name LIKE '%john%'` | Pattern match |
| **Case** | `_.case([(cond, val)])` | `CASE WHEN cond THEN val END` | Conditional |

---

## Migration Checklist

### For Each Existing YAML File

- [ ] Add `name` field at top level
- [ ] Convert `profile` → connection config (separate)
- [ ] Convert all top-level datasets to `datasets` array
- [ ] Rename `table` → `source`
- [ ] Add `primary_key`
- [ ] Convert `dimensions` → `fields` array
- [ ] Convert all `expr:` to `expression:`
- [ ] Convert `_.field` references to `field`
- [ ] Convert Ibis method calls to SQL functions
- [ ] Convert `is_time_dimension` → `dimension.is_time`
- [ ] Convert `smallest_time_grain` → `dimension.time_grain`
- [ ] Convert `measures` → `metrics` array
- [ ] Convert `joins` → `relationships` array
- [ ] Add relationship names and descriptions
- [ ] Update join types (one → many_to_one, etc.)
- [ ] Add primary keys to all datasets
- [ ] Add `ai_context` where applicable
- [ ] Validate against OSI schema
- [ ] Test round-trip conversion

---

## Quick Reference Cheat Sheet

### Container Renames
```
dimensions  →  fields       (becomes array)
measures    →  metrics      (becomes array)
joins       →  relationships (becomes array)
```

### Key Renames
```
table       →  source
expr        →  expression
is_time_dimension → dimension.is_time
smallest_time_grain → dimension.time_grain
model (in join) → to_dataset
(implicit)  →  from_dataset (must add)
type: one   →  type: many_to_one
type: many  →  type: one_to_many
```

### Expression Changes
```
_.column        →  column
_.method()      →  SQL_FUNCTION(column)
```

### New Required Fields
```
name (model level)
datasets (array wrapper)
primary_key (dataset level)
ai_context (at various levels)
```

---

## Implementation Order

1. **Start with simple models** (no joins, few fields)
2. **Graduate to models with joins**
3. **Add time dimensions**
4. **Add descriptions and AI context**
5. **Test round-trip conversion**

