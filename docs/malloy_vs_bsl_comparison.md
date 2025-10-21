# Malloy vs BSL v2: Side-by-Side Comparison

## Feature Coverage Summary

| Feature | Malloy | BSL v2 Status | Notes |
|---------|--------|---------------|-------|
| **Semantic Models** | `source:` | ✅ `to_semantic_table()` | Fully implemented |
| **Dimensions** | `dimension:` | ✅ `.with_dimensions()` | Fully implemented |
| **Measures** | `measure:` | ✅ `.with_measures()` | Fully implemented |
| **Aggregation** | `aggregate:` | ✅ `.aggregate()` | Fully implemented |
| **Grouping** | `group_by:` | ✅ `.group_by()` | Fully implemented |
| **Filtering** | `where:` | ✅ `.filter()` | Fully implemented |
| **Joins** | `join_one:`, `join_many:` | ✅ `.join_one()`, `.join_many()` | Fully implemented |
| **Calculations** | `calculate:` | ✅ `.mutate()` | Window functions supported |
| **Ordering** | `order_by:` | ✅ `.order_by()` | Fully implemented |
| **Limiting** | `limit:` | ✅ `.limit()` | Fully implemented |
| **Percent of Total** | `all()` | ✅ `t.all()` | Helper implemented |
| **Nested Views** | `nest:` | 🚧 Prototype ready | API design complete |

## Example 1: Basic Aggregation

### Malloy
```malloy
run: airports -> {
  group_by: state
  aggregate: airport_count
}
```

### BSL v2
```python
result = (
    airports_st
    .group_by("state")
    .aggregate("airport_count")
    .execute()
)
```

---

## Example 2: With Filtering

### Malloy
```malloy
run: airports -> {
  where: state ? 'CA' | 'NY' | 'MN'
  group_by: state
  aggregate: airport_count
}
```

### BSL v2
```python
result = (
    airports_st
    .filter(lambda t: t.state.isin(['CA', 'NY', 'MN']))
    .group_by("state")
    .aggregate("airport_count")
    .execute()
)
```

---

## Example 3: Calculated Measures

### Malloy
```malloy
run: flights -> {
  group_by: carrier
  aggregate:
    flight_count
    percent_of_total is flight_count / all(flight_count)
}
```

### BSL v2
```python
result = (
    flights_st
    .group_by("carrier")
    .aggregate(
        "flight_count",
        percent_of_total=lambda t: t.flight_count / t.all(t.flight_count)
    )
    .execute()
)
```

---

## Example 4: Window Functions (calculate:)

### Malloy
```malloy
run: flights -> {
  group_by: dep_year
  aggregate: flight_count
  calculate: year_change is flight_count - lag(flight_count)
  order_by: dep_year asc
}
```

### BSL v2
```python
result = (
    flights_st
    .group_by("dep_year")
    .aggregate("flight_count")
    .mutate(
        year_change=lambda t: t.flight_count - t.flight_count.lag()
    )
    .order_by("dep_year")
    .execute()
)
```

---

## Example 5: One-Level Nested Views

### Malloy
```malloy
run: airports -> {
  group_by: state
  aggregate: airport_count
  nest: by_facility is {
    group_by: fac_type
    aggregate: airport_count
  }
}
```

### BSL v2 (Proposed)
```python
result = (
    airports_st
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            by_facility=lambda t: (
                t.group_by("fac_type")
                .aggregate("airport_count")
            )
        )
    )
    .execute()
)
```

---

## Example 6: Two-Level Nested Views

### Malloy
```malloy
run: airports -> {
  group_by: state
  aggregate: airport_count
  nest: top_5_counties is {
    limit: 5
    group_by: county
    aggregate: airport_count
    nest: by_facility is {
      group_by: fac_type
      aggregate: airport_count
    }
  }
}
```

### BSL v2 (Proposed)
```python
result = (
    airports_st
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            top_5_counties=lambda t: (
                t.group_by("county")
                .aggregate("airport_count")
                .limit(5)
                .nest(
                    by_facility=lambda t: (
                        t.group_by("fac_type")
                        .aggregate("airport_count")
                    )
                )
            )
        )
    )
    .execute()
)
```

---

## Example 7: Nested Views with Filters

### Malloy
```malloy
run: airports -> {
  where: state ? 'CA' | 'NY'
  group_by: state
  aggregate: airport_count
  nest: top_5_counties is {
    limit: 5
    group_by: county
    aggregate: airport_count
    nest: major_facilities is {
      where: major = 'Y'
      group_by: name
    }
    nest: by_facility is {
      group_by: fac_type
      aggregate: airport_count
    }
  }
}
```

### BSL v2 (Proposed)
```python
result = (
    airports_st
    .filter(lambda t: t.state.isin(['CA', 'NY']))
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            top_5_counties=lambda t: (
                t.group_by("county")
                .aggregate("airport_count")
                .limit(5)
                .nest(
                    major_facilities=lambda t: (
                        t.filter(lambda x: x.major == 'Y')
                        .group_by("name")
                    ),
                    by_facility=lambda t: (
                        t.group_by("fac_type")
                        .aggregate("airport_count")
                    )
                )
            )
        )
    )
    .execute()
)
```

---

## Example 8: Defining Semantic Models

### Malloy
```malloy
source: airports is duckdb.table('airports') extend {
  dimension: state is state
  dimension: county is county
  dimension: fac_type is fac_type

  measure: airport_count is count()
  measure: percent_of_total is airport_count / all(airport_count)

  view: by_facility is {
    group_by: fac_type
    aggregate: airport_count
  }
}
```

### BSL v2
```python
airports_st = (
    to_semantic_table(airports_tbl, "airports")
    .with_dimensions(
        state=lambda t: t.state,
        county=lambda t: t.county,
        fac_type=lambda t: t.fac_type
    )
    .with_measures(
        airport_count=lambda t: t.count(),
        percent_of_total=lambda t: t.airport_count / t.all(t.airport_count)
    )
)

# Named view as Python variable
by_facility = lambda t: (
    t.group_by("fac_type")
    .aggregate("airport_count")
)

# Use it
result = airports_st.group_by("state").aggregate(
    "airport_count",
    nest=dict(by_facility=by_facility)
).execute()
```

---

## Example 9: Joins

### Malloy
```malloy
source: flights is duckdb.table('flights') extend {
  join_one: carriers with carrier

  dimension: carrier_name is carriers.name
  measure: flight_count is count()
}

run: flights -> {
  group_by: carrier_name
  aggregate: flight_count
}
```

### BSL v2
```python
flights_st = to_semantic_table(flights_tbl, "flights").with_measures(
    flight_count=lambda t: t.count()
)

carriers_st = to_semantic_table(carriers_tbl, "carriers").with_dimensions(
    code=lambda t: t.code,
    name=lambda t: t.name
)

result = (
    flights_st
    .join_one(carriers_st, left_on="carrier", right_on="code")
    .with_dimensions(carrier_name=lambda t: t.carriers__name)
    .group_by("carrier_name")
    .aggregate("flight_count")
    .execute()
)
```

---

## Example 10: Introspection

### Malloy
```malloy
// Not directly available in Malloy
// Must inspect source code
```

### BSL v2
```python
# List available dimensions and measures
print(f"Dimensions: {airports_st.dims}")
print(f"Measures: {airports_st.measures}")

# Output:
# Dimensions: ['state', 'county', 'fac_type']
# Measures: ['airport_count', 'percent_of_total']
```

---

## Key Differences

### Malloy Advantages
1. **Concise syntax** - Less typing, more declarative
2. **Integrated rendering** - Built-in visualization
3. **SQL generation** - Can inspect generated SQL easily

### BSL v2 Advantages
1. **Python ecosystem** - Access to pandas, numpy, scikit-learn, etc.
2. **Type safety** - IDE autocomplete and type checking
3. **Flexibility** - Can mix with raw Ibis/SQL when needed
4. **Debugging** - Standard Python debugging tools
5. **Programmatic** - Easy to generate queries dynamically
6. **Introspection** - `.dims` and `.measures` properties

## Summary

BSL v2 provides **near feature-parity with Malloy** while maintaining Pythonic idioms. The main trade-off is verbosity (more typing) in exchange for type safety and Python ecosystem integration.

**Nested views** are the last major feature to implement, and we've proven it's feasible with Ibis!
