# BSL YAML vs OSI YAML: Alignment Analysis

## Quick Answer: ⚠️ NOT ALIGNED (Major restructuring needed)

BSL's YAML format is **fundamentally different** from OSI's. While both define semantic models, the structures are incompatible and require **significant mapping work**.

---

## Side-by-Side Comparison

### OSI YAML Structure
```yaml
name: "semantic_model"                    # Global model name (REQUIRED)
description: "Model description"
ai_context:                               # Global AI context (MISSING in BSL)
  synonyms: [...]
  business_definition: "..."

datasets:                                 # Array of datasets (MISSING in BSL)
  - name: "flights"                       # Dataset name
    source: "public.flights"              # Physical table (REQUIRED)
    primary_key: ["flight_id"]            # Key definitions (MISSING in BSL)
    description: "Flight records"

    fields:                               # CALLED "dimensions" in BSL
      - name: "origin"
        expression: "SELECT origin FROM flights"
        dimension:                        # Metadata for grouping
          is_time: false
        description: "Origin airport"
        ai_context:                       # Per-field context (MISSING in BSL)
          synonyms: ["dep_airport"]

    metrics:                              # CALLED "measures" in BSL
      - name: "flight_count"
        expression: "COUNT(*)"            # SQL aggregate expression
        description: "Total flights"
        ai_context:
          synonyms: ["num_flights"]

relationships:                            # EXPLICIT relationships (IMPLICIT in BSL)
  - name: "flights_to_carriers"
    from: "flights"
    to: "carriers"
    type: "many_to_one"
    join_keys:
      - {from: "carrier_code", to: "code"}

custom_extensions: {}                     # Vendor-specific extensions
```

### BSL YAML Structure (Current)
```yaml
profile: "duckdb"                         # Connection config (OSI doesn't have this)

flights:                                  # Dataset name (implicit)
  table: "flights_tbl"                    # Physical table (DIFFERENT KEY NAME)
  description: "Flight records"
  # NO: primary_key, ai_context at dataset level

  dimensions:                             # CALLED "fields" in OSI
    origin:                               # Can be simple or complex
      expr: _.origin                      # Ibis deferred syntax (NOT standard SQL)
      description: "Origin airport code"
      # NO: ai_context, synonyms

    arr_time:
      expr: _.arr_time
      description: "Arrival timestamp"
      is_time_dimension: true             # BSL-specific flag (OSI uses dimension.is_time)
      smallest_time_grain: "TIME_GRAIN_DAY"  # BSL-specific

  measures:                               # CALLED "metrics" in OSI
    flight_count:
      expr: _.count()                     # Ibis syntax (NOT SQL)
      description: "Total flights"
      # NO: ai_context, synonyms

  joins:                                  # IMPLICIT relationships (OSI is explicit)
    carriers:
      model: "carriers"                   # Reference to other dataset
      type: "one"                         # one/many/cross (NOT OSI terminology)
      left_on: "carrier"
      right_on: "code"
    # NO: explicit relationship names or documentation
```

---

## Detailed Alignment Issues

### Issue 1: Top-Level Structure
| Aspect | OSI | BSL | Alignment |
|--------|-----|-----|-----------|
| Model wrapper | ✅ Single `semantic_model` object | ❌ Implicit (datasets at top level) | **INCOMPATIBLE** |
| Model name | ✅ Required at top level | ❌ No global name | **NEEDS FIX** |
| Model description | ✅ At top level | ❌ Missing | **NEEDS FIX** |
| Model-level AI context | ✅ At top level | ❌ Missing | **NEEDS FIX** |

**Fix Required:**
```yaml
# Current BSL
flights:
  table: flights_tbl

# Must become (OSI-aligned)
name: "flights_model"
description: "..."
ai_context: {...}
datasets:
  - name: "flights"
    source: "flights_tbl"
```

---

### Issue 2: Dataset Definition
| Aspect | OSI | BSL | Alignment |
|--------|-----|-----|-----------|
| Dataset name | ✅ Explicit `name` key | ⚠️ Top-level key (implicit) | **PARTIAL** |
| Table reference | ✅ `source` key | ❌ `table` key | **DIFFERENT NAMES** |
| Primary key | ✅ Explicit `primary_key` | ❌ Missing | **NEEDS FIX** |
| Unique keys | ✅ `unique_keys` array | ❌ Missing | **NEEDS FIX** |
| Dataset description | ✅ Supported | ✅ Supported | **ALIGNED** |
| Dataset AI context | ✅ `ai_context` object | ❌ Missing | **NEEDS FIX** |

**Fix Required:**
```yaml
# Current BSL
flights:
  table: flights_tbl
  description: "Flight records"

# Must become (OSI-aligned)
datasets:
  - name: flights
    source: flights_tbl
    primary_key: [flight_id]
    unique_keys: [[tail_num, dep_time]]
    description: "Flight records"
    ai_context: {...}
```

---

### Issue 3: Fields/Dimensions Definition
| Aspect | OSI | BSL | Alignment |
|--------|-----|-----|-----------|
| Container name | ✅ `fields` | ❌ `dimensions` | **DIFFERENT NAMES** |
| Field name | ✅ Explicit `name` key | ⚠️ Top-level key (implicit) | **PARTIAL** |
| Expression format | ✅ Multi-dialect SQL | ❌ Ibis deferred (_.column) | **INCOMPATIBLE** |
| Field description | ✅ Supported | ✅ Supported | **ALIGNED** |
| Field AI context | ✅ `ai_context` object | ❌ Missing | **NEEDS FIX** |
| Dimension flag | ✅ `dimension.is_time` | ⚠️ `is_time_dimension` | **SIMILAR BUT DIFFERENT** |
| Time grain | ✅ In dimension metadata | ✅ `smallest_time_grain` | **DIFFERENT STRUCTURE** |

**Fix Required:**
```yaml
# Current BSL
dimensions:
  origin:
    expr: _.origin
    description: "Origin airport"
    is_time_dimension: false

  arr_time:
    expr: _.arr_time
    is_time_dimension: true
    smallest_time_grain: "TIME_GRAIN_DAY"

# Must become (OSI-aligned)
fields:
  - name: origin
    expression: "origin"  # Or multi-dialect: {ansi_sql: "origin", ...}
    description: "Origin airport"
    dimension:
      is_time: false
    ai_context:
      synonyms: [dep_airport]

  - name: arr_time
    expression: "arr_time"
    dimension:
      is_time: true
      time_grain: day
    ai_context:
      synonyms: [arrival_time, arrival_timestamp]
```

---

### Issue 4: Metrics/Measures Definition
| Aspect | OSI | BSL | Alignment |
|--------|-----|-----|-----------|
| Container name | ✅ `metrics` | ❌ `measures` | **DIFFERENT NAMES** |
| Metric name | ✅ Explicit `name` key | ⚠️ Top-level key (implicit) | **PARTIAL** |
| Expression format | ✅ Multi-dialect SQL | ❌ Ibis deferred (_.count()) | **INCOMPATIBLE** |
| Expression multi-dialect | ✅ Built-in support | ❌ Missing | **NEEDS FEATURE** |
| Description | ✅ Supported | ✅ Supported | **ALIGNED** |
| AI context | ✅ `ai_context` object | ❌ Missing | **NEEDS FIX** |
| Aggregation type | ⚠️ Implicit in expression | ⚠️ Implicit in expression | **SAME APPROACH** |

**Fix Required:**
```yaml
# Current BSL
measures:
  flight_count:
    expr: _.count()
    description: "Total flights"

  total_distance:
    expr: _.distance.sum()
    description: "Total distance"

# Must become (OSI-aligned)
metrics:
  - name: flight_count
    expression: "COUNT(*)"  # Or multi-dialect: {ansi_sql: "COUNT(*)", snowflake: "COUNT(*)", ...}
    description: "Total flights"
    ai_context:
      synonyms: [num_flights, flight_volume]

  - name: total_distance
    expression: "SUM(distance)"
    description: "Total distance flown"
    ai_context:
      synonyms: [distance_total, cumulative_distance]
```

---

### Issue 5: Relationships Definition
| Aspect | OSI | BSL | Alignment |
|--------|-----|-----|-----------|
| Relationship definition | ✅ Explicit `relationships` array | ❌ Implicit in `joins` | **INCOMPATIBLE** |
| Relationship naming | ✅ Explicit names | ⚠️ Join names (different semantics) | **PARTIAL** |
| Join cardinality | ✅ `type: "many_to_one"` | ⚠️ `type: "one"/"many"` (different) | **SIMILAR BUT DIFFERENT** |
| Join condition | ✅ `join_keys` array | ⚠️ `left_on`/`right_on` | **SIMILAR BUT DIFFERENT** |
| Documentation | ✅ Relationship-level description | ❌ Missing | **NEEDS FIX** |
| Cardinality metadata | ✅ Explicit in type | ⚠️ Implicit (must infer from type) | **PARTIAL** |

**Fix Required:**
```yaml
# Current BSL
joins:
  carriers:
    model: carriers
    type: one
    left_on: carrier
    right_on: code

# Must become (OSI-aligned)
relationships:
  - name: "flights_to_carriers"
    from_dataset: "flights"
    to_dataset: "carriers"
    type: "many_to_one"
    join_keys:
      - from_column: "carrier"
        to_column: "code"
    description: "Each flight belongs to one carrier"
```

---

### Issue 6: Expression Format
| Aspect | OSI | BSL | Alignment |
|--------|-----|-----|-----------|
| Standard | ✅ SQL (ANSI_SQL, dialect-specific) | ❌ Ibis Python deferred syntax | **INCOMPATIBLE** |
| Multi-dialect | ✅ Built-in (`{ansi_sql: ..., snowflake: ...}`) | ❌ Single expression | **MISSING** |
| Validation | ✅ SQL parsing available | ❌ Runtime evaluation | **DIFFERENT** |
| Portability | ✅ Self-contained in YAML | ❌ Requires Ibis interpretation | **DIFFERENT** |

**Examples:**
```yaml
# OSI format
expression:
  ansi_sql: "SUM(amount)"
  snowflake: "SUM(amount) IGNORE NULLS"
  databricks: "SUM(CAST(amount AS DECIMAL(18,2)))"

# Current BSL format
expr: _.amount.sum()  # Ibis-only, no SQL

# Fallback for BSL (single SQL)
expression: "SUM(amount)"
```

---

## Summary: Gap Matrix

| Feature | OSI | BSL | Gap | Effort |
|---------|-----|-----|-----|--------|
| Global model name | ✅ | ❌ | Add wrapper | Low |
| Global description | ✅ | ❌ | Add field | Low |
| Global AI context | ✅ | ❌ | Add field | Medium |
| Dataset primary keys | ✅ | ❌ | Add field | Low |
| Dataset unique keys | ✅ | ❌ | Add field | Low |
| Dataset AI context | ✅ | ❌ | Add field | Low |
| Field names explicit | ✅ | ⚠️ | Restructure | Medium |
| Field AI context | ✅ | ❌ | Add field | Medium |
| Field time metadata | ✅ | ✅ | Align structure | Low |
| Multi-dialect expressions | ✅ | ❌ | Major feature | High |
| Explicit relationships | ✅ | ❌ | Restructure | High |
| Relationship documentation | ✅ | ❌ | Add field | Low |
| SQL expressions (not Ibis) | ✅ | ❌ | Major refactor | High |

---

## Migration Path

### Phase 1: Add Missing Top-Level Fields (Story #1)
```yaml
# Add to every BSL YAML
name: "semantic_model_name"           # NEW
description: "Model description"      # NEW
ai_context: {...}                     # NEW (Story #6)

# Wrap datasets
datasets:
  - name: flights
    source: flights_tbl
    primary_key: [flight_id]          # NEW
    ...
```

### Phase 2: Rename and Restructure Fields (Story #2)
```yaml
# Change dimensions → fields
# Change measures → metrics
fields:
  - name: origin
    expression: "origin"
    dimension:
      is_time: false
    ai_context: {...}

metrics:
  - name: flight_count
    expression: "COUNT(*)"
    ai_context: {...}
```

### Phase 3: Extract Joins → Relationships (Story #3)
```yaml
relationships:
  - name: "flights_to_carriers"
    from_dataset: "flights"
    to_dataset: "carriers"
    type: "many_to_one"
    join_keys:
      - {from: "carrier", to: "code"}
    description: "Relationship description"
```

### Phase 4: Add Multi-Dialect Support (Story #9)
```yaml
metrics:
  - name: revenue
    expressions:
      ansi_sql: "SUM(amount)"
      snowflake: "SUM(amount) IGNORE NULLS"
      databricks: "SUM(CAST(amount AS DECIMAL))"
```

---

## Implementation Strategy

### Backward Compatibility
✅ Support **both** formats:
- Old BSL YAML format (continue working)
- New OSI-aligned format (for new models)
- Auto-migration tools to convert old → new

### YAML Transformation
```python
# Load old BSL format
model_bsl = from_yaml_legacy("flights.yml")

# Convert to OSI format
model_osi = model_bsl.to_osi_compatible()

# Save OSI format
model_osi.to_yaml("flights.osi.yaml")

# Load OSI format
model_loaded = from_yaml_osi("flights.osi.yaml")
```

### Validation
```python
# Validate YAML schema
validate_osi_yaml("flights.yaml")

# Convert and validate round-trip
original = load_yaml("flights.yaml")
osi_json = original.to_osi()
restored = from_osi(osi_json)
assert_equivalent(original, restored)
```

---

## Example: Full Conversion

### Current BSL YAML
```yaml
profile: duckdb

carriers:
  table: carriers_tbl
  description: "Airline carriers"

  dimensions:
    code: _.code
    name:
      expr: _.name
      description: "Carrier name"

  measures:
    carrier_count:
      expr: _.count()
      description: "Number of carriers"

flights:
  table: flights_tbl
  description: "Flight records"

  dimensions:
    origin:
      expr: _.origin
      description: "Origin airport"
    carrier: _.carrier
    arr_time:
      expr: _.arr_time
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_DAY"

  measures:
    flight_count:
      expr: _.count()
      description: "Total flights"
    total_distance:
      expr: _.distance.sum()
      description: "Total distance"

  joins:
    carriers:
      model: carriers
      type: one
      left_on: carrier
      right_on: code
```

### OSI-Aligned YAML
```yaml
name: "flights_semantic_model"
description: "Flight data with carriers and metrics"
ai_context:
  business_definition: "Semantic layer for flight analytics"
  synonyms: [flights_model]

datasets:
  - name: carriers
    source: carriers_tbl
    primary_key: [code]
    description: "Airline carriers"
    ai_context:
      business_definition: "Carrier information"

    fields:
      - name: code
        expression: code
        dimension:
          is_time: false

      - name: name
        expression: name
        dimension:
          is_time: false
        description: "Carrier name"
        ai_context:
          synonyms: [airline_name, carrier_name]

    metrics:
      - name: carrier_count
        expression: "COUNT(*)"
        description: "Number of carriers"

  - name: flights
    source: flights_tbl
    primary_key: [flight_id]
    description: "Flight records"
    ai_context:
      business_definition: "Individual flight legs"

    fields:
      - name: origin
        expression: origin
        dimension:
          is_time: false
        description: "Origin airport"
        ai_context:
          synonyms: [departure_airport]

      - name: carrier
        expression: carrier
        dimension:
          is_time: false

      - name: arr_time
        expression: arr_time
        dimension:
          is_time: true
          time_grain: day
        ai_context:
          synonyms: [arrival_time]

    metrics:
      - name: flight_count
        expression: "COUNT(*)"
        description: "Total flights"
        ai_context:
          synonyms: [num_flights]

      - name: total_distance
        expressions:
          ansi_sql: "SUM(distance)"
          snowflake: "SUM(distance)"
          databricks: "SUM(distance)"
        description: "Total distance flown"
        ai_context:
          synonyms: [distance_total]

relationships:
  - name: "flights_to_carriers"
    from_dataset: flights
    to_dataset: carriers
    type: many_to_one
    join_keys:
      - {from_column: carrier, to_column: code}
    description: "Each flight belongs to one carrier"
```

---

## Recommendations

### For Story #1 (Define OSI Compliance)
- [ ] Define clear YAML mapping rules
- [ ] Create schema validators for both formats
- [ ] Document migration path

### For Story #2-4 (Phase 1 implementation)
- [ ] Implement dual-format parser
- [ ] Support auto-conversion
- [ ] Maintain backward compatibility

### For Story #9 (Multi-dialect)
- [ ] Add expression format validation
- [ ] Support fallback chain (dialect-specific → ANSI_SQL → Ibis)

### For Story #3 (OSI Export)
- [ ] Generate OSI-compliant YAML from BSL models
- [ ] Validate output against OSI schema

---

## Conclusion

**Current BSL YAML is NOT OSI-aligned** but the concepts map well. The main work is:

1. **Structural changes** (wrapper, key names, nesting)
2. **Expression format** (SQL instead of Ibis deferred)
3. **Metadata enrichment** (AI context, governance)
4. **Relationship formalization** (explicit vs implicit)

All changes can be made with **backward compatibility** preserved through dual-format support.

