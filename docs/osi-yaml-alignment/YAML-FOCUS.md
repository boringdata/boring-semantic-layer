# YAML Format Alignment: Deep Dive

## Focus: Converting BSL YAML to OSI-Compliant YAML

This document focuses exclusively on the YAML format transformation needed for OSI compliance.

---

## Executive Summary

### Current Situation
- BSL uses **custom YAML format** (not standardized)
- OSI defines **standardized YAML schema**
- Formats are **structurally incompatible**
- Need **dual-format parser** for backward compatibility

### Solution
1. Define exact **YAML schema mapping** (BSL ↔ OSI)
2. Implement **bidirectional conversion**
3. Support **both formats** in code
4. **Deprecate old** format gradually

---

## 1. Complete YAML Format Comparison

### Current BSL YAML Structure

```yaml
# Optional: Connection profile
profile: duckdb

# Top-level: Dataset definitions (implicit)
dataset_name:
  table: physical_table_name
  description: "Optional description"

  # Dimension definitions
  dimensions:
    simple_dim: _.column_name
    complex_dim:
      expr: _.column_name
      description: "Field description"
      is_time_dimension: false
      smallest_time_grain: "TIME_GRAIN_DAY"  # If is_time_dimension: true

  # Measure definitions
  measures:
    simple_measure: _.count()
    complex_measure:
      expr: _.amount.sum()
      description: "Measure description"

  # Join definitions (implicit relationships)
  joins:
    join_alias:
      model: other_dataset
      type: one|many|cross
      left_on: local_column
      right_on: remote_column
```

### OSI YAML Structure

```yaml
# Global model wrapper (REQUIRED)
name: "model_identifier"
description: "Model description"
ai_context:
  business_definition: "..."
  synonyms: [...]

# Datasets array (structured)
datasets:
  - name: "dataset_name"
    source: "physical.table.name"  # Can include schema
    primary_key: [column1, column2]
    unique_keys:
      - [column1, column2]
      - [column3]
    description: "Dataset description"
    ai_context:
      business_definition: "..."

    # Fields array (not object)
    fields:
      - name: "field_name"
        expression: "column_name OR sql_expression"
        description: "Field description"
        dimension:
          is_time: false|true
          time_grain: day|month|year  # If is_time: true
        ai_context:
          business_definition: "..."
          synonyms: [alias1, alias2]
          examples: [example1, example2]
          query_hints: "..."

    # Metrics array (not object)
    metrics:
      - name: "metric_name"
        expression: "sql_expression"  # Or expressions for multi-dialect
        expressions:  # Optional: multi-dialect support
          ansi_sql: "SQL_EXPRESSION"
          snowflake: "SNOWFLAKE_SQL"
          databricks: "DATABRICKS_SQL"
        description: "Metric description"
        ai_context:
          business_definition: "..."
          synonyms: [alias1, alias2]

# Explicit relationships (not joins)
relationships:
  - name: "relationship_name"
    from_dataset: "dataset1"
    to_dataset: "dataset2"
    type: one_to_one|one_to_many|many_to_one|many_to_many
    join_keys:
      - from_column: "local_col"
        to_column: "remote_col"
    description: "Relationship description"
```

---

## 2. Detailed Field-by-Field Mapping

### Top-Level

| BSL | OSI | Notes |
|-----|-----|-------|
| `profile: duckdb` | (removed) | Connection handled separately |
| (implicit) | `name: "model_name"` | **REQUIRED in OSI** |
| (missing) | `description: "..."` | Add at top level |
| (missing) | `ai_context: {...}` | Add at top level |

### Dataset Level

| BSL | OSI | Mapping |
|-----|-----|---------|
| Top-level key: `flights` | `datasets[].name: flights` | Name becomes explicit |
| `table: flights_tbl` | `source: flights_tbl` | Key rename |
| `description: "..."` | `description: "..."` | Same |
| (missing) | `primary_key: [id]` | **ADD** |
| (missing) | `unique_keys: [...]` | **ADD** |
| (missing) | `ai_context: {...}` | **ADD** |

### Fields/Dimensions

| BSL | OSI | Mapping |
|-----|-----|---------|
| `dimensions:` (object) | `fields:` (array) | Key rename + structural change |
| `dim_name: _.column` (shorthand) | `fields[{name, expression}]` | Explicitness required |
| `expr: _.column` | `expression: column` | Key rename + expression format |
| `description: "..."` | `description: "..."` | Same |
| `is_time_dimension: true` | `dimension.is_time: true` | Nesting change |
| `smallest_time_grain: TIME_GRAIN_DAY` | `dimension.time_grain: day` | Key rename + value format |
| (missing) | `ai_context: {...}` | **ADD** |

### Measures/Metrics

| BSL | OSI | Mapping |
|-----|-----|---------|
| `measures:` (object) | `metrics:` (array) | Key rename + structural change |
| `measure_name: _.count()` (shorthand) | `metrics[{name, expression}]` | Explicitness required |
| `expr: _.amount.sum()` | `expression: "SUM(amount)"` | Key rename + expression format |
| `description: "..."` | `description: "..."` | Same |
| (missing) | `expressions: {ansi_sql, snowflake, ...}` | **ADD for multi-dialect** |
| (missing) | `ai_context: {...}` | **ADD** |

### Relationships

| BSL | OSI | Mapping |
|-----|-----|---------|
| `joins:` (object) | `relationships:` (array) | Key rename + structural change |
| `join_alias: {model, type, left_on, right_on}` | `relationships[{name, from_dataset, to_dataset, ...}]` | Complete restructure |
| (missing) | `name: "relationship_name"` | **ADD** |
| `model: other` | `to_dataset: other` | Key rename |
| (implied) | `from_dataset: current` | **ADD** |
| `type: one` | `type: many_to_one` | Value rename (cardinality) |
| `left_on: col` `right_on: col` | `join_keys: [{from_column, to_column}]` | Structural change |
| (missing) | `description: "..."` | **ADD** |

---

## 3. Complete Conversion Examples

### Example 1: Simple Model

**Current BSL:**
```yaml
profile: duckdb

orders:
  table: orders
  description: "Order records"

  dimensions:
    order_id: _.order_id
    created_date:
      expr: _.created_at
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_DAY"

  measures:
    order_count:
      expr: _.count()
      description: "Total orders"
    total_revenue:
      expr: _.amount.sum()
      description: "Total revenue"
```

**OSI-Aligned:**
```yaml
name: "orders_model"
description: "E-commerce order data"

datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    description: "Order records"

    fields:
      - name: order_id
        expression: order_id
        dimension:
          is_time: false

      - name: created_date
        expression: created_at
        dimension:
          is_time: true
          time_grain: day

    metrics:
      - name: order_count
        expression: "COUNT(*)"
        description: "Total orders"
        ai_context:
          synonyms: [num_orders]

      - name: total_revenue
        expression: "SUM(amount)"
        description: "Total revenue"
        ai_context:
          synonyms: [revenue_sum, total_sales]
```

### Example 2: Model with Relationships

**Current BSL:**
```yaml
profile: duckdb

customers:
  table: customers

  dimensions:
    customer_id: _.customer_id
    name: _.name

  measures:
    customer_count: _.count()

orders:
  table: orders

  dimensions:
    order_id: _.order_id
    customer_id: _.customer_id

  measures:
    order_count: _.count()
    total_amount: _.amount.sum()

  joins:
    customers:
      model: customers
      type: one
      left_on: customer_id
      right_on: customer_id
```

**OSI-Aligned:**
```yaml
name: "orders_with_customers"
description: "Orders and customer data"

datasets:
  - name: customers
    source: customers
    primary_key: [customer_id]

    fields:
      - name: customer_id
        expression: customer_id
        dimension:
          is_time: false

      - name: name
        expression: name
        dimension:
          is_time: false

    metrics:
      - name: customer_count
        expression: "COUNT(*)"

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

---

## 4. Expression Format Conversion

### The Challenge: Ibis vs SQL

**Current BSL** uses Ibis deferred syntax:
```yaml
expr: _.column_name
expr: _.amount.sum()
expr: _.count()
expr: _.created_at.year()
expr: _.amount > 100
expr: _.fullVisitorId.nunique()
expr: _.hits.product.count()  # Nested access
```

**OSI requires** SQL expressions:
```yaml
expression: column_name
expression: "SUM(amount)"
expression: "COUNT(*)"
expression: "YEAR(created_at)"
expression: "amount > 100"
expression: "COUNT(DISTINCT fullVisitorId)"
expression: "COUNT(hits.product)"
```

### Conversion Rules

| Ibis Pattern | SQL Pattern | Notes |
|---|---|---|
| `_.column` | `column` | Direct reference |
| `_.column.sum()` | `SUM(column)` | Aggregation |
| `_.column.mean()` | `AVG(column)` | Average |
| `_.column.min()` | `MIN(column)` | Minimum |
| `_.column.max()` | `MAX(column)` | Maximum |
| `_.column.count()` | `COUNT(column)` | Count distinct |
| `_.count()` | `COUNT(*)` | Row count |
| `_.column.nunique()` | `COUNT(DISTINCT column)` | Distinct count |
| `_.nested.field` | `nested.field` | Nested access |
| `_.date.year()` | `YEAR(date)` | Date extraction |
| `_.date.month()` | `MONTH(date)` | Month extraction |
| `_.amount > 100` | `amount > 100` | Comparison |
| `_.column.isin([1,2])` | `column IN (1, 2)` | Membership |
| `_.column.like('pattern')` | `column LIKE 'pattern'` | Pattern match |

---

## 5. Migration Strategy

### Phase 1: Support Both Formats

**Goal**: Read and write both formats, auto-detect

```python
# Code pseudocode
def load_yaml(path):
    data = read_yaml_file(path)
    format = detect_format(data)  # 'bsl' or 'osi'

    if format == 'osi':
        return parse_osi_yaml(data)
    else:
        return parse_bsl_yaml(data)

def to_yaml(model, format='bsl'):
    if format == 'osi':
        return generate_osi_yaml(model)
    else:
        return generate_bsl_yaml(model)
```

### Phase 2: Conversion Tools

**Goal**: Bidirectional conversion

```python
# BSL → OSI
bsl_model = load_yaml('model.bsl.yaml')
osi_spec = bsl_model.to_osi()
write_yaml(osi_spec, 'model.osi.yaml')

# OSI → BSL
osi_model = load_osi('model.osi.yaml')
bsl_spec = osi_model.to_bsl()
write_yaml(bsl_spec, 'model.bsl.yaml')
```

### Phase 3: Gradual Deprecation

1. **Year 1**: Both formats fully supported
2. **Year 2**: Warn on BSL format usage
3. **Year 3**: Deprecate BSL format
4. **Year 4+**: OSI only

---

## 6. Implementation Approach

### Option A: Single Internal Format

```
BSL YAML ──parse──→ Internal Model ──generate──→ OSI YAML
                        ↓
                  (Single source of truth)
```

**Pros**: Single representation, clean
**Cons**: More complex conversion logic

### Option B: Format-Specific Parsers

```
BSL YAML ──parse──→ BSL Model Class ──convert──→ OSI Model Class ──generate──→ OSI YAML
```

**Pros**: Isolated logic, easier testing
**Cons**: Duplicate code, more classes

### Recommendation: **Option A**
- Convert YAML → Internal representation immediately
- Store internal representation
- Generate either format from internal representation
- Cleaner, more maintainable

---

## 7. Schema Definitions

### JSON Schema for OSI YAML (Minimal)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OSI Semantic Model",
  "type": "object",
  "required": ["name", "datasets"],
  "properties": {
    "name": {
      "type": "string",
      "description": "Unique model identifier"
    },
    "description": {
      "type": "string"
    },
    "ai_context": {
      "type": "object"
    },
    "datasets": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name", "source"],
        "properties": {
          "name": { "type": "string" },
          "source": { "type": "string" },
          "primary_key": { "type": "array", "items": { "type": "string" } },
          "description": { "type": "string" },
          "fields": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["name", "expression"],
              "properties": {
                "name": { "type": "string" },
                "expression": { "type": "string" },
                "description": { "type": "string" },
                "dimension": {
                  "type": "object",
                  "properties": {
                    "is_time": { "type": "boolean" },
                    "time_grain": { "type": "string", "enum": ["day", "month", "year"] }
                  }
                }
              }
            }
          },
          "metrics": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["name", "expression"],
              "properties": {
                "name": { "type": "string" },
                "expression": { "type": "string" },
                "expressions": {
                  "type": "object",
                  "properties": {
                    "ansi_sql": { "type": "string" },
                    "snowflake": { "type": "string" },
                    "databricks": { "type": "string" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "relationships": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name", "from_dataset", "to_dataset", "type", "join_keys"],
        "properties": {
          "name": { "type": "string" },
          "from_dataset": { "type": "string" },
          "to_dataset": { "type": "string" },
          "type": {
            "type": "string",
            "enum": ["one_to_one", "one_to_many", "many_to_one", "many_to_many"]
          },
          "join_keys": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "from_column": { "type": "string" },
                "to_column": { "type": "string" }
              }
            }
          }
        }
      }
    }
  }
}
```

---

## 8. Format Detection

### How to Detect BSL vs OSI Format

```python
def detect_format(yaml_dict):
    """
    Detect whether YAML is BSL or OSI format
    Returns: 'bsl' or 'osi'
    """

    # OSI format has explicit top-level keys
    if 'datasets' in yaml_dict and isinstance(yaml_dict['datasets'], list):
        return 'osi'  # Explicit datasets array

    # BSL format has implicit datasets at top level
    # Datasets are dict entries that have 'table' key
    for key, value in yaml_dict.items():
        if key == 'profile':
            continue  # Skip profile
        if isinstance(value, dict) and 'table' in value:
            return 'bsl'  # Found implicit dataset

    # Edge case: could be incomplete, check for common patterns
    if 'name' in yaml_dict and 'datasets' in yaml_dict:
        return 'osi'

    if any(isinstance(v, dict) and 'table' in v for v in yaml_dict.values()):
        return 'bsl'

    raise ValueError("Cannot determine YAML format")
```

---

## 9. Implementation Checklist

### Parser Implementation
- [ ] Create `yaml_parser.py` module
- [ ] Implement format detection
- [ ] Implement BSL YAML parser
- [ ] Implement OSI YAML parser
- [ ] Add expression converter (Ibis → SQL)
- [ ] Add round-trip tests

### YAML Generation
- [ ] Create YAML generator for BSL format
- [ ] Create YAML generator for OSI format
- [ ] Test both directions
- [ ] Validate output against schema

### Integration
- [ ] Update `from_yaml()` to use new parser
- [ ] Add `to_yaml()` with format parameter
- [ ] Add format parameter to CLI
- [ ] Update examples with both formats
- [ ] Document format migration

### Testing
- [ ] Round-trip tests (BSL → internal → OSI → internal → BSL)
- [ ] Schema validation tests
- [ ] Format detection tests
- [ ] Expression conversion tests
- [ ] Real-world example tests

---

## 10. Example Conversion Code

### Converting Simple Expression

```python
def convert_ibis_to_sql(expr_str):
    """Convert Ibis deferred expression to SQL"""

    mapping = {
        '_.count()': 'COUNT(*)',
        '_.nunique()': 'COUNT(DISTINCT {col})',
        '.sum()': 'SUM({col})',
        '.mean()': 'AVG({col})',
        '.min()': 'MIN({col})',
        '.max()': 'MAX({col})',
        '.year()': 'YEAR({col})',
        '.month()': 'MONTH({col})',
        '.day()': 'DAY({col})',
    }

    # Simple substitution for common patterns
    result = expr_str
    result = result.replace('_.', '')  # Remove _.

    for ibis_pattern, sql_pattern in mapping.items():
        result = result.replace(ibis_pattern, sql_pattern)

    return result
```

---

## 11. Breaking Down the Work

### Story #1: Schema Definition (2-3 days)
- [ ] Define BSL and OSI YAML schemas
- [ ] Create JSON schema for validation
- [ ] Document field mappings
- [ ] Get team review

### Story #2: Parser Implementation (3-4 days)
- [ ] Implement format detection
- [ ] Implement BSL parser
- [ ] Implement OSI parser
- [ ] Add expression conversion
- [ ] Comprehensive testing

### Story #3: YAML Generation (2-3 days)
- [ ] Implement OSI YAML generator
- [ ] Implement BSL YAML generator
- [ ] Test both directions
- [ ] Add to CLI

### Story #4: Integration & Migration (2-3 days)
- [ ] Update existing code to use new parser
- [ ] Create migration tools
- [ ] Update all examples
- [ ] Documentation

**Total Estimated Effort**: 10-14 days (can be done in parallel across stories)

---

## 12. Key Decisions to Make

### Q1: Backward Compatibility
**Decision Needed**: How long to support old format?
- Option A: 1 year (fast migration)
- Option B: 2-3 years (gradual)
- **Recommendation**: 2 years (gives users time)

### Q2: Expression Format
**Decision Needed**: Keep Ibis fallback or go pure SQL?
- Option A: Pure SQL (fully portable)
- Option B: Support both Ibis and SQL (more flexibility)
- **Recommendation**: Support both, primary SQL (best of both)

### Q3: Multi-Dialect
**Decision Needed**: Add multi-dialect support in Phase 1?
- Option A: Phase 1 (upfront investment)
- Option B: Phase 3 (later addition)
- **Recommendation**: Phase 1 (foundation work)

### Q4: Field Naming
**Decision Needed**: Keep `dimensions`/`measures` in BSL or rename to `fields`/`metrics`?
- Option A: Keep old names (minimize changes)
- Option B: Rename to OSI terms (cleaner long-term)
- **Recommendation**: Support both during transition

---

## Summary

**Next Steps**:
1. Review this YAML-focused document
2. Make decisions on Q1-Q4 above
3. Start Story #1 (Schema Definition)
4. Implement Stories #2-4 in parallel if possible
5. Test round-trip conversion thoroughly

**File Location**: All analysis in `/tmp/claude-1000/.../scratchpad/`

