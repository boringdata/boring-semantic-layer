# BSL <> OSI Compatibility Gap Analysis

**OSI Spec Version**: 0.1.1 (2025-12-11)
**BSL Version**: 0.3.11

## Overview

The [Open Semantic Interchange (OSI)](https://github.com/open-semantic-interchange/OSI) spec defines
a vendor-neutral YAML format for semantic model definitions. This document maps BSL's current format
to OSI and identifies gaps that need bridging for full compatibility.

## Structural Differences

| Aspect | OSI | BSL | Status |
|--------|-----|-----|--------|
| Top-level wrapper | `semantic_model: [{name, datasets, ...}]` | Flat model-per-key | DIFFERENT |
| Version field | `version: "0.1.1"` (required) | None | MISSING |
| Dataset/model | `datasets` array with `name` + `source` | Top-level keys with `table` | DIFFERENT |
| Fields | `fields` array (name + expression object) | `dimensions` dict with `_.expr` syntax | DIFFERENT |
| Metrics | `metrics` array at semantic_model level | `measures`/`calculated_measures` dicts at model level | DIFFERENT |
| Relationships | `relationships` array with `from`/`to` + column arrays | `joins` dict with `model`/`type`/`left_on`/`right_on` | DIFFERENT |

## Field-Level Gaps

### Missing in BSL (needed for OSI export)

| OSI Field | Location | Description | Priority |
|-----------|----------|-------------|----------|
| `ai_context` | Every level | String or structured object with instructions/synonyms/examples | HIGH |
| `primary_key` | Dataset | Array of column names forming PK | MEDIUM |
| `unique_keys` | Dataset | Array of unique key arrays | LOW |
| `custom_extensions` | Every level | Vendor-specific metadata (`vendor_name` + `data` JSON) | LOW |
| `label` | Field | Categorization label (e.g., "filter") | LOW |
| Multi-dialect `expression` | Field/Metric | `dialects: [{dialect, expression}]` | MEDIUM |

### BSL has, OSI doesn't

| BSL Field | Description | Handling |
|-----------|-------------|----------|
| `profile` | Database connection config | Omit from OSI export |
| `filter` | Model-level filter expression | Store in `custom_extensions` |
| `is_entity` | Entity/PK marker on dimension | Map to `primary_key` on dataset |
| `is_event_timestamp` | Event timestamp marker | Store in `custom_extensions` |
| `smallest_time_grain` | Time granularity | Store in `custom_extensions` |
| `derived_dimensions` | Auto-derived time parts | Store in `custom_extensions` |
| `calculated_measures` | Derived metrics referencing other measures | Export as metrics with `custom_extensions` |
| `join.type` / `join.how` | Join cardinality + method | Inferred from relationship + `custom_extensions` |

## Concept Mapping

### Dimensions -> Fields

```
BSL dimension with is_time_dimension=true
  -> OSI field with dimension.is_time=true

BSL dimension with is_entity=true
  -> OSI dataset.primary_key includes this field's column

BSL dimension expression: _.column_name
  -> OSI expression.dialects[{dialect: "ANSI_SQL", expression: "column_name"}]

BSL computed dimension: _.first_name.concat(' ', _.last_name)
  -> OSI expression: "first_name || ' ' || last_name" (SQL form)
```

### Measures -> Metrics

```
BSL measure: _.column.sum()
  -> OSI metric expression: "SUM(dataset_name.column)"

BSL measure: _.count()
  -> OSI metric expression: "COUNT(*)"

BSL calculated_measure: _.meas1 / _.meas2
  -> OSI metric with custom_extension noting it's derived
```

### Joins -> Relationships

```
BSL join:
  carriers:
    model: carriers
    type: one
    left_on: carrier
    right_on: code

  -> OSI relationship:
    name: flights_to_carriers
    from: flights
    to: carriers
    from_columns: [carrier]
    to_columns: [code]
```

## Implementation Plan

### Phase 1: Bidirectional Converter (this PR)

1. **`osi.py`** - New module with `to_osi()` and `from_osi()` functions
2. **`ai_context`** on `Dimension` and `Measure` - Optional field for round-trip fidelity
3. **Tests** - Round-trip conversion tests
4. **Example** - flights.yml converted to OSI format

### Expression Translation Strategy

BSL uses Ibis Deferred expressions (`_.column`), while OSI uses SQL strings.
For simple column references, extraction is straightforward. For complex expressions,
we serialize to ANSI SQL via Ibis's SQL compiler.

**Simple**: `_.column_name` -> `"column_name"`
**Computed**: `_.first_name.concat(' ', _.last_name)` -> `"first_name || ' ' || last_name"`
**Aggregate**: `_.amount.sum()` -> `"SUM(amount)"`
