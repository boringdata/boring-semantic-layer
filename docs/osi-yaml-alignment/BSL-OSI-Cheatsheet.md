# BSL ↔ OSI Integration Cheatsheet

## Core Concept Mapping

```
┌─────────────────┐          ┌─────────────────┐
│  OSI Standard   │  ←────→  │  BSL Codebase   │
└─────────────────┘          └─────────────────┘

Dataset         ↔  SemanticModel (src/expr.py)
  ├─ Fields     ↔    ├─ Dimensions (src/ops.py)
  ├─ Metrics    ↔    ├─ Measures (src/ops.py)
  └─ Relations  ↔    └─ Join Operations (src/expr.py)

AI Context      ↔  NEW: AIContext class (Phase 2)
Governance      ↔  NEW: Governance class (Phase 2)
Lineage         ↔  NEW: Lineage tracking (Phase 4)
Data Contracts  ↔  NEW: DataContract class (Phase 4)
```

---

## Implementation Phases at a Glance

### PHASE 1: Foundation (Stories #1-4)
**Goal**: Export/Import working
```
Story #1: Define OSI compliance spec
Story #2: Add metadata fields to SemanticModel
Story #3: Implement model.to_osi()
Story #4: Implement from_osi(spec)
```
**File Changes**: `expr.py`, `ops.py`, `yaml.py`, `__init__.py`
**New Modules**: `osi_export.py`, `osi_import.py`

### PHASE 2: Metadata & Enrichment (Stories #5-8)
**Goal**: Rich metadata + governance
```
Story #5: Add description/aliases/examples
Story #6: Add ai_context for LLMs
Story #7: Add governance metadata
Story #8: Auto-generate docs from metadata
```
**New Classes**: `AIContext`, `Governance`, `Metadata`
**New Modules**: `governance.py`, `metadata.py`, `doc_generator.py`

### PHASE 3: Multi-Dialect (Stories #9-12)
**Goal**: Portable expressions
```
Story #9: Multi-dialect expression storage
Story #10: Dialect-aware compilation
Story #11: Cross-dialect testing
Story #12: Standard metric library
```
**New Modules**: `dialects.py`, `expression_transpiler.py`, `standard_library.py`

### PHASE 4: Data Passporting (Stories #13-16)
**Goal**: Lineage + governance
```
Story #13: Lineage tracking
Story #14: Audit trail documentation
Story #15: Data contracts & SLAs
Story #16: Passport report generator
```
**New Modules**: `lineage.py`, `audit.py`, `contracts.py`, `passport_generator.py`

### PHASE 5: Integration & Tooling (Stories #17-20)
**Goal**: Ecosystem + production-readiness
```
Story #17: Model registry/catalog
Story #18: OSI compliance validator
Story #19: Multi-model federation
Story #20: Adoption guide + examples
```
**New Modules**: `registry.py`, `validator.py`, `federation.py`
**Updates**: `agents/cli.py` (new CLI commands)

---

## Key Files to Modify/Create

### Core Modules (Modify)
- `src/boring_semantic_layer/expr.py` - Add metadata fields to SemanticModel
- `src/boring_semantic_layer/ops.py` - Extend Dimension/Measure classes
- `src/boring_semantic_layer/yaml.py` - Update YAML schema for new fields
- `src/boring_semantic_layer/__init__.py` - Export new functions
- `src/boring_semantic_layer/agents/cli.py` - Add CLI commands

### New Core Modules (Create)
- `src/boring_semantic_layer/osi_export.py` - Export to OSI format
- `src/boring_semantic_layer/osi_import.py` - Import from OSI format
- `src/boring_semantic_layer/governance.py` - Governance metadata
- `src/boring_semantic_layer/lineage.py` - Dependency tracking
- `src/boring_semantic_layer/contracts.py` - Data contracts & SLAs
- `src/boring_semantic_layer/registry.py` - Model registry
- `src/boring_semantic_layer/validator.py` - OSI compliance checking

### Documentation (Create)
- `docs/osi-compliance.md` - OSI compliance guide
- `docs/osi-adoption-guide.md` - Step-by-step adoption
- `docs/api-osi-export-import.md` - API documentation
- `examples/osi-*.ipynb` - Worked examples

---

## Example Code Snippets

### Phase 1: Export/Import
```python
# Export to OSI
model = to_semantic_table(db.events)
model.with_dimensions(user_id=..., event_date=...)
model.with_measures(event_count=..., revenue=...)

osi_spec = model.to_osi()  # Returns OSI-compliant JSON/dict
with open('model.osi.json', 'w') as f:
    json.dump(osi_spec, f)

# Import from OSI
from boring_semantic_layer import from_osi
model2 = from_osi('model.osi.json', connection=ibis_conn)
```

### Phase 2: AI Context for LLMs
```python
# Define with ai_context
model = (
    to_semantic_table(db.orders)
    .with_dimensions({
        'customer_id': {
            'expr': lambda t: t.customer_id,
            'description': 'Unique customer identifier',
            'aliases': ['cust_id', 'customer'],
            'ai_context': AIContext(
                synonyms=['customer', 'buyer'],
                business_definition='Individual or organization placing orders',
                examples=['cust_123', 'acme_corp'],
                query_hints='Use for grouping/filtering; join with customers table'
            )
        }
    })
    .with_measures({
        'order_count': {
            'expr': lambda t: t.count(),
            'description': 'Total number of orders',
            'ai_context': AIContext(
                synonyms=['num_orders', 'order_volume'],
                query_hints='Aggregates naturally; watch for one-to-many joins'
            )
        }
    })
)

# Claude now understands context
model.to_mcp()  # Better MCP tool descriptions
```

### Phase 3: Multi-Dialect
```python
# Define once, use everywhere
model = (
    to_semantic_table(db.events)
    .with_measures({
        'revenue': {
            'ansi_sql': 'SUM(amount)',
            'snowflake': 'SUM(amount) OVER (PARTITION BY segment ORDER BY date)',
            'databricks': 'SUM(amount)',
            'description': 'Total revenue'
        }
    })
)

# Automatic dialect selection
df_snowflake = model.execute(target_dialect='snowflake')
df_databricks = model.execute(target_dialect='databricks')
df_duckdb = model.execute(target_dialect='duckdb')
```

### Phase 4: Lineage + Governance
```python
# Automatic lineage tracking
model = (
    to_semantic_table(db.events)
    .with_governance(
        owner='analytics-team',
        classification='internal',
        compliance_tags=['pii', 'pci']
    )
    .with_data_contract(
        freshness_sla='1 hour',
        accuracy_sla='99.9%',
        availability_sla='99.95%'
    )
)

# Generate data passport
report = model.to_passport_report()
report.save('data_passport.pdf')

# Query lineage
lineage = model.lineage()
# Shows: event_date (field) → date_dimension (dim) → event_count (measure)

# Check audit trail
history = model.audit_history()
# Who modified this measure? When? Why?
```

### Phase 5: Registry + Compliance
```python
# Validate OSI compliance
from boring_semantic_layer import validate_osi_compliance

compliance_report = validate_osi_compliance(model)
if compliance_report.is_compliant:
    # Publish to catalog
    registry.publish(model, version='1.0.0', visibility='public')
else:
    print(compliance_report.violations)

# Discover models
models = registry.search(
    tags=['revenue', 'customer'],
    classification='public',
    sort_by='popularity'
)

# Use federated models
combined = join_models(
    model1,  # events model
    model2,  # customers model
    on='customer_id'
)
combined.group_by('country').aggregate('revenue', 'customer_count').execute()
```

---

## YAML Schema Evolution

### Current (Before OSI)
```yaml
events:
  table: events_tbl
  dimensions:
    event_date: _.date
    user_id: _.user_id
  measures:
    event_count: _.count()
```

### Phase 1 (Add metadata fields)
```yaml
events:
  table: events_tbl
  governance:
    owner: analytics-team
    classification: internal
  dimensions:
    event_date:
      expr: _.date
      description: Date event occurred
      aliases: [date, occurred_on]
  measures:
    event_count:
      expr: _.count()
      description: Total events
```

### Phase 2 (Add AI context)
```yaml
events:
  table: events_tbl
  governance:
    owner: analytics-team
    classification: internal
  dimensions:
    event_date:
      expr: _.date
      description: Date event occurred
      ai_context:
        synonyms: [date, occurred_on]
        business_definition: The date and time the event was recorded
        examples: ['2024-01-15', '2024-02-20']
  measures:
    event_count:
      expr: _.count()
      description: Total events
      ai_context:
        synonyms: [num_events, count]
        query_hints: "Aggregates to daily/weekly level; join-aware"
```

### Phase 3 (Multi-dialect)
```yaml
events:
  measures:
    revenue:
      expressions:
        ansi_sql: "SUM(amount)"
        snowflake: "SUM(amount) OVER (PARTITION BY segment ORDER BY date)"
        databricks: "SUM(amount)"
        duckdb: "SUM(amount)"
      description: Total revenue
```

### Phase 4 (Contracts)
```yaml
events:
  contracts:
    freshness_sla: "1 hour"
    accuracy_sla: "99.9%"
  measures:
    revenue:
      calculation_notes: "Sum of all transaction amounts in USD"
      audit_tags: ["critical", "financial"]
```

---

## CLI Commands to Add

```bash
# Phase 1: Export/Import
bsl export-osi <model> --output model.osi.json
bsl import-osi model.osi.json --connection duckdb

# Phase 2: Metadata & Docs
bsl validate-metadata <model>
bsl docs generate <model> --output docs/

# Phase 3: Dialects
bsl test-dialects <model> --dialects snowflake,databricks,duckdb

# Phase 4: Lineage & Governance
bsl lineage <model> --output lineage.json
bsl passport <model> --output passport.pdf

# Phase 5: Registry & Compliance
bsl validate-osi-compliance <model>
bsl publish <model> --registry local --visibility public
bsl search --registry public --tags revenue,customer
```

---

## Testing Strategy

### Phase 1 Tests
```python
def test_round_trip_conversion():
    """BSL → OSI → BSL preserves all fields"""

def test_osi_schema_validation():
    """Exported models pass OSI schema validation"""
```

### Phase 2 Tests
```python
def test_metadata_persistence():
    """Metadata survives export/import"""

def test_ai_context_in_mcp():
    """AI context appears in MCP tool descriptions"""
```

### Phase 3 Tests
```python
def test_multi_dialect_equivalence():
    """Expressions produce same results across dialects"""

def test_dialect_fallback():
    """Uses correct fallback when dialect unavailable"""
```

### Phase 4 Tests
```python
def test_lineage_accuracy():
    """Lineage graph correct and complete"""

def test_passport_generation():
    """Report includes all governance info"""
```

### Phase 5 Tests
```python
def test_registry_publish_discover():
    """Models publish/discoverable"""

def test_federation():
    """Multi-model queries work correctly"""
```

---

## Quick Reference: What Gets Added Where

| Story | File | Change Type | Complexity |
|-------|------|-------------|-----------|
| 1 | docs/ | Create spec doc | Low |
| 2 | expr.py, ops.py | Add fields | Medium |
| 3 | osi_export.py | New module | High |
| 4 | osi_import.py | New module | High |
| 5 | ops.py | Extend classes | Low |
| 6 | governance.py | New module | Medium |
| 7 | governance.py | Extend class | Low |
| 8 | doc_generator.py | New module | Medium |
| 9 | ops.py | Extend Measure | Medium |
| 10 | convert.py | Extend compiler | High |
| 11 | tests/ | Add tests | Low |
| 12 | standard_library.py | New module | Medium |
| 13 | lineage.py | New module | High |
| 14 | audit.py | New module | Medium |
| 15 | contracts.py | New module | Medium |
| 16 | passport_generator.py | New module | High |
| 17 | registry.py | New module | High |
| 18 | validator.py | New module | Medium |
| 19 | federation.py | New module | High |
| 20 | docs/ | Create guide | Low |

---

## Dependencies Between Stories

```
Phase 1 (Foundation)
  ├─ Story 1
  ├─ Story 2
  ├─ Story 3 (depends on 2)
  └─ Story 4 (depends on 2)

Phase 2 (Metadata)
  ├─ Story 5 (depends on Phase 1)
  ├─ Story 6 (depends on 5)
  ├─ Story 7 (depends on 5)
  └─ Story 8 (depends on 5,6,7)

Phase 3 (Dialect)
  ├─ Story 9 (depends on Phase 1)
  ├─ Story 10 (depends on 9)
  ├─ Story 11 (depends on 9,10)
  └─ Story 12 (depends on 9)

Phase 4 (Lineage)
  ├─ Story 13 (depends on Phase 1)
  ├─ Story 14 (depends on 13)
  ├─ Story 15 (depends on Phase 2)
  └─ Story 16 (depends on 13,14,15)

Phase 5 (Integration)
  ├─ Story 17 (depends on Phase 1)
  ├─ Story 18 (depends on Phase 1)
  ├─ Story 19 (depends on Phase 1)
  └─ Story 20 (depends on all Phases)
```

---

## Success Indicators by Phase

### Phase 1 ✅
- [ ] Models export to valid OSI JSON
- [ ] OSI models import successfully
- [ ] Round-trip conversion preserves all data
- [ ] CI/CD integration tests passing

### Phase 2 ✅
- [ ] Metadata stored and queryable
- [ ] MCP integration enhanced with context
- [ ] Governance info visible in exports
- [ ] Auto-generated docs are readable

### Phase 3 ✅
- [ ] Multi-dialect expressions work
- [ ] Tests pass across 3+ databases
- [ ] Fallback mechanism tested
- [ ] Standard library published

### Phase 4 ✅
- [ ] Lineage graph accurate
- [ ] Audit trail complete
- [ ] Data contracts enforceable
- [ ] Passports exportable

### Phase 5 ✅
- [ ] Registry functional
- [ ] Compliance checker useful
- [ ] Federation queries work
- [ ] Adoption guide complete

---

Generated: 2026-01-29 | Status: Ready to implement

