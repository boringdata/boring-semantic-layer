# BSL ↔ OSI Standard Compatibility Analysis

## Executive Summary

**Conclusion: EXCELLENT FIT** ✅ BSL is a strong candidate for OSI compliance with focused adaptations.

BSL and OSI share fundamental philosophy:
- **Single source of truth** for semantic definitions
- **Vendor-agnostic** semantic modeling
- **Composable** semantic structures
- **Multi-platform** support through standardized export

---

## Comparative Analysis

### Mapping of Core Concepts

| OSI Concept | BSL Equivalent | Current Alignment | Gap |
|---|---|---|---|
| **Dataset** | SemanticModel | ✅ High | Minor naming/metadata |
| **Fields** | Dimensions + Calculated Fields | ✅ High | Need ai_context enrichment |
| **Metrics** | Measures + Calculated Measures | ✅ High | Need multi-dialect expressions |
| **Relationships** | Join Operations | ✅ High | Need OSI relationship mapping |
| **Metadata** | Sparse (description only) | ⚠️ Partial | Add governance, ownership, tags |
| **Dialect Support** | Single (Ibis → database dialect) | ⚠️ Partial | Support OSI multi-dialect format |
| **AI Context** | Not present | ❌ Missing | Add synonyms, usage examples |
| **Data Governance** | Not present | ❌ Missing | Add lineage, contracts, compliance |

---

## What BSL Already Does Well (OSI-Aligned)

### 1. **Semantic Definition & Composition** ✅
- Define dimensions, measures once
- Compose through joins without re-declaring logic
- Type-safe querying through fluent API
- Exact match to OSI's "single source of truth" goal

### 2. **Multi-Database Support** ✅
- Ibis abstracts 15+ databases
- Single semantic model → any database
- OSI-style interoperability already enabled

### 3. **YAML Configuration** ✅
- Declarative model definitions
- Version-controllable
- Easy to export/import
- Perfect foundation for OSI format mapping

### 4. **Relationship/Join Support** ✅
- Foreign sum semantics (one-to-many awareness)
- Multiple join types: one(), many(), cross()
- Dependency tracking and prefixing
- Aligned with OSI's relationship model

### 5. **Expression Flexibility** ✅
- Lambda and deferred expressions
- Supports complex calculations
- Foundation for multi-dialect support

---

## Identified Gaps & Adaptations Needed

### Gap 1: Metadata Enrichment
**Current**: Minimal metadata (name, optional description)
**OSI Required**:
- Descriptions, aliases, synonyms
- Ownership, governance tags
- Data classifications, compliance notes
- ai_context for LLM integration
- Tags and categorization

### Gap 2: Multi-Dialect Expression Format
**Current**: Single Ibis → native SQL compilation
**OSI Required**:
- Expressions in multiple dialects (ANSI_SQL, Snowflake, Databricks, etc.)
- Portable metric definitions
- Fallback expressions for compatibility

### Gap 3: Data Passporting & Lineage
**Current**: No lineage or governance tracking
**OSI Required**:
- Data source lineage (which measures depend on which fields)
- Calculation documentation
- Data contracts and SLAs
- Governance audit trails
- Access control metadata

### Gap 4: Relationship Model Formalization
**Current**: Implicit joins with prefixing
**OSI Required**:
- Explicit foreign key definitions
- Cardinality declarations (1:1, 1:N, M:N)
- Join conditions as first-class objects

### Gap 5: Standardized Export/Import
**Current**: YAML format is proprietary
**OSI Required**:
- OSI-compliant JSON/YAML export
- Import from OSI format
- Bidirectional transformation
- Version compatibility handling

---

## Adaptation Strategy

### Phase 1: Foundation (Stories 1-4)
Establish OSI compliance groundwork

### Phase 2: Metadata & Enrichment (Stories 5-8)
Add governance and context

### Phase 3: Multi-Dialect & Portability (Stories 9-12)
Enable cross-platform expression support

### Phase 4: Data Passporting (Stories 13-16)
Add lineage and governance tracking

### Phase 5: Integration & Tooling (Stories 17-20)
Create OSI export/import and CLI tools

---

## Implementation Stories (20 Stories)

### **PHASE 1: Foundation (Stories 1-4)**

#### Story 1: Define OSI Compliance Model for BSL
**Description**: Create a specification document defining how BSL maps to OSI core constructs, with examples.
- Document BSL ↔ OSI concept mapping
- Define version strategy for OSI compliance
- Establish naming conventions for OSI-compatible models
- Create reference examples (flights, events, etc.)
- Define backward compatibility constraints

**Acceptance Criteria**:
- [ ] Specification document published in `/docs/osi-compliance.md`
- [ ] Reference examples show complete BSL-to-OSI mapping
- [ ] Version compatibility matrix documented
- [ ] Review from OSI community (if applicable)

---

#### Story 2: Extend SemanticModel with OSI Metadata Fields
**Description**: Add OSI-compliant metadata fields to `SemanticModel` class.
- Add `ai_context` field (synonyms, examples, usage notes)
- Add `governance` field (owner, classification, compliance)
- Add `relationships` field (formal relationship definitions)
- Add `version` and `version_compatibility` fields
- Update serialization/deserialization

**Acceptance Criteria**:
- [ ] SemanticModel accepts and stores OSI metadata
- [ ] YAML schema updated with new fields
- [ ] Backward compatibility maintained
- [ ] Metadata persists through export/import
- [ ] Unit tests for all new fields

---

#### Story 3: Implement OSI-Compliant YAML Export
**Description**: Create `to_osi()` method to export SemanticModel in OSI format.
- Map SemanticModel → OSI Dataset schema
- Map Dimensions → OSI Fields
- Map Measures → OSI Metrics
- Map Joins → OSI Relationships
- Generate valid OSI JSON/YAML output

**Acceptance Criteria**:
- [ ] `model.to_osi()` generates valid OSI-compliant JSON
- [ ] All metadata properly mapped
- [ ] Relationships correctly represented
- [ ] Output passes OSI schema validation
- [ ] Integration tests with real models

---

#### Story 4: Implement OSI Semantic Model Import
**Description**: Create `from_osi()` function to load OSI models into BSL.
- Parse OSI JSON/YAML format
- Map OSI Dataset → SemanticModel
- Map OSI Fields → Dimensions
- Map OSI Metrics → Measures
- Map OSI Relationships → Join definitions
- Handle unsupported OSI extensions gracefully

**Acceptance Criteria**:
- [ ] `from_osi(model_spec)` successfully loads OSI models
- [ ] Bidirectional conversion: OSI → BSL → OSI
- [ ] Metadata preservation during round-trip
- [ ] Error handling for invalid/incomplete specs
- [ ] Integration tests with public OSI examples

---

### **PHASE 2: Metadata & Enrichment (Stories 5-8)**

#### Story 5: Add Field-Level Metadata (Descriptions, Aliases, Examples)
**Description**: Enable rich metadata on Dimensions and Measures.
- Add `description`, `aliases`, `examples` to Dimension class
- Add `description`, `aliases`, `examples` to Measure class
- Update YAML schema to accept metadata
- Propagate to UI/documentation

**Acceptance Criteria**:
- [ ] Dimension/Measure classes accept metadata
- [ ] YAML syntax: `dimension_name: {expr: ..., description: ..., aliases: [...]}`
- [ ] Metadata accessible via property accessor
- [ ] Tests cover all metadata types

---

#### Story 6: Implement AI Context Support for LLM Integration
**Description**: Add `ai_context` field to enable LLM understanding.
- Define `AIContext` class: synonyms, usage_examples, business_definition, query_hints
- Add to Dimension, Measure, and SemanticModel
- Leverage in MCP agent for better Claude understanding
- Document for LLM system prompts

**Acceptance Criteria**:
- [ ] AIContext class defined and tested
- [ ] MCP backend uses ai_context in tool descriptions
- [ ] Claude understands semantic model nuances
- [ ] Integration tests with LangGraph agent

---

#### Story 7: Add Governance Metadata (Owner, Classification, Compliance)
**Description**: Implement governance tracking aligned with OSI.
- Add `governance` field with: owner, classification (public/internal/confidential), compliance_tags
- Add `data_contract` field for SLAs
- Add `access_control` field for permissions
- Integrate with model export

**Acceptance Criteria**:
- [ ] Governance metadata stored and accessible
- [ ] YAML schema supports governance config
- [ ] Exported OSI models include governance
- [ ] Tests verify governance field persistence

---

#### Story 8: Create Metadata Validation & Documentation Generation
**Description**: Auto-generate documentation from model metadata.
- Validate metadata completeness (required fields)
- Generate HTML/Markdown docs from SemanticModel
- Create CLI tool: `bsl docs generate <model>`
- Support templating for custom doc layouts

**Acceptance Criteria**:
- [ ] `SemanticModel.validate_metadata()` checks completeness
- [ ] Documentation generator creates readable docs
- [ ] CLI command exports formatted documentation
- [ ] Tests cover various model configurations

---

### **PHASE 3: Multi-Dialect & Portability (Stories 9-12)**

#### Story 9: Enhance Measure/Dimension Expressions with Multi-Dialect Support
**Description**: Support measure definitions in multiple SQL dialects.
- Update Measure to store expressions by dialect: `{ansi_sql: '...', snowflake: '...', databricks: '...'}`
- Maintain Ibis expression as default/fallback
- Create expression transpiler for common patterns
- Add validation per dialect

**Acceptance Criteria**:
- [ ] Measure accepts multi-dialect expressions
- [ ] YAML schema: `measure_name: {ansi_sql: '...', snowflake: '...', ...}`
- [ ] Fallback mechanism when target dialect not specified
- [ ] Transpiler handles common transformations
- [ ] Tests for all supported dialects

---

#### Story 10: Build Dialect-Aware Compilation Path
**Description**: Allow BSL to compile to target-specific dialect when needed.
- Extend `to_osi()` to include dialect expressions
- Add compilation option: `execute(target_dialect='snowflake')`
- Intelligent fallback strategy (prefer exact match, then ANSI_SQL)
- Performance optimization per dialect

**Acceptance Criteria**:
- [ ] `execute(target_dialect=...)` generates correct SQL
- [ ] Fallback chain: exact match → ANSI_SQL → Ibis default
- [ ] Tests verify dialect-specific optimizations
- [ ] Documentation shows dialect usage patterns

---

#### Story 11: Implement OSI Expression Portability Testing
**Description**: Ensure expressions are portable across dialects.
- Create test suite for multi-dialect compatibility
- Validate expressions generate equivalent results across platforms
- Document dialect-specific limitations
- Create compatibility matrix

**Acceptance Criteria**:
- [ ] Test suite validates cross-dialect compatibility
- [ ] Compatibility matrix published in docs
- [ ] Expression validation catches dialect issues
- [ ] Warning system for unsupported patterns

---

#### Story 12: Add Standard Library of Portable Expressions
**Description**: Create catalog of vetted, portable measure/dimension definitions.
- Implement common metrics (sum, count, avg, distinct count, percentile, YoY growth, etc.)
- Implement common dimensions (date extraction, binning, categorization)
- Ensure all are multi-dialect tested
- Publish as reusable library

**Acceptance Criteria**:
- [ ] Standard library module created
- [ ] 20+ common metrics/dimensions implemented
- [ ] All tested across 3+ dialects
- [ ] Documentation with examples
- [ ] Examples usage in notebooks

---

### **PHASE 4: Data Passporting & Lineage (Stories 13-16)**

#### Story 13: Implement Lineage Tracking (Field → Measure Dependencies)
**Description**: Track and expose data dependencies through the model.
- Analyze Dimension expressions for field dependencies
- Analyze Measure expressions for Dimension/field dependencies
- Build dependency graph: fields → dimensions → measures
- Expose via `model.lineage` and `model.dependency_graph()`

**Acceptance Criteria**:
- [ ] Lineage automatically computed during model construction
- [ ] `model.lineage()` returns dependency relationships
- [ ] Visualizable dependency graph (JSON export)
- [ ] Circular dependency detection
- [ ] Tests cover complex dependency chains

---

#### Story 14: Add Calculation Documentation & Audit Trail
**Description**: Document how measures are calculated and track changes.
- Add `calculation_notes` field to Measure
- Track measure modifications with timestamps
- Document lineage rationale (why this expression?)
- Support audit queries: "what measures changed?"

**Acceptance Criteria**:
- [ ] Measure stores calculation documentation
- [ ] Change history accessible via `model.history()`
- [ ] Audit log queryable by field/measure/date
- [ ] Documentation propagates to exports
- [ ] Tests verify audit trail integrity

---

#### Story 15: Implement Data Contract & SLA Definitions
**Description**: Support OSI-style data contracts and governance policies.
- Define `DataContract` class: freshness SLA, accuracy SLA, availability SLA
- Add contract to SemanticModel and individual measures
- Support validation against actual data (sample checks)
- Export contracts in OSI format

**Acceptance Criteria**:
- [ ] DataContract class with SLA fields
- [ ] Contracts accessible on model/measures
- [ ] Export includes contract definitions
- [ ] Validation framework can check SLA compliance
- [ ] Documentation with contract examples

---

#### Story 16: Create Data Passporting Report Generator
**Description**: Generate comprehensive data passport documents for semantic models.
- Combine lineage, metadata, governance, contracts
- Create HTML/PDF report template
- Show data journey from source to metric
- Include owner, classification, SLAs, examples

**Acceptance Criteria**:
- [ ] `bsl passport <model>` generates PDF/HTML report
- [ ] Report includes all governance + lineage info
- [ ] Visual dependency diagrams
- [ ] Compliance & classification prominently displayed
- [ ] CLI integration tested

---

### **PHASE 5: Integration & Tooling (Stories 17-20)**

#### Story 17: Create OSI ↔ BSL Registry/Catalog
**Description**: Build registry for publishing and discovering OSI-compliant semantic models.
- Implement model registry interface (filesystem, git, HTTP)
- Support model discovery by name, tags, classification
- Version management for semantic models
- Integration with package managers

**Acceptance Criteria**:
- [ ] Registry interface defined
- [ ] Filesystem registry implementation working
- [ ] Model versioning + compatibility checking
- [ ] Discovery API with filtering
- [ ] Tests with sample registries

---

#### Story 18: Build OSI Validation & Schema Compliance Checker
**Description**: Create tools to validate OSI compliance of models.
- Implement `validate_osi_compliance(model)` function
- Check against OSI core-spec schema
- Provide detailed compliance reports
- Suggest fixes for non-compliance

**Acceptance Criteria**:
- [ ] Compliance validator checks all OSI requirements
- [ ] Clear error messages for violations
- [ ] CLI tool: `bsl validate-osi <model>`
- [ ] Generates compliance report (JSON/HTML)
- [ ] Tests cover common violations

---

#### Story 19: Implement OSI Model Federation (Multi-Model Queries)
**Description**: Enable queries across federated OSI-compliant semantic models.
- Support joins across separately-defined models
- Allow shared dimension/measure references
- Handle conflicting definitions gracefully
- Optimize federated queries

**Acceptance Criteria**:
- [ ] `join_models(model1, model2, on=...)` supported
- [ ] Cross-model queries execute correctly
- [ ] Conflict resolution strategy documented
- [ ] Performance acceptable for federated workloads
- [ ] Integration tests with multiple models

---

#### Story 20: Create Comprehensive OSI Integration Guide & Examples
**Description**: Document complete OSI integration with real-world examples.
- Step-by-step OSI adoption guide
- Example: Convert existing BSL model to OSI
- Example: Import OSI model, run queries
- Example: Create data passports
- OSI + Claude integration examples (MCP + semantic models)
- Best practices for governance + lineage

**Acceptance Criteria**:
- [ ] Adoption guide: 5+ steps with real code
- [ ] 3+ complete worked examples
- [ ] Best practices documented
- [ ] Examples tested and runnable
- [ ] Published in docs + blog post

---

## Summary of Changes

### Code Changes
- **Core**: SemanticModel, Dimension, Measure enhancements
- **New Modules**: `osi_export.py`, `osi_import.py`, `lineage.py`, `governance.py`, `registry.py`
- **CLI**: New commands for OSI compliance, validation, documentation
- **Tests**: Comprehensive test coverage for all new features

### Configuration Changes
- YAML schema extensions for metadata and governance
- Multi-dialect expression storage
- OSI export configuration options

### Documentation
- OSI compliance specification document
- API documentation for new features
- Integration guide and examples
- Data passport template

### Benefits
1. **Interoperability**: BSL semantic models can be shared with any OSI-compliant tool
2. **Governance**: Full audit trail, compliance tracking, data contracts
3. **Portability**: Multi-dialect support enables cross-platform usage
4. **Discovery**: Registry enables model reuse and sharing
5. **LLM Integration**: AI context enrichment improves Claude understanding
6. **Enterprise-Ready**: Governance, lineage, compliance features for regulated industries

---

## Risk Mitigation

- **Backward Compatibility**: All changes maintain compatibility with existing models
- **Feature Flags**: New OSI features optional; existing workflows unaffected
- **Phased Rollout**: 5-phase approach allows incremental adoption
- **Community Validation**: Engage OSI community for feedback on design
- **Testing**: Comprehensive test coverage at each phase

---

## Success Metrics

1. ✅ Successful round-trip conversion: BSL ↔ OSI
2. ✅ 100% metadata preservation in exports
3. ✅ Multi-dialect expressions working in 3+ databases
4. ✅ Lineage accuracy verified against sample queries
5. ✅ OSI compliance validated against core-spec
6. ✅ LLM integration improvements measurable (e.g., fewer clarification questions)
7. ✅ Community adoption of OSI-compliant models

