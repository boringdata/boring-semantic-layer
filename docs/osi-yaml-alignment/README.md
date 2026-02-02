# BSL ↔ OSI Analysis: Complete Documentation

Generated: January 29, 2026

## 📋 What's Included

This directory contains comprehensive analysis of BSL's alignment with the Open Semantic Interchange (OSI) standard.

### Files in This Analysis

| File | Size | Purpose |
|------|------|---------|
| **OSI-BSL-Summary.md** | 8.3 KB | Executive summary - START HERE |
| **YAML-Quick-Reference.md** | 9.3 KB | Visual YAML format comparison |
| **OSI-BSL-Analysis.md** | 18 KB | Detailed alignment analysis |
| **YAML-Alignment-Analysis.md** | 16 KB | Deep YAML format analysis |
| **BSL-OSI-Cheatsheet.md** | 13 KB | Implementation quick reference |

---

## 🎯 Quick Summary

### The Question
"Is BSL's YAML aligned with OSI's YAML (if defined)?"

### The Answer
**⚠️ NO - Major restructuring needed**

BSL's YAML and OSI's YAML are fundamentally different in structure:

| Aspect | Current BSL | OSI Standard | Gap |
|--------|---|---|---|
| **Top-level structure** | Implicit (datasets at root) | Explicit wrapper with model name | Large |
| **Container names** | `dimensions`, `measures`, `joins` | `fields`, `metrics`, `relationships` | Large |
| **Expression format** | Ibis deferred (`_.count()`) | Multi-dialect SQL (`COUNT(*)`) | **CRITICAL** |
| **Metadata** | Minimal (description only) | Rich (AI context, governance) | Large |
| **Relationships** | Implicit in joins | Explicit with documentation | Large |
| **Keys** | `expr`, `table` | `expression`, `source` | Medium |

---

## 📊 Alignment Status

### ✅ What's Already Aligned
- Core concepts (semantic tables, dimensions, measures)
- Multi-database support philosophy
- YAML-based configuration approach
- Join/relationship semantics (conceptually)

### ❌ What Needs Major Work
1. **YAML Structure** (Phase 1)
   - Add global model wrapper
   - Rename containers (dimensions→fields, measures→metrics)
   - Restructure joins as explicit relationships

2. **Expression Format** (Phase 3)
   - Convert from Ibis deferred to SQL
   - Add multi-dialect support
   - Maintain backward compatibility

3. **Metadata Enrichment** (Phase 2 & 4)
   - Add AI context (synonyms, business definitions, examples)
   - Add governance (owner, classification, compliance)
   - Add lineage tracking

---

## 🚀 Implementation Plan: 20 Stories Across 5 Phases

### Phase 1: Foundation (Stories #1-4) ⚙️
**Goal**: Define OSI compliance & establish export/import

```
Story #1: Define OSI Compliance Model
  ↓
Story #2: Extend SemanticModel with metadata fields
  ↓
Story #3: Implement to_osi() export
  ↓
Story #4: Implement from_osi() import
```

**Outcome**: Can convert BSL ↔ OSI and validate compliance

### Phase 2: Metadata & Enrichment (Stories #5-8) 📝
**Goal**: Add governance and context for LLMs

```
Story #5: Field-level metadata (descriptions, aliases)
Story #6: AI Context (synonyms, examples, hints)
Story #7: Governance (owner, classification)
Story #8: Documentation generation
```

**Outcome**: Rich metadata for governance and LLM integration

### Phase 3: Multi-Dialect & Portability (Stories #9-12) 🔄
**Goal**: Enable cross-platform semantic models

```
Story #9: Multi-dialect expression storage
Story #10: Dialect-aware compilation
Story #11: Expression portability testing
Story #12: Standard library of portable expressions
```

**Outcome**: Models work across any database

### Phase 4: Data Passporting (Stories #13-16) 📋
**Goal**: Implement lineage and governance

```
Story #13: Lineage tracking
Story #14: Audit trail documentation
Story #15: Data contracts & SLAs
Story #16: Passport report generator
```

**Outcome**: Full data governance and compliance tracking

### Phase 5: Integration & Tooling (Stories #17-20) 🛠️
**Goal**: Build ecosystem around OSI models

```
Story #17: Model registry/catalog
Story #18: OSI compliance validator
Story #19: Multi-model federation
Story #20: Adoption guide + examples
```

**Outcome**: Production-ready OSI ecosystem

---

## 📖 How to Use This Analysis

### For Understanding the Gap
1. Read **OSI-BSL-Summary.md** (5 min) - Big picture
2. Read **YAML-Quick-Reference.md** (10 min) - Visual comparison
3. Skim **YAML-Alignment-Analysis.md** - Detailed specs

### For Implementation Planning
1. Read **OSI-BSL-Analysis.md** - Full 20-story breakdown
2. Use **BSL-OSI-Cheatsheet.md** - Code snippets and examples
3. Reference phase descriptions above for sequencing

### For YAML Conversion
1. **YAML-Quick-Reference.md** shows exact mappings
2. **YAML-Alignment-Analysis.md** has full conversion examples
3. Use conversion path section for migration strategy

---

## 🔍 Key Findings

### YAML Format Gaps

#### 1. **Top-Level Structure** (Easy, Low priority)
```diff
- carriers:
-   table: carriers_tbl
+ name: "flights_model"
+ datasets:
+   - name: carriers
+     source: carriers_tbl
```

#### 2. **Field/Dimension Container** (Critical, Medium priority)
```diff
- dimensions:
-   origin: _.origin
+ fields:
+   - name: origin
+     expression: origin
```

#### 3. **Expression Format** (Critical, High priority)
```diff
- expr: _.count()
+ expression: "COUNT(*)"
+ expressions:  # Multi-dialect support
+   ansi_sql: "COUNT(*)"
+   snowflake: "COUNT(*) FILTER (WHERE ...)"
```

#### 4. **AI Context** (Important, Medium priority)
```diff
+ ai_context:
+   synonyms: [num_flights]
+   business_definition: "..."
+   examples: ["..."]
+   query_hints: "..."
```

#### 5. **Explicit Relationships** (Important, Medium priority)
```diff
- joins:
-   carriers:
-     model: carriers
-     type: one
+ relationships:
+   - name: "flights_to_carriers"
+     from_dataset: flights
+     to_dataset: carriers
+     type: many_to_one
+     join_keys: [...]
```

### Expression Format Challenge

**Current BSL**: Uses Ibis deferred syntax
```python
expr: _.distance.sum()
expr: _.count()
expr: _.created_at > '2024-01-01'
```

**OSI Requires**: SQL expressions (with dialect variants)
```yaml
expression: "SUM(distance)"
expressions:
  ansi_sql: "SUM(distance)"
  snowflake: "SUM(distance) IGNORE NULLS"
  databricks: "SUM(CAST(distance AS DECIMAL))"
```

**Solution**:
- ✅ Convert simple Ibis → SQL (most cases)
- ⚠️ Support both Ibis (fallback) and SQL (primary)
- 🔄 Add multi-dialect support where possible

---

## 💡 Recommendations

### For Story #1 (Define Compliance)
- ✅ Document mapping rules: BSL ↔ OSI
- ✅ Plan dual-format support (backward compatible)
- ✅ Create conversion/validation schemas
- ✅ Define migration path

### For Stories #2-4 (Phase 1)
- ✅ Implement dual-format YAML parser
- ✅ Support auto-conversion both directions
- ✅ Comprehensive round-trip testing
- ✅ Update all examples to new format

### For Phase 3 (Multi-dialect)
- ✅ Add `expressions` field alongside `expr`
- ✅ Create SQL expression validator
- ✅ Implement fallback chain: dialect-specific → ANSI_SQL → Ibis
- ✅ Use transpiler for common patterns

### For Phase 4 (Data Passporting)
- ✅ Implement automatic lineage tracking
- ✅ Add audit trail for model changes
- ✅ Create data contract/SLA definitions
- ✅ Generate comprehensive HTML/PDF reports

---

## 📌 Critical Path

**Dependencies between phases:**
```
Phase 1 (Foundation)
  ├─ MUST complete before Phase 2, 3, 4, 5

Phase 2 (Metadata) — Can proceed in parallel with Phase 3
Phase 3 (Dialect) — Can proceed in parallel with Phase 2

Phase 4 (Lineage) — Depends on Phase 1
Phase 5 (Integration) — Depends on Phases 1-4
```

**Recommended order**:
1. **Do Phase 1 first** (Foundation)
2. **Parallelize Phase 2 & 3** (Metadata + Dialect)
3. **Then Phase 4** (Lineage)
4. **Finally Phase 5** (Integration)

---

## 🎓 Learning Path

If you're new to this analysis:

1. **Start here**: `OSI-BSL-Summary.md` (executive summary)
2. **Then read**: `YAML-Quick-Reference.md` (visual comparison)
3. **Deep dive**: `OSI-BSL-Analysis.md` (full details)
4. **Implementation**: `BSL-OSI-Cheatsheet.md` (code examples)
5. **Reference**: `YAML-Alignment-Analysis.md` (conversion details)

---

## 📊 Metrics to Track

### Phase 1 Success
- [ ] OSI-compliant models export/import successfully
- [ ] Round-trip conversion: BSL → OSI → BSL preserves all data
- [ ] Schema validation passes for exported models

### Phase 2 Success
- [ ] 95%+ metadata coverage in exports
- [ ] MCP integration uses AI context for better descriptions
- [ ] Governance metadata persists and is queryable

### Phase 3 Success
- [ ] Multi-dialect expressions work across 3+ databases
- [ ] Tests verify equivalent results across dialects
- [ ] Standard library published with 20+ vetted metrics

### Phase 4 Success
- [ ] Lineage graph accurate for complex models
- [ ] Audit trail complete and queryable
- [ ] Data contracts enforced and testable

### Phase 5 Success
- [ ] Registry functional with publish/discover
- [ ] Compliance validator useful and accurate
- [ ] Federation queries work across models

---

## 🔗 Resources

### OSI Standard
- **Repository**: https://github.com/open-semantic-interchange/OSI
- **Core Spec**: `core-spec/spec.md` or `core-spec/spec.yaml`

### BSL
- **Repository**: https://github.com/boringdata/boring-semantic-layer
- **Example YAMLs**: `examples/*.yml`

### Current Task List
- 20 tracked stories (check `/tasks` command)
- All stories have detailed acceptance criteria
- Phased approach for incremental development

---

## ❓ FAQ

### Q: Do we have to change BSL's YAML format?
**A**: Not immediately. We can support both formats:
- Keep current BSL YAML working
- Add OSI YAML support alongside
- Auto-convert between formats
- Deprecate old format gradually

### Q: What's the biggest challenge?
**A**: Expression format conversion (Ibis → SQL). Current code uses Ibis deferred syntax which isn't portable. Must convert to SQL while maintaining compatibility.

### Q: Can we be 100% OSI compliant?
**A**: ~95% easily. The remaining 5% involves edge cases like:
- Custom OSI extensions (vendor-specific metadata)
- Circular lineage detection
- Performance optimization specifics

### Q: How long will this take?
**A**: Depends on resources. With full team:
- Phase 1: 1-2 weeks
- Phase 2-3: 2-3 weeks (parallel)
- Phase 4: 1-2 weeks
- Phase 5: 1-2 weeks

### Q: Will this break existing BSL models?
**A**: No. With dual-format support, old YAMLs continue working while new ones use OSI format.

---

## 🎯 Next Steps

### Immediate (Today)
1. Review this analysis with team
2. Decide on dual-format vs full migration
3. Review the 20 stories and prioritize Phase 1

### Week 1
1. Start Story #1 (Define compliance spec)
2. Design YAML mapping rules
3. Plan conversion/validation infrastructure

### Week 2-3
1. Implement Stories #2-4 (Phase 1)
2. Get working export/import
3. Validate against OSI schema

### Ongoing
1. Gather user feedback on new format
2. Plan Phase 2 & 3 (can run in parallel)
3. Engage OSI community for alignment

---

## 📝 Notes

- All code examples use YAML format unless otherwise noted
- "Expression format" is the most complex challenge (Ibis → SQL conversion)
- Multi-dialect support is optional but valuable
- Data passporting features are enterprise-grade additions
- Federation and registry are "nice-to-have" for Phase 5

---

## 📬 Questions or Feedback

For questions about this analysis:
1. Check the cheatsheet for quick answers
2. Review the analysis document for details
3. See implementation checklist for next steps

For OSI standard questions:
→ Check https://github.com/open-semantic-interchange/OSI

For BSL implementation:
→ Check /tasks for tracked stories

---

**Analysis Generated**: 2026-01-29
**Status**: Complete & Ready for Implementation
**Recommendation**: Start with Phase 1 (Stories #1-4)

