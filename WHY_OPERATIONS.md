# Why Custom Operations > Wrappers for Semantic Layers

## TL;DR
Custom operations (inheriting from `Relation`) are architecturally superior to wrapper classes for building semantic layers on top of Ibis because they:
1. Participate in Ibis's type system and schema inference
2. Enable lazy evaluation and query optimization
3. Support backend compilation and portability
4. Are immutable, hashable, and cacheable
5. Integrate cleanly with Ibis's expression API via `to_expr()`

---

## The Architecture Question

Should we build semantic operations as:

**A) Custom Operations** (current approach)
```python
class SemanticTable(Relation):  # Inherits from Ibis Operation
    def filter(self, pred):
        return SemanticFilter(source=self, predicate=pred)
```

**B) Wrapper Classes** (alternative)
```python
class SemanticTable:  # Plain Python class
    def __init__(self, table: ibis.Table):
        self._table = table  # Wraps Ibis expression
```

---

## Why Operations Win

### 1. **Type System Integration**

Operations define `schema` and `values` properties that Ibis uses for type checking:

```python
# Custom Operation
semantic = SemanticTable(...)
print(semantic.schema)  # ibis.Schema - known before execution
# → Schema {x: int64, y: int64, total_pop: int64}

# Wrapper approach
wrapped = SemanticTableWrapper(...)
print(wrapped.schema)  # Must delegate to wrapped._table.schema
# → Loses semantic metadata (dims vs measures)
```

**Benefit**: Operations can augment the schema with semantic metadata (dimensions, measures) while maintaining Ibis compatibility.

### 2. **Lazy Evaluation**

Operations are part of Ibis's deferred execution graph:

```python
# Operations build a graph - no execution yet
query = (semantic
    .filter(lambda t: t.pop > 1000000)
    .group_by("state")
    .aggregate("total_pop"))

# Still no data touched - just graph construction
print(query.schema)  # Schema known without execution

# Only executes when needed
result = query.execute()  # ← Execution happens here
```

**Benefit**: Semantic operations participate in lazy evaluation, enabling:
- Query optimization before execution
- Schema inference without data access
- Composable query building

### 3. **Backend Compilation**

Operations can compile to multiple backends via Ibis:

```python
# Same semantic query compiles to different backends
query = semantic.group_by("state").aggregate("total_pop")

# DuckDB
duckdb_sql = ibis.to_sql(query.to_ibis(), dialect="duckdb")

# PostgreSQL
postgres_sql = ibis.to_sql(query.to_ibis(), dialect="postgres")

# Polars
polars_expr = query.to_ibis()  # Works with Polars backend
```

**Benefit**: Semantic operations inherit Ibis's backend portability. Change one line to switch from DuckDB to Snowflake.

### 4. **Graph Traversal & Introspection**

Operations form a graph that can be traversed and analyzed:

```python
# Operations expose their structure
semantic_agg = semantic.group_by("state").aggregate("total_pop")

print(semantic_agg.source)  # → SemanticGroupBy
print(semantic_agg.source.source)  # → SemanticTable
print(semantic_agg.keys)  # → ('state',)
print(semantic_agg.aggs)  # → {'total_pop': <callable>}
```

**Benefit**: Enables:
- Query plan visualization
- Optimization passes over semantic graphs
- Static analysis and validation
- Debugging and introspection tools

### 5. **Immutability & Caching**

Operations are immutable and hashable:

```python
# Operations can be hashed and cached
query = semantic.filter(lambda t: t.x > 1)
cache_key = hash(query)  # Stable hash

# Same query = same hash = cache hit
query2 = semantic.filter(lambda t: t.x > 1)
assert hash(query) == hash(query2)
```

**Benefit**: Enables query result caching, memoization, and deduplication.

### 6. **Clean Expression Conversion**

Operations convert to expressions via `to_expr()`:

```python
# Operation → Expression conversion is first-class
semantic_op = semantic.filter(lambda t: t.x > 1)  # Operation
ibis_expr = semantic_op.to_expr()  # Expression

# Now it's a regular Ibis Table expression
print(type(ibis_expr))  # ibis.expr.types.relations.Table
```

**Benefit**: Semantic operations can drop down to Ibis expressions when needed, maintaining full Ibis compatibility.

### 7. **Schema Transformation Tracking**

Operations track how schemas transform through the pipeline:

```python
semantic = SemanticTable(...)
print(semantic.schema)
# → {city: string, state: string, population: int64, total_pop: int64}

filtered = semantic.filter(lambda t: t.population > 1000000)
print(filtered.schema)
# → Same schema (filter doesn't change schema)

agg = filtered.group_by("state").aggregate("total_pop")
print(agg.schema)
# → {state: string, total_pop: int64}  (schema transformed correctly)
```

**Benefit**: Operations maintain schema correctness through transformations without executing queries.

---

## What We Lose With Wrappers

### 1. **No Ibis Integration**
Wrappers are opaque to Ibis - they can't participate in:
- Type checking
- Query optimization
- Backend compilation
- Graph analysis

### 2. **No Lazy Evaluation**
Wrappers must eagerly resolve to Ibis expressions:
```python
# Wrapper approach requires immediate resolution
wrapped.filter(pred)  # Must call wrapped._table.filter() immediately
# → Loses opportunity for semantic-level optimization
```

### 3. **No Schema Inference**
Wrappers can't augment schemas with semantic metadata in a way Ibis understands:
```python
# Wrapper approach
wrapped.schema  # Just delegates to _table.schema
# → No distinction between dimensions and measures at schema level
```

### 4. **No Repr Benefits**
Wrappers can delegate to Ibis repr, but they can't add semantic structure:
```python
print(wrapped)
# → Just shows underlying Ibis table
# → Loses semantic metadata in repr
```

---

## Real-World Example

Compare how both approaches handle a complex query:

```python
# Custom Operations (current approach)
query = (cities
    .join_one(regions, "state", "state")
    .filter(lambda t: t.population > 1000000)
    .group_by("region")
    .aggregate("total_pop")
    .order_by(lambda t: t.total_pop.desc())
    .limit(5))

# Each step:
# 1. SemanticJoin - tracks both tables' dimensions/measures
# 2. SemanticFilter - preserves metadata
# 3. SemanticGroupBy - validates "region" exists
# 4. SemanticAggregate - resolves "total_pop" measure
# 5. SemanticOrderBy - deferred execution
# 6. SemanticLimit - still no execution

# Schema known without execution
print(query.schema)  # → {region: string, total_pop: int64}

# Only executes when needed
result = query.execute()
```

With wrappers, we'd lose:
- Semantic validation at each step
- Schema tracking through joins
- Ability to inspect the semantic graph
- Lazy evaluation of semantic transformations

---

## Conclusion

Custom operations are the right architectural choice because they:
- **Integrate deeply with Ibis** (type system, lazy evaluation, backends)
- **Enable semantic-level optimization** (operations can be analyzed before compilation)
- **Maintain portability** (compile to any Ibis backend)
- **Support introspection** (graph traversal, debugging, visualization)
- **Are immutable and cacheable** (functional programming benefits)

Wrappers would be simpler to implement but would forfeit these critical benefits, making them unsuitable for a production semantic layer.

**The operation-based approach is architecturally superior.**
