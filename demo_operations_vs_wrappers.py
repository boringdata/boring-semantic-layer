#!/usr/bin/env python3
"""Demo showing why custom operations are superior to wrappers."""

import pandas as pd
import ibis
from boring_semantic_layer.api import to_semantic_table

# Setup data
con = ibis.duckdb.connect(":memory:")
df = pd.DataFrame({
    "city": ["NYC", "LA", "Chicago"],
    "state": ["NY", "CA", "IL"],
    "population": [8000000, 4000000, 2700000],
})
tbl = con.create_table("cities", df)

# Create semantic table
cities = (
    to_semantic_table(tbl, name="cities")
    .with_dimensions(city=lambda t: t.city, state=lambda t: t.state)
    .with_measures(
        total_pop=lambda t: t.population.sum(),
        avg_pop=lambda t: t.population.mean(),
    )
)

print("=" * 80)
print("CUSTOM OPERATIONS: Advantages Demonstrated")
print("=" * 80)
print()

# 1. SCHEMA INFERENCE WITHOUT EXECUTION
print("1. SCHEMA INFERENCE (no execution required):")
query = cities.filter(lambda t: t.population > 1000000).group_by("state").aggregate("total_pop")
print(f"   Schema: {query.schema}")
print(f"   ✓ Schema known before any data is touched!")
print()

# 2. LAZY EVALUATION
print("2. LAZY EVALUATION:")
print(f"   Query object: {query}")
print(f"   ✓ No data loaded yet - pure graph construction")
print()

# 3. GRAPH INTROSPECTION
print("3. GRAPH INTROSPECTION:")
print(f"   Operation type: {type(query).__name__}")
print(f"   Group by keys: {query.keys}")
print(f"   Aggregations: {list(query.aggs.keys())}")
print(f"   Source: {type(query.source).__name__}")
print(f"   ✓ Full semantic graph is inspectable")
print()

# 4. BACKEND COMPILATION
print("4. BACKEND COMPILATION:")
ibis_expr = query.to_ibis()
sql = ibis.to_sql(ibis_expr, dialect="duckdb")
print(f"   Compiles to SQL:")
print(f"   {sql[:100]}...")
print(f"   ✓ Can compile to any Ibis backend")
print()

# 5. IMMUTABILITY & HASHING
print("5. IMMUTABILITY & HASHING:")
query1 = cities.filter(lambda t: t.population > 1000000)
query2 = cities.filter(lambda t: t.population > 1000000)
print(f"   Query 1 hash: {hash(query1)}")
print(f"   Query 2 hash: {hash(query2)}")
print(f"   Same operation? {hash(query1) == hash(query2)}")
print(f"   ✓ Operations are immutable and cacheable")
print()

# 6. TYPE SAFETY
print("6. TYPE SAFETY (via schema):")
filtered = cities.filter(lambda t: t.population > 1000000)
print(f"   Before filter: {cities.schema}")
print(f"   After filter:  {filtered.schema}")
agg = filtered.group_by("state").aggregate("total_pop")
print(f"   After agg:     {agg.schema}")
print(f"   ✓ Schema transforms correctly through operations")
print()

# 7. CLEAN EXECUTION
print("7. EXECUTION WHEN NEEDED:")
result = query.execute()
print(f"   Result type: {type(result)}")
print(f"   Result shape: {result.shape}")
print(result)
print(f"   ✓ Executes only when .execute() is called")
print()

print("=" * 80)
print("WRAPPER APPROACH: What We'd Lose")
print("=" * 80)
print()

class SemanticTableWrapper:
    """Example wrapper - NOT used in BSL v2."""
    def __init__(self, table, dimensions=None, measures=None):
        self._table = table
        self._dimensions = dimensions or {}
        self._measures = measures or {}

    def filter(self, pred):
        # Must eagerly resolve
        return SemanticTableWrapper(
            self._table.filter(pred(self._table)),
            self._dimensions,
            self._measures
        )

    def __repr__(self):
        return repr(self._table)

wrapped = SemanticTableWrapper(tbl, dimensions={"city": lambda t: t.city})
filtered_wrapped = wrapped.filter(lambda t: t.population > 1000000)

print("✗ No graph introspection:")
print(f"  type(filtered_wrapped): {type(filtered_wrapped).__name__}")
print(f"  No .source, .keys, .aggs attributes")
print()

print("✗ No Ibis integration:")
print(f"  Can't inspect as Relation")
print(f"  Not part of Ibis's operation graph")
print()

print("✗ Lossy repr:")
print(f"  {wrapped}")
print(f"  → Just shows Ibis table, semantic metadata hidden")
print()

print("=" * 80)
print("CONCLUSION: Custom Operations Are Architecturally Superior")
print("=" * 80)
print()
print("Operations provide:")
print("  • Type system integration (schema inference)")
print("  • Lazy evaluation (graph construction)")
print("  • Backend compilation (portability)")
print("  • Graph introspection (debugging, optimization)")
print("  • Immutability (caching, functional composition)")
print("  • Clean Ibis integration (to_expr(), to_ibis())")
print()
print("This is why BSL v2 uses custom operations, not wrappers.")
