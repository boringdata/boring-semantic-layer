#!/usr/bin/env python3
"""Demo showing graph_utils working with semantic operations."""

import pandas as pd
import ibis
from boring_semantic_layer.api import to_semantic_table
from boring_semantic_layer.api.graph_utils import bfs, walk_nodes, find_dimensions_and_measures
from boring_semantic_layer.api.ops import SemanticTable, SemanticFilter, SemanticAggregate

# Setup
con = ibis.duckdb.connect(":memory:")
df = pd.DataFrame({
    "city": ["NYC", "LA", "Chicago"],
    "state": ["NY", "CA", "IL"],
    "population": [8000000, 4000000, 2700000],
})
tbl = con.create_table("cities", df)

cities = (
    to_semantic_table(tbl, name="cities")
    .with_dimensions(city=lambda t: t.city, state=lambda t: t.state)
    .with_measures(
        total_pop=lambda t: t.population.sum(),
        avg_pop=lambda t: t.population.mean(),
    )
)

# Build a query
query = (cities
    .filter(lambda t: t.population > 1000000)
    .group_by("state")
    .aggregate("total_pop"))

print("=" * 80)
print("GRAPH UTILS WITH SEMANTIC OPERATIONS")
print("=" * 80)
print()

# 1. Using to_expr() - wraps semantic operations in Table expression
print("1. USING to_expr() (keeps semantic operations in graph):")
print()
expr = query.to_expr()
print(f"   Expression type: {type(expr).__name__}")
print(f"   Underlying operation: {expr.op()}")
print()

# BFS traversal
graph = bfs(expr)
print(f"   BFS graph size: {len(graph)} nodes")
print()

# Find semantic operations
semantic_tables = list(walk_nodes(SemanticTable, expr))
print(f"   Found {len(semantic_tables)} SemanticTable(s)")
for st in semantic_tables:
    print(f"     → {st}")

filters = list(walk_nodes(SemanticFilter, expr))
print(f"   Found {len(filters)} SemanticFilter(s)")
for f in filters:
    print(f"     → {f}")

aggregates = list(walk_nodes(SemanticAggregate, expr))
print(f"   Found {len(aggregates)} SemanticAggregate(s)")
for agg in aggregates:
    print(f"     → {agg}")
print()

# Find dimensions and measures
dims, meas = find_dimensions_and_measures(expr)
print(f"   Dimensions found: {list(dims.keys())}")
print(f"   Measures found: {list(meas.keys())}")
print()

print("-" * 80)
print()

# 2. Using to_ibis() - compiles to pure Ibis
print("2. USING to_ibis() (compiles semantic operations away):")
print()
ibis_expr = query.to_ibis()
print(f"   Ibis expression type: {type(ibis_expr).__name__}")
print()

# BFS traversal
graph = bfs(ibis_expr)
print(f"   BFS graph size: {len(graph)} nodes")
print()

# Try to find semantic operations (should be none)
semantic_tables = list(walk_nodes(SemanticTable, ibis_expr))
print(f"   Found {len(semantic_tables)} SemanticTable(s) (should be 0 - compiled away)")

filters = list(walk_nodes(SemanticFilter, ibis_expr))
print(f"   Found {len(filters)} SemanticFilter(s) (should be 0 - compiled away)")

aggregates = list(walk_nodes(SemanticAggregate, ibis_expr))
print(f"   Found {len(aggregates)} SemanticAggregate(s) (should be 0 - compiled away)")
print()

# Find dimensions and measures
dims, meas = find_dimensions_and_measures(ibis_expr)
print(f"   Dimensions found: {list(dims.keys())} (empty - no semantic metadata)")
print(f"   Measures found: {list(meas.keys())} (empty - no semantic metadata)")
print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print()
print("✓ to_expr() keeps semantic operations in the graph")
print("  → Can traverse and find SemanticTable, SemanticFilter, SemanticAggregate")
print("  → Can extract dimensions and measures metadata")
print()
print("✓ to_ibis() compiles semantic operations to pure Ibis")
print("  → Semantic operations are compiled away")
print("  → Only Ibis operations remain (Project, Filter, Aggregate, etc.)")
print("  → No semantic metadata in the graph")
print()
print("This demonstrates the dual nature of semantic operations:")
print("  - Operations ARE Relations (can be traversed with graph_utils)")
print("  - Operations COMPILE to Ibis (via to_ibis() for SQL execution)")
