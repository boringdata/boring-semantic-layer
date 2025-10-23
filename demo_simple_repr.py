#!/usr/bin/env python3
"""Current simple reprs - Ibis-style, concise and informative."""

import pandas as pd
import ibis
import ibis.selectors as s
from boring_semantic_layer.api import to_semantic_table

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

print("SIMPLE IBIS-STYLE REPRS")
print("=" * 80)
print()

print("SemanticTable:")
print(cities)
print()

print("Operations:")
print("  Filter:", cities.filter(lambda t: t.population > 1000000))
print("  GroupBy:", cities.group_by("state"))
print("  Aggregate:", cities.group_by("state").aggregate("total_pop"))
print("  OrderBy:", cities.group_by("state").aggregate("total_pop").order_by(lambda t: t.total_pop.desc()))
print("  Limit:", cities.group_by("state").aggregate("total_pop").limit(5))
print("  Index:", cities.index(s.all(), sample=100))
print()

# Join
df2 = pd.DataFrame({"state": ["NY", "CA"], "region": ["East", "West"]})
tbl2 = con.create_table("regions", df2)
regions = (
    to_semantic_table(tbl2, name="regions")
    .with_dimensions(state=lambda t: t.state, region=lambda t: t.region)
)

print("Join:")
print(cities.join_one(regions, "state", "state"))
print()

print("=" * 80)
print("For detailed query plans, use: query.to_ibis()")
print("=" * 80)
print()

query = cities.group_by("state").aggregate("total_pop")
print(query.to_ibis())
