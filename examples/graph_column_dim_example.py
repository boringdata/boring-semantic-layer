"""Example showing column vs dimension distinction in dependency graph."""

import ibis

from boring_semantic_layer.api import to_semantic_table

# Create table with origin and destination columns
flights = ibis.memtable({"origin": ["JFK", "LAX"], "destination": ["SFO", "ORD"]})

# Create dimensions with same names as columns
sm = to_semantic_table(flights).with_dimensions(
    origin=lambda t: t.origin,
    destination=lambda t: t.destination,
)

print(sm.graph)
# BUG: Shows {'origin': {'deps': {'origin': 'dimension'}, 'type': 'dimension'}}
# EXPECTED: {'origin': {'deps': {'origin': 'column'}, 'type': 'dimension'}}
# The dimension should depend on the COLUMN, not itself as a dimension
