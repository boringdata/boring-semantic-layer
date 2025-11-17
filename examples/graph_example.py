#!/usr/bin/env python3
"""Dependency graph example - inspect field dependencies and export to NetworkX."""

import ibis

from boring_semantic_layer import to_semantic_table

# Create sample flights data
flights = (
    to_semantic_table(
        ibis.memtable(
            {
                "carrier_code": ["AA", "DL", "UA"],
                "origin": ["JFK", "LAX", "ORD"],
                "destination": ["LAX", "JFK", "LAX"],
                "distance": [2475, 2475, 1745],
                "duration": [330, 315, 180],
            }
        ),
        name="flights",
    )
    .with_dimensions(
        route=lambda t: t.origin + " → " + t.destination,
    )
    .with_measures(
        total_distance=lambda t: t.distance.sum(),
        total_duration=lambda t: t.duration.sum(),
        avg_speed=lambda m: m.total_distance / m.total_duration * 60,
    )
)

# Create carriers data
carriers = to_semantic_table(
    ibis.memtable(
        {
            "code": ["AA", "DL", "UA"],
            "name": ["American Airlines", "Delta Air Lines", "United Airlines"],
        }
    ),
    name="carriers",
).with_dimensions(
    carrier_name=lambda t: t.name,
)

# Example 1: Single table graph
print("=== Single Table Graph ===")
graph = flights.graph

# Inspect dependencies
print("\nCalculated measure dependencies:")
print(f"  avg_speed depends on: {graph['avg_speed']['deps']}")
print(f"  avg_speed type: {graph['avg_speed']['type']}")

# Navigate the graph
print("\nGraph traversal:")
print(f"  avg_speed predecessors: {graph.predecessors('avg_speed')}")
print(f"  total_distance successors: {graph.successors('total_distance')}")

# Example 2: Joined tables graph
print("\n\n=== Joined Tables Graph ===")
joined = flights.join_one(carriers, left_on="carrier_code", right_on="code")
joined_graph = joined.graph

print("\nFields from both tables (with prefixes):")
for field_name in sorted(joined_graph.keys()):
    deps = joined_graph[field_name]["deps"]
    field_type = joined_graph[field_name]["type"]
    print(f"  {field_name} ({field_type}): depends on {deps}")

# Navigate joined graph
print("\nJoined graph traversal:")
print(f"  flights.avg_speed predecessors: {joined_graph.predecessors('flights.avg_speed')}")
print(f"  All dimension fields: {[k for k in joined_graph.keys() if joined_graph[k]['type'] == 'dimension']}")

# Export to NetworkX JSON format
print("\n\n=== NetworkX Export ===")
json_data = joined_graph.to_networkx_json()
print(f"Nodes: {len(json_data['nodes'])}, Links: {len(json_data['links'])}")

# Optional: use with NetworkX
try:
    import networkx as nx

    G = nx.node_link_graph(json_data)
    print(f"Created NetworkX graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
except ImportError:
    print("Install networkx for graph analysis: pip install networkx")
