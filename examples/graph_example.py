#!/usr/bin/env python3
"""Dependency graph example - inspect field dependencies and export to NetworkX."""

import ibis

from boring_semantic_layer import to_semantic_table

# Create sample flights data
flights = (
    to_semantic_table(
        ibis.memtable(
            {
                "origin": ["JFK", "LAX", "ORD"],
                "destination": ["LAX", "JFK", "LAX"],
                "distance": [2475, 2475, 1745],
                "duration": [330, 315, 180],
            }
        )
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

# Access dependency graph
graph = flights.graph

# Inspect dependencies
print("Calculated measure dependencies:")
print(f"  avg_speed depends on: {graph['avg_speed']['deps']}")
print(f"  avg_speed type: {graph['avg_speed']['type']}")

# Navigate the graph
print("\nGraph traversal:")
print(f"  avg_speed predecessors: {graph.predecessors('avg_speed')}")
print(f"  total_distance successors: {graph.successors('total_distance')}")

# Export to NetworkX JSON format
json_data = graph.to_networkx_json()
print(f"\nNetworkX export: {len(json_data['nodes'])} nodes, {len(json_data['links'])} links")

# Optional: use with NetworkX
try:
    import networkx as nx

    G = nx.node_link_graph(json_data)
    print(f"Created NetworkX graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
except ImportError:
    print("Install networkx for graph analysis: pip install networkx")
