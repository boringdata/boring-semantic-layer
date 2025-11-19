#!/usr/bin/env python3
"""Dependency graph example - inspect field dependencies using YAML models."""

from pprint import pprint

import ibis

from boring_semantic_layer import from_yaml

# Create sample data tables
carriers_tbl = ibis.memtable(
    {
        "code": ["AA", "DL", "UA"],
        "name": ["American Airlines", "Delta Air Lines", "United Airlines"],
        "nickname": ["American", "Delta", "United"],
    }
)

flights_tbl = ibis.memtable(
    {
        "carrier": ["AA", "DL", "UA", "AA"],
        "origin": ["JFK", "LAX", "ORD", "LAX"],
        "destination": ["LAX", "JFK", "LAX", "ORD"],
        "distance": [2475, 2475, 1745, 1745],
        "dep_delay": [10, -5, 15, 0],
        "arr_time": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
    }
)

# Load semantic models from YAML
models = from_yaml(
    "examples/yaml_example.yml", {"carriers_tbl": carriers_tbl, "flights_tbl": flights_tbl}
)

# Get individual models
carriers = models["carriers"]
flights = models["flights"]

# Get the graph for flights model
print("=== flights.get_graph() ===\n")
pprint(dict(flights.get_graph()))

# Get graph for joined model
print("\n\n=== Joined graph (flights with carriers) ===\n")
joined = flights.join_one(carriers, left_on="carrier", right_on="code")
pprint(dict(joined.get_graph()))

# Show some example traversals
print("\n\n=== Example: Dependencies of flights.total_distance ===")
graph = flights.get_graph()
deps = graph.predecessors("flights.total_distance")
print(f"flights.total_distance depends on: {deps}")

print("\n=== Example: What uses flights.distance? ===")
successors = graph.successors("flights.distance")
print(f"Fields depending on flights.distance: {successors}")
