#!/usr/bin/env python3
"""Dependency graph example - inspect field dependencies using YAML models."""

import json
from pathlib import Path
from pprint import pprint

import ibis

from boring_semantic_layer import from_yaml
from boring_semantic_layer.graph_utils import graph_to_dict

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
yaml_path = Path(__file__).parent / "flights.yml"
profile_file = Path(__file__).parent / "profiles.yml"
models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

carriers = models["carriers"]
flights = models["flights"]

# Get the graph for flights model
print("=== flights.get_graph() ===\n")
pprint(dict(flights.get_graph()))

# Get graph for joined model
print("\n\n=== Joined graph (flights with carriers) ===\n")
joined = flights.join_one(carriers, lambda f, c: f.carrier == c.code)
pprint(dict(joined.get_graph()))

print("\n\n=== Graph export to JSON format ===\n")
print(json.dumps(graph_to_dict(joined.get_graph()), indent=2))
