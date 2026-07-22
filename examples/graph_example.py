#!/usr/bin/env python3
"""Dependency graph example - inspect field dependencies using YAML models."""

import json
from pathlib import Path
from pprint import pprint

from boring_semantic_layer import from_yaml
from boring_semantic_layer.graph_utils import graph_to_dict

# Load semantic models from YAML
yaml_path = Path(__file__).parent / "flights.yml"
profile_file = Path(__file__).parent / "profiles.yml"
models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

carriers = models["carriers"]
flights = models["flights"]

# Get the graph for a standalone model
print("=== carriers.get_graph() ===\n")
pprint(dict(carriers.get_graph()))

# ``from_yaml`` has already applied the joins declared on ``flights``.
print("\n\n=== Joined graph (flights with YAML-defined joins) ===\n")
joined = flights
pprint(dict(joined.get_graph()))

print("\n\n=== Graph export to JSON format ===\n")
print(json.dumps(graph_to_dict(joined.get_graph()), indent=2))
