#!/usr/bin/env python3
"""Dependency graph example - inspect field dependencies."""

import json
from pprint import pprint

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

# Get the graph
print("=== flights.graph (raw dict output) ===\n")
pprint(dict(flights.graph))

print("\n\n=== Joined graph ===\n")
joined = flights.join_one(carriers, left_on="carrier_code", right_on="code")
pprint(dict(joined.graph))

print("\n\n=== Graph export to JSON format ===\n")
print(json.dumps(joined.graph.to_dict(), indent=2))
