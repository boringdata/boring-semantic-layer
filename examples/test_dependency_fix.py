"""Test script to verify the dependency graph fix for column/dimension distinction."""
import ibis
from boring_semantic_layer.api import to_semantic_table

# Create table with origin and destination columns
flights = ibis.memtable({"origin": ["JFK", "LAX"], "destination": ["SFO", "ORD"], "distance": [2500, 300]})

# Test Case 1: Dimension with same name as column
print("=" * 80)
print("Test 1: Dimension with same name as column")
print("=" * 80)
sm1 = to_semantic_table(flights).with_dimensions(
    origin=lambda t: t.origin,  # Should depend on COLUMN origin, not dimension origin
)

graph1 = sm1.graph
print(f"Graph: {graph1}")
print(f"\nExpected: {{'origin': {{'deps': {{'origin': 'column'}}, 'type': 'dimension'}}}}")
print(f"Actual:   {graph1}")

assert graph1['origin']['deps']['origin'] == 'column', "BUG: Should be 'column', not 'dimension'!"
print("✓ PASS: Dimension correctly depends on column\n")

# Test Case 2: Dimension depending on another dimension
print("=" * 80)
print("Test 2: Dimension depending on another dimension")
print("=" * 80)
sm2 = to_semantic_table(flights).with_dimensions(
    origin_code=lambda t: t.origin,  # Depends on column
    origin_upper=lambda t: t.origin_code.upper(),  # Depends on dimension
)

graph2 = sm2.graph
print(f"Graph: {graph2}")
print(f"\norigin_code deps: {graph2['origin_code']['deps']}")
print(f"origin_upper deps: {graph2['origin_upper']['deps']}")

assert graph2['origin_code']['deps']['origin'] == 'column', "origin_code should depend on column"
assert graph2['origin_upper']['deps']['origin_code'] == 'dimension', "origin_upper should depend on dimension"
print("✓ PASS: Dimensions correctly distinguish column vs dimension dependencies\n")

# Test Case 3: Measure depending on column with same name as dimension
print("=" * 80)
print("Test 3: Measure depending on column (not dimension with same name)")
print("=" * 80)
sm3 = to_semantic_table(flights).with_dimensions(
    distance=lambda t: t.distance,  # Dimension named 'distance'
).with_measures(
    total_distance=lambda t: t.distance.sum(),  # Should depend on dimension, not column
)

graph3 = sm3.graph
print(f"Graph: {graph3}")
print(f"\ndistance (dim) deps: {graph3['distance']['deps']}")
print(f"total_distance (measure) deps: {graph3['total_distance']['deps']}")

# The measure should depend on the dimension since it's in the extended table
assert graph3['distance']['deps']['distance'] == 'column', "dimension should depend on column"
assert graph3['total_distance']['deps']['distance'] == 'dimension', "measure should depend on dimension (extended table)"
print("✓ PASS: Measure correctly depends on dimension in extended table\n")

print("=" * 80)
print("ALL TESTS PASSED!")
print("=" * 80)
