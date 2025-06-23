"""
Example: Basic Query with Semantic Model

This example demonstrates how to use a semantic model (`flights_sm`) to perform a basic query, retrieving available dimensions and measures, and running a grouped and aggregated query with ordering and limiting.

Semantic Model: `flights_sm`
- Represents a flights dataset with dimensions such as destination and measures such as flight count and average distance.

Query:
- Dimensions: destination
- Measures: flight_count, avg_distance
- Order by: flight_count (descending)
- Limit: 10

Expected Output (example):

| destination | flight_count | avg_distance |
|-------------|-------------|--------------|
|     JFK     |    1200     |    1450.2    |
|     LAX     |    1100     |    2100.5    |
|     ORD     |    950      |    980.7     |
|    ...      |    ...      |     ...      |

"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from example_flight_semantic_model import flights_sm

print("Available dimensions:", flights_sm.available_dimensions)
print("Available measures:", flights_sm.available_measures)

expr = flights_sm.query(
    dimensions=["destination"],
    measures=["flight_count", "avg_distance"],
    order_by=[("flight_count", "desc")],
    limit=10,
)

df = expr.execute()
print("\nTop 10 carriers by flight count:")
print(df)
