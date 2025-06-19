from example_semantic_model import flights_sm, carriers_sm
from ibis import _

print("Available dimensions:", flights_sm.available_dimensions)
print("Available measures:", flights_sm.available_measures)

expr = flights_sm.query(
    dims=["destination"],
    measures=["flight_count", "avg_distance"],
    order_by=[("flight_count", "desc")],
    filters=[_.destination.isin(["LAX", "SFO"])],
    limit=10,
)

df = expr.execute()
print("\nTop 10 carriers by flight count:")
print(df)

