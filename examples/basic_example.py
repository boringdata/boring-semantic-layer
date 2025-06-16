"""
Example: Using SemanticModel with joins on Malloy sample parquet data.

Requires the Malloy sample data directory available at ../malloy-samples/data
--> https://github.com/malloydata/malloy-samples
"""

import xorq as xo
from boring_semantic_layer.semantic_model import Join, SemanticModel


# Path to Malloy sample data
DATA_DIR = "../malloy-samples/data"
con = xo.connect()

carriers_tbl = xo.deferred_read_parquet(
    path=f"{DATA_DIR}/carriers.parquet",
    con=con,
)

flights_tbl = xo.deferred_read_parquet(
    path=f"{DATA_DIR}/flights.parquet",
    con=con,
)

carriers_sm = SemanticModel(
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "nickname": lambda t: t.nickname,
    },
    measures={
        "carrier_count": lambda t: t.count(),
    },
)

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier,
        "tail_num": lambda t: t.tail_num,
    },
    measures={
        "flight_count": lambda t: t.count(),
        "avg_dep_delay": lambda t: t.dep_delay.mean(),
    },
    joins={
        "carriers": Join(
            alias="carriers",
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code,
            how="inner",
        ),
    },
)

print("Available dimensions:", flights_sm.available_dimensions)
print("Available measures:", flights_sm.available_measures)

expr = flights_sm.query(
    dims=["carriers.name"],
    measures=["flight_count"],
    order_by=[("flight_count", "desc")],
    limit=10,
)
df = expr.execute()
print("\nTop 10 carriers by flight count:")
print(df)
