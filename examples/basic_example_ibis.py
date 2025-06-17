from boring_semantic_layer.semantic_model import Join, SemanticModel
import ibis

# Connect to in-memory DuckDB
con = ibis.duckdb.connect(":memory:")

DATA_DIR = "../malloy-samples/data"

# Read Parquet files using the DuckDB connection
flights_tbl = con.read_parquet(f"{DATA_DIR}/flights.parquet")
carriers_tbl = con.read_parquet(f"{DATA_DIR}/carriers.parquet")

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
    transform=lambda t: t.filter(t.dep_time == 20240101),
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier,
        "tail_num": lambda t: t.tail_num,
    },
    measures={
        "flight_count": lambda t: t.count(),
        "avg_dep_delay": lambda t: t.dep_delay.mean(),
        "avg_distance": lambda t: t.distance.mean(),
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
    dims=["destination"],
    measures=["flight_count", "avg_distance"],
    order_by=[("flight_count", "desc")],
    filters={"dep_time": 20240101},
    limit=10,
)

df = expr.execute()
print("\nTop 10 carriers by flight count:")
print(df)

