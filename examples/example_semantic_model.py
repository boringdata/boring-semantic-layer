import os

import ibis
<<<<<<< HEAD

from boring_semantic_layer.semantic_model import SemanticModel, join_one


con = ibis.duckdb.connect(":memory:")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "malloy-samples/data")

flights_tbl = con.read_parquet(f"{DATA_DIR}/flights.parquet")
carriers_tbl = con.read_parquet(f"{DATA_DIR}/carriers.parquet")

carriers_sm = SemanticModel(
    name="carriers",
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "nickname": lambda t: t.nickname,
    },
    measures={
        "carrier_count": lambda t: t.count(),
    },
    primary_key="code",
)

flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier,
        "tail_num": lambda t: t.tail_num,
        "arr_time": lambda t: t.arr_time,
    },
    timeDimension="arr_time",
    smallestTimeGrain="TIME_GRAIN_SECOND",
    measures={
        "flight_count": lambda t: t.count(),
        "avg_dep_delay": lambda t: t.dep_delay.mean(),
        "avg_distance": lambda t: t.distance.mean(),
    },
    joins={
        "carriers": join_one(
            alias="carriers",
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code,
            how="inner",
        ),
    },
)


def query_flights():
    from xorq.caching import ParquetStorage

    con = xo.duckdb.connect(":memory:")
    storage = ParquetStorage(source=con, path="cache")

    cube = flights_sm.materialize(
        time_grain="TIME_GRAIN_DAY",
        cutoff="2030-01-01",
        dims=["origin", "arr_time"],
        storage=storage,
    )
    print("Cube model definition:", cube.json_definition)
    df = cube.query(dims=["arr_time", "origin"], measures=["flight_count"]).execute()
    print("\nSample cube output:")
    print(df.head())


query_flights()
