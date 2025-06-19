from boring_semantic_layer.semantic_model import Join, SemanticModel
import ibis
import os 
con = ibis.duckdb.connect(":memory:")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "malloy-samples", "data")

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
        "carriers": Join(
            alias="carriers",
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code,
            how="inner",
        ),
    }
)
