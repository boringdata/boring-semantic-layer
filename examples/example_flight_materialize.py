"""
Example: Serve a materialized semantic model via XORQ Flight Server
"""
import pandas as pd
import xorq as xo

from xorq.flight import FlightServer
from boring_semantic_layer import SemanticModel

df = pd.DataFrame(
    {
        "date": pd.date_range("2025-01-01", periods=5, freq="D"),
        "region": ["north", "south", "north", "east", "south"],
        "sales": [100, 200, 150, 300, 250],
    }
)

con = xo.connect()
tbl = con.create_table("sales", df)

sales_model = SemanticModel(
    table=tbl,
    dimensions={
        "region": lambda t: t.region,
        "date": lambda t: t.date,
    },
    measures={
        "total_sales": lambda t: t.sales.sum(),
        "order_count": lambda t: t.sales.count(),
    },
    time_dimension="date",
    smallest_time_grain="TIME_GRAIN_DAY",
)

cube = sales_model.materialize(
    time_grain="TIME_GRAIN_DAY",
    cutoff="2025-01-04",
    dimensions=["region", "date"],
    storage=None,
)

print("Materialized cube definition:", cube.json_definition)

server = FlightServer(connection=lambda: con)

if __name__ == "__main__":
    try:
        print(f"Starting Flight server at {server.flight_url}")
        server.serve(block=True)
    except KeyboardInterrupt:
        print("Shutting down Flight server...")
    finally:
        server.close()

# -----------------------------------------------------------------------------
#from xorq.flight.client import FlightClient
#client = FlightClient(host="localhost", port=7702)
#expr = cube.table  # Ibis table expression of the materialized cube
#result = client.execute(expr)
#df_remote = result.to_pandas()
#print(df_remote)
#-----------------------------------------------------------------------------
