"""
Example: Demonstrate SemanticModel.materialize() using a xorq backend.
"""

import pandas as pd
import xorq as xo

from boring_semantic_layer.semantic_model import SemanticModel

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
    dimensions={"region": lambda t: t.region, "date": lambda t: t.date},
    measures={
        "total_sales": lambda t: t.sales.sum(),
        "order_count": lambda t: t.sales.count(),
    },
    timeDimension="date",
    smallestTimeGrain="TIME_GRAIN_DAY",
)

cube = sales_model.materialize(
    time_grain="TIME_GRAIN_DAY",
    cutoff="2025-01-04",
    dims=["region"],
    storage=None,
)

print("Cube model definition:", cube.json_definition)

df_cube = cube.query(
    dims=["date", "region"], measures=["total_sales", "order_count"]
).execute()
print("\nSample cube output:")
print(df_cube)
