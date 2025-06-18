from mcp.server.fastmcp import FastMCP
import pandas as pd
from typing import Optional
from example_semantic_model import flights_sm, carriers_sm

mcp = FastMCP("Flight Semantic Layer")

models = {
    "flights": flights_sm,
    "carriers": carriers_sm,
}

@mcp.tool()
def list_models() -> list[str]:
    """List all available semantic models"""
    return [{
        "name": model_name,
        "dimensions": model.available_dimensions,
        "measures": model.available_measures,
    } for model_name, model in models.items()]

@mcp.tool()
def query_model(
        model_name: str, 
        dimensions: Optional[list[str]] = [], 
        measures: Optional[list[str]] = [], 
        order_by: Optional[list[str]] = [], 
        limit: Optional[int] = None
        ) -> list[dict]:
    """Query a semantic model.

    Args:
        model_name: The name of the model to query.
        dimensions: The dimensions to group by.
        measures: The measures to aggregate.
        filters: The filters to apply to the query. These filters should be ibis expressions: lambda t: <filter_expression>
        order_by: The order by clause to apply to the query (list of tuples: [("field", "asc|desc")]).
        limit: The limit to apply to the query (integer).
    """
    model = models[model_name]
    output_df = model.query(
        dimensions=dimensions, 
        measures=measures, 
        filters=[],     
        order_by=order_by, 
        limit=limit).execute()
    return output_df.to_dict(orient="records")