import pandas as pd
import pytest
import xorq as xo

from boring_semantic_layer.semantic_model import (
    SemanticModel,
    Join,
)


@pytest.fixture
def db_con():
    return xo.connect()


@pytest.fixture
def customers_model(db_con):
    # DataFrame with two customers
    df = pd.DataFrame({"customer_id": [1, 2], "name": ["Alice", "Bob"]})
    tbl = db_con.create_table("customers", df)
    # Define a semantic model with primary_key
    return SemanticModel(
        table=tbl,
        dimensions={"customer_id": lambda t: t.customer_id, "name": lambda t: t.name},
        measures={},
        primary_key="customer_id",
    )


@pytest.fixture
def orders_model(db_con):
    # Orders with some customer_ids
    df = pd.DataFrame({"order_id": [10, 20], "customer_id": [1, 2]})
    tbl = db_con.create_table("orders", df)
    # Base order model without joins
    return SemanticModel(
        table=tbl,
        dimensions={
            "order_id": lambda t: t.order_id,
            "customer_id": lambda t: t.customer_id,
        },
        measures={"order_count": lambda t: t.order_id.count()},
    )


def test_filter_behaviour(db_con, customers_model, orders_model):
    # Filter joined model
    j = Join.one("cust", customers_model, with_=lambda t: t.customer_id)
    model = orders_model
    # Attach join
    model = SemanticModel(
        table=model.table,
        dimensions=model.dimensions,
        measures=model.measures,
        joins={"cust": j},
    )
    # Filter on joined dimension
    expr = model.query(
        dimensions=["customer_id", "cust.name"],
        measures=["order_count"],
        filters=[
            {
                "field": "cust.name",
                "operator": "=",
                "value": "Alice",
            }
        ],
    )
    result = expr.execute().reset_index(drop=True)
    # Only one matching customer
    assert list(result["customer_id"]) == [1]
    assert list(result["cust_name"]) == ["Alice"]
