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
    # Orders with some customer_ids, including one missing customer
    df = pd.DataFrame({"order_id": [10, 20, 30], "customer_id": [1, 2, 3]})
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


def test_join_one_errors(customers_model):
    # Missing with_ parameter
    with pytest.raises(ValueError):
        Join.one("cust", customers_model)
    # Non-callable with_
    with pytest.raises(TypeError):
        Join.one("cust", customers_model, with_="not callable")


def test_join_one_properties(customers_model):
    # Correct Join object attributes for inner join
    j = Join.one("cust", customers_model, with_=lambda t: t.customer_id)
    assert isinstance(j, Join)
    assert j.alias == "cust"
    assert j.model is customers_model
    assert j.how == "inner"
    assert j.kind == "one"


def test_join_many_errors(customers_model):
    # Missing with_
    with pytest.raises(ValueError):
        Join.many("cust", customers_model)
    # Non-callable with_
    with pytest.raises(TypeError):
        Join.many("cust", customers_model, with_=123)


def test_join_many_properties(customers_model):
    # Correct Join object attributes for left join
    j = Join.many("cust", customers_model, with_=lambda t: t.customer_id)
    assert isinstance(j, Join)
    assert j.alias == "cust"
    assert j.how == "left"
    assert j.kind == "many"


def test_join_behaviour_inner(db_con, customers_model, orders_model):
    # Apply join_one: should drop orders with no matching customer
    j = Join.one("cust", customers_model, with_=lambda t: t.customer_id)
    model = orders_model
    # Attach join
    model = SemanticModel(
        table=model.table,
        dimensions=model.dimensions,
        measures=model.measures,
        joins={"cust": j},
    )
    # Query by customer name
    expr = model.query(
        dimensions=["customer_id", "cust.name"], measures=["order_count"]
    )
    result = expr.execute().sort_values(["customer_id"]).reset_index(drop=True)
    # Only two matching customers
    assert list(result["customer_id"]) == [1, 2]
    assert list(result["cust_name"]) == ["Alice", "Bob"]


def test_join_behaviour_left(db_con, customers_model, orders_model):
    # Apply join_many: should retain all orders including unmatched
    j = Join.many("cust", customers_model, with_=lambda t: t.customer_id)
    model = orders_model
    model = SemanticModel(
        table=model.table,
        dimensions=model.dimensions,
        measures=model.measures,
        joins={"cust": j},
    )
    expr = model.query(
        dimensions=["customer_id", "cust.name"], measures=["order_count"]
    )
    result = expr.execute().sort_values(["customer_id"]).reset_index(drop=True)
    # Expect three rows: for customer_id 1,2 and 3 (name None)
    assert list(result["customer_id"]) == [1, 2, 3]
    # 'cust.name' yields None for unmatched id=3
    assert list(result["cust_name"]) == ["Alice", "Bob", None]
