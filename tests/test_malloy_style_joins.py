import pandas as pd
import pytest
import xorq as xo

from boring_semantic_layer.semantic_model import (
    SemanticModel,
    Join,
)


def test_join_one_with_callable_foreign_key():
    # Orders joined to customers via foreign key using a callable
    orders_df = pd.DataFrame({"order_id": [1, 2, 3], "customer_id": [101, 102, 101]})
    customers_df = pd.DataFrame({"customer_id": [101, 102], "name": ["Alice", "Bob"]})
    con = xo.connect()
    orders_tbl = con.create_table("orders_tbl2", orders_df)
    customers_tbl = con.create_table("customers_tbl2", customers_df)

    customers_model = SemanticModel(
        table=customers_tbl,
        dimensions={"customer_id": lambda t: t.customer_id, "name": lambda t: t.name},
        measures={},
        primary_key="customer_id",
    )
    # Use callable to specify foreign key expression
    j = Join.one("cust", customers_model, with_=lambda t: t.customer_id)
    orders_model = SemanticModel(
        table=orders_tbl,
        dimensions={
            "order_id": lambda t: t.order_id,
            "customer_id": lambda t: t.customer_id,
        },
        measures={"order_count": lambda t: t.order_id.count()},
        joins={"cust": j},
    )
    expr = orders_model.query(
        dimensions=["customer_id", "cust.name"], measures=["order_count"]
    )
    result = expr.execute().sort_values("customer_id").reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "customer_id": [101, 102],
            "cust_name": ["Alice", "Bob"],
            "order_count": [2, 1],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_join_many_counts_children():
    # Departments with many employees
    dept_df = pd.DataFrame({"dept_id": [10, 20], "dept_name": ["HR", "Eng"]})
    emp_df = pd.DataFrame({"id": [1, 2, 3], "dept_id": [10, 10, 20]})
    con = xo.connect()
    dept_tbl = con.create_table("dept_tbl", dept_df)
    emp_tbl = con.create_table("emp_tbl", emp_df)

    emp_model = SemanticModel(
        table=emp_tbl,
        dimensions={"id": lambda t: t.id, "dept_id": lambda t: t.dept_id},
        measures={"child_count": lambda t: t.id.count()},
        # Primary key for one-to-many join
        primary_key="dept_id",
    )
    # One-to-many join using foreign key mapping
    j = Join.many("emp", emp_model, with_=lambda t: t.dept_id)
    d_model = SemanticModel(
        table=dept_tbl,
        dimensions={"dept_name": lambda t: t.dept_name},
        measures={},
        joins={"emp": j},
    )

    expr = d_model.query(dimensions=["dept_name"], measures=["emp.child_count"])
    result = expr.execute().sort_values("dept_name").reset_index(drop=True)

    expected = pd.DataFrame({"dept_name": ["Eng", "HR"], "emp_child_count": [1, 2]})
    pd.testing.assert_frame_equal(result, expected)


def test_join_cross_cartesian_product():
    # Cross join example
    a_df = pd.DataFrame({"a": [1, 2]})
    b_df = pd.DataFrame({"b": [10, 20, 30]})
    con = xo.connect()
    a_tbl = con.create_table("a_tbl", a_df)
    b_tbl = con.create_table("b_tbl", b_df)

    b_model = SemanticModel(
        table=b_tbl,
        dimensions={"b": lambda t: t.b},
        measures={},
    )
    # Cross join
    j = Join.cross("b", b_model)
    c_model = SemanticModel(
        table=a_tbl,
        dimensions={},
        measures={"count": lambda t: t.a.count()},
        joins={"b": j},
    )
    expr = c_model.query(measures=["count"])
    result = expr.execute()

    # Should equal 2 * 3 = 6 rows
    assert len(result) == 1  # single aggregated count row
    assert int(result["count"].iloc[0]) == 6


@pytest.mark.parametrize(
    "factory,args,kwargs",
    [
        (Join.one, (), {}),
        (Join.many, (), {}),
    ],
)
def test_join_factory_missing_arguments(factory, args, kwargs):
    # Must provide with_
    dummy_model = SemanticModel(
        table=xo.connect().create_table("t", pd.DataFrame({"x": [1]})),
        dimensions={"x": lambda t: t.x},
        measures={},
    )
    with pytest.raises(ValueError):
        factory("x", dummy_model, *args, **kwargs)


# Removed tests for 'on' parameter since join_one/join_many only accept 'with_' callable now


def test_join_with_missing_primary_key():
    # with_ requires primary_key on model
    model_no_pk = SemanticModel(
        table=xo.connect().create_table("t3", pd.DataFrame({"y": [1]})),
        dimensions={"y": lambda t: t.y},
        measures={},
    )
    with pytest.raises(ValueError):
        Join.one("x", model_no_pk, with_=lambda t: t.y)
    with pytest.raises(ValueError):
        Join.many("x", model_no_pk, with_=lambda t: t.y)
