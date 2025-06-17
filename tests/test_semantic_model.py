import pandas as pd
import pytest
import xorq as xo

from boring_semantic_layer.semantic_model import SemanticModel


def test_group_by_sum_and_count():
    df = pd.DataFrame({"group": ["a", "b", "a", "b", "c"], "val": [1, 2, 3, 4, 5]})
    con = xo.connect()
    table = con.create_table("test", df)
    model = SemanticModel(
        table=table,
        dimensions={"group": lambda t: t.group},
        measures={"sum_val": lambda t: t.val.sum(), "count": lambda t: t.val.count()},
    )
    expr = model.query(dims=["group"], measures=["sum_val", "count"])
    result = expr.execute()
    result = result.sort_values("group").reset_index(drop=True)
    expected = pd.DataFrame(
        {"group": ["a", "b", "c"], "sum_val": [4, 6, 5], "count": [2, 2, 1]}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_filter_and_order():
    df = pd.DataFrame({"group": ["a", "b", "a"], "val": [10, 20, 30]})
    con = xo.connect()
    table = con.create_table("test2", df)
    model = SemanticModel(
        table=table,
        dimensions={"group": lambda t: t.group},
        measures={"sum_val": lambda t: t.val.sum()},
    )
    expr = model.query(dims=["group"], measures=["sum_val"], filters= lambda t: t.group == "a")
    result = expr.execute().reset_index(drop=True)
    expected = pd.DataFrame({"group": ["a"], "sum_val": [40]})
    pd.testing.assert_frame_equal(result, expected)


def test_unknown_dimension_raises():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    con = xo.connect()
    table = con.create_table("test3", df)
    model = SemanticModel(table=table, dimensions={"a": lambda t: t.a}, measures={})
    with pytest.raises(KeyError):
        _ = model.query(dims=["b"], measures=[])

