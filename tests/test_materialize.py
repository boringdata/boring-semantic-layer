import pytest
import pandas as pd
import ibis
import xorq as xo
from boring_semantic_layer import semantic_model
from boring_semantic_layer.semantic_model import SemanticModel


@pytest.fixture
def simple_vendor_model():
    df = pd.DataFrame({"col_test": [1, 2, 3], "val": [10, 20, 30]})
    con = xo.connect()
    tbl = con.create_table("t", df)
    return SemanticModel(
        table=tbl,
        dimensions={"col_test": lambda t: t.col_test},
        measures={"sum_val": lambda t: t.val.sum()},
    )


def test_materialize_requires_xorq(monkeypatch, simple_vendor_model):
    monkeypatch.setattr(semantic_model, "IS_XORQ_USED", False)
    with pytest.raises(RuntimeError, match="requires xorq vendor ibis backend"):
        simple_vendor_model.materialize()


def test_materialize_requires_vendor_expr(monkeypatch):
    # Xorq issue: https://github.com/xorq-labs/xorq/issues/1036
    df = pd.DataFrame({"x": [1, 2, 3]})
    pure_tbl = ibis.memtable(df)
    model = SemanticModel(
        table=pure_tbl,
        dimensions={"x": lambda t: t.x},
        measures={"sum_x": lambda t: t.x.sum()},
    )
    monkeypatch.setattr(semantic_model, "IS_XORQ_USED", True)
    with pytest.raises(RuntimeError, match="requires xorq.vendor.ibis expressions"):
        model.materialize()


def test_materialize_vendor_simple(simple_vendor_model):
    m2 = simple_vendor_model.materialize()
    assert isinstance(m2, SemanticModel)
    mod = m2.table.__class__.__module__
    assert mod.startswith("xorq.vendor.ibis")
    assert "col_test" in m2.dimensions
    assert "sum_val" in m2.measures


def test_additive_only_materialization():
    df = pd.DataFrame({"key": ["a", "a", "b", "b", "a"], "val": [1, 2, 3, 4, 5]})
    con = xo.connect()
    tbl = con.create_table("t_add", df)
    model = SemanticModel(
        table=tbl,
        dimensions={"key": lambda t: t.key},
        measures={"sum_val": lambda t: t.val.sum(), "cnt": lambda t: t.val.count()},
    )
    m2 = model.materialize()
    # Schema should contain key, sum_val, cnt
    names = set(m2.table.schema().names)
    assert names == {"key", "sum_val", "cnt"}
    result = m2.table.execute().sort_values("key").reset_index(drop=True)
    expected = pd.DataFrame({"key": ["a", "b"], "sum_val": [8, 7], "cnt": [3, 2]})
    pd.testing.assert_frame_equal(result, expected)


def test_non_additive_only_materialization():
    df = pd.DataFrame({"key": ["a", "b", "a"], "val": [1, 2, 3]})
    con = xo.connect()
    tbl = con.create_table("t_non", df)
    model = SemanticModel(
        table=tbl,
        dimensions={"key": lambda t: t.key},
        measures={"avg_val": lambda t: t.val.mean()},
    )
    m2 = model.materialize()
    names = set(m2.table.schema().names)
    assert names == set(df.columns)
    result = (
        m2.query(dimensions=["key"], measures=["avg_val"])
        .execute()
        .sort_values("key")
        .reset_index(drop=True)
    )
    expected = pd.DataFrame({"key": ["a", "b"], "avg_val": [2.0, 2.0]})
    pd.testing.assert_frame_equal(result, expected)


def test_dimensions_override_materialization():
    df = pd.DataFrame({"k1": ["x", "y", "x"], "k2": ["u", "u", "v"], "val": [1, 2, 3]})
    con = xo.connect()
    tbl = con.create_table("t_dimensions", df)
    model = SemanticModel(
        table=tbl,
        dimensions={"k1": lambda t: t.k1, "k2": lambda t: t.k2},
        measures={"sum_val": lambda t: t.val.sum()},
    )
    m2 = model.materialize(dimensions=["k2"])
    names = set(m2.table.schema().names)
    assert names == {"k2", "sum_val"}
    result = m2.table.execute().sort_values("k2").reset_index(drop=True)
    expected = pd.DataFrame({"k2": ["u", "v"], "sum_val": [3, 3]})
    pd.testing.assert_frame_equal(result, expected)


def test_cutoff_with_time_dimension():
    dates = pd.date_range("2025-01-01", "2025-01-05", freq="D")
    df = pd.DataFrame({"date": dates, "val": [1, 2, 3, 4, 5]})
    con = xo.connect()
    tbl = con.create_table("t_time", df)
    model = SemanticModel(
        table=tbl,
        dimensions={"date": lambda t: t.date},
        measures={"sum_val": lambda t: t.val.sum()},
        time_dimension="date",
    )
    m2 = model.materialize(cutoff="2025-01-03")
    result = m2.table.execute().sort_values("date").reset_index(drop=True)
    expected = pd.DataFrame({"date": dates[:3], "sum_val": [1, 2, 3]})
    # After using truncate, we get timestamps truncated to day, not date objects
    expected["date"] = pd.to_datetime(expected["date"]).dt.floor("D")
    pd.testing.assert_frame_equal(result, expected)
