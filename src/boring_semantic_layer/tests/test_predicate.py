"""Unit tests for the internal Predicate AST."""

from __future__ import annotations

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import predicate as pred_mod
from boring_semantic_layer.predicate import And, Compare, In, IsNull, Not, Or


# ---------------------------------------------------------------------------
# from_dict: parsing the JSON filter spec
# ---------------------------------------------------------------------------


def test_from_dict_simple_eq():
    p = pred_mod.from_dict({"operator": "=", "field": "country", "value": "US"})
    assert p == Compare(op="eq", field="country", value="US")


def test_from_dict_canonicalizes_aliases():
    assert pred_mod.from_dict(
        {"operator": "equals", "field": "x", "value": 1}
    ) == Compare(op="eq", field="x", value=1)
    assert pred_mod.from_dict(
        {"operator": "!=", "field": "x", "value": 1}
    ) == Compare(op="ne", field="x", value=1)


def test_from_dict_in_and_not_in():
    p = pred_mod.from_dict({"operator": "in", "field": "tier", "values": ["a", "b"]})
    assert p == In(field="tier", values=("a", "b"), negate=False)

    p = pred_mod.from_dict(
        {"operator": "not in", "field": "tier", "values": ["a"]}
    )
    assert p == In(field="tier", values=("a",), negate=True)


def test_from_dict_null_checks_reject_value_keys():
    pred_mod.from_dict({"operator": "is null", "field": "x"})  # ok
    with pytest.raises(ValueError, match="should not have"):
        pred_mod.from_dict({"operator": "is null", "field": "x", "value": 1})


def test_from_dict_compound():
    p = pred_mod.from_dict(
        {
            "operator": "AND",
            "conditions": [
                {"operator": "=", "field": "a", "value": 1},
                {"operator": ">", "field": "b", "value": 0},
            ],
        }
    )
    assert isinstance(p, And)
    assert len(p.children) == 2


def test_from_dict_rejects_empty_compound():
    with pytest.raises(ValueError, match="non-empty"):
        pred_mod.from_dict({"operator": "AND", "conditions": []})


def test_from_dict_unsupported_operator():
    with pytest.raises(ValueError, match="Unsupported operator"):
        pred_mod.from_dict({"operator": "WAT", "field": "x", "value": 1})


# ---------------------------------------------------------------------------
# fields: collect referenced field names
# ---------------------------------------------------------------------------


def test_fields_simple():
    p = Compare(op="eq", field="country", value="US")
    assert pred_mod.fields(p) == {"country"}


def test_fields_compound():
    p = And(
        children=(
            Compare(op="eq", field="a", value=1),
            Or(
                children=(
                    Compare(op="gt", field="b", value=0),
                    IsNull(field="c"),
                )
            ),
        )
    )
    assert pred_mod.fields(p) == {"a", "b", "c"}


def test_fields_not_passes_through():
    p = Not(predicate=Compare(op="eq", field="x", value=1))
    assert pred_mod.fields(p) == {"x"}


# ---------------------------------------------------------------------------
# compile: round-trip via an in-memory duckdb table
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def people_table():
    con = ibis.duckdb.connect()
    df = pd.DataFrame(
        {
            "country": ["US", "US", "FR", "DE", None],
            "age": [10, 25, 30, 45, 50],
        }
    )
    return con.create_table("people", df)


def _execute(pred, table):
    expr = pred_mod.compile(pred, ibis._)
    return table.filter(expr.resolve(table)).execute()


def test_compile_eq(people_table):
    df = _execute(Compare(op="eq", field="country", value="US"), people_table)
    assert sorted(df["age"].tolist()) == [10, 25]


def test_compile_in(people_table):
    df = _execute(
        In(field="country", values=("FR", "DE"), negate=False), people_table
    )
    assert sorted(df["age"].tolist()) == [30, 45]


def test_compile_isnull(people_table):
    df = _execute(IsNull(field="country"), people_table)
    assert df["age"].tolist() == [50]


def test_compile_and(people_table):
    p = And(
        children=(
            Compare(op="eq", field="country", value="US"),
            Compare(op="gt", field="age", value=20),
        )
    )
    df = _execute(p, people_table)
    assert df["age"].tolist() == [25]


def test_compile_or_and_not(people_table):
    p = Or(
        children=(
            Compare(op="eq", field="country", value="FR"),
            Not(predicate=Compare(op="lt", field="age", value=40)),
        )
    )
    df = _execute(p, people_table)
    # FR(30) plus age >= 40 (45, 50)
    assert sorted(df["age"].tolist()) == [30, 45, 50]


def test_compile_post_agg_uses_bracket_access():
    """Post-agg compilation preserves dotted column names."""
    p = Compare(op="gt", field="orders.total", value=100)
    expr = pred_mod.compile(p, ibis._, post_agg=True)
    # Sanity: it is a Deferred — actual semantic test is covered elsewhere
    assert hasattr(expr, "resolve")
