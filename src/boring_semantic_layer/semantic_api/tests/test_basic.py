import ibis
from ibis.expr.sql import convert  # lowering is registered in semantic_api.lower
from boring_semantic_layer.semantic_api.api import (
    select_,
    to_semantic_table,
    where_,
    with_dimensions,
    with_measures,
)


def test_semantic_select_and_filter_basic():
    data = {"origin": ["A", "B", "A"], "value": [1, 2, 3]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    sem = with_dimensions(sem, origin=lambda t: t.origin)
    sem = with_measures(sem, total=lambda t: t.value.sum())

    q = select_(where_(sem, lambda t: t.origin == "A"), "origin", "total")

    expr = convert(q, catalog={})
    expected = (
        tbl.filter(tbl.origin == "A")
        .group_by([tbl.origin])
        .aggregate(total=tbl.value.sum())
    )
    assert repr(expr) == repr(expected)

def test_bare_table_has_no_semantic_methods():
    tbl = ibis.memtable({"x": [1, 2, 3]}, name="tbl")
    # Only semantic-DSL methods (beyond Ibis's core API) should be missing
    for method in (
        "with_dimensions",
        "with_measures",
        "join_one",
        "join_many",
        "join_cross",
    ):
        assert not hasattr(tbl, method), f"Bare table unexpectedly has {method}"

def test_to_semantic_table_binds_methods():
    tbl = ibis.memtable({"x": [1, 2, 3]}, name="tbl")
    sem = to_semantic_table(tbl)
    for method in (
        "with_dimensions",
        "with_measures",
        "group_by",
        "aggregate",
        "mutate",
    ):
        assert hasattr(sem, method), f"Semantic table missing {method}"
    # Methods should be callable and return a TableExpr
    sem2 = sem.with_dimensions(x=lambda t: t.x)
    assert isinstance(sem2, ibis.expr.types.Table)