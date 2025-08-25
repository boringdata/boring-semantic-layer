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