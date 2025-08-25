import pytest

import ibis
from ibis.expr.sql import convert
from boring_semantic_layer.semantic_api.api import (
    join_,
    join_cross,
    join_many,
    join_one,
    to_semantic_table,
    with_dimensions,
    group_by_,
    aggregate_,
    mutate_,
)


@pytest.mark.parametrize(
    "how,left_on,right_on,expected_how",
    [
        ("inner", "a", "b", "inner"),
        ("left", "a", "b", "left"),
        ("cross", None, None, "cross"),
    ],
)
def test_semantic_join_variants(how, left_on, right_on, expected_how):
    data_l = {"a": [1, 2, 3]}
    data_r = {"b": [1, 2, 3]}
    tbl_l = ibis.memtable(data_l, name="l")
    tbl_r = ibis.memtable(data_r, name="r")

    sem_l = with_dimensions(to_semantic_table(tbl_l), a=lambda t: t.a)
    sem_r = with_dimensions(to_semantic_table(tbl_r), b=lambda t: t.b)

    if how == "inner":
        joined = join_one(sem_l, sem_r, left_on, right_on)
    elif how == "left":
        joined = join_many(sem_l, sem_r, left_on, right_on)
    elif how == "cross":
        joined = join_cross(sem_l, sem_r)
    else:
        joined = join_(sem_l, sem_r, how=how, on=(lambda l, r: l.a == r.b))

    expr = convert(joined, catalog={})
    if expected_how == "cross":
        expected = tbl_l.join(tbl_r, how="cross")
    else:
        expected = tbl_l.join(tbl_r, tbl_l.a == tbl_r.b, how=expected_how)
    assert repr(expr) == repr(expected)


# Test window passthrough: aggregate+mutate with window over specification
def test_window_passthrough():
    data = {"x": [1, 2, 3, 4, 5]}
    tbl = ibis.memtable(data, name="t")
    sem = with_dimensions(to_semantic_table(tbl), x=lambda t: t.x)

    # one following row frame (0 preceding, 1 following) for window passthrough test
    # Adapt to new ibis.window signature (order_by, rows=(preceding, following))
    w = ibis.window(order_by="x", rows=(0, 1))
    # Use semantic functions for group_by, aggregate, and mutate
    gb = group_by_(sem, "x")
    agg = aggregate_(gb, sum_x=lambda t: t.x.sum())
    q = mutate_(agg, rolling=lambda t: t.sum_x.sum().over(w))
    expr = convert(q, catalog={})
    # Build expected ibis expression with new window signature over the aggregated sum_x
    expected_agg = tbl.group_by([tbl.x]).aggregate(sum_x=tbl.x.sum())
    expected = expected_agg.mutate(rolling=expected_agg.sum_x.sum().over(w))
    assert repr(expr) == repr(expected)
