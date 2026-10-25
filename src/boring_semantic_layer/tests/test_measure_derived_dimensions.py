"""Measures that read derived dimensions (#310)."""

import ibis
import pytest
from ibis import _

from boring_semantic_layer.api import to_semantic_table


@pytest.fixture
def sales():
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
            "category": ["A", "B", "A"],
        }
    )
    return (
        to_semantic_table(tbl, name="sales")
        .with_dimensions(
            revenue=lambda t: t.quantity * t.price,
            big_revenue=lambda t: t.revenue * 100,
            cat=lambda t: t.category,
        )
        .with_measures(
            total_revenue=lambda t: t.revenue.sum(),
            total_big_revenue=lambda t: t.big_revenue.sum(),
            revenue_share=lambda t: t.total_revenue / t.all(t.total_revenue),
        )
    )


def _records(expr, sort_by):
    return expr.execute().sort_values(sort_by).to_dict("records")


def test_measure_over_derived_dim_grouped_by_other_dim(sales):
    result = _records(sales.group_by("cat").aggregate("total_revenue"), "cat")
    assert result == [
        {"cat": "A", "total_revenue": 90.0},
        {"cat": "B", "total_revenue": 40.0},
    ]


def test_measure_over_derived_dim_ungrouped(sales):
    result = sales.aggregate("total_revenue").execute()
    assert result["total_revenue"].tolist() == [130.0]


def test_measure_over_chained_derived_dims(sales):
    result = _records(sales.group_by("cat").aggregate("total_big_revenue"), "cat")
    assert [r["total_big_revenue"] for r in result] == [9000.0, 4000.0]


def test_calc_measure_over_derived_dim_measure(sales):
    result = _records(sales.group_by("cat").aggregate("revenue_share"), "cat")
    assert [round(r["revenue_share"], 4) for r in result] == [
        round(90 / 130, 4),
        round(40 / 130, 4),
    ]


def test_measure_over_derived_dim_with_filter(sales):
    expr = sales.filter(lambda t: t.cat == "A").aggregate("total_revenue")
    assert expr.execute()["total_revenue"].tolist() == [90.0]


def test_query_api_measure_over_derived_dim(sales):
    result = sales.query(dimensions=["cat"], measures=["total_revenue"]).execute()
    assert sorted(result["total_revenue"].tolist()) == [40.0, 90.0]


def test_derived_dim_name_close_to_column_is_not_a_typo():
    tbl = ibis.memtable({"url": ["https://example.com", None, "https://b.com"]})
    model = (
        to_semantic_table(tbl, name="stories")
        .with_dimensions(url_s=_.url.notnull())
        .with_measures(link_count=_.url_s.sum())
    )
    assert model.aggregate("link_count").execute()["link_count"].tolist() == [2]


def test_measure_over_derived_dim_on_joined_model(sales):
    categories = to_semantic_table(
        ibis.memtable({"category": ["A", "B"], "label": ["Alpha", "Beta"]}),
        name="categories",
    ).with_dimensions(label=lambda t: t.label)
    joined = sales.join_one(categories, on="category")
    result = _records(
        joined.group_by("categories.label").aggregate("sales.total_revenue"),
        "categories.label",
    )
    assert [r["sales.total_revenue"] for r in result] == [90.0, 40.0]


def test_measure_over_derived_dim_on_join_many(sales):
    categories = to_semantic_table(
        ibis.memtable({"category": ["A", "B"], "label": ["Alpha", "Beta"]}),
        name="categories",
    ).with_dimensions(label=lambda t: t.label)
    joined = categories.join_many(sales, on="category")
    result = _records(
        joined.group_by("categories.label").aggregate("sales.total_revenue"),
        "categories.label",
    )
    assert [r["sales.total_revenue"] for r in result] == [90.0, 40.0]
