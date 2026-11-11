"""Measures that read derived dimensions (#310)."""

import ibis
import pytest
from ibis import _

from boring_semantic_layer import Dimension
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


def _sales_and_categories(entity=False, colliding_dim=False):
    sales = (
        to_semantic_table(
            ibis.memtable({"id": [1, 2], "quantity": [10, 20], "price": [1.5, 2.0]}),
            name="sales",
        )
        .with_dimensions(revenue=lambda t: t.quantity * t.price)
        .with_measures(total_revenue=lambda t: t.revenue.sum())
    )
    dims = {"id": Dimension(expr=lambda t: t.id, is_entity=entity), "label": lambda t: t.label}
    if colliding_dim:
        dims["revenue"] = lambda t: t.id * 100
    categories = to_semantic_table(
        ibis.memtable({"id": [1, 2], "label": ["A", "B"]}), name="categories"
    ).with_dimensions(**dims)
    return sales.join_one(categories, on="id")


@pytest.mark.parametrize(
    ("entity", "colliding_dim"),
    [(True, False), (False, True)],
    ids=["deferred-join", "other-model-has-same-dim-name"],
)
def test_measure_over_derived_dim_join_variants(entity, colliding_dim):
    joined = _sales_and_categories(entity=entity, colliding_dim=colliding_dim)
    result = _records(
        joined.group_by("categories.id").aggregate("sales.total_revenue"), "categories.id"
    )
    assert [r["sales.total_revenue"] for r in result] == [15.0, 40.0]


def test_calc_measure_with_inline_reduction_over_derived_dim(sales):
    model = sales.with_measures(pct=lambda t: t.revenue.sum() / t.all(t.revenue.sum()))
    result = _records(model.group_by("cat").aggregate("pct"), "cat")
    assert [round(r["pct"], 4) for r in result] == [round(90 / 130, 4), round(40 / 130, 4)]


def test_measure_named_like_dimension_still_resolves_as_measure():
    model = (
        to_semantic_table(ibis.memtable({"q": [2, 3]}), name="s")
        .with_dimensions(revenue=lambda t: t.q * 10)
        .with_measures(revenue=lambda t: t.q.sum(), twice=lambda t: t.revenue * 2)
    )
    assert "twice" in model.get_calculated_measures()
    assert model.aggregate("twice").execute()["twice"].tolist() == [10]


@pytest.mark.timeout(30)
def test_unresolvable_dotted_dimension_name_raises_instead_of_looping():
    model = (
        to_semantic_table(ibis.memtable({"q": [2, 3]}), name="s")
        .with_measures(total=lambda t: t.revenue.sum())
        .with_dimensions(**{"outer.inner.revenue": lambda t: t.q * 10})
    )
    with pytest.raises(AttributeError, match="revenue"):
        model.aggregate("total").execute()


def test_dimension_depending_on_measure_named_dim_does_not_hide_measure():
    model = (
        to_semantic_table(ibis.memtable({"q": [2, 3]}), name="s")
        .with_dimensions(revenue=lambda t: t.q * 10, extra=lambda t: t.revenue + 1)
        .with_measures(revenue=lambda t: t.q.sum(), twice=lambda t: t.revenue * 2)
    )
    assert "twice" in model.get_calculated_measures()
    assert model.aggregate("twice").execute()["twice"].tolist() == [10]


@pytest.mark.parametrize("measure", ["sales.total_revenue", "total_revenue", "sales.big_total"])
def test_owning_model_wins_over_same_named_dim_on_other_model(measure):
    sales = (
        to_semantic_table(
            ibis.memtable({"id": [1, 2], "quantity": [10, 20], "price": [1.5, 2.0]}),
            name="sales",
        )
        .with_dimensions(revenue=lambda t: t.quantity * t.price, big=lambda t: t.revenue * 10)
        .with_measures(total_revenue=lambda t: t.revenue.sum(), big_total=lambda t: t.big.sum())
    )
    categories = to_semantic_table(
        ibis.memtable({"id": [1, 2], "label": ["A", "B"]}), name="categories"
    ).with_dimensions(label=lambda t: t.label, revenue=lambda t: t.id * 100)
    joined = sales.join_one(categories, on="id")
    result = _records(joined.group_by("categories.label").aggregate(measure), "categories.label")
    factor = 10 if "big" in measure else 1
    assert [r[measure] for r in result] == [15.0 * factor, 40.0 * factor]
