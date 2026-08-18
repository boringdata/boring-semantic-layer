"""Regression tests for expression-layer join, filter, and grain lineage."""

import warnings

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import Dimension, to_semantic_table


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def fanout_models(con):
    customers_tbl = con.create_table(
        "lineage_customers",
        pd.DataFrame(
            {
                "customer_id": [1, 2],
                "region": ["east", "west"],
            }
        ),
    )
    orders_tbl = con.create_table(
        "lineage_orders",
        pd.DataFrame(
            {
                "order_id": [10, 11, 12],
                "customer_id": [1, 1, 2],
                "amount": [5, 7, 3],
            }
        ),
    )

    customers = (
        to_semantic_table(customers_tbl, name="customers")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            region=lambda t: t.region,
        )
        .with_measures(customer_count=lambda t: t.count())
    )
    orders = (
        to_semantic_table(orders_tbl, name="orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            order_id=lambda t: t.order_id,
            amount=lambda t: t.amount,
        )
        .with_measures(order_total=lambda t: t.amount.sum())
    )
    return customers.join_many(orders, on="customer_id")


def _filtered_fanout_result(model):
    return (
        model.group_by("customers.customer_id")
        .aggregate("customers.customer_count", "orders.order_total")
        .execute()
        .sort_values("customers.customer_id")
        .reset_index(drop=True)
    )


@pytest.mark.parametrize("metadata_method", ["with_dimensions", "mutate"])
def test_filter_then_dimension_metadata_keeps_filter_and_fanout_protection(
    fanout_models, metadata_method
):
    filtered = fanout_models.filter(lambda t: t["orders.amount"] > 5)
    transformed = getattr(filtered, metadata_method)(region_label=lambda t: t.region.upper())

    result = _filtered_fanout_result(transformed)

    assert result["customers.customer_id"].tolist() == [1]
    assert result["customers.customer_count"].tolist() == [1]
    assert result["orders.order_total"].tolist() == [7]


def test_filter_then_with_measures_keeps_filter_and_fanout_protection(fanout_models):
    transformed = fanout_models.filter(lambda t: t["orders.amount"] > 5).with_measures(
        filtered_row_count=lambda t: t.count()
    )

    result = (
        transformed.group_by("customers.customer_id")
        .aggregate(
            "customers.customer_count",
            "orders.order_total",
            "filtered_row_count",
        )
        .execute()
    )

    assert result["customers.customer_id"].tolist() == [1]
    assert result["customers.customer_count"].tolist() == [1]
    assert result["orders.order_total"].tolist() == [7]
    assert result["filtered_row_count"].tolist() == [1]


def test_filter_metadata_update_retains_query_surface(fanout_models):
    transformed = fanout_models.filter(lambda t: t["orders.amount"] > 5).with_dimensions(
        region_label=lambda t: t.region.upper()
    )

    result = transformed.query(
        dimensions=["customers.customer_id"],
        measures=["customers.customer_count", "orders.order_total"],
    ).execute()

    assert result["customers.customer_id"].tolist() == [1]
    assert result["customers.customer_count"].tolist() == [1]
    assert result["orders.order_total"].tolist() == [7]


def test_multiple_filters_survive_metadata_update_on_join(fanout_models):
    transformed = (
        fanout_models.filter(lambda t: t["orders.amount"] >= 5)
        .filter(lambda t: t["orders.amount"] < 7)
        .with_dimensions(region_label=lambda t: t.region.upper())
    )

    result = _filtered_fanout_result(transformed)

    assert result["customers.customer_id"].tolist() == [1]
    assert result["customers.customer_count"].tolist() == [1]
    assert result["orders.order_total"].tolist() == [5]


def _grain_models(con):
    monthly_tbl = con.create_table(
        "lineage_monthly",
        pd.DataFrame({"year": [2024], "month": [1], "revenue": [100]}),
    )
    daily_tbl = con.create_table(
        "lineage_daily",
        pd.DataFrame(
            {
                "year": [2024, 2024],
                "month": [1, 1],
                "day": [1, 2],
                "hours": [8, 7],
            }
        ),
    )
    calendar_tbl = con.create_table(
        "lineage_calendar",
        pd.DataFrame({"year": [2024], "month": [1], "label": ["Jan"]}),
    )

    monthly = (
        to_semantic_table(monthly_tbl, name="monthly")
        .with_dimensions(
            year=Dimension(expr=lambda t: t.year, is_entity=True),
            month=Dimension(expr=lambda t: t.month, is_entity=True),
        )
        .with_measures(revenue=lambda t: t.revenue.sum())
    )
    daily = (
        to_semantic_table(daily_tbl, name="daily")
        .with_dimensions(
            year=Dimension(expr=lambda t: t.year, is_entity=True),
            month=Dimension(expr=lambda t: t.month, is_entity=True),
            day=Dimension(expr=lambda t: t.day, is_entity=True),
        )
        .with_measures(hours=lambda t: t.hours.sum())
    )
    calendar = to_semantic_table(calendar_tbl, name="calendar").with_dimensions(
        year=Dimension(expr=lambda t: t.year, is_entity=True),
        month=Dimension(expr=lambda t: t.month, is_entity=True),
        label=lambda t: t.label,
    )
    return monthly, daily, calendar


def test_filter_join_one_reuses_automatic_grain_detection(con):
    monthly, daily, _ = _grain_models(con)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        joined = monthly.filter(lambda t: t.year == 2024).join_one(
            daily,
            on=lambda m, d: (m.year == d.year) & (m.month == d.month),
        )

    assert joined.op().cardinality == "many"
    assert any("Grain mismatch" in str(item.message) for item in caught)


def test_join_wrapper_join_one_reuses_automatic_grain_detection(con):
    monthly, daily, calendar = _grain_models(con)
    enriched_monthly = monthly.join_one(
        calendar,
        on=lambda m, c: (m.year == c.year) & (m.month == c.month),
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        joined = enriched_monthly.join_one(
            daily,
            on=lambda m, d: (m.year == d.year) & (m.month == d.month),
        )

    assert joined.op().cardinality == "many"
    assert any("Grain mismatch" in str(item.message) for item in caught)


def test_join_wrapper_local_entity_dimension_participates_in_grain_detection(con):
    orders_tbl = con.create_table(
        "lineage_wrapper_orders",
        pd.DataFrame({"order_id": [1, 2], "account_id": [10, 10], "amount": [5, 7]}),
    )
    accounts_tbl = con.create_table(
        "lineage_wrapper_accounts",
        pd.DataFrame({"account_id": [10], "label": ["A"]}),
    )
    lines_tbl = con.create_table(
        "lineage_wrapper_lines",
        pd.DataFrame({"line_id": [100, 101], "account_id": [10, 10], "value": [2, 3]}),
    )
    orders = to_semantic_table(orders_tbl, "orders").with_measures(total=lambda t: t.amount.sum())
    accounts = to_semantic_table(accounts_tbl, "accounts")
    lines = (
        to_semantic_table(lines_tbl, "lines")
        .with_dimensions(entity_id=Dimension(expr=lambda t: t.line_id, is_entity=True))
        .with_measures(total=lambda t: t.value.sum())
    )
    enriched_orders = orders.join_one(accounts, on="account_id").with_dimensions(
        entity_id=Dimension(expr=lambda t: t.order_id, is_entity=True)
    )

    with pytest.warns(UserWarning, match="Grain mismatch"):
        joined = enriched_orders.join_one(lines, on="account_id")

    assert joined.op().cardinality == "many"


def test_aggregate_does_not_inherit_stale_source_entity_grain(con):
    from boring_semantic_layer.expr import (
        _get_entity_dims,
        _get_entity_source_columns,
    )

    daily_tbl = con.create_table(
        "lineage_daily_aggregate",
        pd.DataFrame(
            {
                "year": [2024, 2024],
                "month": [1, 1],
                "day": [1, 2],
                "hours": [8, 7],
            }
        ),
    )
    month_lookup_tbl = con.create_table(
        "lineage_month_lookup",
        pd.DataFrame({"year": [2024], "month": [1], "target": [20]}),
    )
    daily = (
        to_semantic_table(daily_tbl, "daily")
        .with_dimensions(
            year=Dimension(expr=lambda t: t.year, is_entity=True),
            month=Dimension(expr=lambda t: t.month, is_entity=True),
            day=Dimension(expr=lambda t: t.day, is_entity=True),
        )
        .with_measures(hours=lambda t: t.hours.sum())
    )
    monthly_result = daily.group_by("year", "month").aggregate("hours")
    month_lookup = (
        to_semantic_table(month_lookup_tbl, "month_lookup")
        .with_dimensions(
            year=Dimension(expr=lambda t: t.year, is_entity=True),
            month=Dimension(expr=lambda t: t.month, is_entity=True),
        )
        .with_measures(target=lambda t: t.target.sum())
    )

    assert _get_entity_dims(monthly_result.op()) == frozenset()
    assert _get_entity_source_columns(monthly_result.op()) == frozenset()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        joined = monthly_result.join_one(month_lookup, on=["year", "month"])

    assert joined.op().cardinality == "one"
    assert not any("Grain mismatch" in str(item.message) for item in caught)
