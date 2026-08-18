"""Compiled-SQL snapshots over a fixed query corpus.

The phase-3 refactor restructures the compiler without changing what it
emits. Each test compiles one representative query shape to SQL and
compares it against a golden file in ``snapshots/``. A diff here means the
refactor changed compilation output — investigate before regenerating.

To regenerate after an *intentional* change:
    BSL_UPDATE_SNAPSHOTS=1 python3 -m pytest tests/test_sql_snapshots.py
"""

from __future__ import annotations

import os
from pathlib import Path

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"
UPDATE = os.environ.get("BSL_UPDATE_SNAPSHOTS") == "1"


@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def orders(con):
    return con.create_table(
        "orders",
        pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "customer_id": [10, 20, 10, 30],
                "region": ["N", "S", "N", "W"],
                "amount": [100.0, 250.0, 75.0, 500.0],
                "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-02-01", "2024-02-15"]),
            }
        ),
    )


@pytest.fixture(scope="module")
def customers(con):
    return con.create_table(
        "customers",
        pd.DataFrame(
            {
                "customer_id": [10, 20, 30],
                "country": ["US", "UK", "US"],
                "segment": ["a", "b", "a"],
            }
        ),
    )


@pytest.fixture(scope="module")
def items(con):
    return con.create_table(
        "items",
        pd.DataFrame(
            {
                "item_id": [1, 2, 3, 4, 5],
                "order_id": [1, 1, 2, 3, 4],
                "qty": [1, 2, 3, 1, 5],
            }
        ),
    )


def _orders_model(orders):
    return (
        to_semantic_table(orders, name="orders")
        .with_dimensions(
            region=lambda t: t.region,
            customer_id=lambda t: t.customer_id,
            month=lambda t: t.ts.truncate("M"),
        )
        .with_measures(
            order_count=lambda t: t.count(),
            revenue=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
        )
    )


def _customers_model(customers):
    return (
        to_semantic_table(customers, name="customers")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            country=lambda t: t.country,
        )
        .with_measures(customer_count=lambda t: t.count())
    )


def _items_model(items):
    return (
        to_semantic_table(items, name="items")
        .with_dimensions(order_id=lambda t: t.order_id)
        .with_measures(total_qty=lambda t: t.qty.sum())
    )


def _check(name: str, expr) -> None:
    sql = str(expr.sql())
    path = SNAPSHOT_DIR / f"{name}.sql"
    if UPDATE or not path.exists():
        SNAPSHOT_DIR.mkdir(exist_ok=True)
        path.write_text(sql + "\n")
        if not UPDATE:
            pytest.skip(f"snapshot created: {path.name}")
        return
    expected = path.read_text().rstrip("\n")
    assert sql == expected, (
        f"Compiled SQL for {name!r} changed. If intentional, regenerate with "
        f"BSL_UPDATE_SNAPSHOTS=1; otherwise the refactor changed behavior.\n"
    )


def test_flat_aggregate(orders):
    m = _orders_model(orders)
    _check("flat_aggregate", m.group_by("region").aggregate("order_count", "revenue"))


def test_flat_filter_and_order(orders):
    m = _orders_model(orders)
    q = (
        m.filter(lambda t: t.amount > 80)
        .group_by("region")
        .aggregate("revenue")
        .order_by("revenue")
        .limit(5)
    )
    _check("flat_filter_order_limit", q)


def test_derived_time_dimension(orders):
    m = _orders_model(orders)
    _check("derived_time_dimension", m.group_by("month").aggregate("order_count"))


def test_join_one_aggregate(orders, customers):
    o = _orders_model(orders)
    c = _customers_model(customers)
    j = o.join_one(c, lambda o_, c_: o_.customer_id == c_.customer_id)
    _check(
        "join_one_aggregate",
        j.group_by("customers.country").aggregate("orders.revenue"),
    )


def test_join_many_fanout_preagg(orders, items):
    o = _orders_model(orders)
    i = _items_model(items)
    j = o.join_many(i, lambda o_, i_: o_.order_id == i_.order_id)
    _check(
        "join_many_fanout_preagg",
        j.group_by("orders.region").aggregate("orders.revenue", "items.total_qty"),
    )


def test_join_many_mean_preagg(orders, items):
    o = _orders_model(orders)
    i = _items_model(items)
    j = o.join_many(i, lambda o_, i_: o_.order_id == i_.order_id)
    _check(
        "join_many_mean_preagg",
        j.group_by("orders.region").aggregate("orders.avg_amount"),
    )


def test_calc_measure_percent_of_total(orders):
    m = _orders_model(orders).with_measures(
        revenue_share=lambda t: t.revenue / t.all(t.revenue),
    )
    _check(
        "calc_percent_of_total",
        m.group_by("region").aggregate("revenue", "revenue_share"),
    )


def test_three_way_join(orders, customers, items):
    o = _orders_model(orders)
    c = _customers_model(customers)
    i = _items_model(items)
    j = o.join_one(c, lambda o_, c_: o_.customer_id == c_.customer_id).join_many(
        i, lambda o_, i_: o_.order_id == i_.order_id
    )
    _check(
        "three_way_join",
        j.group_by("customers.country").aggregate("orders.order_count", "items.total_qty"),
    )


def test_filtered_join_aggregate(orders, customers):
    o = _orders_model(orders)
    c = _customers_model(customers)
    j = o.join_one(c, lambda o_, c_: o_.customer_id == c_.customer_id)
    q = (
        j.filter(lambda t: t["customers.country"] == "US")
        .group_by("customers.segment")
        .aggregate("orders.revenue")
    )
    _check("filtered_join_aggregate", q)


def test_json_query_path(orders):
    m = _orders_model(orders)
    q = m.query(
        dimensions=["region"],
        measures=["revenue"],
        filters=[{"field": "region", "operator": "in", "values": ["N", "S"]}],
        order_by=[("revenue", "desc")],
        limit=3,
    )
    _check("json_query_path", q)
