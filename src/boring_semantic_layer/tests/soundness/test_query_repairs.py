"""Regression coverage for query-layer semantic soundness repairs."""

from __future__ import annotations

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.query import Filter


@pytest.fixture
def con():
    return ibis.duckdb.connect()


def test_qualified_dict_filter_targets_the_requested_join_source(con):
    """A qualified JSON filter must not fall back to a colliding left column."""
    orders_tbl = con.create_table(
        "query_soundness_orders",
        pd.DataFrame(
            {
                "order_id": [1, 2],
                "status": ["bad", "open"],
            }
        ),
    )
    items_tbl = con.create_table(
        "query_soundness_items",
        pd.DataFrame(
            {
                "item_id": [10, 11, 12],
                "order_id": [1, 1, 2],
                "status": ["ok", "ok", "bad"],
            }
        ),
    )

    orders = to_semantic_table(orders_tbl, "orders").with_dimensions(
        order_id=lambda t: t.order_id,
        status=lambda t: t.status,
    )
    items = (
        to_semantic_table(items_tbl, "items")
        .with_dimensions(
            order_id=lambda t: t.order_id,
            status=lambda t: t.status,
        )
        .with_measures(item_count=lambda t: t.item_id.count())
    )

    result = (
        orders.join_many(items, on="order_id")
        .query(
            measures=["items.item_count"],
            filters=[{"field": "items.status", "operator": "=", "value": "bad"}],
        )
        .execute()
    )

    assert result["items.item_count"].iloc[0] == 1


def test_qualified_dict_filter_targets_undeclared_raw_right_column(con):
    """A valid qualified raw field must not fall back to the left collision."""
    orders_tbl = con.create_table(
        "query_soundness_raw_orders",
        pd.DataFrame(
            {
                "order_id": [1, 2],
                "status": ["bad", "open"],
            }
        ),
    )
    items_tbl = con.create_table(
        "query_soundness_raw_items",
        pd.DataFrame(
            {
                "order_id": [1, 1, 2],
                "status": ["ok", "ok", "bad"],
                "amount": [1, 2, 30],
            }
        ),
    )
    orders = to_semantic_table(orders_tbl, "orders").with_dimensions(status=lambda t: t.status)
    # ``status`` intentionally remains an undeclared raw column on this side.
    items = to_semantic_table(items_tbl, "items").with_measures(total=lambda t: t.amount.sum())

    result = (
        orders.join_many(items, on="order_id")
        .query(
            measures=["items.total"],
            filters=[{"field": "items.status", "operator": "=", "value": "bad"}],
        )
        .execute()
    )

    assert result["items.total"].iloc[0] == 30


@pytest.mark.parametrize("overlay", ["dimensions", "measures"])
def test_qualified_dict_filter_metadata_survives_metadata_overlay(con, overlay):
    """Rebinding filtered metadata must retain JSON's exact field ownership."""
    orders_tbl = con.create_table(
        f"query_soundness_overlay_orders_{overlay}",
        pd.DataFrame(
            {
                "order_id": [1, 2],
                "status": ["bad", "open"],
            }
        ),
    )
    items_tbl = con.create_table(
        f"query_soundness_overlay_items_{overlay}",
        pd.DataFrame(
            {
                "order_id": [1, 1, 2],
                "status": ["ok", "ok", "bad"],
                "amount": [1, 2, 30],
            }
        ),
    )
    orders = to_semantic_table(orders_tbl, "orders")
    items = to_semantic_table(items_tbl, "items").with_measures(total=lambda t: t.amount.sum())
    predicate = Filter(
        filter={"field": "items.status", "operator": "=", "value": "bad"}
    ).to_callable()
    filtered = orders.join_many(items, on="order_id").filter(predicate)

    if overlay == "dimensions":
        rebound = filtered.with_dimensions(bucket=lambda t: t.status)
    else:
        rebound = filtered.with_measures(joined_total=lambda t: t.amount.sum())

    result = rebound.aggregate("items.total").execute()

    assert result["items.total"].iloc[0] == 30


def test_qualified_dict_filter_rejects_unknown_join_prefix(con):
    orders_tbl = con.create_table(
        "query_soundness_bad_prefix_orders",
        pd.DataFrame(
            {
                "order_id": [1, 2],
                "status": ["bad", "open"],
            }
        ),
    )
    items_tbl = con.create_table(
        "query_soundness_bad_prefix_items",
        pd.DataFrame(
            {
                "order_id": [1, 2],
                "status": ["ok", "bad"],
                "amount": [10, 20],
            }
        ),
    )
    orders = to_semantic_table(orders_tbl, "orders")
    items = to_semantic_table(items_tbl, "items").with_measures(total=lambda t: t.amount.sum())

    with pytest.raises(KeyError, match="Unknown semantic model prefix 'bogus'"):
        orders.join_many(items, on="order_id").query(
            measures=["items.total"],
            filters=[{"field": "bogus.status", "operator": "=", "value": "bad"}],
        ).execute()


def test_standalone_model_prefixed_dict_filter_remains_supported(con):
    """Exact qualified resolution retains the standalone prefix convenience."""
    tbl = con.create_table(
        "query_soundness_standalone",
        pd.DataFrame({"status": ["open", "closed"], "amount": [10, 20]}),
    )
    model = (
        to_semantic_table(tbl, "orders")
        .with_dimensions(status=lambda t: t.status)
        .with_measures(total=lambda t: t.amount.sum())
    )

    result = model.query(
        measures=["total"],
        filters=[{"field": "orders.status", "operator": "=", "value": "open"}],
    ).execute()

    assert result["total"].iloc[0] == 10


def test_standalone_model_prefixed_raw_dict_filter_remains_supported(con):
    """The standalone-prefix fallback also supports undeclared raw columns."""
    tbl = con.create_table(
        "query_soundness_standalone_raw",
        pd.DataFrame({"status": ["open", "closed"], "amount": [10, 20]}),
    )
    model = to_semantic_table(tbl, "orders").with_measures(total=lambda t: t.amount.sum())

    result = model.query(
        measures=["total"],
        filters=[{"field": "orders.status", "operator": "=", "value": "open"}],
    ).execute()

    assert result["total"].iloc[0] == 10


def test_compare_periods_matches_null_dimension_members(con):
    """NULL is one semantic group and must match itself across periods."""
    tbl = con.create_table(
        "query_soundness_periods",
        pd.DataFrame(
            {
                "occurred_at": pd.to_datetime(["2024-01-05", "2024-02-05", "2024-03-05"]),
                # The out-of-range anchor gives DuckDB a concrete string type.
                "category": [None, None, "anchor"],
                "amount": [10, 20, 0],
            }
        ),
    )
    model = (
        to_semantic_table(tbl, "events")
        .with_dimensions(
            occurred_at={
                "expr": lambda t: t.occurred_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
            category=lambda t: t.category,
        )
        .with_measures(total=lambda t: t.amount.sum())
    )

    result = model.compare_periods(
        dimensions=["category"],
        measures=["total"],
        current_time_range={"start": "2024-02-01", "end": "2024-02-29"},
        previous_time_range={"start": "2024-01-01", "end": "2024-01-31"},
    ).execute()

    assert len(result) == 1
    row = result.iloc[0]
    assert pd.isna(row["category"])
    assert row["total_current"] == 20
    assert row["total_previous"] == 10
    assert row["total_delta"] == 10
    assert row["total_pct_change"] == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("argument", "value", "message"),
    [
        (
            "having",
            [{"field": "total", "operator": ">", "value": 0}],
            "post-aggregation filter",
        ),
        ("order_by", [("total", "desc")], "order_by"),
    ],
)
def test_unselected_post_aggregation_fields_fail_early(con, argument, value, message):
    tbl = con.create_table(
        f"query_soundness_unselected_{argument}",
        pd.DataFrame({"group": ["a", "b"], "amount": [10, 20]}),
    )
    model = (
        to_semantic_table(tbl, "events")
        .with_dimensions(group=lambda t: t.group)
        .with_measures(
            row_count=lambda t: t.count(),
            total=lambda t: t.amount.sum(),
        )
    )

    kwargs = {
        "dimensions": ["group"],
        "measures": ["row_count"],
        argument: value,
    }
    with pytest.raises(ValueError, match=message):
        model.query(**kwargs)
