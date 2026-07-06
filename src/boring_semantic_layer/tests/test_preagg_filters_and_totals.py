"""Regression tests: filter ownership and fan-out-safe totals in the pre-agg path.

Covers two silent-wrong-answer defects found in the July 2026 soundness
evaluation:

1. ``t.all(...)`` totals were computed by re-running agg specs on the
   fanned-out join under ``join_many``, inflating denominators (e.g.
   percent-of-total summed to 0.5 with 2 line items per order).
2. Filters written with table-prefixed names (``t["orders.status"]``)
   resolved against no raw table and were silently dropped; bare names
   shared by several tables (``t.status``) were pushed to every table,
   zeroing out unrelated measures.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def orders_items(con):
    """Orders (sum 300) with an uneven 1:N fan into line items (3/1/2)."""
    orders = con.create_table(
        "orders",
        pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [10, 10, 20],
                "status": ["open", "closed", "open"],  # order status
                "amount": [100, 120, 80],
            }
        ),
    )
    items = con.create_table(
        "items",
        pd.DataFrame(
            {
                "item_id": [1, 2, 3, 4, 5, 6],
                "order_id": [1, 1, 1, 2, 3, 3],
                "status": ["ok"] * 6,  # item QC status — same name, other meaning
                "qty": [1, 2, 1, 3, 1, 1],
            }
        ),
    )
    o_st = (
        to_semantic_table(orders, name="orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            status=lambda t: t.status,
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
        )
    )
    i_st = to_semantic_table(items, name="items").with_measures(
        item_count=lambda t: t.count(),
    )
    return o_st, i_st


def _joined(orders_items):
    o_st, i_st = orders_items
    return o_st.join_many(i_st, lambda o, i: o.order_id == i.order_id)


class TestFanoutSafeTotals:
    def test_percent_of_total_sums_to_one(self, orders_items):
        joined = _joined(orders_items).with_measures(
            pot=lambda t: t["orders.total_amount"] / t.all(t["orders.total_amount"]),
        )
        df = (
            joined.group_by("orders.customer_id")
            .aggregate("orders.total_amount", "pot")
            .order_by("orders.customer_id")
            .execute()
        )
        assert df["orders.total_amount"].tolist() == [220, 80]
        # Denominator must be 300 (orders grain), not 580 (fanned-out rows)
        assert df["pot"].tolist() == pytest.approx([220 / 300, 80 / 300])
        assert df["pot"].sum() == pytest.approx(1.0)

    def test_all_of_mean_uses_source_grain(self, orders_items):
        joined = _joined(orders_items).with_measures(
            mean_ratio=lambda t: t["orders.avg_amount"] / t.all(t["orders.avg_amount"]),
        )
        df = (
            joined.group_by("orders.customer_id")
            .aggregate("orders.avg_amount", "mean_ratio")
            .order_by("orders.customer_id")
            .execute()
        )
        assert df["orders.avg_amount"].tolist() == pytest.approx([110.0, 80.0])
        # Grand mean over orders is 100; the fanned-out join would give 580/6
        assert df["mean_ratio"].tolist() == pytest.approx([110 / 100, 80 / 100])

    def test_totals_respect_filters(self, orders_items):
        joined = _joined(orders_items).with_measures(
            pot=lambda t: t["orders.total_amount"] / t.all(t["orders.total_amount"]),
        )
        df = (
            joined.filter(lambda t: t["orders.status"] == "open")
            .group_by("orders.customer_id")
            .aggregate("orders.total_amount", "pot")
            .order_by("orders.customer_id")
            .execute()
        )
        # Open orders: #1 (cust 10, 100) and #3 (cust 20, 80) → total 180
        assert df["orders.total_amount"].tolist() == [100, 80]
        assert df["pot"].tolist() == pytest.approx([100 / 180, 80 / 180])
        assert df["pot"].sum() == pytest.approx(1.0)


class TestPreaggFilterOwnership:
    def test_prefixed_filter_is_applied(self, orders_items):
        df = (
            _joined(orders_items)
            .filter(lambda t: t["orders.status"] == "open")
            .aggregate("items.item_count", "orders.total_amount")
            .execute()
        )
        assert df["orders.total_amount"].iloc[0] == 180
        # Items of orders 1 and 3 → 3 + 2
        assert df["items.item_count"].iloc[0] == 5

    def test_prefixed_filter_with_group_by(self, orders_items):
        df = (
            _joined(orders_items)
            .filter(lambda t: t["orders.status"] == "open")
            .group_by("orders.customer_id")
            .aggregate("items.item_count", "orders.total_amount")
            .order_by("orders.customer_id")
            .execute()
        )
        assert df["orders.total_amount"].tolist() == [100, 80]
        assert df["items.item_count"].tolist() == [3, 2]

    def test_bare_ambiguous_filter_follows_join_semantics(self, orders_items):
        # `status` exists on both tables. Bare access must follow the joined
        # table's semantics (left table wins → orders.status), not get pushed
        # into every table that happens to have the column.
        df = (
            _joined(orders_items)
            .filter(lambda t: t.status == "open")
            .aggregate("items.item_count", "orders.total_amount")
            .execute()
        )
        assert df["orders.total_amount"].iloc[0] == 180
        assert df["items.item_count"].iloc[0] == 5

    def test_unowned_filter_pushes_to_owner_only(self, orders_items):
        # `qty` exists only on items — must not disturb orders' measures
        df = (
            _joined(orders_items)
            .filter(lambda t: t.qty >= 2)
            .aggregate("items.item_count")
            .execute()
        )
        assert df["items.item_count"].iloc[0] == 2

    def test_unresolvable_filter_raises(self, orders_items):
        expr = (
            _joined(orders_items)
            .filter(lambda t: t["orders.no_such_field"] == 1)
            .aggregate("items.item_count")
        )
        with pytest.raises(ValueError, match="does not resolve"):
            expr.execute()


class TestPrefixedFilterJoinOnePath:
    def test_prefixed_filter_on_join_one_dimension_table(self, con):
        orders = con.create_table(
            "orders2",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [10, 10, 20],
                    "amount": [100, 120, 80],
                }
            ),
        )
        customers = con.create_table(
            "customers2",
            pd.DataFrame({"customer_id": [10, 20], "region": ["west", "east"]}),
        )
        o_st = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total_amount=lambda t: t.amount.sum())
        )
        c_st = to_semantic_table(customers, name="customers").with_dimensions(
            customer_id=lambda t: t.customer_id,
            region=lambda t: t.region,
        )
        df = (
            o_st.join_one(c_st, lambda o, c: o.customer_id == c.customer_id)
            .filter(lambda t: t["customers.region"] == "west")
            .group_by("customers.region")
            .aggregate("orders.total_amount")
            .execute()
        )
        assert len(df) == 1
        assert df["orders.total_amount"].iloc[0] == 220
