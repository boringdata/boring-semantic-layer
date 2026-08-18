"""Regression tests for the July 2026 round-5 soundness findings.

Each test pins a defect found while building the World Cup example
(examples/worldcup.py) against ground truth:

- R5-1  inline ``t.count()`` inside a calc measure raising
        TotalsNotAvailableError: CountStar holds its relation as a direct
        argument (no Field child), so the inline-reduction lift never
        classified it as a base reduction and no totals column was built.
- R5-2  declared right-table dimensions over a colliding column silently
        reading the LEFT table's column after a join whenever the first
        root declared no dimensions (the ``is_dimensions`` sample gate in
        ``_merge_fields_with_prefixing`` only looked at ``all_roots[0]``).
- R5-3  raw ``<table>.<column>`` group keys unusable on joined models:
        undeclared columns compiled to a physical lookup on the flattened
        join and failed with an error leaking the internal schema
        (``name_right``, ``tournament_id_right2``, …).
"""

import ibis
import pandas as pd
import pytest
from ibis import _

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def orders_customers(con):
    """Fact table with a column colliding with the dimension table.

    ``orders.name`` and ``customers.name`` share a name but carry
    different values so a wrong-side resolution is detectable.
    """
    orders_tbl = con.create_table(
        "orders",
        pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "cust_id": [10, 10, 20, 30],
                "amount": [5.0, 7.0, 11.0, 13.0],
                "name": ["o1", "o2", "o3", "o4"],
            }
        ),
    )
    cust_tbl = con.create_table(
        "customers",
        pd.DataFrame(
            {
                "cust_id": [10, 20, 30],
                "region": ["east", "west", "east"],
                "name": ["alice", "bob", "cara"],
            }
        ),
    )
    return orders_tbl, cust_tbl


# ---------------------------------------------------------------------------
# R5-1: inline t.count() in percent-of-total calc measures
# ---------------------------------------------------------------------------


class TestInlineCountStarTotals:
    @pytest.fixture
    def flights(self, con):
        tbl = con.create_table(
            "flights",
            pd.DataFrame(
                {
                    "origin": ["A", "A", "A", "B", "B", "C"],
                    "distance": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                }
            ),
        )
        return to_semantic_table(tbl, name="flights")

    def test_inline_count_star_percent_of_total(self, flights):
        st = flights.with_measures(
            flight_count=_.count(),
            pct=lambda t: t.count() / t.all(t.count()) * 100,
        )
        df = (
            st.group_by("origin")
            .aggregate("flight_count", "pct")
            .execute()
            .set_index("origin")
            .sort_index()
        )
        assert df.loc["A", "pct"] == pytest.approx(50.0)
        assert df.loc["B", "pct"] == pytest.approx(100 / 3)
        assert df.loc["C", "pct"] == pytest.approx(100 / 6)
        assert df["pct"].sum() == pytest.approx(100.0)

    def test_inline_count_star_without_declared_count(self, flights):
        """The lift must not depend on a declared count measure existing."""
        st = flights.with_measures(
            pct=lambda t: t.count() / t.all(t.count()) * 100,
        )
        df = st.group_by("origin").aggregate("pct").execute()
        assert df["pct"].sum() == pytest.approx(100.0)

    def test_inline_count_star_mixed_with_field_reduction(self, flights):
        """CountStar in the denominator alongside a Field-based numerator."""
        st = flights.with_measures(
            dist_per_pct=lambda t: t.distance.sum() / t.all(t.count()),
        )
        df = (
            st.group_by("origin")
            .aggregate("dist_per_pct")
            .execute()
            .set_index("origin")
            .sort_index()
        )
        assert df.loc["A", "dist_per_pct"] == pytest.approx(60.0 / 6)
        assert df.loc["C", "dist_per_pct"] == pytest.approx(60.0 / 6)

    def test_inline_count_star_on_joined_model(self, orders_customers):
        orders_tbl, cust_tbl = orders_customers
        customers = to_semantic_table(cust_tbl, name="customers").with_dimensions(region=_.region)
        orders = to_semantic_table(orders_tbl, name="orders").with_measures(
            order_count=_.count(),
            pct=lambda t: t.count() / t.all(t.count()) * 100,
        )
        j = orders.join_one(customers, on="cust_id", how="left")
        df = (
            j.group_by("customers.region")
            .aggregate("orders.order_count", "orders.pct")
            .execute()
            .set_index("customers.region")
        )
        assert df.loc["east", "orders.pct"] == pytest.approx(75.0)
        assert df.loc["west", "orders.pct"] == pytest.approx(25.0)


# ---------------------------------------------------------------------------
# R5-2: colliding right-table dimension must read the RIGHT column
# ---------------------------------------------------------------------------


class TestCollidingRightDimension:
    def test_declared_dim_when_fact_table_has_no_dims(self, orders_customers):
        """Fact table with no declared dims must not disable rename-wrapping."""
        orders_tbl, cust_tbl = orders_customers
        customers = to_semantic_table(cust_tbl, name="customers").with_dimensions(cust_name=_.name)
        orders = to_semantic_table(orders_tbl, name="orders").with_measures(order_count=_.count())
        j = orders.join_one(customers, on="cust_id", how="left")
        df = (
            j.group_by("customers.cust_name")
            .aggregate("orders.order_count")
            .execute()
            .set_index("customers.cust_name")
        )
        # Before the fix this returned the LEFT table's name column
        # (o1..o4, four rows) instead of the customers' names.
        assert sorted(df.index) == ["alice", "bob", "cara"]
        assert df.loc["alice", "orders.order_count"] == 2


# ---------------------------------------------------------------------------
# R5-3: raw <table>.<column> group keys on joined models
# ---------------------------------------------------------------------------


class TestRawPrefixedGroupKeys:
    @pytest.fixture
    def joined(self, orders_customers):
        orders_tbl, cust_tbl = orders_customers
        customers = to_semantic_table(cust_tbl, name="customers").with_dimensions(region=_.region)
        orders = to_semantic_table(orders_tbl, name="orders").with_measures(
            order_count=_.count(),
            total_amount=_.amount.sum(),
        )
        return orders, customers

    def test_raw_right_column(self, joined):
        orders, customers = joined
        j = orders.join_one(customers, on="cust_id", how="left")
        df = (
            j.group_by("customers.cust_id")
            .aggregate("orders.order_count")
            .execute()
            .set_index("customers.cust_id")
            .sort_index()
        )
        assert df["orders.order_count"].tolist() == [2, 1, 1]

    def test_raw_left_column(self, joined):
        orders, customers = joined
        j = orders.join_one(customers, on="cust_id", how="left")
        df = j.group_by("orders.amount").aggregate("orders.order_count").execute()
        assert len(df) == 4
        assert df["orders.order_count"].sum() == 4

    def test_raw_colliding_right_column(self, joined):
        """A raw right column colliding with a left column resolves right."""
        orders, customers = joined
        j = orders.join_one(customers, on="cust_id", how="left")
        df = (
            j.group_by("customers.name")
            .aggregate("orders.order_count")
            .execute()
            .set_index("customers.name")
        )
        assert sorted(df.index) == ["alice", "bob", "cara"]
        assert df.loc["alice", "orders.order_count"] == 2

    def test_raw_prefixed_on_join_many_preagg(self, joined):
        orders, customers = joined
        j = orders.join_many(customers, on="cust_id", how="left")
        df = (
            j.group_by("customers.cust_id")
            .aggregate("orders.order_count")
            .execute()
            .set_index("customers.cust_id")
            .sort_index()
        )
        assert df["orders.order_count"].tolist() == [2, 1, 1]

    def test_raw_prefixed_on_single_table(self, joined):
        """Prefixed raw columns also resolve without any join."""
        orders, _customers = joined
        df = (
            orders.group_by("orders.cust_id")
            .aggregate("order_count")
            .execute()
            .set_index("orders.cust_id")
            .sort_index()
        )
        assert df["order_count"].tolist() == [2, 1, 1]

    def test_declared_dimension_still_wins(self, orders_customers):
        """A declared dimension sharing a raw column's name keeps priority."""
        orders_tbl, cust_tbl = orders_customers
        customers = to_semantic_table(cust_tbl, name="customers").with_dimensions(
            region=lambda t: t.region.upper()
        )
        orders = to_semantic_table(orders_tbl, name="orders").with_measures(order_count=_.count())
        j = orders.join_one(customers, on="cust_id", how="left")
        df = j.group_by("customers.region").aggregate("orders.order_count").execute()
        assert sorted(df["customers.region"]) == ["EAST", "WEST"]

    def test_unknown_key_raises_semantic_error(self, joined):
        """Typos raise a semantic-layer error, not a physical-schema dump."""
        orders, customers = joined
        j = orders.join_one(customers, on="cust_id", how="left")
        with pytest.raises(KeyError) as excinfo:
            j.group_by("customers.regoin").aggregate("orders.order_count").execute()
        msg = str(excinfo.value)
        assert "customers.regoin" in msg
        assert "customers.region" in msg  # did-you-mean suggestion
        # No physical join-schema leak:
        assert "_right" not in msg
