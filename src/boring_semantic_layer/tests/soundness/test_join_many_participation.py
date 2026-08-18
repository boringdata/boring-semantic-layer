"""Regression tests for the July 2026 round-4 soundness evaluation.

Pins the join_many participation defect: measures of a ``join_many``
(many-side) table were pre-aggregated on the RAW unjoined table unless
cross-table filter routing happened to force a join-key bridge. Orphan
rows — a NULL foreign key, or a key matching no left-side row — were
silently counted in grand totals and in many-side-only group-bys, while
mixed group-bys excluded them, so the sum over groups stopped matching
the ungrouped grand total on the same model.

Required semantics: a many-side row that the LEFT JOIN can never
produce is never counted, regardless of query shape, and the invariant
``sum over groups == ungrouped grand total`` holds for additive
measures. Every expectation below is checked against pandas ground
truth computed from an explicit LEFT JOIN.

Round-2 C1 (NULL group KEYS preserved through the re-join) must keep
holding: rows that DO join but carry a NULL dimension value still form
a NULL group. NULL join keys and NULL dimension values are different
things.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


CUSTOMERS = pd.DataFrame(
    {
        "cust_id": [10, 20, 30, 40],
        # cust 30 has a NULL tier (NULL dimension VALUE on the one side);
        # cust 40 has no orders at all.
        "tier": ["gold", "silver", None, "bronze"],
    }
)

ORDERS = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        # id 3: NULL FK orphan; id 4: FK matching no customer.
        "cust_id": [10, 20, None, 99, 10, 30],
        # 'weird' exists ONLY on the unmatched-FK orphan row;
        # id 5 is a JOINED row with a NULL dimension value (C1).
        "status": ["open", "closed", "open", "weird", None, "open"],
        "amount": [10.0, 20.0, 40.0, 80.0, 5.0, 7.0],
    }
)


def _left_join_truth():
    """Pandas ground truth: explicit customers LEFT JOIN orders."""
    return CUSTOMERS.merge(ORDERS, on="cust_id", how="left")


@pytest.fixture
def joined(con):
    customers = con.create_table("customers", CUSTOMERS)
    orders = con.create_table("orders", ORDERS)
    c_st = (
        to_semantic_table(customers, name="customers")
        .with_dimensions(tier=lambda t: t.tier)
        .with_measures(n_cust=lambda t: t.count())
    )
    o_st = (
        to_semantic_table(orders, name="orders")
        .with_dimensions(status=lambda t: t.status)
        .with_measures(n=lambda t: t.count(), total=lambda t: t.amount.sum())
    )
    return c_st.join_many(o_st, lambda c, o: c.cust_id == o.cust_id)


class TestGrandTotalJoinParticipation:
    """Ungrouped aggregates must count only rows the LEFT JOIN produces."""

    def test_grand_total_matches_left_join(self, joined):
        truth = _left_join_truth()
        df = joined.aggregate("orders.n", "orders.total").execute()
        assert df["orders.n"].iloc[0] == truth["id"].count()  # 4, not 6
        assert df["orders.total"].iloc[0] == pytest.approx(
            truth["amount"].sum()  # 42.0, not 162.0
        )

    def test_null_fk_orphan_excluded(self, con):
        """A NULL foreign key alone (no unmatched-value orphan) is excluded."""
        orders = ORDERS[ORDERS.cust_id != 99]
        c = con.create_table("customers_nf", CUSTOMERS)
        o = con.create_table("orders_nf", orders)
        jm = (
            to_semantic_table(c, name="customers")
            .with_dimensions(tier=lambda t: t.tier)
            .join_many(
                to_semantic_table(o, name="orders").with_measures(n=lambda t: t.count()),
                lambda c, o: c.cust_id == o.cust_id,
            )
        )
        truth = CUSTOMERS.merge(orders, on="cust_id", how="left")
        df = jm.aggregate("orders.n").execute()
        assert df["orders.n"].iloc[0] == truth["id"].count()  # 4, not 5

    def test_unmatched_fk_orphan_excluded(self, con):
        """A non-NULL key matching no left-side row is excluded."""
        orders = ORDERS[ORDERS.cust_id.notna()]
        c = con.create_table("customers_uf", CUSTOMERS)
        o = con.create_table("orders_uf", orders)
        jm = (
            to_semantic_table(c, name="customers")
            .with_dimensions(tier=lambda t: t.tier)
            .join_many(
                to_semantic_table(o, name="orders").with_measures(n=lambda t: t.count()),
                lambda c, o: c.cust_id == o.cust_id,
            )
        )
        truth = CUSTOMERS.merge(orders, on="cust_id", how="left")
        df = jm.aggregate("orders.n").execute()
        assert df["orders.n"].iloc[0] == truth["id"].count()  # 4, not 5


class TestGroupedJoinParticipation:
    """Grouped aggregates use the same participating rows as grand totals."""

    def test_group_by_many_side_dim_only(self, joined):
        truth = (
            _left_join_truth().groupby("status", dropna=False)["id"].count().loc[lambda s: s > 0]
        )
        df = (
            joined.group_by("orders.status")
            .aggregate("orders.n")
            .execute()
            .set_index("orders.status")
        )
        assert df.loc["open", "orders.n"] == truth["open"]  # 2, not 3
        assert df.loc["closed", "orders.n"] == truth["closed"]
        # C1: the joined row with a NULL dimension value forms a NULL group
        assert df.loc[[pd.isna(i) for i in df.index], "orders.n"].iloc[0] == 1

    def test_group_by_left_dim_only(self, joined):
        truth = _left_join_truth().groupby("tier", dropna=False)["id"].count()
        df = (
            joined.group_by("customers.tier")
            .aggregate("orders.n")
            .execute()
            .set_index("customers.tier")
        )
        # counts re-aggregated through the pre-agg path come back as
        # Decimal — coerce before comparing against numpy integers
        n = pd.to_numeric(df["orders.n"])
        assert n.loc["gold"] == truth["gold"]  # 2, not 3
        assert n.loc["silver"] == truth["silver"]  # 1
        # NULL tier (cust 30) is a real group with one participating order
        assert n.loc[[pd.isna(i) for i in n.index]].iloc[0] == 1
        # cust 40 has no orders: NULL/0 either way, never a positive count
        assert pd.isna(n.loc["bronze"]) or n.loc["bronze"] == 0

    def test_orphan_only_dim_value_absent_everywhere(self, joined):
        """'weird' lives only on an orphan row: absent from every result."""
        by_status = joined.group_by("orders.status").aggregate("orders.n").execute()
        assert "weird" not in set(by_status["orders.status"].dropna())
        mixed = joined.group_by("customers.tier", "orders.status").aggregate("orders.n").execute()
        assert "weird" not in set(mixed["orders.status"].dropna())
        # ...and its amount is absent from the grand total too
        total = joined.aggregate("orders.total").execute()["orders.total"].iloc[0]
        assert total == pytest.approx(_left_join_truth()["amount"].sum())


class TestSumOverGroupsInvariant:
    """sum over groups of an additive measure == ungrouped grand total."""

    def test_many_side_dim_groups_sum_to_grand_total(self, joined):
        grand = joined.aggregate("orders.n", "orders.total").execute()
        by_status = joined.group_by("orders.status").aggregate("orders.n", "orders.total").execute()
        assert by_status["orders.n"].sum() == grand["orders.n"].iloc[0]
        assert by_status["orders.total"].sum() == pytest.approx(grand["orders.total"].iloc[0])

    def test_left_dim_groups_sum_to_grand_total(self, joined):
        grand = joined.aggregate("orders.n", "orders.total").execute()
        by_tier = joined.group_by("customers.tier").aggregate("orders.n", "orders.total").execute()
        assert pd.to_numeric(by_tier["orders.n"]).fillna(0).sum() == grand["orders.n"].iloc[0]
        assert pd.to_numeric(by_tier["orders.total"]).fillna(0).sum() == pytest.approx(
            grand["orders.total"].iloc[0]
        )

    def test_mixed_dim_groups_sum_to_grand_total(self, joined):
        grand = joined.aggregate("orders.n").execute()
        mixed = joined.group_by("customers.tier", "orders.status").aggregate("orders.n").execute()
        assert pd.to_numeric(mixed["orders.n"]).fillna(0).sum() == grand["orders.n"].iloc[0]


class TestNullDimValueStillGroups:
    """C1 regression guard: NULL dimension VALUES on joined rows survive."""

    def test_null_status_group_measures(self, joined):
        truth = _left_join_truth()
        null_truth = truth[truth["id"].notna() & truth["status"].isna()]
        df = joined.group_by("orders.status").aggregate("orders.n", "orders.total").execute()
        null_rows = df[df["orders.status"].isna()]
        assert len(null_rows) == 1
        assert null_rows["orders.n"].iloc[0] == len(null_truth)  # 1
        assert null_rows["orders.total"].iloc[0] == pytest.approx(
            null_truth["amount"].sum()  # 5.0
        )

    def test_null_tier_by_null_status(self, joined):
        """NULL keys on BOTH sides of a mixed group-by stay distinct groups."""
        df = joined.group_by("customers.tier", "orders.status").aggregate("orders.n").execute()
        gold_null = df[(df["customers.tier"] == "gold") & (df["orders.status"].isna())]
        assert len(gold_null) == 1
        assert gold_null["orders.n"].iloc[0] == 1  # order id 5
        null_open = df[(df["customers.tier"].isna()) & (df["orders.status"] == "open")]
        assert len(null_open) == 1
        assert null_open["orders.n"].iloc[0] == 1  # order id 6, cust 30
