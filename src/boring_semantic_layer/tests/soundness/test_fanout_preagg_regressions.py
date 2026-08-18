"""Regression tests for the July 2026 round-2 soundness evaluation.

Each test pins a confirmed silent-wrong-answer defect (or its loud-error
replacement) against pandas-derived ground truth. Finding IDs reference
the round-2 soundness report:

- A1/A2  non-decomposable measures re-aggregated with SUM
- A3     mean(where=...) losing its condition under pre-aggregation
- B1     bare derived-dim filters restricting only the owning table
- B2     cross-table compound predicates inflating many-side measures
- B3     group_by().filter().aggregate() discarding the grouping
- B4     derived dims as group keys missing from pre-agg output
- C1     NULL group keys dropped by plain equi-joins in the re-join
- D1     join_one default join type depending on the receiver class
- E1/E2  serialization round-trips changing results
- F1     deferred-join path dropping dims / returning hidden grain
- F2     dimension-only shortcut disabled by prefixed filter spelling
- F3     index() selector typos silently indexing every field
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
    """Orders with an uneven 1:N fan into items (3/1/2 line items)."""
    orders = con.create_table(
        "orders",
        pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [10, 10, 20],
                "status": ["open", "closed", "open"],
                "amount": [100.0, 120.0, 80.0],
            }
        ),
    )
    items = con.create_table(
        "items",
        pd.DataFrame(
            {
                "item_id": [1, 2, 3, 4, 5, 6],
                "order_id": [1, 1, 1, 2, 3, 3],
                "qty": [1, 2, 1, 3, 1, 1],
                "sku": ["a", "b", "a", "c", "a", "b"],
            }
        ),
    )
    o_st = (
        to_semantic_table(orders, name="orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            status=lambda t: t.status,
            size=lambda t: (t.amount > 90).ifelse("big", "small"),
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
            avg_open_amount=lambda t: t.amount.mean(where=t.status == "open"),
            median_amount=lambda t: t.amount.median(),
            aov=lambda t: t.amount.sum() / t.count(),
        )
    )
    i_st = (
        to_semantic_table(items, name="items")
        .with_dimensions(sku=lambda t: t.sku)
        .with_measures(
            item_count=lambda t: t.count(),
            total_qty=lambda t: t.qty.sum(),
        )
    )
    return o_st, i_st


def _joined(orders_items):
    o_st, i_st = orders_items
    return o_st.join_many(i_st, lambda o, i: o.order_id == i.order_id)


class TestNonDecomposableReagg:
    """A1/A2: median/stddev/ratios must not be summed at cross-table grain."""

    def test_median_by_cross_table_dim(self, orders_items):
        df = (
            _joined(orders_items)
            .group_by("items.sku")
            .aggregate("orders.median_amount")
            .execute()
            .set_index("items.sku")
        )
        # sku a touches orders 1 (100) and 3 (80) -> median 90, not 180
        assert df.loc["a", "orders.median_amount"] == pytest.approx(90.0)
        assert df.loc["c", "orders.median_amount"] == pytest.approx(120.0)

    def test_ratio_measure_by_cross_table_dim(self, orders_items):
        df = (
            _joined(orders_items)
            .group_by("items.sku")
            .aggregate("orders.aov")
            .execute()
            .set_index("items.sku")
        )
        # sku a: (100 + 80) / 2 = 90, not the summed per-order ratios (180)
        assert df.loc["a", "orders.aov"] == pytest.approx(90.0)

    def test_items_median_by_orders_dim(self, con):
        orders = con.create_table(
            "orders_m",
            pd.DataFrame({"order_id": [1, 2], "band": ["A", "A"]}),
        )
        items = con.create_table(
            "items_m",
            pd.DataFrame({"order_id": [1, 1, 1, 2, 2], "qty": [1, 5, 9, 1, 3]}),
        )
        o = to_semantic_table(orders, name="orders").with_dimensions(band=lambda t: t.band)
        i = to_semantic_table(items, name="items").with_measures(
            median_qty=lambda t: t.qty.median(),
            avg_qty=lambda t: t.qty.mean(),
        )
        df = (
            o.join_many(i, lambda a, b: a.order_id == b.order_id)
            .group_by("orders.band")
            .aggregate("items.median_qty", "items.avg_qty")
            .execute()
        )
        # median over item rows in band A = 3, not sum of per-order medians (5+2)
        assert df["items.median_qty"].iloc[0] == pytest.approx(3.0)
        assert df["items.avg_qty"].iloc[0] == pytest.approx(19 / 5)


class TestConditionalMeanDecomposition:
    """A3: mean(where=...) keeps its condition on joined grains."""

    def test_scalar_grain(self, orders_items):
        df = _joined(orders_items).aggregate("orders.avg_open_amount").execute()
        # mean of open orders [100, 80] = 90, not mean of all three (100)
        assert df["orders.avg_open_amount"].iloc[0] == pytest.approx(90.0)

    def test_grouped_grain(self, orders_items):
        df = (
            _joined(orders_items)
            .group_by("orders.customer_id")
            .aggregate("orders.avg_open_amount")
            .order_by("orders.customer_id")
            .execute()
        )
        # customer 10's only open order is 100 (closed 120 excluded)
        assert df["orders.avg_open_amount"].tolist() == pytest.approx([100.0, 80.0])


class TestNullGroupKeys:
    """C1: NULL group keys keep their measures, independent of measure order."""

    @pytest.fixture
    def null_qty(self, con):
        orders = con.create_table(
            "orders_n",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4],
                    "customer_id": [10, 10, 20, 30],
                    "amount": [100.0, 120.0, 80.0, 50.0],
                }
            ),
        )
        items = con.create_table(
            "items_n",
            pd.DataFrame(
                {
                    "item_id": [1, 2, 3, 4, 5, 6],
                    "order_id": [1, 1, 1, 2, 3, 3],
                    "qty": [1.0, 2.0, 1.0, 3.0, None, 1.0],
                }
            ),
        )
        o = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total_amount=lambda t: t.amount.sum())
        )
        i = (
            to_semantic_table(items, name="items")
            .with_dimensions(qty=lambda t: t.qty)
            .with_measures(item_count=lambda t: t.count())
        )
        return o.join_many(i, lambda a, b: a.order_id == b.order_id)

    @staticmethod
    def _null_row(df):
        return df[df["items.qty"].isna()].iloc[0]

    def test_null_group_measures_present(self, null_qty):
        # Both a real NULL qty (order 3) and the no-item order 4 land in
        # the NULL group: item_count 1, distinct-order amount 80 + 50.
        df = (
            null_qty.group_by("items.qty")
            .aggregate("items.item_count", "orders.total_amount")
            .execute()
        )
        row = self._null_row(df)
        assert row["items.item_count"] == 1
        assert row["orders.total_amount"] == pytest.approx(130.0)

    def test_measure_order_does_not_change_answer(self, null_qty):
        df1 = (
            null_qty.group_by("items.qty")
            .aggregate("items.item_count", "orders.total_amount")
            .execute()
        )
        df2 = (
            null_qty.group_by("items.qty")
            .aggregate("orders.total_amount", "items.item_count")
            .execute()
        )
        r1, r2 = self._null_row(df1), self._null_row(df2)
        assert r1["items.item_count"] == r2["items.item_count"]
        assert r1["orders.total_amount"] == r2["orders.total_amount"]


class TestJoinOneDefaults:
    """D1: join_one defaults to how="left" on every receiver class."""

    def test_noop_filter_does_not_change_totals(self, con):
        orders = con.create_table(
            "orders_d",
            pd.DataFrame(
                {"order_id": [1, 2, 3], "customer_id": [10, 10, 20], "amount": [100, 120, 80]}
            ),
        )
        customers = con.create_table(
            "customers_d",
            pd.DataFrame({"customer_id": [10], "region": ["west"]}),
        )
        o = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total_amount=lambda t: t.amount.sum())
        )
        c = to_semantic_table(customers, name="customers").with_dimensions(
            customer_id=lambda t: t.customer_id, region=lambda t: t.region
        )
        on = lambda a, b: a.customer_id == b.customer_id  # noqa: E731
        direct = o.join_one(c, on).aggregate("orders.total_amount").execute()
        filtered = (
            o.filter(lambda t: t.amount > 0)
            .join_one(c, on)
            .aggregate("orders.total_amount")
            .execute()
        )
        assert (
            direct["orders.total_amount"].iloc[0] == filtered["orders.total_amount"].iloc[0] == 300
        )


class TestFilterRouting:
    """B1/B2: derived-dim and compound filters restrict every table."""

    def test_bare_derived_dim_filter_matches_prefixed(self, orders_items):
        joined = _joined(orders_items)
        bare = (
            joined.filter(lambda t: t.size == "big")
            .group_by("orders.customer_id")
            .aggregate("orders.total_amount", "items.item_count")
            .order_by("orders.customer_id")
            .execute()
        )
        prefixed = (
            joined.filter(lambda t: t["orders.size"] == "big")
            .group_by("orders.customer_id")
            .aggregate("orders.total_amount", "items.item_count")
            .order_by("orders.customer_id")
            .execute()
        )
        pd.testing.assert_frame_equal(bare, prefixed)
        # big orders: 1 (100) and 2 (120), both customer 10 — no ghost rows
        assert bare["orders.customer_id"].tolist() == [10]
        assert bare["orders.total_amount"].tolist() == [220.0]
        assert bare["items.item_count"].tolist() == [4]

    def test_compound_and_matches_chained_filters(self, orders_items):
        joined = _joined(orders_items)
        compound = (
            joined.filter(lambda t: (t["orders.status"] == "open") & (t.qty >= 2))
            .group_by("orders.customer_id")
            .aggregate("items.item_count", "items.total_qty")
            .order_by("orders.customer_id")
            .execute()
        )
        chained = (
            joined.filter(lambda t: t["orders.status"] == "open")
            .filter(lambda t: t.qty >= 2)
            .group_by("orders.customer_id")
            .aggregate("items.item_count", "items.total_qty")
            .order_by("orders.customer_id")
            .execute()
        )
        pd.testing.assert_frame_equal(compound, chained)
        # open orders 1 & 3; items with qty>=2: item 2 (order 1) only
        assert compound["items.item_count"].tolist() == [1]

    def test_cross_table_or_raises(self, orders_items):
        expr = (
            _joined(orders_items)
            .filter(lambda t: (t["orders.status"] == "closed") | (t.qty >= 5))
            .aggregate("items.item_count")
        )
        with pytest.raises(ValueError, match="row-precisely"):
            expr.execute()


class TestGroupByFilterAggregate:
    """B3: the grouping survives a filter between group_by and aggregate."""

    def test_keys_preserved(self, orders_items):
        df = (
            _joined(orders_items)
            .group_by("orders.customer_id")
            .filter(lambda t: t["orders.status"] == "open")
            .aggregate("orders.total_amount", "items.item_count")
            .order_by("orders.customer_id")
            .execute()
        )
        assert "orders.customer_id" in df.columns
        assert df["orders.customer_id"].tolist() == [10, 20]
        assert df["orders.total_amount"].tolist() == [100.0, 80.0]
        assert df["items.item_count"].tolist() == [3, 2]


class TestDerivedDimGroupKeys:
    """B4: derived dims as group keys appear in the pre-agg output."""

    def test_key_column_present(self, orders_items):
        df = (
            _joined(orders_items)
            .group_by("orders.size")
            .aggregate("orders.total_amount")
            .execute()
            .set_index("orders.size")
        )
        assert df.loc["big", "orders.total_amount"] == pytest.approx(220.0)
        assert df.loc["small", "orders.total_amount"] == pytest.approx(80.0)

    def test_with_many_side_measure(self, orders_items):
        df = (
            _joined(orders_items)
            .group_by("orders.size")
            .aggregate("orders.total_amount", "items.item_count")
            .execute()
            .set_index("orders.size")
        )
        assert df.loc["big", "items.item_count"] == 4
        assert df.loc["small", "items.item_count"] == 2


class TestDeferredDimensionJoins:
    """F1: deferral keeps the requested grain and never drops dims."""

    @pytest.fixture
    def entity_join(self, con):
        orders = con.create_table(
            "orders_e",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4],
                    "customer_id": [10, 10, 20, 30],
                    "amount": [100.0, 120.0, 80.0, 50.0],
                }
            ),
        )
        customers = con.create_table(
            "customers_e",
            pd.DataFrame({"customer_id": [10, 20, 30], "region": ["east", "west", "east"]}),
        )
        o = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total_amount=lambda t: t.amount.sum())
        )
        c = to_semantic_table(customers, name="customers").with_dimensions(
            customer_id={"expr": lambda t: t.customer_id, "is_entity": True},
            region=lambda t: t.region,
            region_band=lambda t: t.region.upper(),
            region_display=lambda t: t.region_band + "!",
        )
        return o.join_one(c, lambda l, r: l.customer_id == r.customer_id)

    def test_coarser_dim_grain_is_regrouped(self, entity_join):
        df = (
            entity_join.group_by("customers.region")
            .aggregate("orders.total_amount")
            .execute()
            .set_index("customers.region")
        )
        # 2 region rows, not 3 entity rows with duplicate labels
        assert len(df) == 2
        assert df.loc["east", "orders.total_amount"] == pytest.approx(270.0)
        assert df.loc["west", "orders.total_amount"] == pytest.approx(80.0)

    def test_derived_of_derived_dim_resolves(self, entity_join):
        df = (
            entity_join.group_by("customers.region_display")
            .aggregate("orders.total_amount")
            .execute()
            .set_index("customers.region_display")
        )
        assert "EAST!" in df.index and "WEST!" in df.index
        assert df.loc["EAST!", "orders.total_amount"] == pytest.approx(270.0)

    def test_entity_grain_deferral_still_works(self, entity_join):
        df = (
            entity_join.group_by("customers.customer_id", "customers.region")
            .aggregate("orders.total_amount")
            .order_by("customers.customer_id")
            .execute()
        )
        assert df["orders.total_amount"].tolist() == pytest.approx([220.0, 80.0, 50.0])
        assert df["customers.region"].tolist() == ["east", "west", "east"]


class TestDimensionOnlyShortcut:
    """F2: zero-fact members survive every filter spelling."""

    @pytest.fixture
    def dim_join(self, con):
        orders = con.create_table(
            "orders_s",
            pd.DataFrame({"order_id": [1, 2], "customer_id": [10, 20], "amount": [1, 2]}),
        )
        customers = con.create_table(
            "customers_s",
            pd.DataFrame({"customer_id": [10, 20, 30], "region": ["west", "east", "north"]}),
        )
        o = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total_amount=lambda t: t.amount.sum())
        )
        c = to_semantic_table(customers, name="customers").with_dimensions(
            customer_id=lambda t: t.customer_id,
            region=lambda t: t.region,
            region_band=lambda t: t.region.upper(),
        )
        return o.join_one(c, lambda l, r: l.customer_id == r.customer_id)

    def test_prefixed_filter_keeps_zero_fact_members(self, dim_join):
        df = (
            dim_join.filter(lambda t: t["customers.region"] != "zzz")
            .group_by("customers.region")
            .aggregate()
            .execute()
        )
        # 'north' has no orders but must still be returned (#224)
        assert sorted(df["customers.region"]) == ["east", "north", "west"]

    def test_derived_dim_filter_keeps_zero_fact_members(self, dim_join):
        df = (
            dim_join.filter(lambda t: t.region_band != "ZZZ")
            .group_by("customers.region")
            .aggregate()
            .execute()
        )
        assert sorted(df["customers.region"]) == ["east", "north", "west"]


class TestIndexSelector:
    """F3: a selector that matches nothing raises instead of matching all."""

    @pytest.fixture
    def flat(self, con):
        tbl = con.create_table(
            "flat_i",
            pd.DataFrame({"status": ["a", "b"], "region": ["x", "y"], "amount": [1, 2]}),
        )
        return to_semantic_table(tbl, name="flat").with_dimensions(
            status=lambda t: t.status, region=lambda t: t.region
        )

    def test_typo_raises(self, flat):
        with pytest.raises(ValueError, match="staus"):
            flat.index("staus").execute()

    def test_exact_field_indexes_only_that_field(self, flat):
        df = flat.index("status").execute()
        assert set(df["fieldName"]) == {"status"}


class TestSerializationRoundTrip:
    """E1/E2: round-trips preserve results or fail loudly."""

    @pytest.fixture(autouse=True)
    def _requires_xorq(self):
        pytest.importorskip("xorq")

    def _wrapper_model(self, con):
        orders = con.create_table(
            "orders_rt",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [10, 10, 20],
                    "amount": [100.0, 120.0, 80.0],
                }
            ),
        )
        items = con.create_table(
            "items_rt",
            pd.DataFrame(
                {"item_id": [1, 2, 3, 4, 5, 6], "order_id": [1, 1, 1, 2, 3, 3], "qty": [1] * 6}
            ),
        )
        o = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(
                total_amount=lambda t: t.amount.sum(),
                avg_amount=lambda t: t.amount.mean(),
            )
        )
        i = to_semantic_table(items, name="items").with_measures(item_count=lambda t: t.count())
        return o.join_many(i, lambda a, b: a.order_id == b.order_id).with_measures(
            pot=lambda t: t["orders.total_amount"] / t.all(t["orders.total_amount"]),
        )

    def test_wrapper_roundtrip_keeps_preagg(self, con):
        from boring_semantic_layer.serialization import from_tagged, to_tagged

        model = self._wrapper_model(con)
        query = lambda m: (  # noqa: E731
            m.group_by("orders.customer_id")
            .aggregate("orders.total_amount", "orders.avg_amount", "pot")
            .order_by("orders.customer_id")
            .execute()
        )
        before = query(model)
        restored = from_tagged(to_tagged(model))
        after = query(restored)
        pd.testing.assert_frame_equal(before, after)
        # Fan-out-safe numbers, not the lowered-join ones (420/160)
        assert after["orders.total_amount"].tolist() == pytest.approx([220.0, 80.0])
        assert after["pot"].sum() == pytest.approx(1.0)

    def test_preagg_query_roundtrip_raises_not_wrong(self, con):
        from boring_semantic_layer.serialization import from_tagged, to_tagged

        model = self._wrapper_model(con)
        expr = model.filter(lambda t: t.qty >= 1).aggregate("items.item_count")
        tagged = to_tagged(expr)
        with pytest.raises(ValueError, match="could not recover"):
            from_tagged(tagged).execute()
