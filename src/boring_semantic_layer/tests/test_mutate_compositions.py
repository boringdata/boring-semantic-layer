"""Regression tests pinning chained-``.mutate()`` compositions (ADR 0001, Phase 3).

Each test pins the observable behavior of a composition from the ADR's
"composition gotchas" table so that the ``.mutate()`` desugaring (alias to
the ``with_measures``/aggregate path) can be verified observationally
equivalent after ``SemanticMutateOp`` is deleted.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def flights_model():
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA", "DL", "DL", "DL"],
            "origin": ["JFK", "LGA", "JFK", "ORD", "ATL", "ATL", "JFK"],
            "distance": [100, 200, 300, 400, 500, 600, 700],
            "dep_time": pd.to_datetime(
                [
                    "2024-01-05",
                    "2024-01-20",
                    "2024-02-10",
                    "2024-02-15",
                    "2024-03-01",
                    "2024-03-10",
                    "2024-03-20",
                ]
            ),
        }
    )
    tbl = con.create_table("flights", df)
    return (
        to_semantic_table(tbl, "flights")
        .with_dimensions(
            carrier=lambda t: t.carrier,
            origin=lambda t: t.origin,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
    )


@pytest.fixture
def joined_model():
    """join_many model exercising the pre-aggregation planner path."""
    con = ibis.duckdb.connect(":memory:")
    customers_df = pd.DataFrame({"cid": [1, 2], "name": ["alice", "bob"]})
    accounts_df = pd.DataFrame(
        {
            "aid": [10, 11, 12, 13],
            "cid": [1, 1, 2, 2],
            "date": pd.to_datetime(
                ["2024-01-05", "2024-02-10", "2024-01-15", "2024-02-20"]
            ),
            "balance": [100, 200, 300, 400],
        }
    )
    customers = con.create_table("customers_comp", customers_df)
    accounts = con.create_table("accounts_comp", accounts_df)

    cust_model = to_semantic_table(customers, "customers").with_dimensions(
        cid=lambda t: t.cid,
        name=lambda t: t.name,
    )
    acct_model = (
        to_semantic_table(accounts, "accounts")
        .with_dimensions(
            aid=lambda t: t.aid,
            cid=lambda t: t.cid,
            date=lambda t: t.date,
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )
    return cust_model.join_many(acct_model, on=lambda left, right: left.cid == right.cid)


class TestMutateAfterAggregate:
    def test_mutate_basic_ratio(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["avg_distance"].tolist() == [150.0, 600.0, 350.0]

    def test_mutate_then_filter_sees_mutated_column(self, flights_model):
        """Filter after mutate applies to the table containing the new column."""
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
            .filter(lambda t: t.avg_distance > 200)
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["carrier"].tolist() == ["DL", "UA"]
        assert df["avg_distance"].tolist() == [600.0, 350.0]

    def test_mutate_then_order_by_mutated_column(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
            .order_by("avg_distance")
            .execute()
        )
        assert df["avg_distance"].tolist() == [150.0, 350.0, 600.0]

    def test_mutate_then_limit(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(doubled=lambda t: t.flight_count * 2)
            .order_by("carrier")
            .limit(2)
            .execute()
        )
        assert len(df) == 2
        assert "doubled" in df.columns
        assert df["doubled"].tolist() == (df["flight_count"] * 2).tolist()

    def test_chained_mutates_second_sees_first(self, flights_model):
        """A later mutate can reference a column added by an earlier mutate."""
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(doubled=lambda t: t.flight_count * 2)
            .mutate(quadrupled=lambda t: t.doubled * 2)
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["quadrupled"].tolist() == (df["flight_count"] * 4).tolist()

    def test_mutate_multiple_columns_in_one_call(self, flights_model):
        """Later entries in a single mutate(...) call see earlier entries."""
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(
                doubled=lambda t: t.flight_count * 2,
                tripled_plus=lambda t: t.doubled + t.flight_count,
            )
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["tripled_plus"].tolist() == (df["flight_count"] * 3).tolist()

    def test_mutate_alias_beats_same_named_source_column(self):
        """A later post-agg mutate reads prior aliases, not raw columns."""
        con = ibis.duckdb.connect(":memory:")
        df = pd.DataFrame(
            {
                "carrier": ["AA", "AA", "UA"],
                "doubled": [100, 200, 300],
            }
        )
        tbl = con.create_table("alias_collision_flights", df)
        model = (
            to_semantic_table(tbl, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(flight_count=lambda t: t.count())
        )

        actual = (
            model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(
                doubled=lambda t: t.flight_count * 2,
                quadrupled=lambda t: t.doubled * 2,
            )
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )

        assert actual["doubled"].tolist() == (actual["flight_count"] * 2).tolist()
        assert actual["quadrupled"].tolist() == (actual["flight_count"] * 4).tolist()

    def test_mutate_with_percent_of_total(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(pct=lambda t: t.flight_count / t.all(t.flight_count) * 100)
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["pct"].tolist() == pytest.approx([2 / 7 * 100, 3 / 7 * 100, 2 / 7 * 100])

    def test_mutate_with_window_rank(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("total_distance")
            .mutate(rnk=lambda t: t.total_distance.rank())
            .execute()
            .sort_values("total_distance")
            .reset_index(drop=True)
        )
        assert df["rnk"].tolist() == [0, 1, 2]

    def test_mutate_with_case_bucketing(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(bucket=lambda t: (t.flight_count >= 3).ifelse("hi", "lo"))
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["bucket"].tolist() == ["lo", "hi", "lo"]

    def test_mutate_with_deferred_expression(self, flights_model):
        from ibis import _

        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(doubled=_.flight_count * 2)
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["doubled"].tolist() == (df["flight_count"] * 2).tolist()

    def test_mutate_column_order_appends_at_end(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
            .execute()
        )
        assert list(df.columns) == [
            "carrier",
            "flight_count",
            "total_distance",
            "avg_distance",
        ]

    def test_mutate_referencing_group_key(self, flights_model):
        """A mutate lambda may reference a group-by key column."""
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .mutate(label=lambda t: t.carrier.upper())
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["label"].tolist() == ["AA", "DL", "UA"]

    def test_mutate_result_usable_as_chart_input(self, flights_model):
        """Chart metadata extraction sees mutate-derived columns as measures."""
        expr = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
        )
        from boring_semantic_layer.chart.utils import extract_aggregate_metadata

        dimensions, measures, _mutated, _agg_op = extract_aggregate_metadata(expr)
        assert dimensions == ["carrier"]
        assert "avg_distance" in measures


class TestMutateFilterMutate:
    def test_second_mutate_sees_filtered_rows(self, flights_model):
        """b after a filter is computed over rows surviving the filter."""
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
            .filter(lambda t: t.avg_distance > 200)
            .mutate(rnk=lambda t: t.avg_distance.rank())
            .execute()
            .sort_values("avg_distance")
            .reset_index(drop=True)
        )
        # Only DL (600) and UA (350) survive; rank computed over the 2 rows.
        assert df["rnk"].tolist() == [0, 1]

    def test_mutate_after_limit(self, flights_model):
        """Mutate after limit sees the post-limit table."""
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .order_by("carrier")
            .limit(2)
            .mutate(doubled=lambda t: t.flight_count * 2)
            .execute()
        )
        assert len(df) == 2
        assert df["doubled"].tolist() == (df["flight_count"] * 2).tolist()

    def test_mutate_after_order_by(self, flights_model):
        df = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .order_by("carrier")
            .mutate(doubled=lambda t: t.flight_count * 2)
            .execute()
        )
        assert df["doubled"].tolist() == (df["flight_count"] * 2).tolist()


class TestPreAggregateMutate:
    def test_mutate_before_group_by_is_dimension_grain(self, flights_model):
        """Pre-agg mutate adds a row-grain column usable as a group-by key."""
        df = (
            flights_model.mutate(month=lambda t: t.dep_time.month())
            .group_by("month")
            .aggregate("flight_count")
            .execute()
            .sort_values("month")
            .reset_index(drop=True)
        )
        assert df["month"].tolist() == [1, 2, 3]
        assert df["flight_count"].tolist() == [2, 2, 3]

    def test_mutate_on_join_used_as_group_key(self, joined_model):
        """Pre-agg mutate on a join_many model; key survives the preagg path."""
        df = (
            joined_model.mutate(period=ibis._["date"].truncate("M"))
            .group_by("period")
            .aggregate("accounts.total_balance")
            .execute()
            .sort_values("period")
            .reset_index(drop=True)
        )
        assert len(df) == 2
        assert df["accounts.total_balance"].tolist() == [400, 600]

    def test_chained_mutate_on_join_preserves_preagg_path(self):
        """Chained pre-agg mutate keeps the join reference for fan-out safety."""
        con = ibis.duckdb.connect(":memory:")
        customers = con.create_table(
            "customers_chain_mutate",
            pd.DataFrame({"cid": [1, 2]}),
        )
        accounts = con.create_table(
            "accounts_chain_mutate",
            pd.DataFrame(
                {
                    "aid": [10, 11, 12],
                    "cid": [1, 2, 2],
                    "balance": [100, 200, 300],
                    "date": pd.to_datetime(["2024-01-05", "2024-02-10", "2024-03-15"]),
                }
            ),
        )
        orders = con.create_table(
            "orders_chain_mutate",
            pd.DataFrame(
                {
                    "oid": [100, 101, 102],
                    "cid": [1, 2, 2],
                    "amount": [10, 20, 30],
                }
            ),
        )

        customer_model = to_semantic_table(customers, "customers").with_dimensions(
            cid=lambda t: t.cid,
        )
        account_model = (
            to_semantic_table(accounts, "accounts")
            .with_dimensions(cid=lambda t: t.cid, aid=lambda t: t.aid)
            .with_measures(total_balance=lambda t: t.balance.sum())
        )
        order_model = to_semantic_table(orders, "orders").with_dimensions(
            cid=lambda t: t.cid,
            oid=lambda t: t.oid,
        )
        joined = customer_model.join_many(
            account_model,
            on=lambda left, right: left.cid == right.cid,
        ).join_many(order_model, on=lambda left, right: left.cid == right.cid)

        actual = (
            joined.mutate(period=lambda t: t.date.truncate("M"))
            .mutate(is_large=lambda t: t.balance > 150)
            .group_by("is_large")
            .aggregate("accounts.total_balance")
            .execute()
            .sort_values("is_large")
            .reset_index(drop=True)
        )

        assert actual["is_large"].tolist() == [False, True]
        assert actual["accounts.total_balance"].tolist() == [100, 500]

    def test_with_measures_after_filter_sees_filtered_table(self, flights_model):
        """Pinned per ADR review: filter-then-derive scopes on filtered rows."""
        df = (
            flights_model.filter(lambda t: t.carrier != "DL")
            .with_measures(short_count=lambda t: (t.distance < 350).sum())
            .group_by("carrier")
            .aggregate("short_count")
            .execute()
            .sort_values("carrier")
            .reset_index(drop=True)
        )
        assert df["carrier"].tolist() == ["AA", "UA"]
        assert df["short_count"].tolist() == [2, 1]


class TestMutateAsJoinAndSerializationInput:
    def test_legacy_semantic_mutate_tag_is_rejected(self):
        """Hard cutoff: old SemanticMutateOp tags are no longer supported."""
        from boring_semantic_layer.serialization.context import BSLSerializationContext
        from boring_semantic_layer.serialization.reconstruct import (
            reconstruct_bsl_operation,
        )

        with pytest.raises(
            ValueError,
            match="Unknown BSL operation type: SemanticMutateOp",
        ):
            reconstruct_bsl_operation(
                {"bsl_op_type": "SemanticMutateOp"},
                None,
                BSLSerializationContext(),
            )

    def test_mutated_aggregate_roundtrips_through_tagged(self, flights_model):
        pytest.importorskip("xorq", reason="xorq not installed")
        expr = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
        )
        expected = expr.execute().sort_values("carrier").reset_index(drop=True)

        from boring_semantic_layer.serialization import from_tagged, to_tagged

        rebuilt = from_tagged(to_tagged(expr))
        actual = rebuilt.execute().sort_values("carrier").reset_index(drop=True)
        pd.testing.assert_frame_equal(expected, actual)
