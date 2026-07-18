"""Regression tests for the July 2026 round-4 soundness evaluation.

Each test pins a confirmed silent-wrong-answer defect (or its loud-error
replacement) against ground truth. Finding numbers reference the round-4
soundness report:

- R4-2  query() time filters evaluated against grain-truncated dimensions
- R4-3  date-only time_range ends excluded intra-day end-date rows
- R4-4  in/not-in dict filters iterating bare-string values as characters
- R4-5  partial date strings coerced with today's month/day
- R4-6  dimension shadowing a raw column: double-applied lambda filters,
        spelling-dependent results, measures over the mutated column
- R4-7  join_cross silently dropping requested group keys
- R4-9  base->calc measure redefinition silently serving the old measure
- R4-11 smallest_time_grain long form bypassing grain validation
- R4-12 limit=0 treated as "no limit"
- R4-13 order_by directions other than "desc" silently sorting ascending
- R4-14 bool-guard remediation unusable inside filter lambdas

The calc-window, nest=, and join_many-bridge findings are pinned in their
own suites (test_soundness_round4_{calc,nest,join_many}.py).
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def events(con):
    """Timestamps spanning month boundaries with intra-day times."""
    tbl = con.create_table(
        "events",
        pd.DataFrame(
            {
                "ts": pd.to_datetime(
                    [
                        "2025-01-05 09:00:00",
                        "2025-01-31 12:00:00",
                        "2025-02-10 08:00:00",
                        "2025-03-02 00:00:00",
                    ]
                ),
                "v": [1.0, 2.0, 3.0, 4.0],
            }
        ),
    )
    return (
        to_semantic_table(tbl, name="events")
        .with_dimensions(ts={"expr": lambda t: t.ts, "is_time_dimension": True})
        .with_measures(cnt=lambda t: t.count(), total=lambda t: t.v.sum())
    )


@pytest.fixture
def orders(con):
    tbl = con.create_table(
        "orders",
        pd.DataFrame(
            {
                "amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                "qty": [1, 2, 3, 4, 5, 6],
                "status": list("aabbcc"),
            }
        ),
    )
    return tbl


class TestTimeRangeVsGrain:
    """R4-2: range filters must compare raw values, not truncated buckets."""

    def test_month_grain_keeps_partially_covered_buckets(self, events):
        result = (
            events.query(
                dimensions=["ts"],
                measures=["cnt"],
                time_grain="TIME_GRAIN_MONTH",
                time_range={"start": "2025-01-15", "end": "2025-02-15"},
            )
            .execute()
            .sort_values("ts")
        )
        # Jan 31 12:00 is inside the range; its month bucket (Jan 1) is not.
        # The row must survive and appear in the January bucket.
        assert [str(d)[:10] for d in result["ts"]] == ["2025-01-01", "2025-02-01"]
        assert result["cnt"].tolist() == [1, 1]

    def test_lambda_filter_matches_chained_spelling(self, events):
        via_query = (
            events.query(
                dimensions=["ts"],
                measures=["cnt"],
                time_grain="TIME_GRAIN_MONTH",
                filters=[lambda t: (t.ts >= "2025-01-15") & (t.ts <= "2025-02-15")],
            )
            .execute()
            .sort_values("ts")
            .reset_index(drop=True)
        )
        chained = (
            events.with_dimensions(month=lambda t: t.ts.truncate("M"))
            .filter(lambda t: (t.ts >= "2025-01-15") & (t.ts <= "2025-02-15"))
            .group_by("month")
            .aggregate("cnt")
            .execute()
            .sort_values("month")
            .reset_index(drop=True)
        )
        assert via_query["cnt"].tolist() == chained["cnt"].tolist()
        assert len(via_query) == 2


class TestTimeRangeEndInclusivity:
    """R4-3: a date-only end covers the whole end day."""

    def test_date_only_end_includes_intraday_rows(self, events):
        result = events.query(
            dimensions=["ts"],
            measures=["cnt"],
            time_range={"start": "2025-01-01", "end": "2025-01-31"},
        ).execute()
        assert len(result) == 2  # Jan 5 and Jan 31 12:00

    def test_explicit_time_end_stays_inclusive_at_instant(self, events):
        result = events.query(
            dimensions=["ts"],
            measures=["cnt"],
            time_range={"start": "2025-01-01", "end": "2025-01-31 11:00:00"},
        ).execute()
        assert len(result) == 1  # only Jan 5


class TestDictFilterValueCoercion:
    def test_bare_string_values_rejected(self, con, orders):
        sm = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(status=lambda t: t.status)
            .with_measures(cnt=lambda t: t.count())
        )
        # R4-4: 'not in "gold"' previously excluded the characters g,o,l,d —
        # returning exactly the rows the user asked to remove.
        with pytest.raises(ValueError, match="list of values"):
            sm.query(
                measures=["cnt"],
                filters=[{"field": "status", "operator": "in", "values": "aa"}],
            )
        with pytest.raises(ValueError, match="list of values"):
            sm.query(
                measures=["cnt"],
                filters=[{"field": "status", "operator": "not in", "values": "aa"}],
            )

    def test_partial_date_strings_not_coerced(self, events):
        import duckdb

        # R4-5: "2024" was parsed with today's month/day, so results changed
        # depending on the day the query ran. Now it reaches the backend as a
        # plain string and fails loudly instead.
        with pytest.raises(duckdb.Error, match="timestamp"):
            events.query(
                dimensions=["ts"],
                measures=["cnt"],
                filters=[{"field": "ts", "operator": ">=", "value": "2024"}],
            ).execute()

    def test_full_iso_dates_still_coerced(self, events):
        result = events.query(
            dimensions=["ts"],
            measures=["cnt"],
            filters=[{"field": "ts", "operator": ">=", "value": "2025-02-01"}],
        ).execute()
        assert len(result) == 2


class TestDimensionShadowing:
    """R4-6: dimension sharing a raw column's name."""

    @pytest.fixture
    def shadowed(self, orders):
        return (
            to_semantic_table(orders, name="orders")
            .with_dimensions(amount=lambda t: t.amount * 2, status=lambda t: t.status)
            .with_measures(
                total=lambda t: t.amount.sum(),
                cnt=lambda t: t.count(),
                qty_sum=lambda t: t.qty.sum(),
            )
        )

    def test_filter_spellings_agree_and_apply_dimension_once(self, shadowed):
        # dim value = amount*2; > 55 keeps raw rows 30,40,50,60.
        # Measures aggregate RAW amounts: b -> 30+40, c -> 50+60.
        expected = [["b", 70.0], ["c", 110.0]]
        lam = (
            shadowed.filter(lambda t: t.amount > 55)
            .group_by("status")
            .aggregate("total")
            .execute()
        )
        dic = shadowed.query(
            dimensions=["status"],
            measures=["total"],
            filters=[{"field": "amount", "operator": ">", "value": 55}],
        ).execute()
        sst = shadowed.query(
            dimensions=["status"], measures=["total"], filters=["_.amount > 55"]
        ).execute()
        for frame in (lam, dic, sst):
            assert sorted(frame.values.tolist()) == expected

    def test_unfiltered_totals_use_raw_column(self, shadowed):
        result = shadowed.group_by().aggregate("total").execute()
        assert result["total"].tolist() == [210.0]

    def test_group_by_shadow_dim_with_conflicting_measure_raises(self, shadowed):
        with pytest.raises(ValueError, match="redefines column"):
            shadowed.group_by("amount").aggregate("total").execute()

    def test_group_by_shadow_dim_with_unrelated_measures_allowed(self, shadowed):
        result = (
            shadowed.group_by("amount").aggregate("cnt", "qty_sum").execute()
        ).sort_values("amount")
        assert result["amount"].tolist() == [20.0, 40.0, 60.0, 80.0, 100.0, 120.0]
        assert result["qty_sum"].tolist() == [1, 2, 3, 4, 5, 6]

    def test_identity_dimension_group_key_still_allowed(self, orders):
        sm = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(amount=lambda t: t.amount)
            .with_measures(total=lambda t: t.amount.sum())
        )
        result = sm.group_by("amount").aggregate("total").execute()
        assert sorted(result["total"].tolist()) == [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]


class TestJoinCrossGroupKeys:
    """R4-7: requested group keys must never be silently dropped."""

    @pytest.fixture
    def crossed(self, con):
        orders = con.create_table(
            "xorders",
            pd.DataFrame({"status": ["open", "open", "closed"], "amount": [10.0, 20.0, 30.0]}),
        )
        custs = con.create_table(
            "xcusts", pd.DataFrame({"cid": [1, 2, 3, 4], "region": ["e", "e", "w", "w"]})
        )
        o = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(status=lambda t: t.status)
            .with_measures(order_total=lambda t: t.amount.sum())
        )
        c = (
            to_semantic_table(custs, name="customers")
            .with_dimensions(region=lambda t: t.region)
            .with_measures(cust_count=lambda t: t.count())
        )
        return o.join_cross(c)

    def test_cross_group_key_with_other_side_measure_raises(self, crossed):
        with pytest.raises(ValueError, match="could not attach"):
            crossed.group_by("orders.status").aggregate("customers.cust_count").execute()

    def test_cross_grand_totals_stay_defanned(self, crossed):
        result = (
            crossed.group_by()
            .aggregate("orders.order_total", "customers.cust_count")
            .execute()
        )
        assert result["orders.order_total"].tolist() == [60.0]
        assert result["customers.cust_count"].tolist() == [4]


class TestMeasureRedefinition:
    """R4-9: redefinitions must take effect regardless of classification."""

    @pytest.fixture
    def base(self, orders):
        return to_semantic_table(orders, name="orders").with_measures(
            total=lambda t: t.amount.sum(), qty_sum=lambda t: t.qty.sum()
        )

    def test_base_to_calc_redefinition_wins(self, base):
        redefined = base.with_measures(total=lambda t: t.qty_sum * 1000)
        result = redefined.group_by().aggregate("total").execute()
        assert result["total"].tolist() == [21000]

    def test_self_referential_redefinition_raises(self, base):
        with pytest.raises(ValueError, match="in terms of itself"):
            base.with_measures(total=lambda t: t.total / 2)

    def test_same_kind_redefinition_still_wins(self, base):
        result = (
            base.with_measures(total=lambda t: t.amount.max())
            .group_by()
            .aggregate("total")
            .execute()
        )
        assert result["total"].tolist() == [60.0]


class TestGrainValidation:
    """R4-11: both smallest_time_grain spellings must validate."""

    @pytest.mark.parametrize("smallest", ["day", "TIME_GRAIN_DAY"])
    def test_finer_grain_rejected(self, con, smallest):
        tbl = con.create_table(
            f"g_{smallest.lower()}",
            pd.DataFrame({"ts": pd.to_datetime(["2025-01-01"]), "v": [1.0]}),
        )
        sm = (
            to_semantic_table(tbl, name="g")
            .with_dimensions(
                ts={
                    "expr": lambda t: t.ts,
                    "is_time_dimension": True,
                    "smallest_time_grain": smallest,
                }
            )
            .with_measures(cnt=lambda t: t.count())
        )
        with pytest.raises(ValueError, match="finer than the smallest"):
            sm.query(dimensions=["ts"], measures=["cnt"], time_grain="TIME_GRAIN_HOUR")


class TestLimitAndOrderValidation:
    def test_limit_zero_returns_zero_rows(self, events):
        # R4-12: `if limit:` treated 0 as "no limit" and returned everything.
        result = events.query(dimensions=["ts"], measures=["cnt"], limit=0).execute()
        assert len(result) == 0

    def test_bool_limit_rejected(self, events):
        with pytest.raises(ValueError, match="integer"):
            events.query(dimensions=["ts"], measures=["cnt"], limit=True)

    def test_descending_direction_accepted(self, events):
        result = events.query(
            dimensions=["ts"], measures=["total"], order_by=[("total", "descending")]
        ).execute()
        assert result["total"].tolist() == sorted(result["total"].tolist(), reverse=True)

    def test_unknown_direction_rejected(self, events):
        # R4-13: anything != "desc" silently sorted ascending.
        with pytest.raises(ValueError, match="Invalid order_by direction"):
            events.query(dimensions=["ts"], measures=["cnt"], order_by=[("cnt", "dsc")])


class TestBoolGuardRemediation:
    """R4-14: the guard's suggested escape hatch must work where it fires."""

    def test_flavored_constant_predicate_works_in_filter(self, orders):
        from boring_semantic_layer.nested_compile import get_ibis_module

        sm = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(status=lambda t: t.status)
            .with_measures(cnt=lambda t: t.count())
        )
        result = (
            sm.filter(lambda t: get_ibis_module(t).literal(True))
            .group_by("status")
            .aggregate("cnt")
            .execute()
        )
        assert result["cnt"].sum() == 6

    def test_bare_python_bool_still_raises_with_usable_advice(self, orders):
        sm = (
            to_semantic_table(orders, name="orders")
            .with_dimensions(status=lambda t: t.status)
            .with_measures(cnt=lambda t: t.count())
        )
        with pytest.raises(TypeError, match="literal"):
            sm.filter(lambda t: True).group_by("status").aggregate("cnt").execute()
