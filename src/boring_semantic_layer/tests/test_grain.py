"""Tests for grain-aware join_one using is_entity dimensions.

When both models declare is_entity dimensions and their entity sets differ,
join_one auto-upgrades to join_many so BSL's pre-aggregation logic aligns
the grains before joining — preventing fan-out / double-counting.
"""

import warnings

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import Dimension, to_semantic_table


@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def multi_fact_tables(con):
    """Monthly financials + daily hours — classic multi-fact grain mismatch."""
    financials_df = pd.DataFrame(
        {
            "year": [2024, 2024, 2024],
            "month": [1, 2, 3],
            "revenue": [10000.0, 12000.0, 11000.0],
        }
    )
    hours_df = pd.DataFrame(
        {
            "year": [2024, 2024, 2024, 2024, 2024, 2024],
            "month": [1, 1, 2, 2, 3, 3],
            "day": [1, 15, 1, 15, 1, 15],
            "hours_worked": [80.0, 85.0, 90.0, 75.0, 88.0, 82.0],
        }
    )
    return {
        "financials": con.create_table("financials", financials_df),
        "hours": con.create_table("hours", hours_df),
    }


def _make_financials_model(tbl):
    return (
        to_semantic_table(tbl, name="financials")
        .with_dimensions(
            year=Dimension(expr=lambda t: t.year, is_entity=True),
            month=Dimension(expr=lambda t: t.month, is_entity=True),
        )
        .with_measures(total_revenue=lambda t: t.revenue.sum())
    )


def _make_hours_model(tbl):
    return (
        to_semantic_table(tbl, name="hours")
        .with_dimensions(
            year=Dimension(expr=lambda t: t.year, is_entity=True),
            month=Dimension(expr=lambda t: t.month, is_entity=True),
            day=Dimension(expr=lambda t: t.day, is_entity=True),
        )
        .with_measures(total_hours=lambda t: t.hours_worked.sum())
    )


class TestGrainMismatchDetection:
    """Test that join_one detects grain mismatch via is_entity dims."""

    def test_different_entity_dims_upgrades_to_many(self, multi_fact_tables):
        """join_one auto-upgrades to join_many when entity sets differ."""
        fin = _make_financials_model(multi_fact_tables["financials"])
        hrs = _make_hours_model(multi_fact_tables["hours"])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            joined = fin.join_one(
                hrs, on=lambda f, h: (f.year == h.year) & (f.month == h.month)
            )
            assert len(w) == 1
            assert "Grain mismatch" in str(w[0].message)
            assert "join_many" in str(w[0].message)

        # Verify the join has cardinality="many" internally
        assert joined.op().cardinality == "many"

    def test_same_entity_dims_stays_one(self, multi_fact_tables):
        """join_one stays join_one when both sides have the same entity dims."""
        fin = _make_financials_model(multi_fact_tables["financials"])

        # Create another model with same grain (year, month)
        other = (
            to_semantic_table(multi_fact_tables["hours"], name="hours_agg")
            .with_dimensions(
                year=Dimension(expr=lambda t: t.year, is_entity=True),
                month=Dimension(expr=lambda t: t.month, is_entity=True),
            )
            .with_measures(total_hours=lambda t: t.hours_worked.sum())
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            joined = fin.join_one(
                other, on=lambda f, h: (f.year == h.year) & (f.month == h.month)
            )
            grain_warnings = [x for x in w if "Grain mismatch" in str(x.message)]
            assert len(grain_warnings) == 0

        assert joined.op().cardinality == "one"

    def test_no_entity_dims_stays_one(self, multi_fact_tables):
        """join_one stays join_one when neither side has entity dims (backward compat)."""
        fin = (
            to_semantic_table(multi_fact_tables["financials"], name="financials")
            .with_dimensions(
                year=lambda t: t.year,
                month=lambda t: t.month,
            )
            .with_measures(total_revenue=lambda t: t.revenue.sum())
        )
        hrs = (
            to_semantic_table(multi_fact_tables["hours"], name="hours")
            .with_dimensions(
                year=lambda t: t.year,
                month=lambda t: t.month,
                day=lambda t: t.day,
            )
            .with_measures(total_hours=lambda t: t.hours_worked.sum())
        )

        joined = fin.join_one(
            hrs, on=lambda f, h: (f.year == h.year) & (f.month == h.month)
        )
        assert joined.op().cardinality == "one"

    def test_one_side_has_entities_other_doesnt_stays_one(self, multi_fact_tables):
        """join_one stays join_one when only one side has entity dims."""
        fin = _make_financials_model(multi_fact_tables["financials"])
        hrs = (
            to_semantic_table(multi_fact_tables["hours"], name="hours")
            .with_dimensions(
                year=lambda t: t.year,
                month=lambda t: t.month,
                day=lambda t: t.day,
            )
            .with_measures(total_hours=lambda t: t.hours_worked.sum())
        )

        joined = fin.join_one(
            hrs, on=lambda f, h: (f.year == h.year) & (f.month == h.month)
        )
        assert joined.op().cardinality == "one"


class TestGrainAwareQueryResults:
    """Test that grain-aware joins produce correct aggregated results."""

    def test_multi_fact_no_fanout(self, multi_fact_tables):
        """Monthly revenue + daily hours: no fan-out, correct totals per month."""
        fin = _make_financials_model(multi_fact_tables["financials"])
        hrs = _make_hours_model(multi_fact_tables["hours"])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            combined = fin.join_one(
                hrs, on=lambda f, h: (f.year == h.year) & (f.month == h.month)
            )

        result = (
            combined.group_by("financials.month")
            .aggregate("financials.total_revenue", "hours.total_hours")
            .execute()
            .sort_values("financials.month")
            .reset_index(drop=True)
        )

        # Revenue should NOT be multiplied by number of days
        # financials has 3 months: 10000, 12000, 11000
        assert list(result["financials.total_revenue"]) == [10000.0, 12000.0, 11000.0]

        # Hours should be correctly summed per month
        # month 1: 80+85=165, month 2: 90+75=165, month 3: 88+82=170
        assert list(result["hours.total_hours"]) == [165.0, 165.0, 170.0]

    def test_multi_fact_scalar_aggregate(self, multi_fact_tables):
        """Total revenue + total hours across all months — no group by."""
        fin = _make_financials_model(multi_fact_tables["financials"])
        hrs = _make_hours_model(multi_fact_tables["hours"])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            combined = fin.join_one(
                hrs, on=lambda f, h: (f.year == h.year) & (f.month == h.month)
            )

        result = combined.aggregate(
            "financials.total_revenue", "hours.total_hours"
        ).execute()

        # Total revenue: 10000 + 12000 + 11000 = 33000
        assert result["financials.total_revenue"].iloc[0] == 33000.0
        # Total hours: 80+85+90+75+88+82 = 500
        assert result["hours.total_hours"].iloc[0] == 500.0
