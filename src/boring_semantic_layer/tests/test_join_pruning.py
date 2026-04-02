"""Tests for join pruning — only join tables whose columns are needed.

When a query only uses measures/dimensions from a subset of joined tables,
BSL should skip joining the unused dimension tables.  This avoids expensive
joins that don't contribute to the result.  (Fixes #227.)
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import Dimension, to_semantic_table


@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def star_schema(con):
    """Classic star schema: fact table + 3 dimension tables."""
    facts = con.create_table(
        "facts",
        pd.DataFrame(
            {
                "date_id": [1, 2, 3, 1, 2],
                "store_id": [10, 20, 10, 20, 10],
                "item_id": [100, 100, 200, 200, 100],
                "sale_amount": [10.0, 20.0, 30.0, 40.0, 50.0],
                "quantity": [1, 2, 3, 4, 5],
            }
        ),
    )
    dates = con.create_table(
        "dates",
        pd.DataFrame(
            {
                "date_id": [1, 2, 3],
                "date_name": ["Jan", "Feb", "Mar"],
                "quarter": ["Q1", "Q1", "Q1"],
            }
        ),
    )
    stores = con.create_table(
        "stores",
        pd.DataFrame(
            {
                "store_id": [10, 20],
                "store_name": ["Downtown", "Mall"],
                "city": ["NYC", "LA"],
            }
        ),
    )
    items = con.create_table(
        "items",
        pd.DataFrame(
            {
                "item_id": [100, 200],
                "item_name": ["Widget", "Gadget"],
                "category": ["A", "B"],
            }
        ),
    )
    return {"facts": facts, "dates": dates, "stores": stores, "items": items}


def _build_star_model(star_schema):
    """Build a joined model with fact + 3 dimension tables."""
    facts = (
        to_semantic_table(star_schema["facts"], name="facts")
        .with_dimensions(
            date_id=lambda t: t.date_id,
            store_id=lambda t: t.store_id,
            item_id=lambda t: t.item_id,
        )
        .with_measures(
            total_sales=lambda t: t.sale_amount.sum(),
            total_qty=lambda t: t.quantity.sum(),
        )
    )
    dates = (
        to_semantic_table(star_schema["dates"], name="dates")
        .with_dimensions(
            date_id=lambda t: t.date_id,
            date_name=lambda t: t.date_name,
            quarter=lambda t: t.quarter,
        )
    )
    stores = (
        to_semantic_table(star_schema["stores"], name="stores")
        .with_dimensions(
            store_id=lambda t: t.store_id,
            store_name=lambda t: t.store_name,
            city=lambda t: t.city,
        )
    )
    items = (
        to_semantic_table(star_schema["items"], name="items")
        .with_dimensions(
            item_id=lambda t: t.item_id,
            item_name=lambda t: t.item_name,
            category=lambda t: t.category,
        )
    )

    return (
        facts.join_one(dates, on=lambda f, d: f.date_id == d.date_id)
        .join_one(stores, on=lambda f, s: f.store_id == s.store_id)
        .join_one(items, on=lambda f, i: f.item_id == i.item_id)
    )


class TestJoinPruningMeasureOnly:
    """Measure-only queries should not join any dimension tables."""

    def test_aggregate_no_dimensions(self, star_schema):
        """total_sales with no group-by should NOT join dimension tables."""
        model = _build_star_model(star_schema)
        result = model.aggregate("facts.total_sales").execute()

        # 10 + 20 + 30 + 40 + 50 = 150
        assert result["facts.total_sales"].iloc[0] == 150.0

    def test_sql_excludes_unused_tables(self, star_schema):
        """Compiled SQL should not reference dimension tables for measure-only query."""
        model = _build_star_model(star_schema)
        expr = model.aggregate("facts.total_sales")
        # to_untagged() returns the ibis expression we can compile
        ibis_expr = expr.op().to_untagged()
        sql = str(ibis.to_sql(ibis_expr)).lower()

        # The SQL should NOT contain joins to dates, stores, or items
        assert "dates" not in sql
        assert "stores" not in sql
        assert "items" not in sql

    def test_multiple_measures_same_table(self, star_schema):
        """Multiple measures from fact table — still no dimension joins needed."""
        model = _build_star_model(star_schema)
        result = model.aggregate("facts.total_sales", "facts.total_qty").execute()

        assert result["facts.total_sales"].iloc[0] == 150.0
        assert result["facts.total_qty"].iloc[0] == 15


class TestJoinPruningSingleDimension:
    """Queries using dimensions from one table should only join that table."""

    def test_group_by_date_only(self, star_schema):
        """Group by date dimension — only date table should be joined."""
        model = _build_star_model(star_schema)
        result = (
            model.group_by("dates.date_name")
            .aggregate("facts.total_sales")
            .execute()
            .sort_values("dates.date_name")
            .reset_index(drop=True)
        )

        assert len(result) == 3
        assert list(result["dates.date_name"]) == ["Feb", "Jan", "Mar"]
        # Jan: 10+40=50, Feb: 20+50=70, Mar: 30
        assert list(result["facts.total_sales"]) == [70.0, 50.0, 30.0]

    def test_group_by_store_only(self, star_schema):
        """Group by store dimension — only store table should be joined."""
        model = _build_star_model(star_schema)
        result = (
            model.group_by("stores.store_name")
            .aggregate("facts.total_sales")
            .execute()
            .sort_values("stores.store_name")
            .reset_index(drop=True)
        )

        assert len(result) == 2
        # Downtown (store_id=10): 10+30+50=90, Mall (store_id=20): 20+40=60
        assert list(result["stores.store_name"]) == ["Downtown", "Mall"]
        assert list(result["facts.total_sales"]) == [90.0, 60.0]


class TestJoinPruningMultipleDimensions:
    """Queries using dimensions from multiple tables should join only those."""

    def test_group_by_date_and_store(self, star_schema):
        """Group by date + store — items table should be pruned."""
        model = _build_star_model(star_schema)
        result = (
            model.group_by("dates.date_name", "stores.store_name")
            .aggregate("facts.total_sales")
            .execute()
        )

        # Should have correct number of rows
        assert len(result) > 0
        # Total should still be 150
        assert result["facts.total_sales"].sum() == 150.0

    def test_all_dimensions_used(self, star_schema):
        """All dimension tables used — no pruning, same result as unpruned."""
        model = _build_star_model(star_schema)
        result = (
            model.group_by("dates.date_name", "stores.store_name", "items.item_name")
            .aggregate("facts.total_sales")
            .execute()
        )

        assert len(result) > 0
        assert result["facts.total_sales"].sum() == 150.0


class TestJoinPruningCorrectness:
    """Ensure pruned results match unpruned results."""

    def test_pruned_matches_unpruned_scalar(self, star_schema):
        """Scalar aggregate: pruned path must match full join path."""
        model = _build_star_model(star_schema)

        # Pruned: measure-only query
        pruned = model.aggregate("facts.total_sales").execute()

        # Manual: query directly on fact table
        manual = star_schema["facts"].aggregate(
            total_sales=star_schema["facts"].sale_amount.sum()
        ).execute()

        assert pruned["facts.total_sales"].iloc[0] == manual["total_sales"].iloc[0]

    def test_pruned_matches_unpruned_grouped(self, star_schema):
        """Grouped aggregate: pruned path must match full join path."""
        model = _build_star_model(star_schema)

        result = (
            model.group_by("stores.store_name")
            .aggregate("facts.total_sales")
            .execute()
            .sort_values("stores.store_name")
            .reset_index(drop=True)
        )

        # Manual join + aggregate
        facts = star_schema["facts"]
        stores = star_schema["stores"]
        manual = (
            facts.join(stores, facts.store_id == stores.store_id)
            .group_by("store_name")
            .aggregate(total_sales=lambda t: t.sale_amount.sum())
            .execute()
            .sort_values("store_name")
            .reset_index(drop=True)
        )

        assert list(result["facts.total_sales"]) == list(manual["total_sales"])
