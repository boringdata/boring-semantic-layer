"""
Tests for dimension-only queries on joined star-schema tables.

Verifies that querying only dimensions (no measures) from a joined
semantic table returns all dimension members, including those with
no matching fact rows.  (Fixes #224.)
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def star_schema():
    """Star schema with fact table joined to dimension tables.

    Stores dim has 4 stores, but only 2 appear in the fact table.
    Items dim has 3 items, but only 2 appear in the fact table.
    """
    con = ibis.duckdb.connect(":memory:")

    stores_df = pd.DataFrame(
        {
            "store_sk": [1, 2, 3, 4],
            "store_name": ["Alpha", "Beta", "Gamma", "Delta"],
            "city": ["Zurich", "Geneva", "Basel", "Bern"],
        }
    )

    items_df = pd.DataFrame(
        {
            "item_sk": [10, 20, 30],
            "item_name": ["Widget", "Gadget", "Doohickey"],
        }
    )

    # Only stores 1,2 and items 10,20 appear in fact rows
    transactions_df = pd.DataFrame(
        {
            "txn_id": [1, 2, 3, 4],
            "store_sk": [1, 1, 2, 2],
            "item_sk": [10, 20, 10, 20],
            "amount": [100.0, 200.0, 150.0, 250.0],
        }
    )

    stores_tbl = con.create_table("stores", stores_df)
    items_tbl = con.create_table("items", items_df)
    txn_tbl = con.create_table("transactions", transactions_df)

    stores_st = (
        to_semantic_table(stores_tbl, "stores")
        .with_dimensions(
            store_name=lambda t: t.store_name,
            city=lambda t: t.city,
        )
    )

    items_st = (
        to_semantic_table(items_tbl, "items")
        .with_dimensions(item_name=lambda t: t.item_name)
    )

    txn_st = (
        to_semantic_table(txn_tbl, "transactions")
        .with_dimensions(store_sk=lambda t: t.store_sk)
        .with_measures(total_sales=lambda t: t.amount.sum())
    )

    joined = (
        txn_st
        .join_one(stores_st, lambda l, r: l.store_sk == r.store_sk)
        .join_one(items_st, lambda l, r: l.item_sk == r.item_sk)
    )

    return joined, stores_st, items_st, txn_st


class TestDimensionOnlyQueryReturnsAllMembers:
    """Dimension-only queries on joined tables must return all members."""

    def test_single_dimension_all_stores(self, star_schema):
        """All 4 stores should appear, not just the 2 in the fact table."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name"],
                order_by=[("stores.store_name", "asc")],
            )
            .execute()
        )
        store_names = sorted(result["stores.store_name"].tolist())
        assert store_names == ["Alpha", "Beta", "Delta", "Gamma"]

    def test_multiple_dimensions_same_table(self, star_schema):
        """Multiple dims from the same table should still use the shortcut."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name", "stores.city"],
                order_by=[("stores.store_name", "asc")],
            )
            .execute()
        )
        assert len(result) == 4
        assert sorted(result["stores.store_name"].tolist()) == [
            "Alpha", "Beta", "Delta", "Gamma",
        ]
        assert sorted(result["stores.city"].tolist()) == [
            "Basel", "Bern", "Geneva", "Zurich",
        ]

    def test_single_dimension_all_items(self, star_schema):
        """All 3 items should appear, not just the 2 in the fact table."""
        joined, *_ = star_schema
        result = (
            joined.query(dimensions=["items.item_name"])
            .execute()
        )
        item_names = sorted(result["items.item_name"].tolist())
        assert item_names == ["Doohickey", "Gadget", "Widget"]

    def test_dimensions_across_tables_uses_standard_path(self, star_schema):
        """Dims from multiple tables should NOT use the shortcut (standard join)."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name", "items.item_name"],
                order_by=[("stores.store_name", "asc")],
            )
            .execute()
        )
        # Only combinations present in fact rows appear (standard join behavior)
        assert len(result) == 4  # 2 stores x 2 items from fact

    def test_with_measures_uses_standard_path(self, star_schema):
        """When measures are present, the standard path should be used."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name"],
                measures=["transactions.total_sales"],
                order_by=[("stores.store_name", "asc")],
            )
            .execute()
        )
        # Standard join behavior: only stores with sales appear
        assert len(result) == 2
        assert sorted(result["stores.store_name"].tolist()) == ["Alpha", "Beta"]

    def test_column_names_are_prefixed(self, star_schema):
        """Result columns should have dotted (prefixed) names."""
        joined, *_ = star_schema
        result = (
            joined.query(dimensions=["stores.store_name"]).execute()
        )
        assert "stores.store_name" in result.columns


class TestDimensionOnlyWithFilters:
    """Dimension-only shortcut should handle filters correctly."""

    def test_filter_on_target_dim_table(self, star_schema):
        """Filter referencing only the target dim table should still use shortcut."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name"],
                filters=[lambda t: t.store_name != "Delta"],
                order_by=[("stores.store_name", "asc")],
            )
            .execute()
        )
        store_names = sorted(result["stores.store_name"].tolist())
        # All stores except Delta — Gamma still present despite zero fact rows
        assert store_names == ["Alpha", "Beta", "Gamma"]

    def test_filter_on_other_table_disables_shortcut(self, star_schema):
        """Filter referencing another table should disable the shortcut."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name"],
                filters=[lambda t: t.amount > 0],
            )
            .execute()
        )
        # Falls back to standard join path — only fact-present stores
        store_names = sorted(result["stores.store_name"].tolist())
        assert store_names == ["Alpha", "Beta"]


class TestDimensionOnlyDerived:
    """Shortcut handles derived dimensions within the same table."""

    def test_derived_dimension_on_same_table(self):
        """Derived dim (depends on another dim in same table) should work."""
        con = ibis.duckdb.connect(":memory:")

        regions_df = pd.DataFrame(
            {
                "region_id": [1, 2, 3],
                "region_name": ["North", "South", "East"],
            }
        )
        sales_df = pd.DataFrame(
            {
                "sale_id": [1, 2],
                "region_id": [1, 2],
                "revenue": [100.0, 200.0],
            }
        )

        region_tbl = con.create_table("regions", regions_df)
        sales_tbl = con.create_table("sales", sales_df)

        region_st = (
            to_semantic_table(region_tbl, "regions")
            .with_dimensions(
                region_name=lambda t: t.region_name,
                region_upper=lambda t: t.region_name.upper(),
            )
        )
        sales_st = (
            to_semantic_table(sales_tbl, "sales")
            .with_measures(total_revenue=lambda t: t.revenue.sum())
        )

        joined = sales_st.join_one(
            region_st, lambda l, r: l.region_id == r.region_id
        )

        result = joined.query(dimensions=["regions.region_upper"]).execute()
        names = sorted(result["regions.region_upper"].tolist())
        # All 3 regions including East (no matching sales)
        assert names == ["EAST", "NORTH", "SOUTH"]


class TestDimensionOnlySqlVerification:
    """Verify the shortcut actually queries the dimension table directly."""

    def test_sql_does_not_contain_fact_table(self, star_schema):
        """Generated SQL should reference only the dim table, not the fact."""
        joined, *_ = star_schema
        sql = joined.query(dimensions=["stores.store_name"]).sql()
        sql_lower = sql.lower()
        # Should reference the stores table
        assert "stores" in sql_lower
        # Should NOT reference the transactions fact table
        assert "transactions" not in sql_lower

    def test_sql_uses_distinct(self, star_schema):
        """Generated SQL should produce distinct dimension values."""
        joined, *_ = star_schema
        sql = joined.query(dimensions=["stores.store_name"]).sql()
        assert "DISTINCT" in sql.upper()


class TestDimensionOnlyFactTableDims:
    """Shortcut should work when querying dims from the fact (left) table."""

    def test_fact_table_dimension_returns_all_members(self, star_schema):
        """Querying a fact-table dimension via shortcut returns all its values."""
        joined, *_ = star_schema
        result = (
            joined.query(dimensions=["transactions.store_sk"]).execute()
        )
        sks = sorted(result["transactions.store_sk"].tolist())
        # Only fact-table SKs (1, 2) — the fact table only has those rows
        assert sks == [1, 2]


class TestDimensionOnlyJsonFilter:
    """JSON dict filters with the shortcut."""

    def test_json_filter_falls_back_to_standard_path(self, star_schema):
        """JSON dict filters use deferred resolution that prevents column
        extraction, so the shortcut disables and falls back to the standard
        join path.  Only fact-present dimension values are returned."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.city"],
                filters=[{"field": "stores.city", "operator": "!=", "value": "Bern"}],
                order_by=[("stores.city", "asc")],
            )
            .execute()
        )
        cities = sorted(result["stores.city"].tolist())
        # Standard path: only fact-present cities minus the filtered one
        assert cities == ["Geneva", "Zurich"]


class TestDimensionOnlyWithLimit:
    """Limit composes correctly with the shortcut."""

    def test_limit_with_shortcut(self, star_schema):
        """Limit applied after shortcut aggregate should restrict rows."""
        joined, *_ = star_schema
        result = (
            joined.query(
                dimensions=["stores.store_name"],
                order_by=[("stores.store_name", "asc")],
                limit=2,
            )
            .execute()
        )
        assert len(result) == 2
        assert result["stores.store_name"].tolist() == ["Alpha", "Beta"]


class TestDimensionOnlyMethodChaining:
    """Method-chaining API (group_by/aggregate) shares the same shortcut."""

    def test_group_by_aggregate_returns_all_members(self, star_schema):
        """group_by().aggregate() with no measures uses the shortcut."""
        joined, *_ = star_schema
        result = (
            joined.group_by("stores.store_name")
            .aggregate()
            .execute()
        )
        store_names = sorted(result["stores.store_name"].tolist())
        assert store_names == ["Alpha", "Beta", "Delta", "Gamma"]


class TestDimensionOnlyJoinMany:
    """Dimension-only shortcut should work with join_many too."""

    def test_dimension_only_on_join_many(self):
        """Dimension-only query on a join_many should still return all members."""
        con = ibis.duckdb.connect(":memory:")

        categories_df = pd.DataFrame(
            {
                "cat_id": [1, 2, 3],
                "cat_name": ["Electronics", "Books", "Sports"],
            }
        )

        products_df = pd.DataFrame(
            {
                "prod_id": [1, 2],
                "cat_id": [1, 1],
                "price": [10.0, 20.0],
            }
        )

        cat_tbl = con.create_table("categories", categories_df)
        prod_tbl = con.create_table("products", products_df)

        cat_st = (
            to_semantic_table(cat_tbl, "categories")
            .with_dimensions(cat_name=lambda t: t.cat_name)
        )

        prod_st = (
            to_semantic_table(prod_tbl, "products")
            .with_measures(avg_price=lambda t: t.price.mean())
        )

        joined = prod_st.join_many(
            cat_st, lambda l, r: l.cat_id == r.cat_id
        )

        result = joined.query(dimensions=["categories.cat_name"]).execute()
        cat_names = sorted(result["categories.cat_name"].tolist())
        assert cat_names == ["Books", "Electronics", "Sports"]
