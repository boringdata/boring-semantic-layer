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
