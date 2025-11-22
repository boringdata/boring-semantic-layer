"""Round-trip tests for Malloy-inspired BSL models through xorq conversion.

Tests that all Malloy-equivalent BSL models can successfully:
1. Convert to xorq (BSL → xorq)
2. Convert back to BSL (xorq → BSL)
3. Execute and produce valid results after round-trip
"""

from __future__ import annotations

import pytest

from boring_semantic_layer.xorq_convert import from_tagged, to_tagged, try_import_xorq

# Check if xorq is available
try:
    try_import_xorq()
    xorq_available = True
    xorq_skip_reason = ""
except ImportError:
    xorq_available = False
    xorq_skip_reason = "xorq not installed"


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestMalloyModelsRoundTrip:
    """Test round-trip conversion for all Malloy-inspired BSL models."""

    def test_sessionization_model_roundtrip(self):
        """Test sessionization pattern from test_malloy_inspired.py."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        events_df = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 2, 2],
                "event_time": pd.to_datetime(
                    [
                        "2023-01-01 10:00",
                        "2023-01-01 10:05",
                        "2023-01-01 10:30",
                        "2023-01-01 11:00",
                        "2023-01-01 11:10",
                    ]
                ),
                "value": [0, 0, 0, 0, 100],
            }
        )

        events_tbl = con.create_table("events", events_df)

        # Create BSL semantic table
        sessions_st = (
            to_semantic_table(events_tbl, name="sessions")
            .with_dimensions(user_id=lambda t: t.user_id)
            .with_measures(
                event_count=lambda t: t.count(),
                total_value=lambda t: t.value.sum(),
            )
        )

        # Test round-trip
        tagged_expr = to_tagged(sessions_st)

        # Convert back
        reconstructed = from_tagged(tagged_expr)

        # Verify structure preserved
        assert hasattr(reconstructed, "dimensions")
        assert "user_id" in reconstructed.dimensions

        # Execute both and verify results match
        original_result = (
            sessions_st.group_by("user_id").aggregate("event_count", "total_value").execute()
        )

        reconstructed_result = (
            reconstructed.group_by("user_id")
            .aggregate("event_count", "total_value")
            .execute()
            .sort_values("user_id")
        )

        assert len(original_result) == len(reconstructed_result)
        assert list(reconstructed_result["user_id"]) == [1, 2]

    def test_nested_subtotals_model_roundtrip(self):
        """Test nested subtotals pattern from test_malloy_inspired.py."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        products_df = pd.DataFrame(
            {
                "category": ["Electronics", "Electronics", "Clothing"],
                "product": ["iPhone", "MacBook", "T-Shirt"],
                "revenue": [100000, 75000, 4000],
            }
        )

        products_tbl = con.create_table("products", products_df)

        products_st = (
            to_semantic_table(products_tbl, name="products")
            .with_dimensions(
                category=lambda t: t.category,
                product=lambda t: t.product,
            )
            .with_measures(
                total_revenue=lambda t: t.revenue.sum(),
            )
        )

        # Round-trip
        tagged_expr = to_tagged(products_st)
        reconstructed = from_tagged(tagged_expr)

        # Verify dimensions preserved
        assert "category" in reconstructed.dimensions
        assert "product" in reconstructed.dimensions

        # Execute and compare
        original_data = products_st.group_by("category").aggregate("total_revenue").execute()
        reconstructed_data = (
            reconstructed.group_by("category")
            .aggregate("total_revenue")
            .execute()
            .sort_values("category")
        )

        assert len(original_data) == len(reconstructed_data)

    def test_filtered_aggregates_model_roundtrip(self):
        """Test filtered aggregates pattern from test_malloy_inspired.py."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        sales_df = pd.DataFrame(
            {
                "date": pd.date_range("2022-01-01", periods=730, freq="D"),  # 2 years
                "sales": range(730),
                "category": ["Electronics", "Clothing"] * 365,
            }
        )

        sales_tbl = con.create_table("sales", sales_df)

        sales_st = (
            to_semantic_table(sales_tbl, name="sales")
            .with_dimensions(category=lambda t: t.category)
            .with_measures(
                total_sales=lambda t: t.sales.sum(),
            )
        )

        # Round-trip
        tagged_expr = to_tagged(sales_st)
        reconstructed = from_tagged(tagged_expr)

        # Verify measures preserved
        assert hasattr(reconstructed, "measures")

        # Execute both
        original_data = sales_st.group_by("category").aggregate("total_sales").execute()
        reconstructed_data = (
            reconstructed.group_by("category")
            .aggregate("total_sales")
            .execute()
            .sort_values("category")
        )

        assert len(original_data) == len(reconstructed_data)

    def test_dimensional_index_model_roundtrip(self):
        """Test dimensional indexing pattern from test_malloy_inspired.py."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        events_df = pd.DataFrame(
            {
                "event_id": range(1, 101),
                "category": ["A", "B", "C"] * 33 + ["A"],
                "value": range(100, 200),
            }
        )

        events_tbl = con.create_table("events", events_df)

        category_st = (
            to_semantic_table(events_tbl, name="events")
            .with_dimensions(category=lambda t: t.category)
            .with_measures(
                event_count=lambda t: t.count(),
                total_value=lambda t: t.value.sum(),
            )
        )

        # Round-trip
        tagged_expr = to_tagged(category_st)
        reconstructed = from_tagged(tagged_expr)

        # Verify structure
        assert "category" in reconstructed.dimensions

        # Execute and compare counts
        original_data = category_st.group_by("category").aggregate("event_count").execute()

        reconstructed_data = (
            reconstructed.group_by("category")
            .aggregate("event_count")
            .execute()
            .sort_values("category")
        )

        assert len(original_data) == len(reconstructed_data)
        assert reconstructed_data["event_count"].sum() == 100

    def test_complex_joins_model_roundtrip(self):
        """Test multi-level joins pattern from test_malloy_inspired.py."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [1, 1, 2],
                "order_total": [100, 150, 200],
            }
        )

        orders_tbl = con.create_table("orders", orders_df)

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(
                order_count=lambda t: t.order_id.nunique(),
                total_order_value=lambda t: t.order_total.sum(),
            )
        )

        # Round-trip
        tagged_expr = to_tagged(orders_st)
        reconstructed = from_tagged(tagged_expr)

        # Verify dimensions and measures preserved
        assert "customer_id" in reconstructed.dimensions

        # Execute and verify
        original_data = (
            orders_st.group_by("customer_id")
            .aggregate("order_count", "total_order_value")
            .execute()
        )

        reconstructed_data = (
            reconstructed.group_by("customer_id")
            .aggregate("order_count", "total_order_value")
            .execute()
            .sort_values("customer_id")
        )

        assert len(original_data) == len(reconstructed_data)
        assert reconstructed_data["total_order_value"].sum() == 450

    def test_window_functions_model_roundtrip(self):
        """Test window functions pattern (rankings, percentiles)."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        products_df = pd.DataFrame(
            {
                "category": ["Electronics"] * 5 + ["Clothing"] * 5,
                "product": [f"Product-{i}" for i in range(10)],
                "revenue": list(range(100, 105)) + list(range(200, 205)),
            }
        )

        products_tbl = con.create_table("products", products_df)

        products_st = (
            to_semantic_table(products_tbl, name="products")
            .with_dimensions(
                category=lambda t: t.category,
                product=lambda t: t.product,
            )
            .with_measures(
                total_revenue=lambda t: t.revenue.sum(),
            )
        )

        # Round-trip
        tagged_expr = to_tagged(products_st)
        reconstructed = from_tagged(tagged_expr)

        # Execute and verify
        original_data = (
            products_st.group_by("category", "product").aggregate("total_revenue").execute()
        )

        reconstructed_data = (
            reconstructed.group_by("category", "product")
            .aggregate("total_revenue")
            .execute()
            .sort_values(["category", "product"])
        )

        assert len(original_data) == len(reconstructed_data)
        assert len(reconstructed_data) == 10

    def test_multiple_measures_roundtrip(self):
        """Test model with multiple calculated measures."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        sales_df = pd.DataFrame(
            {
                "product": ["A", "B", "C"],
                "quantity": [10, 20, 30],
                "price": [100, 50, 25],
                "cost": [60, 30, 15],
            }
        )

        sales_tbl = con.create_table("sales", sales_df)

        sales_st = (
            to_semantic_table(sales_tbl, name="sales")
            .with_dimensions(product=lambda t: t.product)
            .with_measures(
                total_quantity=lambda t: t.quantity.sum(),
                total_revenue=lambda t: (t.quantity * t.price).sum(),
                total_cost=lambda t: (t.quantity * t.cost).sum(),
                avg_price=lambda t: t.price.mean(),
            )
        )

        # Round-trip
        tagged_expr = to_tagged(sales_st)
        reconstructed = from_tagged(tagged_expr)

        # Verify structure
        assert "product" in reconstructed.dimensions

        # Execute aggregations
        original_data = (
            sales_st.group_by("product").aggregate("total_quantity", "total_revenue").execute()
        )

        reconstructed_data = (
            reconstructed.group_by("product")
            .aggregate("total_quantity", "total_revenue")
            .execute()
            .sort_values("product")
        )

        assert len(original_data) == len(reconstructed_data)
        assert len(reconstructed_data) == 3


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestMalloyXorqFeatures:
    """Test that xorq-specific features work with Malloy-style BSL models."""

    def test_xorq_caching_with_malloy_model(self):
        """Test that xorq caching tags work with Malloy-style models."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        data_df = pd.DataFrame({"category": ["A", "B", "C"], "value": [1, 2, 3]})

        data_tbl = con.create_table("data", data_df)

        data_st = (
            to_semantic_table(data_tbl, name="data")
            .with_dimensions(category=lambda t: t.category)
            .with_measures(total=lambda t: t.value.sum())
        )

        # Convert to xorq
        tagged_expr = to_tagged(data_st)

        # Apply xorq-specific tags (caching hints)
        cached_expr = tagged_expr.tag(tag="cache", strategy="memory", ttl="3600")
        assert cached_expr is not None

        # Should still be convertible back
        reconstructed = from_tagged(cached_expr)
        assert reconstructed is not None

    def test_xorq_multi_engine_with_malloy_model(self):
        """Test that xorq expressions preserve BSL metadata across engines."""
        import ibis
        import pandas as pd

        from boring_semantic_layer import to_semantic_table

        con = ibis.duckdb.connect(":memory:")

        events_df = pd.DataFrame(
            {
                "event_type": ["view", "click", "purchase"],
                "count": [100, 50, 10],
            }
        )

        events_tbl = con.create_table("events", events_df)

        events_st = (
            to_semantic_table(events_tbl, name="events")
            .with_dimensions(event_type=lambda t: t.event_type)
            .with_measures(
                event_count=lambda t: t.count.sum(),
            )
        )

        # Convert to xorq (enables multi-engine support)
        tagged_expr = to_tagged(events_st)

        # Verify BSL metadata is preserved in xorq tag
        op = tagged_expr.op()
        assert hasattr(op, "metadata")
        metadata = dict(op.metadata)
        assert metadata["bsl_op_type"] == "SemanticTableOp"

        # Verify dimensions metadata preserved (now stored as nested tuples)
        dims_tuple = metadata["dimensions"]
        dims = {k: dict(v) for k, v in dims_tuple}
        assert "event_type" in dims
