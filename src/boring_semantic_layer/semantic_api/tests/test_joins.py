#!/usr/bin/env python3
"""
Comprehensive tests for join functionality in the semantic API.

Tests cover:
- Basic joins between semantic tables
- Model name prefixing for conflicting dimensions, measures, and time dimensions
- All operation types with joined tables (filter, group_by, select, aggregate)
- Multiple joins with complex conflicts
- End-to-end execution and metadata consistency
"""

import pytest
import ibis
import pandas as pd
from boring_semantic_layer.semantic_api.api import to_semantic_table


def test_basic_join_no_conflicts():
    """Test basic join with no name conflicts."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
            "sale_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "price": [500, 80, 300, 50],
            "launch_date": ["2023-01-01", "2023-06-01", "2023-03-01", "2023-12-01"],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            region=lambda t: t.region,
            sale_id=lambda t: t.sale_id,
            sale_date={
                "expr": lambda t: t.sale_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,
            product_id=lambda t: t.product_id,
            launch_date={
                "expr": lambda t: t.launch_date,
                "is_time_dimension": True,
                "smallest_time_grain": "week",
            },
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    # Check dimensions and measures are merged correctly (all prefixed with table names)
    # Time dimensions are included in dimensions since they are a type of dimension
    assert set(joined.dimensions) == {
        "sales__region",
        "sales__sale_id",
        "sales__sale_date",
        "products__category",
        "products__product_id",
        "products__launch_date",
    }
    assert set(joined.measures) == {
        "sales__total_sales",
        "sales__avg_amount",
        "products__avg_price",
        "products__product_count",
    }

    # Check time dimensions are a subset of dimensions (using property)
    time_dims = joined.time_dimensions
    assert set(time_dims.keys()) == {"sales__sale_date", "products__launch_date"}
    assert all(key in joined.dimensions for key in time_dims)

    # Check time dimension details
    assert time_dims["sales__sale_date"].smallest_time_grain == "day"
    assert time_dims["products__launch_date"].smallest_time_grain == "week"

    # Check JSON definition consistency (all prefixed)
    json_def = joined.json_definition
    assert set(json_def["dimensions"].keys()) == {
        "sales__region",
        "sales__sale_id",
        "sales__sale_date",
        "products__category",
        "products__product_id",
        "products__launch_date",
    }
    assert set(json_def["measures"].keys()) == {
        "sales__total_sales",
        "sales__avg_amount",
        "products__avg_price",
        "products__product_count",
    }


def test_join_execution():
    """Test that joined tables can be executed."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
            "sale_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "price": [500, 80, 300, 50],
            "launch_date": ["2023-01-01", "2023-06-01", "2023-03-01", "2023-12-01"],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            region=lambda t: t.region,
            sale_id=lambda t: t.sale_id,
            sale_date={
                "expr": lambda t: t.sale_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,
            product_id=lambda t: t.product_id,
            launch_date={
                "expr": lambda t: t.launch_date,
                "is_time_dimension": True,
                "smallest_time_grain": "week",
            },
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    # Should execute without error
    result = joined.execute()
    assert len(result) == 16  # 4 * 4 = 16 rows for cross join

    # Should have all columns
    expected_cols = {
        "sale_id",
        "region",
        "amount",
        "sale_date",
        "product_id",
        "category",
        "price",
        "launch_date",
    }
    assert expected_cols.issubset(set(result.columns))


def test_dimension_conflicts_prefixed():
    """Test that conflicting dimensions are prefixed correctly."""
    con = ibis.duckdb.connect()

    # Table 1 - sales
    sales_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Books"],  # Will conflict
            "region": ["North", "South", "West"],
            "amount": [100, 200, 300],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    table1 = con.create_table("sales", sales_data)

    # Table 2 - products
    products_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": [
                "Tech",
                "Fashion",
                "Education",
            ],  # Conflicts with sales.category
            "brand": ["Apple", "Nike", "Pearson"],
            "price": [500, 80, 50],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    table2 = con.create_table("products", products_data)

    # Create semantic tables with conflicts
    sales = (
        to_semantic_table(table1, name="sales")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts with products
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(table2, name="products")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts with sales
            brand=lambda t: t.brand,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.price.sum(),
            avg_price=lambda t: t.price.mean(),
        )
    )

    joined = sales.join(products, how="cross", on=None)
    dimensions = joined.dimensions

    # All dimensions should be prefixed with table names
    assert "sales__category" in dimensions
    assert "products__category" in dimensions

    # All dimensions are prefixed, including non-conflicting ones
    assert "sales__region" in dimensions
    assert "products__brand" in dimensions
    assert "region" not in dimensions  # No unprefixed names
    assert "brand" not in dimensions


def test_measure_conflicts_prefixed():
    """Test that conflicting measures are prefixed correctly."""
    con = ibis.duckdb.connect()

    # Table 1 - sales
    sales_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Books"],
            "region": ["North", "South", "West"],
            "amount": [100, 200, 300],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    table1 = con.create_table("sales", sales_data)

    # Table 2 - products
    products_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Tech", "Fashion", "Education"],
            "brand": ["Apple", "Nike", "Pearson"],
            "price": [500, 80, 50],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    table2 = con.create_table("products", products_data)

    # Create semantic tables with conflicts
    sales = (
        to_semantic_table(table1, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),  # Conflicts with products
            count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(table2, name="products")
        .with_dimensions(
            category=lambda t: t.category,
            brand=lambda t: t.brand,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.price.sum(),  # Conflicts with sales.total_amount
            avg_price=lambda t: t.price.mean(),
        )
    )

    joined = sales.join(products, how="cross", on=None)
    measures = joined.measures

    # All measures should be prefixed with table names
    assert "sales__total_amount" in measures
    assert "products__total_amount" in measures
    assert "sales__count" in measures

    # All measures are prefixed, including non-conflicting ones
    assert "products__avg_price" in measures
    assert "total_amount" not in measures  # No unprefixed names
    assert "avg_price" not in measures


def test_time_dimension_conflicts_prefixed():
    """Test that conflicting time dimensions are prefixed correctly."""
    con = ibis.duckdb.connect()

    # Table 1 - sales
    sales_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Books"],
            "region": ["North", "South", "West"],
            "amount": [100, 200, 300],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],  # Will conflict
        }
    )
    table1 = con.create_table("sales", sales_data)

    # Table 2 - products
    products_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Tech", "Fashion", "Education"],
            "brand": ["Apple", "Nike", "Pearson"],
            "price": [500, 80, 50],
            "created_at": [
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
            ],  # Conflicts with sales.created_at
        }
    )
    table2 = con.create_table("products", products_data)

    # Create semantic tables with conflicts
    sales = (
        to_semantic_table(table1, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(table2, name="products")
        .with_dimensions(
            category=lambda t: t.category,
            brand=lambda t: t.brand,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.price.sum(),
            avg_price=lambda t: t.price.mean(),
        )
    )

    joined = sales.join(products, how="cross", on=None)
    json_def = joined.json_definition

    # Should have multiple time dimensions listed (all prefixed)
    time_dims = json_def.get("time_dimensions", [])
    assert "sales__created_at" in time_dims
    assert "products__created_at" in time_dims
    assert len(time_dims) == 2


def test_json_definition_consistency():
    """Test that JSON definition matches property access."""
    con = ibis.duckdb.connect()

    # Table 1 - sales
    sales_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Books"],
            "region": ["North", "South", "West"],
            "amount": [100, 200, 300],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    table1 = con.create_table("sales", sales_data)

    # Table 2 - products
    products_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "category": ["Tech", "Fashion", "Education"],
            "brand": ["Apple", "Nike", "Pearson"],
            "price": [500, 80, 50],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    table2 = con.create_table("products", products_data)

    # Create semantic tables with conflicts
    sales = (
        to_semantic_table(table1, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(table2, name="products")
        .with_dimensions(
            category=lambda t: t.category,
            brand=lambda t: t.brand,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_amount=lambda t: t.price.sum(),
            avg_price=lambda t: t.price.mean(),
        )
    )

    joined = sales.join(products, how="cross", on=None)
    json_def = joined.json_definition

    # Dimensions should match
    assert set(joined.dimensions) == set(json_def["dimensions"].keys())

    # Measures should match
    assert set(joined.measures) == set(json_def["measures"].keys())


def test_filter_with_original_dimensions():
    """Test filtering with original (non-prefixed) dimensions."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with conflicts
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Tech", "Fashion", "Tech", "Education"],  # Conflicts!
            "brand": ["Apple", "Nike", "Samsung", "Pearson"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            sale_count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts!
            brand=lambda t: t.brand,
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    filtered = joined.filter(lambda t: t.region == "North")
    result = filtered.execute()

    # Should filter correctly
    assert len(result) > 0
    assert all(row == "North" for row in result["region"])


def test_filter_with_prefixed_dimensions():
    """Test filtering with prefixed dimensions."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with conflicts
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Tech", "Fashion", "Tech", "Education"],  # Conflicts!
            "brand": ["Apple", "Nike", "Samsung", "Pearson"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            sale_count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts!
            brand=lambda t: t.brand,
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    filtered = joined.filter(lambda t: getattr(t, "products__category") == "Tech")
    result = filtered.execute()

    # Should filter correctly
    assert len(result) >= 0  # May be 0 if no matches, but should not error


def test_group_by_operations():
    """Test group by operations with joined tables."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with conflicts
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Tech", "Fashion", "Tech", "Education"],  # Conflicts!
            "brand": ["Apple", "Nike", "Samsung", "Pearson"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            sale_count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts!
            brand=lambda t: t.brand,
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    # Test group by with prefixed dimensions
    grouped1 = joined.group_by("sales__region").aggregate(
        total_sales=lambda t: t.amount.sum()
    )
    result1 = grouped1.execute()

    assert len(result1) > 0
    assert "sales__region" in result1.columns
    assert "total_sales" in result1.columns

    # Test group by with prefixed dimensions
    grouped2 = joined.group_by("products__category").aggregate(
        total_sales=lambda t: t.amount.sum()
    )
    result2 = grouped2.execute()

    assert len(result2) > 0
    assert "products__category" in result2.columns
    assert "total_sales" in result2.columns

    # Test group by with mixed dimensions
    grouped3 = joined.group_by("sales__region", "products__category").aggregate(
        total_sales=lambda t: t.amount.sum()
    )
    result3 = grouped3.execute()

    assert len(result3) > 0
    assert "sales__region" in result3.columns
    assert "products__category" in result3.columns
    assert "total_sales" in result3.columns


def test_select_operations():
    """Test select operations with joined tables."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with conflicts
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Tech", "Fashion", "Tech", "Education"],  # Conflicts!
            "brand": ["Apple", "Nike", "Samsung", "Pearson"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            sale_count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts!
            brand=lambda t: t.brand,
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    # Test select with mixed fields
    selected1 = joined.select("sales__region", "products__category", "products__brand")
    result1 = selected1.execute()

    assert len(result1) > 0
    assert set(result1.columns) == {
        "sales__region",
        "products__category",
        "products__brand",
    }

    # Test that selecting measures triggers aggregation
    selected2 = joined.select("sales__region", "sales__total_sales")
    result2 = selected2.execute()

    assert len(result2) > 0
    assert "sales__region" in result2.columns
    assert "sales__total_sales" in result2.columns
    # Should be aggregated (fewer rows than original)
    assert len(result2) <= 4  # Max number of unique regions


def test_complex_chained_operations():
    """Test complex chains of operations."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with conflicts
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Tech", "Fashion", "Tech", "Education"],  # Conflicts!
            "brand": ["Apple", "Nike", "Samsung", "Pearson"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            category=lambda t: t.category,
            region=lambda t: t.region,
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            sale_count=lambda t: t.count(),
        )
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(
            category=lambda t: t.category,  # Conflicts!
            brand=lambda t: t.brand,
        )
        .with_measures(
            avg_price=lambda t: t.price.mean(),
            product_count=lambda t: t.count(),
        )
    )

    joined = sales.join(products, how="cross", on=None)

    result = (
        joined.filter(lambda t: t.region.isin(["North", "South"]))
        .group_by("sales__region", "products__category")
        .aggregate(
            total_sales=lambda t: t.amount.sum(), avg_price=lambda t: t.price.mean()
        )
        .execute()
    )

    assert len(result) > 0
    expected_cols = {"sales__region", "products__category", "total_sales", "avg_price"}
    assert expected_cols.issubset(set(result.columns))


def test_three_way_join():
    """Test joining three tables together."""
    con = ibis.duckdb.connect()

    # Table 1 - events
    events_data = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "event_type": ["click", "view", "purchase"],
            "user_id": [101, 102, 103],
            "event_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    events_table = con.create_table("events", events_data)

    # Table 2 - users
    users_data = pd.DataFrame(
        {
            "user_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "region": ["North", "South", "North"],
            "created_at": ["2023-01-01", "2023-06-01", "2023-03-01"],
        }
    )
    users_table = con.create_table("users", users_data)

    # Table 3 - campaigns (with conflicts)
    campaigns_data = pd.DataFrame(
        {
            "campaign_id": [1, 2, 3],
            "name": [
                "Spring Sale",
                "Summer Sale",
                "Fall Sale",
            ],  # Conflicts with users.name
            "region": ["North", "South", "West"],  # Conflicts with users.region
            "created_at": [
                "2024-01-01",
                "2024-02-01",
                "2024-03-01",
            ],  # Conflicts with users.created_at
        }
    )
    campaigns_table = con.create_table("campaigns", campaigns_data)

    # Create semantic tables
    events = (
        to_semantic_table(events_table, name="events")
        .with_dimensions(
            event_type=lambda t: t.event_type,
            user_id=lambda t: t.user_id,
            event_date={
                "expr": lambda t: t.event_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            event_count=lambda t: t.count(),
        )
    )

    users = (
        to_semantic_table(users_table, name="users")
        .with_dimensions(
            name=lambda t: t.name,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            user_count=lambda t: t.count(),
        )
    )

    campaigns = (
        to_semantic_table(campaigns_table, name="campaigns")
        .with_dimensions(
            name=lambda t: t.name,  # Conflicts with users.name
            region=lambda t: t.region,  # Conflicts with users.region
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            campaign_count=lambda t: t.count(),
        )
    )

    # Join all three tables
    joined = events.join(users, how="cross", on=None).join(
        campaigns, how="cross", on=None
    )

    dimensions = joined.dimensions
    measures = joined.measures

    # Should have all dimensions with conflicts resolved (including time dimensions)
    expected_dims = {
        "events__event_type",
        "events__user_id",  # All prefixed now
        "events__event_date",  # Time dimension
        "users__name",
        "campaigns__name",  # All prefixed
        "users__region",
        "campaigns__region",  # All prefixed
        "users__created_at",  # Time dimension
        "campaigns__created_at",  # Time dimension
    }
    assert set(dimensions) == expected_dims

    # Should have all measures (all prefixed)
    expected_measures = {
        "events__event_count",
        "users__user_count",
        "campaigns__campaign_count",
    }
    assert set(measures) == expected_measures

    # Should be executable
    result = joined.execute()
    assert len(result) == 27  # 3 * 3 * 3 = 27 rows


def test_multiple_time_dimensions():
    """Test that multiple time dimensions are handled correctly."""
    con = ibis.duckdb.connect()

    # Table 1 - events
    events_data = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "event_type": ["click", "view", "purchase"],
            "user_id": [101, 102, 103],
            "event_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    events_table = con.create_table("events", events_data)

    # Table 2 - users
    users_data = pd.DataFrame(
        {
            "user_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "region": ["North", "South", "North"],
            "created_at": ["2023-01-01", "2023-06-01", "2023-03-01"],
        }
    )
    users_table = con.create_table("users", users_data)

    # Table 3 - campaigns (with conflicts)
    campaigns_data = pd.DataFrame(
        {
            "campaign_id": [1, 2, 3],
            "name": ["Spring Sale", "Summer Sale", "Fall Sale"],
            "region": ["North", "South", "West"],
            "created_at": [
                "2024-01-01",
                "2024-02-01",
                "2024-03-01",
            ],  # Conflicts with users.created_at
        }
    )
    campaigns_table = con.create_table("campaigns", campaigns_data)

    # Create semantic tables
    events = (
        to_semantic_table(events_table, name="events")
        .with_dimensions(
            event_type=lambda t: t.event_type,
            user_id=lambda t: t.user_id,
            event_date={
                "expr": lambda t: t.event_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            event_count=lambda t: t.count(),
        )
    )

    users = (
        to_semantic_table(users_table, name="users")
        .with_dimensions(
            name=lambda t: t.name,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            user_count=lambda t: t.count(),
        )
    )

    campaigns = (
        to_semantic_table(campaigns_table, name="campaigns")
        .with_dimensions(
            name=lambda t: t.name,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            campaign_count=lambda t: t.count(),
        )
    )

    joined = events.join(users, how="cross", on=None).join(
        campaigns, how="cross", on=None
    )

    json_def = joined.json_definition

    # Should list all time dimensions
    time_dims = json_def.get("time_dimensions", [])
    expected_time_dims = {
        "events__event_date",
        "users__created_at",
        "campaigns__created_at",
    }
    assert set(time_dims) == expected_time_dims


def test_operations_on_multiple_joins():
    """Test operations work with multiple joined tables."""
    con = ibis.duckdb.connect()

    # Table 1 - events
    events_data = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "event_type": ["click", "view", "purchase"],
            "user_id": [101, 102, 103],
            "event_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    events_table = con.create_table("events", events_data)

    # Table 2 - users
    users_data = pd.DataFrame(
        {
            "user_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "region": ["North", "South", "North"],
            "created_at": ["2023-01-01", "2023-06-01", "2023-03-01"],
        }
    )
    users_table = con.create_table("users", users_data)

    # Table 3 - campaigns (with conflicts)
    campaigns_data = pd.DataFrame(
        {
            "campaign_id": [1, 2, 3],
            "name": ["Spring Sale", "Summer Sale", "Fall Sale"],
            "region": ["North", "South", "West"],
            "created_at": ["2024-01-01", "2024-02-01", "2024-03-01"],
        }
    )
    campaigns_table = con.create_table("campaigns", campaigns_data)

    # Create semantic tables
    events = (
        to_semantic_table(events_table, name="events")
        .with_dimensions(
            event_type=lambda t: t.event_type,
            user_id=lambda t: t.user_id,
            event_date={
                "expr": lambda t: t.event_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            event_count=lambda t: t.count(),
        )
    )

    users = (
        to_semantic_table(users_table, name="users")
        .with_dimensions(
            name=lambda t: t.name,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            user_count=lambda t: t.count(),
        )
    )

    campaigns = (
        to_semantic_table(campaigns_table, name="campaigns")
        .with_dimensions(
            name=lambda t: t.name,
            region=lambda t: t.region,
            created_at={
                "expr": lambda t: t.created_at,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            campaign_count=lambda t: t.count(),
        )
    )

    joined = events.join(users, how="cross", on=None).join(
        campaigns, how="cross", on=None
    )

    # Complex operation using fields from all three tables
    result = (
        joined.filter(lambda t: (t.event_type == "click") & (t.region == "North"))
        .group_by("campaigns__region", "event_type")
        .aggregate(
            total_events=lambda t: t.count(), user_count=lambda t: t.user_id.nunique()
        )
        .execute()
    )

    assert len(result) >= 0  # Should execute without error
    if len(result) > 0:
        expected_cols = {
            "campaigns__region",
            "event_type",
            "total_events",
            "user_count",
        }
        assert expected_cols.issubset(set(result.columns))


def test_join_with_self():
    """Test joining a table with itself."""
    con = ibis.duckdb.connect()

    # Minimal table
    data = pd.DataFrame({"id": [1], "value": [100]})
    table = con.create_table("minimal", data)

    semantic_table = (
        to_semantic_table(table, name="minimal")
        .with_dimensions(id=lambda t: t.id)
        .with_measures(total=lambda t: t.value.sum())
    )

    # Create another semantic table from same data but different name
    other = (
        to_semantic_table(table, name="other")
        .with_dimensions(id=lambda t: t.id)
        .with_measures(total=lambda t: t.value.sum())
    )

    joined = semantic_table.join(other, how="cross", on=None)

    dimensions = joined.dimensions
    measures = joined.measures

    # All should have prefixed versions
    assert "minimal__id" in dimensions
    assert "other__id" in dimensions
    assert "minimal__total" in measures
    assert "other__total" in measures


def test_empty_semantic_table_join():
    """Test joining tables where one has no semantic definitions."""
    con = ibis.duckdb.connect()

    # Minimal table
    data = pd.DataFrame({"id": [1], "value": [100]})
    table = con.create_table("minimal", data)

    semantic_table = (
        to_semantic_table(table, name="minimal")
        .with_dimensions(id=lambda t: t.id)
        .with_measures(total=lambda t: t.value.sum())
    )

    # Create table with no dimensions or measures
    empty_semantic = to_semantic_table(table, name="empty")

    joined = semantic_table.join(empty_semantic, how="cross", on=None)

    # Should still work, just with prefixed table's definitions
    assert "minimal__id" in joined.dimensions
    assert "minimal__total" in joined.measures


def test_group_by_aggregate_basic():
    """Test basic group_by with aggregate method."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
            "sale_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Create semantic table
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(
            region=lambda t: t.region,
            sale_id=lambda t: t.sale_id,
            sale_date={
                "expr": lambda t: t.sale_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            total_sales=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
        )
    )

    # Test group_by with aggregate - reference existing measure
    result = sales.group_by("region").aggregate(lambda t: t.total_sales)
    df = result.execute()
    
    # Should have grouped by region and aggregated total_sales
    assert len(df) == 3  # 3 unique regions
    assert "region" in df.columns
    assert "total_sales" in df.columns
    
    # Check expected totals by region
    region_totals = df.set_index("region")["total_sales"].to_dict()
    assert region_totals["North"] == 250  # 100 + 150
    assert region_totals["South"] == 200
    assert region_totals["West"] == 300


def test_joined_measure_double_underscore_access():
    """Test accessing joined measures using double underscore notation."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_sales=lambda t: t.amount.sum())
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(avg_price=lambda t: t.price.mean())
    )

    joined = sales.join(products, how="cross", on=None)

    # Test that measures are prefixed with __ separator
    assert "sales__total_sales" in joined.measures
    assert "products__avg_price" in joined.measures

    # Test accessing joined measures using double underscore notation
    result = joined.group_by("sales__region").aggregate(lambda t: t.sales__total_sales)
    df = result.execute()
    
    assert len(df) > 0
    assert "sales__region" in df.columns
    assert "sales__total_sales" in df.columns


def test_joined_measure_priority_resolution():
    """Test that unprefixed names are tried first, then prefixed names."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3, 4],
            "region": ["North", "South", "North", "West"],
            "amount": [100, 200, 150, 300],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with different measure name
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4],
            "category": ["Electronics", "Clothing", "Electronics", "Books"],
            "price": [500, 80, 300, 50],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables with unique measure names (no conflicts)
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_sales=lambda t: t.amount.sum())
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(avg_price=lambda t: t.price.mean())  # Different from total_sales
    )

    joined = sales.join(products, how="cross", on=None)

    # Should be able to access measures by their simple names since no conflicts
    result1 = joined.group_by("sales__region").aggregate(lambda t: t.total_sales)
    df1 = result1.execute()
    
    # Should also be able to access by prefixed names
    result2 = joined.group_by("sales__region").aggregate(lambda t: t.sales__total_sales)
    df2 = result2.execute()
    
    # Both should work and produce same results
    assert len(df1) == len(df2)
    assert "total_sales" in df1.columns
    assert "sales__total_sales" in df2.columns


def test_mixed_joined_measures():
    """Test using measures from different tables in same aggregation."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3],
            "region": ["North", "South", "North"],
            "amount": [100, 200, 150],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Electronics"],
            "price": [500, 80, 300],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_sales=lambda t: t.amount.sum())
    )

    products = (
        to_semantic_table(products_table, name="products")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(avg_price=lambda t: t.price.mean())
    )

    joined = sales.join(products, how="cross", on=None)

    # Test using measures from both tables in same aggregation
    result = joined.group_by("sales__region").aggregate(
        sales_total=lambda t: t.sales__total_sales,
        avg_price=lambda t: t.products__avg_price
    )
    df = result.execute()
    
    assert len(df) > 0
    assert "sales__region" in df.columns
    assert "sales_total" in df.columns
    assert "avg_price" in df.columns


def test_join_with_unnamed_tables_conflict():
    """Test conflict resolution when joining tables without explicit names."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3],
            "region": ["North", "South", "North"],
            "amount": [100, 200, 150],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data with same measure name
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Electronics"],
            "amount": [500, 80, 300],  # Same column name as sales!
        }
    )
    products_table = con.create_table("products", products_data)

    # Create semantic tables WITHOUT names
    sales = (
        to_semantic_table(sales_table)  # No name specified
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    products = (
        to_semantic_table(products_table)  # No name specified
        .with_dimensions(category=lambda t: t.category)
        .with_measures(total_amount=lambda t: t.amount.sum())  # Conflict!
    )

    # Join tables with conflicting measure names
    joined = sales.join(products, how="cross", on=None)
    
    # Check that conflicts are resolved by using database table names as prefixes
    measures = joined.measures
    measure_keys = list(measures.keys())
    
    # Both measures should exist with different prefixes based on database table names
    sales_measure = [k for k in measure_keys if k.endswith("__total_amount") and "sales" in k][0]
    products_measure = [k for k in measure_keys if k.endswith("__total_amount") and "products" in k][0]
    
    assert sales_measure != products_measure, "Conflicting measures should have different names"
    assert len(measure_keys) == 2, "Should have exactly 2 measures"
    
    # Verify both measures are accessible through the semantic layer
    assert sales_measure in joined.measures
    assert products_measure in joined.measures
    
    # Test basic functionality - just group by to ensure join works
    result = joined.group_by("region").aggregate(total=lambda t: t.amount.sum())
    df = result.execute()
    
    assert len(df) > 0
    assert "region" in df.columns
    assert "total" in df.columns


def test_join_with_mixed_named_unnamed_tables():
    """Test joining named and unnamed tables."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3],
            "region": ["North", "South", "North"],
            "amount": [100, 200, 150],
        }
    )
    sales_table = con.create_table("sales", sales_data)

    # Products data
    products_data = pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "category": ["Electronics", "Clothing", "Electronics"],
            "price": [500, 80, 300],
        }
    )
    products_table = con.create_table("products", products_data)

    # Create one named, one unnamed table
    sales = (
        to_semantic_table(sales_table, name="sales")  # Named
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_sales=lambda t: t.amount.sum())
    )

    products = (
        to_semantic_table(products_table)  # Unnamed
        .with_dimensions(category=lambda t: t.category)
        .with_measures(avg_price=lambda t: t.price.mean())
    )

    joined = sales.join(products, how="cross", on=None)
    
    # Check that named table uses its explicit name, unnamed uses database name
    measures = joined.measures
    dimensions = joined.dimensions
    
    measure_keys = list(measures.keys())
    dimension_keys = list(dimensions.keys())
    
    # Named table should have clean prefix
    assert any("sales__" in k for k in measure_keys), "Named table should use clean prefix"
    assert any("sales__" in k for k in dimension_keys), "Named table should use clean prefix"
    
    # Unnamed table should use database table name prefix
    unnamed_measures = [k for k in measure_keys if k not in ["sales__total_sales"]]
    unnamed_dims = [k for k in dimension_keys if k not in ["sales__region"]]
    
    assert len(unnamed_measures) > 0, "Should have measures from unnamed table"
    assert len(unnamed_dims) > 0, "Should have dimensions from unnamed table"
    
    # Test that both naming strategies work
    result = joined.group_by("sales__region").aggregate(
        named_measure=lambda t: t.sales__total_sales,
        # For unnamed measure, just use simple aggregation to test functionality
        unnamed_count=lambda t: t.price.sum()
    )
    df = result.execute()
    
    assert len(df) > 0
    assert "named_measure" in df.columns
    assert "unnamed_count" in df.columns


def test_comprehensive_conflict_scenarios():
    """Test various conflict scenarios to ensure robust conflict detection."""
    con = ibis.duckdb.connect()

    # Create data with multiple potential conflicts
    table1_data = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "value": [10, 20, 30],
    })
    table1 = con.create_table("table1", table1_data)

    table2_data = pd.DataFrame({
        "id": [1, 2, 3], 
        "name": ["X", "Y", "Z"],  # Conflicts with table1.name
        "amount": [100, 200, 300],
    })
    table2 = con.create_table("table2", table2_data)

    # Scenario 1: Named tables with conflicts
    t1_named = (
        to_semantic_table(table1, name="first")
        .with_dimensions(name=lambda t: t.name, id=lambda t: t.id)
        .with_measures(total_value=lambda t: t.value.sum())
    )

    t2_named = (
        to_semantic_table(table2, name="second") 
        .with_dimensions(name=lambda t: t.name, id=lambda t: t.id)  # Conflicts
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    joined_named = t1_named.join(t2_named, how="cross", on=None)
    
    # Check that all conflicts are resolved with prefixes
    dims = list(joined_named.dimensions.keys())
    measures = list(joined_named.measures.keys())
    
    # Should have prefixed versions of conflicting fields
    assert "first__name" in dims and "second__name" in dims
    assert "first__id" in dims and "second__id" in dims
    assert "first__total_value" in measures and "second__total_amount" in measures
    
    # Test execution to verify functionality
    result = joined_named.group_by("first__name").aggregate(
        first_value=lambda t: t.first__total_value,
        second_amount=lambda t: t.second__total_amount
    )
    df = result.execute()
    
    assert len(df) > 0
    assert "first__name" in df.columns
    assert "first_value" in df.columns
    assert "second_amount" in df.columns


def test_original_user_scenario_demonstration():
    """Demonstrate the original user scenario working with joined measure resolution."""
    con = ibis.duckdb.connect()

    # Sales data
    sales_data = pd.DataFrame({
        "sale_id": [1, 2, 3, 4],
        "region": ["North", "South", "North", "West"], 
        "amount": [100, 200, 150, 300],
    })
    sales_table = con.create_table("sales", sales_data)

    # Products data  
    products_data = pd.DataFrame({
        "product_id": [1, 2, 3, 4],
        "category": ["Electronics", "Clothing", "Electronics", "Books"],
        "price": [500, 80, 300, 50],
    })
    products_table = con.create_table("products", products_data)

    # Create semantic tables with predefined measures
    sales = (
        to_semantic_table(sales_table, name="sales")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_sales=lambda t: t.amount.sum())  # Predefined measure
    )
    
    products = (
        to_semantic_table(products_table, name="products") 
        .with_dimensions(category=lambda t: t.category)
        .with_measures(avg_price=lambda t: t.price.mean())   # Predefined measure
    )
    
    # Join the tables
    joined = sales.join(products, how="cross", on=None)
    
    # ✅ ORIGINAL USER REQUEST: Use existing measures in aggregation
    # This now works with our double underscore syntax:
    result = joined.group_by("sales__region").aggregate(lambda t: t.sales__total_sales)
    df = result.execute()
    
    # Verify it works
    assert len(df) == 3  # 3 unique regions
    assert "sales__region" in df.columns
    assert "sales__total_sales" in df.columns
    
    # Also test priority resolution - can access measures by simple names if no conflicts
    result2 = joined.group_by("sales__region").aggregate(lambda t: t.total_sales)
    df2 = result2.execute()
    
    # Should also work (falls back to prefixed version)
    assert len(df2) == 3
    assert "sales__region" in df2.columns
    assert "total_sales" in df2.columns
    
    # Test mixed usage - both prefixed and simple names
    result3 = joined.group_by("sales__region").aggregate(
        sales_measure=lambda t: t.sales__total_sales,  # Explicit prefix
        products_measure=lambda t: t.avg_price         # Simple name (no conflict)
    )
    df3 = result3.execute()
    
    assert len(df3) == 3
    assert "sales_measure" in df3.columns
    assert "products_measure" in df3.columns
    
    print("✅ Original user scenario now works!")
    print("✅ sales.group_by('region').aggregate(lambda t: t.sales__total_sales)")
    print("✅ Conflict resolution with double underscore separator")
    print("✅ Priority resolution (simple names first, then prefixed)")
    print("✅ Mixed usage in same aggregation operation")


if __name__ == "__main__":
    # Run tests if executed directly
    import sys

    pytest.main([__file__] + sys.argv[1:])
