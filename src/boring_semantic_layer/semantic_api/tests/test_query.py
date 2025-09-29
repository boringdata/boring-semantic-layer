#!/usr/bin/env python3
"""
Tests for the build_query function in the semantic API.

Tests cover:
- Basic query building with dimensions and measures
- Time dimension handling and time grain transformations
- Filtering with various filter types
- Time range filtering
- Ordering and limiting
- Edge cases and error handling
"""

import pytest
import ibis
import pandas as pd
from boring_semantic_layer.semantic_api.api import to_semantic_table
from boring_semantic_layer.semantic_api.query import build_query
from boring_semantic_layer.filters import Filter


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "region": ["North", "South", "North", "West", "South"],
            "amount": [100, 200, 150, 300, 250],
            "count": [1, 1, 2, 1, 3],
            "sale_date": [
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
                "2024-02-01",
                "2024-02-02",
            ],
            "year": [2024, 2024, 2024, 2024, 2024],
        }
    )


@pytest.fixture
def basic_semantic_table(sample_data):
    """Create a basic semantic table for testing."""
    con = ibis.duckdb.connect()
    table = con.create_table("test_data", sample_data)

    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(
            region=lambda t: t.region,
            id=lambda t: t.id,
            year=lambda t: t.year,
            sale_date={
                "expr": lambda t: t.sale_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
                "description": "Sale date",
            },
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            avg_amount=lambda t: t.amount.mean(),
            count_sales=lambda t: t.count.sum(),
        )
    )
    return semantic_table._node


def test_basic_query_with_dimensions_only(basic_semantic_table):
    """Test building a query with only dimensions."""
    result = build_query(
        semantic_table=basic_semantic_table, dimensions=["region", "year"]
    )

    # Should return a SemanticTableExpr
    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)

    # Should have grouped by the specified dimensions
    # We can't easily inspect the internal structure, but we can verify it doesn't raise errors
    assert result is not None


def test_query_with_dimensions_and_measures(basic_semantic_table):
    """Test building a query with dimensions and measures."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount", "avg_amount"],
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_with_filters(basic_semantic_table):
    """Test building a query with filters."""
    # Test with string filter
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        filters=["region == 'North'"],
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)

    # Test with callable filter
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        filters=[lambda t: t.amount > 150],
    )

    assert isinstance(result, SemanticTableExpr)


def test_query_with_filter_objects(basic_semantic_table):
    """Test building a query with Filter objects."""
    filter_obj = Filter(filter=lambda t: t.region == "North")

    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        filters=[filter_obj],
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_with_time_range_filter(basic_semantic_table):
    """Test building a query with time range filtering."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        time_range={"start": "2024-01-01", "end": "2024-01-31"},
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_with_time_range_partial(basic_semantic_table):
    """Test time range filtering with only start or end date."""
    # Only start date
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        time_range={"start": "2024-01-01"},
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)

    # Only end date
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        time_range={"end": "2024-01-31"},
    )

    assert isinstance(result, SemanticTableExpr)


def test_query_with_time_grain_transformation(basic_semantic_table):
    """Test building a query with time grain transformation."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        time_grain="month",
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_with_order_by(basic_semantic_table):
    """Test building a query with ordering."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        order_by=[("total_amount", "desc"), ("region", "asc")],
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_with_limit(basic_semantic_table):
    """Test building a query with limit."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        limit=10,
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_complex_query_all_parameters(basic_semantic_table):
    """Test building a complex query with all parameters."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount", "count_sales"],
        filters=[lambda t: t.amount > 100],
        order_by=[("total_amount", "desc")],
        limit=5,
        time_range={"start": "2024-01-01", "end": "2024-02-28"},
        time_grain="month",
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_no_dimensions_or_measures(basic_semantic_table):
    """Test building a query with no dimensions or measures (should work)."""
    result = build_query(semantic_table=basic_semantic_table)

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_empty_dimensions_list(basic_semantic_table):
    """Test building a query with empty dimensions list."""
    result = build_query(
        semantic_table=basic_semantic_table, dimensions=[], measures=["total_amount"]
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_with_no_time_dimensions():
    """Test building a query on a table with no time dimensions."""
    con = ibis.duckdb.connect()
    data = pd.DataFrame(
        {"region": ["North", "South", "North"], "amount": [100, 200, 150]}
    )
    table = con.create_table("no_time_data", data)

    semantic_table = (
        to_semantic_table(table, name="no_time")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    # Should work even with time_grain specified (should be ignored)
    result = build_query(
        semantic_table=semantic_table._node,
        dimensions=["region"],
        measures=["total_amount"],
        time_grain="month",
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_time_range_without_time_dimensions():
    """Test that time_range is ignored when no time dimensions exist."""
    con = ibis.duckdb.connect()
    data = pd.DataFrame(
        {"region": ["North", "South", "North"], "amount": [100, 200, 150]}
    )
    table = con.create_table("no_time_data2", data)

    semantic_table = (
        to_semantic_table(table, name="no_time")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    # Should work and ignore time_range
    result = build_query(
        semantic_table=semantic_table._node,
        dimensions=["region"],
        measures=["total_amount"],
        time_range={"start": "2024-01-01", "end": "2024-01-31"},
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_multiple_time_dimensions():
    """Test building a query with multiple time dimensions."""
    con = ibis.duckdb.connect()
    data = pd.DataFrame(
        {
            "region": ["North", "South", "North"],
            "amount": [100, 200, 150],
            "sale_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "ship_date": ["2024-01-05", "2024-01-06", "2024-01-07"],
        }
    )
    table = con.create_table("multi_time_data", data)

    semantic_table = (
        to_semantic_table(table, name="multi_time")
        .with_dimensions(
            region=lambda t: t.region,
            sale_date={
                "expr": lambda t: t.sale_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
            ship_date={
                "expr": lambda t: t.ship_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    # Should work with multiple time dimensions
    result = build_query(
        semantic_table=semantic_table._node,
        dimensions=["region"],
        measures=["total_amount"],
        time_grain="month",
        time_range={"start": "2024-01-01", "end": "2024-01-31"},
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_invalid_time_grain():
    """Test behavior with invalid time grain."""
    con = ibis.duckdb.connect()
    data = pd.DataFrame(
        {
            "region": ["North", "South"],
            "amount": [100, 200],
            "sale_date": ["2024-01-01", "2024-01-02"],
        }
    )
    table = con.create_table("invalid_grain_data", data)

    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(
            region=lambda t: t.region,
            sale_date={
                "expr": lambda t: t.sale_date,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    # Should work but ignore invalid time grain
    result = build_query(
        semantic_table=semantic_table._node,
        dimensions=["region"],
        measures=["total_amount"],
        time_grain="invalid_grain",  # Not in TIME_GRAIN_TRANSFORMATIONS
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_mixed_filter_types(basic_semantic_table):
    """Test building a query with mixed filter types."""
    filter_obj = Filter(filter=lambda t: t.amount > 100)

    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        filters=[
            filter_obj,  # Filter object
            lambda t: t.region != "West",  # Callable
            "year == 2024",  # String
        ],
    )

    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)


def test_query_builder_preserves_semantic_table_structure(basic_semantic_table):
    """Test that the query builder preserves the semantic table structure."""
    result = build_query(
        semantic_table=basic_semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
    )

    # The result should be a SemanticTableExpr with the expected structure
    from boring_semantic_layer.semantic_api.api import SemanticTableExpr

    assert isinstance(result, SemanticTableExpr)

    # Should have access to the underlying node
    assert hasattr(result, "_node")
    assert result._node is not None


def test_build_query_validation_semantic_table():
    """Test that build_query works with SemanticTable nodes."""
    con = ibis.duckdb.connect()
    
    data = pd.DataFrame({
        "region": ["North", "South", "West"],
        "amount": [100, 200, 300],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"]
    })
    table = con.create_table("test", data)
    
    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(
            region=lambda t: t.region,
            date={
                "expr": lambda t: t.date,
                "is_time_dimension": True,
                "smallest_time_grain": "day"
            }
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum()
        )
    )
    
    # Should work with SemanticTable node directly
    query = build_query(
        semantic_table._node,  # Pass the node directly
        dimensions=["region"],
        measures=["total_amount"]
    )
    result = query.execute()
    assert len(result) > 0


def test_build_query_validation_semantic_expr():
    """Test that build_query works with SemanticTableExpr."""
    con = ibis.duckdb.connect()
    
    data = pd.DataFrame({
        "region": ["North", "South", "West"],
        "amount": [100, 200, 300],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"]
    })
    table = con.create_table("test", data)
    
    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(
            region=lambda t: t.region,
            date={
                "expr": lambda t: t.date,
                "is_time_dimension": True,
                "smallest_time_grain": "day"
            }
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum()
        )
    )
    
    # Should work with SemanticTableExpr
    query = build_query(
        semantic_table,  # Pass the expression
        dimensions=["region"],
        measures=["total_amount"]
    )
    result = query.execute()
    assert len(result) > 0


def test_build_query_validation_semantic_join():
    """Test that build_query works with joined tables (SemanticJoin nodes)."""
    con = ibis.duckdb.connect()
    
    # Sales data
    sales_data = pd.DataFrame({
        "region": ["North", "South"],
        "amount": [100, 200]
    })
    sales_table = con.create_table("sales", sales_data)
    
    # Products data  
    products_data = pd.DataFrame({
        "category": ["Electronics", "Clothing"],
        "price": [500, 80]
    })
    products_table = con.create_table("products", products_data)
    
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
    
    # Should work with SemanticJoin - use double underscore format for joined tables
    query = build_query(
        joined,
        dimensions=["sales__region", "products__category"],
        measures=["sales__total_sales"]
    )
    result = query.execute()
    assert len(result) > 0


def test_build_query_validation_fails_on_aggregate():
    """Test that build_query raises ValueError on aggregated results."""
    con = ibis.duckdb.connect()
    
    data = pd.DataFrame({
        "region": ["North", "South", "West"],
        "amount": [100, 200, 300]
    })
    table = con.create_table("test", data)
    
    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_amount=lambda t: t.amount.sum())
    )
    
    # Create an aggregated result
    aggregated = semantic_table.group_by("region").aggregate(
        total_amount=lambda t: t.amount.sum()
    )
    
    # Should raise ValueError
    with pytest.raises(ValueError, match="build_query\\(\\) can only be used with SemanticTable or SemanticJoin nodes"):
        build_query(
            aggregated,
            dimensions=["region"],
            measures=["total_amount"]
        )


def test_build_query_validation_fails_on_filtered_aggregate():
    """Test that build_query raises ValueError on filtered aggregated results."""
    con = ibis.duckdb.connect()
    
    data = pd.DataFrame({
        "region": ["North", "South", "West"],
        "amount": [100, 200, 300]
    })
    table = con.create_table("test", data)
    
    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(total_amount=lambda t: t.amount.sum())
    )
    
    # Create an aggregated result and then filter it
    aggregated_and_filtered = (
        semantic_table
        .group_by("region")
        .aggregate(total_amount=lambda t: t.amount.sum())
        .filter(lambda t: t.total_amount > 150)
    )
    
    # Should raise ValueError
    with pytest.raises(ValueError, match="build_query\\(\\) can only be used with SemanticTable or SemanticJoin nodes"):
        build_query(
            aggregated_and_filtered,
            dimensions=["region"]
        )


def test_build_query_with_predefined_measures():
    """Test build_query with predefined measures (new functionality)."""
    con = ibis.duckdb.connect()
    
    data = pd.DataFrame({
        "region": ["North", "South", "North", "West"],
        "category": ["A", "B", "A", "C"],
        "amount": [100, 200, 150, 300],
        "quantity": [10, 5, 15, 8]
    })
    table = con.create_table("test", data)
    
    # Create semantic table with predefined measures
    semantic_table = (
        to_semantic_table(table, name="test")
        .with_dimensions(
            region=lambda t: t.region,
            category=lambda t: t.category,
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            avg_quantity=lambda t: t.quantity.mean(),
            count_rows=lambda t: t.count(),
        )
    )
    
    # Test 1: Build query using predefined measures
    query1 = build_query(
        semantic_table,
        dimensions=["region"],
        measures=["total_amount", "count_rows"]
    )
    result1 = query1.execute()
    
    assert len(result1) == 3  # Three regions: North, South, West
    assert "region" in result1.columns
    assert "total_amount" in result1.columns
    assert "count_rows" in result1.columns
    
    # Test 2: Build query with mixed predefined measures and filtering
    query2 = build_query(
        semantic_table,
        dimensions=["category"],
        measures=["total_amount", "avg_quantity"],
        filters=[lambda t: t.amount > 120]
    )
    result2 = query2.execute()
    
    assert len(result2) >= 1  # At least one category after filtering
    assert "category" in result2.columns
    assert "total_amount" in result2.columns
    assert "avg_quantity" in result2.columns
    
    # Test 3: Build query with ordering and limiting on predefined measures
    query3 = build_query(
        semantic_table,
        dimensions=["region"],
        measures=["total_amount"],
        order_by=[("total_amount", "desc")],
        limit=2
    )
    result3 = query3.execute()
    
    assert len(result3) == 2  # Limited to 2 rows
    assert "total_amount" in result3.columns
    
    # Should be ordered by total_amount descending
    assert result3["total_amount"].iloc[0] >= result3["total_amount"].iloc[1]
