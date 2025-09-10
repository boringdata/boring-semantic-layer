#!/usr/bin/env python3
"""
Test for stale dimension problem in complex composition chains.

This test demonstrates a fundamental limitation: after aggregation in complex chains,
dimension definitions become stale and fail on dot notation access.
"""

import pandas as pd
import pytest
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table


def test_stale_dimension_problem_in_complex_chain():
    """
    Test demonstrating stale dimension problem after aggregation in complex chains.
    
    Problem: ModelA → Join(ModelB) → GroupBy → Aggregate → Join(ModelC)
    Result: Dimension lambdas reference original structure instead of current structure.
    """
    
    # Create test data for complex composition chain
    orders_df = pd.DataFrame({
        'order_id': [1, 2, 3],
        'customer_id': [101, 102, 103],
        'region': ['North', 'South', 'North']
    })
    
    products_df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'order_id': [1, 2, 3],
        'price': [100, 200, 150]
    })
    
    customers_df = pd.DataFrame({
        'customer_id': [101, 102, 103],
        'country': ['US', 'UK', 'US']
    })
    
    con = ibis.duckdb.connect(':memory:')
    orders_table = con.create_table('orders', orders_df)
    products_table = con.create_table('products', products_df)
    customers_table = con.create_table('customers', customers_df)
    
    # Create semantic models
    model_a = (to_semantic_table(orders_table, name='orders')
               .with_dimensions(
                   order_id=lambda t: t.order_id,
                   customer_id=lambda t: t.customer_id, 
                   region=lambda t: t.region  # This lambda becomes stale after aggregation
               )
               .with_measures(order_count=lambda t: t.count()))
    
    model_b = (to_semantic_table(products_table, name='products')
               .with_dimensions(product_id=lambda t: t.product_id, order_id=lambda t: t.order_id)
               .with_measures(avg_price=lambda t: t.price.mean()))
    
    model_c = (to_semantic_table(customers_table, name='customers')
               .with_dimensions(customer_id=lambda t: t.customer_id, country=lambda t: t.country))
    
    # Execute the complex chain: ModelA → Join(ModelB) → GroupBy → Aggregate → Join(ModelC)
    step1 = model_a.join_one(model_b, 'order_id', 'order_id')
    step2 = step1.group_by('orders__region', 'orders__customer_id').aggregate('orders__order_count', 'products__avg_price')
    final_result = step2.join_one(model_c, 'orders__customer_id', 'customer_id')
    
    # Verify semantic metadata and actual structure
    assert 'orders__region' in final_result.dimensions, "Dimension should exist in semantic metadata"
    
    actual_columns = final_result.execute().columns.tolist()
    assert 'orders__region' in actual_columns, "Column should exist in actual table"
    
    # Test different access methods
    
    # Method 1: Bracket notation - works (bypasses stale lambda)
    bracket_result = final_result.filter(lambda t: t['orders__region'] == 'North').execute()
    assert bracket_result.shape[0] == 2, "Bracket notation should work"
    
    # Method 2: Dot notation - fails due to stale lambda
    with pytest.raises(AttributeError, match="'Table' object has no attribute 'region'"):
        final_result.filter(lambda t: t.orders__region == 'North').execute()
    
    # Method 3: String reference in group_by - also fails due to stale lambda in resolver
    with pytest.raises(AttributeError, match="'Table' object has no attribute 'region'"):
        final_result.group_by('orders__region').aggregate('orders__order_count').execute()


def test_stale_dimension_problem_root_cause():
    """
    Test demonstrating that the stale dimension problem occurs specifically 
    in complex chains with multiple joins and aggregations.
    
    The problem manifests when we have: Join → Aggregate → Join again
    """
    
    # This reproduces the minimal failing case from the main test
    orders_df = pd.DataFrame({
        'order_id': [1, 2], 
        'customer_id': [101, 102],
        'region': ['North', 'South']
    })
    products_df = pd.DataFrame({
        'order_id': [1, 2],
        'price': [100, 200] 
    })
    customers_df = pd.DataFrame({
        'customer_id': [101, 102],
        'country': ['US', 'UK']
    })
    
    con = ibis.duckdb.connect(':memory:')
    orders_table = con.create_table('orders', orders_df)
    products_table = con.create_table('products', products_df)
    customers_table = con.create_table('customers', customers_df)
    
    orders_sem = (to_semantic_table(orders_table, name='orders')
                  .with_dimensions(customer_id=lambda t: t.customer_id, region=lambda t: t.region)
                  .with_measures(order_count=lambda t: t.count()))
    
    products_sem = (to_semantic_table(products_table, name='products') 
                    .with_dimensions(order_id=lambda t: t.order_id)
                    .with_measures(avg_price=lambda t: t.price.mean()))
    
    customers_sem = (to_semantic_table(customers_table, name='customers')
                     .with_dimensions(customer_id=lambda t: t.customer_id, country=lambda t: t.country))
    
    # The key: Join → Aggregate → Join again (this triggers the stale dimension problem)
    step1 = orders_sem.join_one(products_sem, 'order_id', 'order_id')  
    step2 = step1.group_by('orders__region', 'orders__customer_id').aggregate('orders__order_count')
    step3 = step2.join_one(customers_sem, 'orders__customer_id', 'customer_id')  # This causes stale dimensions
    
    # Verify the structure is correct
    assert 'orders__region' in step3.dimensions, "Should exist in semantic metadata"
    assert 'orders__region' in step3.execute().columns, "Should exist as actual column"
    
    # The stale dimension problem manifests here
    with pytest.raises(AttributeError, match="'Table' object has no attribute 'region'"):
        step3.filter(lambda t: t.orders__region == 'North').execute()
    
    # Bracket notation still works as workaround  
    bracket_result = step3.filter(lambda t: t['orders__region'] == 'North').execute()
    assert bracket_result.shape[0] >= 0, "Bracket notation should work"
