"""Tests for dependency graph functionality."""

import ibis

from boring_semantic_layer.api import to_semantic_table
from boring_semantic_layer.dependency_graph import get_dependents


def test_simple_dimension_dependencies():
    """Test that dimensions track their column dependencies."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
        }
    )

    sm = to_semantic_table(tbl).with_dimensions(
        revenue=lambda t: t.quantity * t.price,
    )

    graph = sm.graph

    assert "revenue" in graph
    assert set(graph["revenue"]["deps"].keys()) == {"quantity", "price"}
    assert graph["revenue"]["type"] == "dimension"


def test_measure_dependencies():
    """Test that measures track their dependencies."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
        }
    )

    sm = (
        to_semantic_table(tbl)
        .with_dimensions(revenue=lambda t: t.quantity * t.price)
        .with_measures(total_revenue=lambda t: t.revenue.sum())
    )

    graph = sm.graph

    # Check dimension dependencies
    assert "revenue" in graph
    assert set(graph["revenue"]["deps"].keys()) == {"quantity", "price"}

    # Check measure dependencies
    assert "total_revenue" in graph
    assert set(graph["total_revenue"]["deps"].keys()) == {"revenue"}


def test_calculated_measure_dependencies():
    """Test that calculated measures track their measure dependencies."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "amount": [100, 200, 300],
        }
    )

    sm = (
        to_semantic_table(tbl)
        .with_measures(
            total_quantity=lambda t: t.quantity.sum(),
            total_amount=lambda t: t.amount.sum(),
        )
        .with_measures(
            # This creates a calculated measure
            avg_price=lambda m: m.total_amount / m.total_quantity,
        )
    )

    graph = sm.graph

    # Base measures depend on columns
    assert set(graph["total_quantity"]["deps"].keys()) == {"quantity"}
    assert set(graph["total_amount"]["deps"].keys()) == {"amount"}

    # Calculated measure depends on other measures
    assert set(graph["avg_price"]["deps"].keys()) == {"total_amount", "total_quantity"}


def test_get_dependents():
    """Test the reverse dependency lookup (what depends on this field)."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
        }
    )

    sm = (
        to_semantic_table(tbl)
        .with_dimensions(revenue=lambda t: t.quantity * t.price)
        .with_measures(
            total_revenue=lambda t: t.revenue.sum(),
            avg_revenue=lambda t: t.revenue.mean(),
        )
    )

    graph = sm.graph
    dependents = get_dependents(graph)

    # quantity is used by revenue
    assert "quantity" in dependents
    assert dependents["quantity"] == {"revenue"}

    # price is used by revenue
    assert "price" in dependents
    assert dependents["price"] == {"revenue"}

    # revenue is used by both measures
    assert "revenue" in dependents
    assert dependents["revenue"] == {"total_revenue", "avg_revenue"}


def test_complex_dependency_chain():
    """Test a complex chain of dependencies."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "unit_price": [1.5, 2.0, 2.5],
            "discount_rate": [0.1, 0.15, 0.2],
        }
    )

    sm = (
        to_semantic_table(tbl)
        .with_dimensions(
            gross_revenue=lambda t: t.quantity * t.unit_price,
            discount_amount=lambda t: t.gross_revenue * t.discount_rate,
            net_revenue=lambda t: t.gross_revenue - t.discount_amount,
        )
        .with_measures(
            total_net_revenue=lambda t: t.net_revenue.sum(),
        )
    )

    graph = sm.graph

    # Check all dependency chains
    assert set(graph["gross_revenue"]["deps"].keys()) == {"quantity", "unit_price"}
    assert set(graph["discount_amount"]["deps"].keys()) == {"gross_revenue", "discount_rate"}
    assert set(graph["net_revenue"]["deps"].keys()) == {"gross_revenue", "discount_amount"}
    assert set(graph["total_net_revenue"]["deps"].keys()) == {"net_revenue"}

    # Check reverse dependencies
    dependents = get_dependents(graph)
    assert dependents["quantity"] == {"gross_revenue"}
    assert dependents["gross_revenue"] == {"discount_amount", "net_revenue"}
    assert dependents["net_revenue"] == {"total_net_revenue"}


def test_multiple_dimension_usage():
    """Test when a column is used by multiple dimensions."""
    tbl = ibis.memtable(
        {
            "value": [10, 20, 30],
        }
    )

    sm = to_semantic_table(tbl).with_dimensions(
        doubled=lambda t: t.value * 2,
        tripled=lambda t: t.value * 3,
        squared=lambda t: t.value * t.value,
    )

    graph = sm.graph
    dependents = get_dependents(graph)

    # All three dimensions depend on value
    assert dependents["value"] == {"doubled", "tripled", "squared"}


def test_no_dependencies():
    """Test dimensions that reference base columns only."""
    tbl = ibis.memtable(
        {
            "col_a": [1, 2, 3],
            "col_b": [4, 5, 6],
        }
    )

    sm = to_semantic_table(tbl).with_dimensions(
        dim_a=lambda t: t.col_a,
        dim_b=lambda t: t.col_b,
    )

    graph = sm.graph

    # Simple column references
    assert set(graph["dim_a"]["deps"].keys()) == {"col_a"}
    assert set(graph["dim_b"]["deps"].keys()) == {"col_b"}


def test_measure_with_no_dimension_dependency():
    """Test measures that only depend on columns, not dimensions."""
    tbl = ibis.memtable(
        {
            "amount": [100, 200, 300],
        }
    )

    sm = to_semantic_table(tbl).with_measures(
        total=lambda t: t.amount.sum(),
    )

    graph = sm.graph

    assert set(graph["total"]["deps"].keys()) == {"amount"}


def test_graph_immutability_after_with_dimensions():
    """Test that graph updates when dimensions are added."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
        }
    )

    sm1 = to_semantic_table(tbl).with_dimensions(
        revenue=lambda t: t.quantity * t.price,
    )

    # Add another dimension
    sm2 = sm1.with_dimensions(
        double_revenue=lambda t: t.revenue * 2,
    )

    # Original graph only has revenue
    assert "revenue" in sm1.graph
    assert "double_revenue" not in sm1.graph

    # New graph has both
    assert "revenue" in sm2.graph
    assert "double_revenue" in sm2.graph
    assert set(sm2.graph["double_revenue"]["deps"].keys()) == {"revenue"}


def test_empty_semantic_table():
    """Test graph for a semantic table with no dimensions or measures."""
    tbl = ibis.memtable(
        {
            "col_a": [1, 2, 3],
        }
    )

    sm = to_semantic_table(tbl)

    graph = sm.graph

    assert graph == {}


def test_graph_with_deferred_expressions():
    """Test that Deferred expressions (using _) work correctly."""
    from ibis import _

    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
        }
    )

    sm = to_semantic_table(tbl).with_dimensions(
        revenue=_.quantity * _.price,
    )

    graph = sm.graph

    assert "revenue" in graph
    assert set(graph["revenue"]["deps"].keys()) == {"quantity", "price"}
