"""Tests for dependency graph functionality."""

import ibis

from boring_semantic_layer.api import to_semantic_table


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


def test_successors_and_predecessors():
    """Test successors (dependents) and predecessors (dependencies) methods."""
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

    # Test predecessors (what this node depends on)
    assert graph.predecessors("revenue") == {"quantity", "price"}
    assert graph.predecessors("total_revenue") == {"revenue"}

    # Test successors (what depends on this node)
    assert graph.successors("quantity") == {"revenue"}
    assert graph.successors("price") == {"revenue"}
    assert graph.successors("revenue") == {"total_revenue", "avg_revenue"}


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

    # Check reverse dependencies (successors)
    assert graph.successors("quantity") == {"gross_revenue"}
    assert graph.successors("gross_revenue") == {"discount_amount", "net_revenue"}
    assert graph.successors("net_revenue") == {"total_net_revenue"}


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

    # All three dimensions depend on value
    assert graph.successors("value") == {"doubled", "tripled", "squared"}


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


def test_to_networkx_json():
    """Test conversion to NetworkX node-link JSON format."""
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
    json_data = graph.to_networkx_json()

    # Check structure
    assert json_data["directed"] is True
    assert json_data["multigraph"] is False
    assert "nodes" in json_data
    assert "links" in json_data

    # Check nodes
    node_ids = {node["id"] for node in json_data["nodes"]}
    assert node_ids == {"quantity", "price", "revenue", "total_revenue", "avg_revenue"}

    # Check node types
    node_types = {node["id"]: node["field_type"] for node in json_data["nodes"]}
    assert node_types["quantity"] == "column"
    assert node_types["price"] == "column"
    assert node_types["revenue"] == "dimension"
    assert node_types["total_revenue"] == "measure"
    assert node_types["avg_revenue"] == "measure"

    # Check links (edges)
    links = {(link["source"], link["target"]) for link in json_data["links"]}
    assert ("quantity", "revenue") in links
    assert ("price", "revenue") in links
    assert ("revenue", "total_revenue") in links
    assert ("revenue", "avg_revenue") in links

    # Check link types
    link_types = {
        (link["source"], link["target"]): link["dependency_type"] for link in json_data["links"]
    }
    assert link_types[("quantity", "revenue")] == "column"
    assert link_types[("price", "revenue")] == "column"
    assert link_types[("revenue", "total_revenue")] == "dimension"
    assert link_types[("revenue", "avg_revenue")] == "dimension"


def test_graph_accessible_from_all_node_types():
    """Test that graph is accessible from filter, group_by, join, etc."""
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

    # Test graph on SemanticModel
    model_graph = sm.graph
    assert "revenue" in model_graph
    assert "total_revenue" in model_graph

    # Test graph on filtered table
    filtered = sm.filter(lambda t: t.quantity > 15)
    filtered_graph = filtered.graph
    assert "revenue" in filtered_graph
    assert "total_revenue" in filtered_graph

    # Test graph on grouped table
    grouped = sm.group_by("revenue")
    grouped_graph = grouped.graph
    assert "revenue" in grouped_graph
    assert "total_revenue" in grouped_graph

    # All graphs should be the same
    assert model_graph == filtered_graph == grouped_graph


def test_graph_merge_on_join():
    """Test that graphs are computed correctly for joined semantic tables."""
    flights_tbl = ibis.memtable(
        {
            "carrier_code": ["AA", "DL", "UA"],
            "distance": [1000, 1500, 2000],
        }
    )

    carriers_tbl = ibis.memtable(
        {
            "code": ["AA", "DL", "UA"],
            "name": ["American", "Delta", "United"],
        }
    )

    # Create two semantic tables with different dimensions/measures
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(carrier=lambda t: t.carrier_code)
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    carriers = (
        to_semantic_table(carriers_tbl, name="carriers")
        .with_dimensions(carrier_name=lambda t: t.name)
        .with_measures(carrier_count=lambda t: t.count())
    )

    # Join the tables
    joined = flights.join_one(carriers, left_on="carrier_code", right_on="code")

    # Graph should contain fields from both tables (without prefixes in graph keys)
    graph = joined.graph
    assert "carrier" in graph
    assert "total_distance" in graph
    assert "carrier_name" in graph
    assert "carrier_count" in graph

    # Verify dependencies are preserved
    assert set(graph["carrier"]["deps"].keys()) == {"carrier_code"}
    assert set(graph["total_distance"]["deps"].keys()) == {"distance"}
    assert set(graph["carrier_name"]["deps"].keys()) == {"name"}
    assert set(graph["carrier_count"]["deps"].keys()) == set()
