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


def test_bfs_traversal():
    """Test breadth-first search traversal of dependencies."""
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

    # BFS from total_net_revenue should visit in breadth-first order
    bfs_order = list(graph.bfs("total_net_revenue"))
    assert bfs_order[0] == "total_net_revenue"
    assert bfs_order[1] == "net_revenue"
    # gross_revenue and discount_amount should come before their dependencies
    assert "gross_revenue" in bfs_order
    assert "discount_amount" in bfs_order
    assert bfs_order.index("gross_revenue") < bfs_order.index("quantity")
    assert bfs_order.index("gross_revenue") < bfs_order.index("unit_price")

    # Test BFS from multiple starting points
    bfs_multi = list(graph.bfs(["gross_revenue", "discount_amount"]))
    assert "gross_revenue" in bfs_multi[:2]
    assert "discount_amount" in bfs_multi[:2]


def test_dfs_traversal():
    """Test depth-first search traversal of dependencies."""
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

    # DFS from total_net_revenue should visit deeply first
    dfs_order = list(graph.dfs("total_net_revenue"))
    assert dfs_order[0] == "total_net_revenue"
    assert dfs_order[1] == "net_revenue"
    # Should visit all nodes
    assert (
        len(dfs_order) == 7
    )  # total_net_revenue, net_revenue, gross_revenue, discount_amount, quantity, unit_price, discount_rate

    # Test DFS from multiple starting points (both should be visited early, order may vary)
    dfs_multi = list(graph.dfs(["gross_revenue", "discount_amount"]))
    assert "gross_revenue" in dfs_multi
    assert "discount_amount" in dfs_multi
    # Both starting points should appear before their common dependency
    assert dfs_multi.index("gross_revenue") < dfs_multi.index("quantity")
    assert dfs_multi.index("discount_amount") < dfs_multi.index("discount_rate")


def test_bfs_simple_chain():
    """Test BFS on a simple dependency chain."""
    tbl = ibis.memtable({"quantity": [10, 20, 30], "price": [1.5, 2.0, 2.5]})

    sm = (
        to_semantic_table(tbl)
        .with_dimensions(revenue=lambda t: t.quantity * t.price)
        .with_measures(total_revenue=lambda t: t.revenue.sum())
    )

    graph = sm.graph
    bfs_order = list(graph.bfs("total_revenue"))

    # Should visit in order: total_revenue -> revenue -> [quantity, price] (order of quantity/price may vary)
    assert bfs_order[0] == "total_revenue"
    assert bfs_order[1] == "revenue"
    assert set(bfs_order[2:]) == {"quantity", "price"}
    assert len(bfs_order) == 4


def test_invert_graph():
    """Test graph inversion to get dependents instead of dependencies."""
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
    inverted = graph.invert()

    # In inverted graph, dependencies become dependents
    # Original: revenue depends on [quantity, price]
    # Inverted: quantity and price depend on [revenue]
    assert "quantity" in inverted
    assert "price" in inverted
    assert "revenue" in inverted["quantity"]["deps"]
    assert "revenue" in inverted["price"]["deps"]

    # Original: total_revenue and avg_revenue depend on revenue
    # Inverted: revenue depends on [total_revenue, avg_revenue]
    assert set(inverted["revenue"]["deps"].keys()) == {"total_revenue", "avg_revenue"}

    # Check that types are preserved
    assert inverted["revenue"]["type"] == "dimension"
    assert inverted["total_revenue"]["type"] == "measure"
    assert inverted["quantity"]["type"] == "column"


def test_invert_preserves_all_nodes():
    """Test that invert includes all nodes including base columns."""
    tbl = ibis.memtable({"col_a": [1, 2, 3], "col_b": [4, 5, 6]})

    sm = to_semantic_table(tbl).with_dimensions(
        dim_a=lambda t: t.col_a,
        dim_b=lambda t: t.col_b,
    )

    graph = sm.graph
    inverted = graph.invert()

    # Both base columns and dimensions should be in inverted graph
    assert "col_a" in inverted
    assert "col_b" in inverted
    assert "dim_a" in inverted
    assert "dim_b" in inverted

    # Base columns should have type "column"
    assert inverted["col_a"]["type"] == "column"
    assert inverted["col_b"]["type"] == "column"


def test_bfs_dfs_visit_all_dependencies():
    """Test that BFS and DFS visit all transitive dependencies."""
    tbl = ibis.memtable(
        {
            "a": [1, 2],
            "b": [3, 4],
            "c": [5, 6],
        }
    )

    sm = (
        to_semantic_table(tbl)
        .with_dimensions(
            ab=lambda t: t.a + t.b,
            abc=lambda t: t.ab + t.c,
        )
        .with_measures(total_abc=lambda t: t.abc.sum())
    )

    graph = sm.graph

    # Both BFS and DFS should visit all 6 nodes
    bfs_nodes = set(graph.bfs("total_abc"))
    dfs_nodes = set(graph.dfs("total_abc"))

    expected = {"total_abc", "abc", "ab", "a", "b", "c"}
    assert bfs_nodes == expected
    assert dfs_nodes == expected


def test_to_dict_export():
    """Test exporting graph to dictionary format."""
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
    graph_dict = graph.to_dict()

    # Check structure
    assert "nodes" in graph_dict
    assert "edges" in graph_dict
    assert isinstance(graph_dict["nodes"], list)
    assert isinstance(graph_dict["edges"], list)

    # Check nodes
    node_ids = {node["id"] for node in graph_dict["nodes"]}
    assert node_ids == {"quantity", "price", "revenue", "total_revenue", "avg_revenue"}

    # Check node types
    node_types = {node["id"]: node["type"] for node in graph_dict["nodes"]}
    assert node_types["quantity"] == "column"
    assert node_types["price"] == "column"
    assert node_types["revenue"] == "dimension"
    assert node_types["total_revenue"] == "measure"
    assert node_types["avg_revenue"] == "measure"

    # Check edges
    edges = {(edge["source"], edge["target"]) for edge in graph_dict["edges"]}
    assert ("quantity", "revenue") in edges
    assert ("price", "revenue") in edges
    assert ("revenue", "total_revenue") in edges
    assert ("revenue", "avg_revenue") in edges

    # Check edge types
    edge_types = {(edge["source"], edge["target"]): edge["type"] for edge in graph_dict["edges"]}
    assert edge_types[("quantity", "revenue")] == "column"
    assert edge_types[("price", "revenue")] == "column"
    assert edge_types[("revenue", "total_revenue")] == "dimension"
    assert edge_types[("revenue", "avg_revenue")] == "dimension"

    # Verify it's JSON serializable
    import json

    json_str = json.dumps(graph_dict)
    assert isinstance(json_str, str)
    assert len(json_str) > 0


def test_graph_accessible_from_all_node_types():
    """Test that graph is accessible from all semantic table node types."""
    tbl = ibis.memtable(
        {
            "quantity": [10, 20, 30],
            "price": [1.5, 2.0, 2.5],
            "category": ["A", "B", "A"],
        }
    )

    sm = (
        to_semantic_table(tbl)
        .with_dimensions(
            revenue=lambda t: t.quantity * t.price,
            cat=lambda t: t.category,
        )
        .with_measures(total_revenue=lambda t: t.revenue.sum())
    )

    # Test graph on SemanticModel
    model_graph = sm.graph
    assert "revenue" in model_graph
    assert "total_revenue" in model_graph

    # Test graph on SemanticFilter
    filtered = sm.filter(lambda t: t.quantity > 15)
    filtered_graph = filtered.graph
    assert "revenue" in filtered_graph
    assert "total_revenue" in filtered_graph

    # Test graph on SemanticGroupBy
    grouped = sm.group_by("revenue")
    grouped_graph = grouped.graph
    assert "revenue" in grouped_graph
    assert "total_revenue" in grouped_graph

    # Test graph on SemanticAggregate
    aggregated = sm.group_by("cat").aggregate("total_revenue")
    aggregated_graph = aggregated.graph
    assert "revenue" in aggregated_graph
    assert "total_revenue" in aggregated_graph

    # Test graph on SemanticOrderBy
    ordered = sm.order_by("revenue")
    ordered_graph = ordered.graph
    assert "revenue" in ordered_graph
    assert "total_revenue" in ordered_graph

    # Test graph on SemanticLimit
    limited = sm.limit(2)
    limited_graph = limited.graph
    assert "revenue" in limited_graph
    assert "total_revenue" in limited_graph

    # Test graph on SemanticMutate
    mutated = sm.mutate(double_qty=lambda t: t.quantity * 2)
    mutated_graph = mutated.graph
    assert "revenue" in mutated_graph
    assert "total_revenue" in mutated_graph

    # All graphs should be the same (pass-through nodes)
    assert model_graph == filtered_graph == grouped_graph == aggregated_graph
    assert model_graph == ordered_graph == limited_graph == mutated_graph


def test_bfs_dfs_nonexistent_node():
    """Test BFS/DFS on non-existent nodes."""
    tbl = ibis.memtable({"a": [1, 2, 3]})
    sm = to_semantic_table(tbl).with_dimensions(dim_a=lambda t: t.a)

    graph = sm.graph

    # BFS/DFS should still yield the starting node even if it doesn't exist in graph
    assert list(graph.bfs("nonexistent")) == ["nonexistent"]
    assert list(graph.dfs("nonexistent")) == ["nonexistent"]


def test_predecessors_successors_nonexistent():
    """Test predecessors/successors for non-existent nodes."""
    tbl = ibis.memtable({"a": [1, 2, 3]})
    sm = to_semantic_table(tbl).with_dimensions(dim_a=lambda t: t.a)

    graph = sm.graph

    # Should return empty sets for non-existent nodes
    assert graph.predecessors("nonexistent") == set()
    assert graph.successors("nonexistent") == set()


def test_invert_empty_graph():
    """Test inverting an empty graph."""
    tbl = ibis.memtable({"a": [1, 2, 3]})
    sm = to_semantic_table(tbl)

    graph = sm.graph
    inverted = graph.invert()

    # Empty graph should invert to empty graph
    assert inverted == {}


def test_to_dict_empty_graph():
    """Test to_dict on empty graph."""
    tbl = ibis.memtable({"a": [1, 2, 3]})
    sm = to_semantic_table(tbl)

    graph = sm.graph
    graph_dict = graph.to_dict()

    # Should return empty nodes and edges
    assert graph_dict == {"nodes": [], "edges": []}


def test_bfs_dfs_circular_protection():
    """Test that BFS/DFS handle visited nodes correctly (no infinite loops)."""
    tbl = ibis.memtable({"a": [1, 2], "b": [3, 4]})
    sm = to_semantic_table(tbl).with_dimensions(
        dim_a=lambda t: t.a,
        dim_b=lambda t: t.b,
    )

    graph = sm.graph

    # Even with multiple starting points that share dependencies, each node visited once
    bfs_result = list(graph.bfs(["dim_a", "dim_b"]))
    dfs_result = list(graph.dfs(["dim_a", "dim_b"]))

    # Check no duplicates
    assert len(bfs_result) == len(set(bfs_result))
    assert len(dfs_result) == len(set(dfs_result))


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

    # Graph should contain fields from both tables with prefixes (matching get_dimensions())
    graph = joined.graph
    assert "flights.carrier" in graph
    assert "flights.total_distance" in graph
    assert "carriers.carrier_name" in graph
    assert "carriers.carrier_count" in graph

    # Verify dependencies are preserved with prefixes
    assert set(graph["flights.carrier"]["deps"].keys()) == {"flights.carrier_code"}
    assert set(graph["flights.total_distance"]["deps"].keys()) == {"flights.distance"}
    assert set(graph["carriers.carrier_name"]["deps"].keys()) == {"carriers.name"}
    assert set(graph["carriers.carrier_count"]["deps"].keys()) == set()
