#!/usr/bin/env python3
"""Example demonstrating dependency graph features in BSL.

This example shows:
1. How to access the dependency graph
2. How to use predecessors() and successors() for graph traversal
3. How to export to NetworkX JSON format for visualization
"""

import ibis

from boring_semantic_layer import to_semantic_table

# Create sample data
flights_data = {
    "flight_id": [1, 2, 3, 4, 5],
    "origin": ["JFK", "LAX", "ORD", "JFK", "LAX"],
    "destination": ["LAX", "JFK", "LAX", "ORD", "ORD"],
    "distance": [2475, 2475, 1745, 740, 1745],
    "duration": [330, 315, 180, 95, 195],
}

flights_tbl = ibis.memtable(flights_data)


def example_1_basic_graph():
    """Example 1: Basic dependency graph access."""
    print("\n" + "=" * 70)
    print("Example 1: Basic Dependency Graph")
    print("=" * 70)

    # Create semantic model with dependencies
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            route=lambda t: t.origin + " → " + t.destination,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            total_duration=lambda t: t.duration.sum(),
            avg_speed=lambda m: m.total_distance / m.total_duration * 60,
        )
    )

    print("\n1. Access the dependency graph:")
    graph = flights.graph
    print(f"   Graph contains {len(graph)} fields")

    print("\n2. Inspect a dimension's dependencies:")
    print(f"   route depends on: {graph['route']['deps']}")
    print(f"   route type: {graph['route']['type']}")

    print("\n3. Inspect a measure's dependencies:")
    print(f"   total_distance depends on: {graph['total_distance']['deps']}")
    print(f"   total_distance type: {graph['total_distance']['type']}")

    print("\n4. Inspect a calculated measure's dependencies:")
    print(f"   avg_speed depends on: {graph['avg_speed']['deps']}")
    print(f"   avg_speed type: {graph['avg_speed']['type']}")

    return graph


def example_2_graph_traversal(graph):
    """Example 2: Navigate the graph with predecessors() and successors()."""
    print("\n" + "=" * 70)
    print("Example 2: Graph Traversal")
    print("=" * 70)

    print("\n1. What does 'route' depend on? (predecessors)")
    deps = graph.predecessors("route")
    print(f"   {deps}")

    print("\n2. What depends on 'origin'? (successors)")
    dependents = graph.successors("origin")
    print(f"   {dependents}")

    print("\n3. What depends on 'total_distance'? (successors)")
    dependents = graph.successors("total_distance")
    print(f"   {dependents}")

    print("\n4. Full dependency chain for 'avg_speed':")
    print(f"   avg_speed depends on: {graph.predecessors('avg_speed')}")
    for dep in graph.predecessors("avg_speed"):
        if dep in graph:
            print(f"   └─ {dep} depends on: {graph.predecessors(dep)}")


def example_3_networkx_export(graph):
    """Example 3: Export to NetworkX JSON format."""
    print("\n" + "=" * 70)
    print("Example 3: Export to NetworkX JSON Format")
    print("=" * 70)

    print("\n1. Export graph to NetworkX node-link format:")
    json_data = graph.to_networkx_json()

    print("\n2. Graph structure:")
    print(f"   Directed: {json_data['directed']}")
    print(f"   Multigraph: {json_data['multigraph']}")
    print(f"   Number of nodes: {len(json_data['nodes'])}")
    print(f"   Number of links: {len(json_data['links'])}")

    print("\n3. Sample nodes:")
    for node in json_data["nodes"][:3]:
        print(f"   {node}")

    print("\n4. Sample links:")
    for link in json_data["links"][:3]:
        print(f"   {link}")

    print("\n5. This JSON can be used with:")
    print("   - NetworkX: nx.node_link_graph(json_data)")
    print("   - d3.js: for interactive graph visualization")
    print("   - Any graph visualization tool that accepts node-link format")

    # Optional: demonstrate NetworkX integration if available
    try:
        import json

        import networkx as nx

        print("\n6. Converting to NetworkX graph:")
        G = nx.node_link_graph(json_data)
        print(f"   Created NetworkX DiGraph with {G.number_of_nodes()} nodes")
        print(f"   and {G.number_of_edges()} edges")

        # Show serialization
        print("\n7. JSON serialization (first 200 chars):")
        json_str = json.dumps(json_data, indent=2)
        print(f"   {json_str[:200]}...")

    except ImportError:
        print("\n6. NetworkX not installed - skipping NetworkX integration example")
        print("   Install with: pip install 'boring-semantic-layer[networkx]'")


def example_4_use_cases():
    """Example 4: Practical use cases for the dependency graph."""
    print("\n" + "=" * 70)
    print("Example 4: Practical Use Cases")
    print("=" * 70)

    print("\n1. Impact Analysis:")
    print("   'What breaks if I change the distance column?'")
    print("   → Check graph.successors('distance')")

    print("\n2. Understanding Complex Models:")
    print("   'How is avg_speed calculated?'")
    print("   → Check graph['avg_speed']['deps'] and traverse recursively")

    print("\n3. Documentation Generation:")
    print("   'Create a data lineage diagram'")
    print("   → Export to NetworkX JSON and visualize with d3.js")

    print("\n4. Validation:")
    print("   'Detect circular dependencies'")
    print("   → Use NetworkX: nx.is_directed_acyclic_graph()")

    print("\n5. Optimization:")
    print("   'Find leaf nodes (base columns)'")
    print("   → Find nodes with no predecessors")

    print("\n6. Testing:")
    print("   'Test all fields that depend on this column'")
    print("   → Use graph.successors() to find affected fields")


def main():
    """Run all examples."""
    print("=" * 70)
    print("BSL Dependency Graph Examples")
    print("=" * 70)

    # Example 1: Basic graph access
    graph = example_1_basic_graph()

    # Example 2: Graph traversal
    example_2_graph_traversal(graph)

    # Example 3: NetworkX export
    example_3_networkx_export(graph)

    # Example 4: Use cases
    example_4_use_cases()

    print("\n" + "=" * 70)
    print("✓ All examples completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
