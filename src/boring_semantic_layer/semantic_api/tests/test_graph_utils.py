import ibis

import pytest

from boring_semantic_layer.semantic_api.graph_utils import (
    to_node,
    gen_children_of,
    bfs,
    walk_nodes,
)
from ibis.expr.operations.relations import Aggregate


def test_bfs_and_gen_children_of_simple_expr():
    # Build a simple aggregation expression
    t = ibis.memtable({"x": [1, 2, 3]}, name="t")
    expr = t.group_by("x").aggregate(sum_x=t.x.sum())

    # BFS should map each Node to its children
    graph = bfs(expr)
    root = to_node(expr)
    assert root in graph, "Root node not in BFS graph"
    children = graph[root]
    assert isinstance(children, tuple) and children, "Expected non-empty children tuple"

    # gen_children_of should agree for the root
    direct = gen_children_of(root)
    assert isinstance(direct, tuple)
    assert set(direct) == set(children)


def test_walk_nodes_finds_aggregation():
    t = ibis.memtable({"x": [1, 2, 3]}, name="t")
    expr = t.group_by("x").aggregate(sum_x=t.x.sum())

    # walk_nodes should find the aggregation op
    agg_nodes = list(walk_nodes(Aggregate, expr))
    assert agg_nodes, "walk_nodes did not locate any Aggregate nodes"


def test_to_node_errors_on_bad_input():
    with pytest.raises(ValueError):
        to_node(123)
