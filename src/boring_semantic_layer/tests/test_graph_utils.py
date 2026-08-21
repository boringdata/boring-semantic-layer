import ibis
import pytest
from ibis.expr.operations.relations import Aggregate

from boring_semantic_layer.graph_utils import (
    to_node,
    walk_nodes,
)


def test_walk_nodes_finds_aggregation():
    t = ibis.memtable({"x": [1, 2, 3]})
    expr = t.group_by("x").aggregate(sum_x=t.x.sum())

    # walk_nodes should find the aggregation op
    agg_nodes = list(walk_nodes(Aggregate, expr))
    assert agg_nodes, "walk_nodes did not locate any Aggregate nodes"


def test_to_node_errors_on_bad_input():
    with pytest.raises(ValueError):
        to_node(123)
