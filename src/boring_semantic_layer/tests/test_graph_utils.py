import ibis
import pytest
from ibis.expr.operations.relations import Aggregate

from boring_semantic_layer.expr import SemanticModel
from boring_semantic_layer.graph_utils import (
    bfs,
    find_dimensions_and_measures,
    find_entity_dimensions,
    find_event_timestamp_dimensions,
    gen_children_of,
    is_feature_view,
    replace_nodes,
    to_node,
    walk_nodes,
)
from boring_semantic_layer.ops import Dimension, Measure


def test_bfs_and_gen_children_of_simple_expr():
    # Build a simple aggregation expression
    t = ibis.memtable({"x": [1, 2, 3]})
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
    t = ibis.memtable({"x": [1, 2, 3]})
    expr = t.group_by("x").aggregate(sum_x=t.x.sum())

    # walk_nodes should find the aggregation op
    agg_nodes = list(walk_nodes(Aggregate, expr))
    assert agg_nodes, "walk_nodes did not locate any Aggregate nodes"


def test_to_node_errors_on_bad_input():
    with pytest.raises(ValueError):
        to_node(123)


def test_replace_nodes_identity_replacer_leaves_expr_unchanged():
    expr = ibis.literal(1) + ibis.literal(2)
    # A replacer that always returns the original op should leave the expression unchanged
    new_expr = replace_nodes(lambda op, kwargs: op, expr)
    assert str(new_expr) == str(expr)


def test_find_dimensions_and_measures_no_semantic_table():
    t = ibis.memtable({"x": [1, 2, 3]})
    dims, meas = find_dimensions_and_measures(t)
    assert dims == {}
    assert meas == {}


def test_find_dimensions_and_measures_semantic_table():
    t = ibis.memtable({"x": [1, 2, 3]})
    dims_defs = {"x": Dimension(expr=lambda tbl: tbl.x, description="dim x")}
    meas_defs = {
        "sum_x": Measure(expr=lambda tbl: tbl.x.sum(), description="measure sum_x"),
    }
    semantic = SemanticModel(
        table=t,
        dimensions=dims_defs,
        measures=meas_defs,
        calc_measures=None,
        name="mytable",
    )
    # SemanticModel is the Expression - use it directly
    dims, meas = find_dimensions_and_measures(semantic)
    assert dims == {"mytable.x": dims_defs["x"]}
    assert meas == {"mytable.sum_x": meas_defs["sum_x"]}


def test_find_entity_dimensions_no_semantic_table():
    """Test that find_entity_dimensions returns empty dict for non-semantic tables."""
    t = ibis.memtable({"x": [1, 2, 3]})
    entities = find_entity_dimensions(t)
    assert entities == {}


def test_find_entity_dimensions_no_entities():
    """Test that find_entity_dimensions returns empty dict when no entity dimensions exist."""
    t = ibis.memtable({"x": [1, 2, 3]})
    dims_defs = {"x": Dimension(expr=lambda tbl: tbl.x, description="regular dim")}
    semantic = SemanticModel(
        table=t,
        dimensions=dims_defs,
        measures=None,
        calc_measures=None,
        name="mytable",
    )
    entities = find_entity_dimensions(semantic)
    assert entities == {}


def test_find_entity_dimensions_with_entities():
    """Test that find_entity_dimensions finds entity dimensions correctly."""
    t = ibis.memtable({"business_id": [1, 2, 3], "user_id": [10, 20, 30], "x": [100, 200, 300]})
    dims_defs = {
        "business_id": Dimension(expr=lambda tbl: tbl.business_id, is_entity=True),
        "user_id": Dimension(expr=lambda tbl: tbl.user_id, is_entity=True),
        "x": Dimension(expr=lambda tbl: tbl.x, description="regular dim"),
    }
    semantic = SemanticModel(
        table=t,
        dimensions=dims_defs,
        measures=None,
        calc_measures=None,
        name="features",
    )
    entities = find_entity_dimensions(semantic)
    assert len(entities) == 2
    assert "features.business_id" in entities
    assert "features.user_id" in entities
    assert entities["features.business_id"].is_entity is True
    assert entities["features.user_id"].is_entity is True


def test_find_event_timestamp_dimensions_no_semantic_table():
    """Test that find_event_timestamp_dimensions returns empty dict for non-semantic tables."""
    t = ibis.memtable({"x": [1, 2, 3]})
    timestamps = find_event_timestamp_dimensions(t)
    assert timestamps == {}


def test_find_event_timestamp_dimensions_no_timestamps():
    """Test that find_event_timestamp_dimensions returns empty dict when no event timestamps exist."""
    t = ibis.memtable({"x": [1, 2, 3]})
    dims_defs = {"x": Dimension(expr=lambda tbl: tbl.x, description="regular dim")}
    semantic = SemanticModel(
        table=t,
        dimensions=dims_defs,
        measures=None,
        calc_measures=None,
        name="mytable",
    )
    timestamps = find_event_timestamp_dimensions(semantic)
    assert timestamps == {}


def test_find_event_timestamp_dimensions_with_timestamp():
    """Test that find_event_timestamp_dimensions finds event timestamp dimensions correctly."""
    t = ibis.memtable(
        {
            "statement_date": ["2024-01-01", "2024-01-02"],
            "order_date": ["2024-01-01", "2024-01-02"],
            "x": [100, 200],
        }
    )
    dims_defs = {
        "statement_date": Dimension(
            expr=lambda tbl: tbl.statement_date,
            is_event_timestamp=True,
            is_time_dimension=True,
            smallest_time_grain="TIME_GRAIN_DAY",
        ),
        "order_date": Dimension(expr=lambda tbl: tbl.order_date, is_time_dimension=True),
        "x": Dimension(expr=lambda tbl: tbl.x, description="regular dim"),
    }
    semantic = SemanticModel(
        table=t,
        dimensions=dims_defs,
        measures=None,
        calc_measures=None,
        name="features",
    )
    timestamps = find_event_timestamp_dimensions(semantic)
    assert len(timestamps) == 1
    assert "features.statement_date" in timestamps
    assert timestamps["features.statement_date"].is_event_timestamp is True


def test_find_entity_and_event_timestamp_together():
    """Test that both entity and event timestamp dimensions can be found in the same model."""
    t = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "balance": [1000, 2000, 3000],
        }
    )
    dims_defs = {
        "business_id": Dimension(expr=lambda tbl: tbl.business_id, is_entity=True),
        "statement_date": Dimension(
            expr=lambda tbl: tbl.statement_date,
            is_event_timestamp=True,
        ),
    }
    semantic = SemanticModel(
        table=t,
        dimensions=dims_defs,
        measures=None,
        calc_measures=None,
        name="balance",
    )

    entities = find_entity_dimensions(semantic)
    timestamps = find_event_timestamp_dimensions(semantic)

    assert len(entities) == 1
    assert "balance.business_id" in entities

    assert len(timestamps) == 1
    assert "balance.statement_date" in timestamps


def test_is_feature_view_valid():
    """Test that is_feature_view returns True for valid FeatureViews (1 entity, 1 timestamp)."""
    from boring_semantic_layer import entity_dimension, time_dimension, to_semantic_table

    t = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "balance": [1000, 2000, 3000],
        }
    )

    # Valid FeatureView - exactly 1 entity, exactly 1 event timestamp
    model = (
        to_semantic_table(t, name="balance_features")
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id),
            statement_date=time_dimension(lambda t: t.statement_date),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    assert is_feature_view(model) is True
    assert model.is_feature_view() is True  # Test method version too


def test_is_feature_view_no_entity():
    """Test that is_feature_view returns False when there's no entity dimension."""
    from boring_semantic_layer import time_dimension, to_semantic_table

    t = ibis.memtable(
        {
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "balance": [1000, 2000, 3000],
        }
    )

    # No entity dimension
    model = (
        to_semantic_table(t)
        .with_dimensions(
            statement_date=time_dimension(lambda t: t.statement_date),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    assert is_feature_view(model) is False
    assert model.is_feature_view() is False


def test_is_feature_view_no_timestamp():
    """Test that is_feature_view returns False when there's no event timestamp."""
    from boring_semantic_layer import entity_dimension, to_semantic_table

    t = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "balance": [1000, 2000, 3000],
        }
    )

    # No event timestamp
    model = (
        to_semantic_table(t)
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    assert is_feature_view(model) is False
    assert model.is_feature_view() is False


def test_is_feature_view_multiple_entities():
    """Test that is_feature_view returns False when there are multiple entity dimensions."""
    from boring_semantic_layer import entity_dimension, time_dimension, to_semantic_table

    t = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "user_id": [101, 102, 103],
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "balance": [1000, 2000, 3000],
        }
    )

    # Multiple entity dimensions
    model = (
        to_semantic_table(t)
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id),
            user_id=entity_dimension(lambda t: t.user_id),
            statement_date=time_dimension(lambda t: t.statement_date),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    assert is_feature_view(model) is False
    assert model.is_feature_view() is False


def test_is_feature_view_multiple_timestamps():
    """Test that is_feature_view returns False when there are multiple event timestamps."""
    from boring_semantic_layer import entity_dimension, time_dimension, to_semantic_table

    t = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "created_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "balance": [1000, 2000, 3000],
        }
    )

    # Multiple event timestamps
    model = (
        to_semantic_table(t)
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id),
            statement_date=time_dimension(lambda t: t.statement_date),
            created_date=time_dimension(lambda t: t.created_date),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    assert is_feature_view(model) is False
    assert model.is_feature_view() is False


def test_is_feature_view_regular_dimensions_ok():
    """Test that is_feature_view allows additional regular dimensions (not entity or timestamp)."""
    from boring_semantic_layer import entity_dimension, time_dimension, to_semantic_table

    t = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "category": ["A", "B", "C"],
            "balance": [1000, 2000, 3000],
        }
    )

    # Valid FeatureView with additional regular dimension
    model = (
        to_semantic_table(t, name="balance_features")
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id),
            statement_date=time_dimension(lambda t: t.statement_date),
            category=lambda t: t.category,  # Regular dimension is OK
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    assert is_feature_view(model) is True
    assert model.is_feature_view() is True
