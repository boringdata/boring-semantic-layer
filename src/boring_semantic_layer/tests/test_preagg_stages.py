"""Contracts for the staged source-aware pre-aggregation planner."""

from __future__ import annotations

import attrs
import ibis
import pandas as pd
import pytest
from attr.exceptions import FrozenInstanceError

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.ops import (
    _collect_join_tree_info,
    _find_all_root_models,
    _rebind_to_canonical_backend,
)
from boring_semantic_layer.preagg import (
    AggregationPlanningStage,
    CalculatedMeasureStage,
    FilterPlanningStage,
    FinalProjectionStage,
    JoinPreparationStage,
    MetadataStage,
    PreAggregationPlanner,
    PreAggregationRequest,
    ResultCombinationStage,
    SourcePreAggregationStage,
)


@pytest.fixture
def staged_query():
    connection = ibis.duckdb.connect(":memory:")
    orders_table = connection.create_table(
        "stage_orders",
        pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "region": ["east", "east", "west"],
                "amount": [100, 200, 50],
            }
        ),
    )
    items_table = connection.create_table(
        "stage_items",
        pd.DataFrame(
            {
                "item_id": [10, 11, 12, 13, 14],
                "order_id": [1, 1, 2, 3, 3],
                "quantity": [1, 2, 3, 4, 5],
            }
        ),
    )
    orders = (
        to_semantic_table(orders_table, "orders")
        .with_dimensions(
            order_id=lambda table: table.order_id,
            region=lambda table: table.region,
        )
        .with_measures(
            revenue=lambda table: table.amount.sum(),
            order_count=lambda table: table.count(),
        )
    )
    items = (
        to_semantic_table(items_table, "items")
        .with_dimensions(
            item_id=lambda table: table.item_id,
            order_id=lambda table: table.order_id,
        )
        .with_measures(
            item_count=lambda table: table.count(),
            units=lambda table: table.quantity.sum(),
        )
    )
    joined = orders.join_many(items, on="order_id")
    query = joined.group_by("orders.region").aggregate(
        "orders.revenue",
        "items.units",
    )
    aggregate_op = query.op()
    join_op = joined.op()
    request = PreAggregationRequest(
        aggregate_op=aggregate_op,
        all_roots=_find_all_root_models(aggregate_op.source),
        join_op=join_op,
        join_tree_info=_collect_join_tree_info(join_op),
    )
    return query, request


def test_request_is_an_attrs_frozen_record(staged_query):
    _query, request = staged_query

    assert attrs.has(type(request))
    assert request.filters == ()
    with pytest.raises(FrozenInstanceError):
        request.filters = (lambda table: table.order_id > 1,)


def test_planner_declares_its_stage_order():
    assert PreAggregationPlanner.stage_types == (
        MetadataStage,
        JoinPreparationStage,
        FilterPlanningStage,
        AggregationPlanningStage,
        SourcePreAggregationStage,
        ResultCombinationStage,
        CalculatedMeasureStage,
        FinalProjectionStage,
    )
    assert all(attrs.has(stage_type) for stage_type in PreAggregationPlanner.stage_types)


def test_stage_outputs_make_source_partitioning_explicit(staged_query):
    _query, request = staged_query

    metadata = MetadataStage(request).run()
    prepared_join = JoinPreparationStage(metadata).run()
    filter_plan = FilterPlanningStage(prepared_join).run()
    partition = AggregationPlanningStage(filter_plan).run()

    assert metadata.group_by_cols == ("orders.region",)
    assert set(partition.measures_by_source) == {"orders", "items"}
    assert set(partition.measures_by_source["orders"]) == {"orders.revenue"}
    assert set(partition.measures_by_source["items"]) == {"items.units"}
    with pytest.raises(TypeError):
        partition.measures_by_source["other"] = {}


def test_filter_stage_records_exact_source_ownership(staged_query):
    _query, base_request = staged_query
    request = PreAggregationRequest(
        aggregate_op=base_request.aggregate_op,
        all_roots=base_request.all_roots,
        join_op=base_request.join_op,
        join_tree_info=base_request.join_tree_info,
        filters=(lambda table: table["items.item_id"] > 10,),
    )

    metadata = MetadataStage(request).run()
    prepared_join = JoinPreparationStage(metadata).run()
    filter_plan = FilterPlanningStage(prepared_join).run()

    assert filter_plan.owners == (frozenset({"items"}),)
    assert filter_plan.table is not None


def test_staged_planner_preserves_fanout_safe_results(staged_query):
    query, request = staged_query

    planned = PreAggregationPlanner(request).run()
    direct = _rebind_to_canonical_backend(planned).execute().sort_values("orders.region")
    public = query.execute().sort_values("orders.region")

    pd.testing.assert_frame_equal(
        direct.reset_index(drop=True),
        public.reset_index(drop=True),
    )
    assert direct["orders.revenue"].tolist() == [300, 50]
    assert direct["items.units"].tolist() == [6, 9]
