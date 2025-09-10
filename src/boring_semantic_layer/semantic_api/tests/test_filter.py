import ibis
import pandas as pd
import pytest
from ibis.expr.sql import convert
from boring_semantic_layer.semantic_api.api import (
    to_semantic_table,
    where_,
    with_dimensions,
)


# Common test data for all scenarios
TEST_DATA = {
    "category": ["A", "A", "B", "B", "C", "C"],
    "group": ["X", "Y", "X", "Y", "X", "Z"],
    "value": [10, 20, 30, 40, 50, 60],
    "priority": [1, 2, 1, 2, 1, 2],
    "timestamp": [
        "2020-01-15",
        "2020-02-20",
        "2021-03-25",
        "2021-04-10",
        "2022-05-15",
        "2022-06-20",
    ],
}


def setup_semantic_table():
    """Common setup function for all tests."""
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    tbl = tbl.mutate(timestamp=tbl.timestamp.cast("timestamp"))

    return with_dimensions(
        to_semantic_table(tbl),
        year=lambda t: t.timestamp.year(),
        month=lambda t: t.timestamp.month(),
    )


# Test scenarios using common data and setup
FILTER_TEST_CASES = [
    # Basic pre-aggregation filtering
    (
        lambda sem: where_(sem, lambda t: t.category == "A"),
        lambda result: (len(result) == 2 and all(result["category"] == "A")),
    ),
    # Semantic dimensions pre-aggregation
    (
        lambda sem: sem.filter(lambda t: t.year == 2021),
        lambda result: (
            len(result) == 2
            and all(pd.to_datetime(result["timestamp"]).dt.year == 2021)
        ),
    ),
    # Post-aggregation on dimension
    (
        lambda sem: (
            sem.group_by("category")
            .aggregate(total=lambda t: t.value.sum())
            .filter(lambda t: t.category == "A")
        ),
        lambda result: (
            len(result) == 1
            and result["category"].iloc[0] == "A"
            and result["total"].iloc[0] == 30  # 10 + 20
        ),
    ),
    # Post-aggregation on measure
    (
        lambda sem: (
            sem.group_by("category")
            .aggregate(total=lambda t: t.value.sum())
            .filter(lambda t: t.total > 50)
        ),
        lambda result: (len(result) == 2 and set(result["category"]) == {"B", "C"}),
    ),
    # Post-aggregation with semantic dimensions
    (
        lambda sem: (
            sem.group_by("year")
            .aggregate(total=lambda t: t.value.sum())
            .filter(lambda t: t.year > 2020)
        ),
        lambda result: (len(result) == 2 and set(result["year"]) == {2021, 2022}),
    ),
    # Filter chaining
    (
        lambda sem: (
            sem.filter(lambda t: t.category.isin(["A", "B"]))
            .filter(lambda t: t.priority == 2)
            .group_by("category")
            .aggregate(total=lambda t: t.value.sum())
        ),
        lambda result: (len(result) == 2 and set(result["category"]) == {"A", "B"}),
    ),
    # Multiple conditions (chained) - using only total to avoid count issues
    (
        lambda sem: (
            sem.group_by("category")
            .aggregate(total=lambda t: t.value.sum())
            .filter(lambda t: t.total > 30)
            .filter(lambda t: t.total < 100)
        ),
        lambda result: (
            len(result) == 1
            and result["category"].iloc[0] == "B"
            and result["total"].iloc[0] == 70
        ),
    ),
    # Timestamp comparison
    (
        lambda sem: (
            sem.group_by("timestamp")
            .aggregate(total=lambda t: t.value.sum())
            .filter(lambda t: t.timestamp >= pd.Timestamp("2021-01-01"))
        ),
        lambda result: (
            len(result) == 4
            and all(pd.to_datetime(result["timestamp"]) >= pd.Timestamp("2021-01-01"))
        ),
    ),
    # Year extraction from timestamp
    (
        lambda sem: (
            sem.group_by("timestamp")
            .aggregate(total=lambda t: t.value.sum())
            .filter(lambda t: t.timestamp.year() > 2020)
        ),
        lambda result: (
            len(result) == 4 and all(pd.to_datetime(result["timestamp"]).dt.year > 2020)
        ),
    ),
]


@pytest.mark.parametrize("query_fn,expected_fn", FILTER_TEST_CASES)
def test_filter_scenarios(query_fn, expected_fn):
    """Parametrized test for all filter scenarios."""
    # Setup semantic table
    sem = setup_semantic_table()

    # Execute query
    query = query_fn(sem)
    result = query.execute()

    # Validate result
    assert expected_fn(result), "Filter test failed"


def test_filter_converts_to_ibis():
    """Test that filter operations convert properly to Ibis operations."""
    sem = setup_semantic_table()
    filtered = where_(sem, lambda t: t.value > 30)

    # Convert to Ibis and check it's a filter operation
    ibis_expr = convert(filtered, catalog={})

    # The converted expression should be equivalent to tbl.filter(tbl.value > 30)
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    expected = tbl.filter(tbl.value > 30)

    # Both should execute to the same result
    result1 = ibis_expr.execute()
    result2 = expected.execute()

    assert len(result1) == len(result2) == 3
    assert set(result1["value"]) == set(result2["value"]) == {40, 50, 60}


def test_filter_method_availability():
    """Test that filter methods are available on semantic tables."""
    sem = setup_semantic_table()

    # Check that both filter and where methods exist
    assert hasattr(sem, "filter")
    assert hasattr(sem, "where")

    # Both should return semantic table expressions
    filtered1 = sem.filter(lambda t: t.value > 30)
    filtered2 = sem.where(lambda t: t.value > 30)

    assert hasattr(filtered1, "execute")
    assert hasattr(filtered2, "execute")

    # Both should produce the same result
    result1 = filtered1.execute()
    result2 = filtered2.execute()

    assert len(result1) == len(result2) == 3
    assert set(result1["value"]) == set(result2["value"]) == {40, 50, 60}


def test_filter_with_predefined_measures():
    """Test filtering operations with predefined measures (new functionality)."""
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    tbl = tbl.mutate(timestamp=tbl.timestamp.cast("timestamp"))
    
    # Create semantic table with predefined measures
    sem = (
        to_semantic_table(tbl)
        .with_dimensions(
            category=lambda t: t.category,
            group=lambda t: t.group,
        )
        .with_measures(
            total_value=lambda t: t.value.sum(),
            avg_value=lambda t: t.value.mean(),
            count_rows=lambda t: t.count(),
        )
    )
    
    # Test 1: Pre-aggregation filter, then use predefined measure
    result1 = (
        sem.filter(lambda t: t.priority > 1)
        .group_by("category")
        .aggregate(lambda t: t.total_value)
    )
    df1 = result1.execute()
    
    # Should only include priority > 1 rows: [20, 40, 60]
    # Category A: 20, Category B: 40, Category C: 60
    assert len(df1) == 3
    assert "category" in df1.columns
    assert "total_value" in df1.columns
    
    # Test 2: Aggregate with predefined measure, then filter on result
    result2 = (
        sem.group_by("category")
        .aggregate(total=lambda t: t.total_value, count=lambda t: t.count_rows)
        .filter(lambda t: t.total > 50)
    )
    df2 = result2.execute()
    
    # Category totals: A=30, B=70, C=110 -> B and C have totals > 50
    assert len(df2) == 2
    assert set(df2["category"]) == {"B", "C"}
    assert all(df2["total"] > 50)
    
    # Test 3: Filter with predefined measures in complex chain
    result3 = (
        sem.filter(lambda t: t.category.isin(["A", "B"]))
        .group_by("group")
        .aggregate(
            total=lambda t: t.total_value,
            average=lambda t: t.avg_value
        )
        .filter(lambda t: t.total > 25)
    )
    df3 = result3.execute()
    
    assert len(df3) >= 1  # Should have at least one group with total > 25
    assert "total" in df3.columns
    assert "average" in df3.columns
    assert all(df3["total"] > 25)
