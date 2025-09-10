import ibis
import pytest
from boring_semantic_layer.semantic_api.api import to_semantic_table


# Common test data for all scenarios
TEST_DATA = {
    "category": ["A", "B", "C", "D", "E"],
    "value": [10, 20, 30, 40, 50],
    "priority": [1, 2, 1, 2, 1],
}


def setup_semantic_table():
    """Common setup function for all tests."""
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    return to_semantic_table(tbl)


# Test scenarios using common data and setup
LIMIT_TEST_CASES = [
    # Basic limit
    (
        lambda sem: sem.limit(3),
        lambda result: (
            len(result) == 3
            and list(result["category"]) == ["A", "B", "C"]
            and list(result["value"]) == [10, 20, 30]
        ),
    ),
    # Limit with offset
    (
        lambda sem: sem.limit(2, offset=1),
        lambda result: (
            len(result) == 2
            and list(result["category"]) == ["B", "C"]
            and list(result["value"]) == [20, 30]
        ),
    ),
    # Limit after aggregation
    (
        lambda sem: sem.with_dimensions(high_priority=lambda t: t.priority == 1)
        .group_by("high_priority")
        .aggregate(total_value=lambda t: t.value.sum())
        .limit(1),
        lambda result: (
            len(result) == 1
            and result["total_value"].iloc[0]
            in [60, 90]  # Either True (90: 10+30+50) or False (60: 20+40) group
        ),
    ),
    # Limit after filter
    (
        lambda sem: sem.filter(lambda t: t.priority == 1).limit(2),
        lambda result: (
            len(result) == 2
            and all(result["priority"] == 1)
            and set(result["category"]) <= {"A", "C", "E"}
        ),
    ),
    # Limit with order_by
    (
        lambda sem: sem.order_by("value").limit(2),
        lambda result: (
            len(result) == 2
            and list(result["value"]) == [10, 20]
            and list(result["category"]) == ["A", "B"]
        ),
    ),
]


@pytest.mark.parametrize("query_fn,expected_fn", LIMIT_TEST_CASES)
def test_limit_scenarios(query_fn, expected_fn):
    """Parametrized test for all limit scenarios."""
    # Setup semantic table
    sem = setup_semantic_table()

    # Execute query
    query = query_fn(sem)
    result = query.execute()

    # Validate result
    assert expected_fn(result), "Limit test failed"


def test_limit_returns_semantic_table():
    """Test that limit returns a proper SemanticTableExpr."""
    sem = setup_semantic_table()

    # Should return SemanticTableExpr
    result = sem.limit(3)

    # Should have semantic methods available
    assert hasattr(result, "aggregate")
    assert hasattr(result, "filter")
    assert hasattr(result, "group_by")
    assert hasattr(result, "limit")
    assert hasattr(result, "execute")

    # Should be able to chain operations
    chained_result = result.filter(lambda t: t.priority == 1).execute()
    assert len(chained_result) <= 3
    assert all(chained_result["priority"] == 1)


def test_limit_edge_cases():
    """Test limit edge cases."""
    sem = setup_semantic_table()

    # Limit larger than table size
    result = sem.limit(10).execute()
    assert len(result) == 5  # All rows

    # Zero limit
    result = sem.limit(0).execute()
    assert len(result) == 0

    # Offset larger than table size
    result = sem.limit(5, offset=10).execute()
    assert len(result) == 0


def test_limit_method_signature():
    """Test limit method accepts correct parameters."""
    sem = setup_semantic_table()

    # Basic limit
    result1 = sem.limit(3)
    assert result1.execute() is not None

    # Limit with offset as positional arg
    result2 = sem.limit(2, 1)
    assert len(result2.execute()) == 2

    # Limit with offset as keyword arg
    result3 = sem.limit(2, offset=1)
    assert len(result3.execute()) == 2

    # Results should be the same
    result2_data = result2.execute()
    result3_data = result3.execute()
    assert result2_data.equals(result3_data)


def test_limit_with_predefined_measures():
    """Test limit operations with predefined measures (new functionality)."""
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    
    # Create semantic table with predefined measures
    sem = (
        to_semantic_table(tbl)
        .with_dimensions(
            category=lambda t: t.category,
            priority=lambda t: t.priority,
        )
        .with_measures(
            total_value=lambda t: t.value.sum(),
            max_value=lambda t: t.value.max(),
            count_rows=lambda t: t.count(),
        )
    )
    
    # Test 1: Aggregate with predefined measure, then limit
    result1 = (
        sem.group_by("priority")
        .aggregate(lambda t: t.total_value)
        .order_by("total_value")
        .limit(1)
    )
    df1 = result1.execute()
    
    assert len(df1) == 1
    assert "priority" in df1.columns
    assert "total_value" in df1.columns
    
    # Test 2: Multiple predefined measures with limit
    result2 = (
        sem.group_by("category")
        .aggregate(
            total=lambda t: t.total_value,
            max_val=lambda t: t.max_value,
            count=lambda t: t.count_rows
        )
        .order_by("total")
        .limit(2)
    )
    df2 = result2.execute()
    
    assert len(df2) == 2
    assert "total" in df2.columns
    assert "max_val" in df2.columns
    assert "count" in df2.columns
    
    # Should be ordered by total (ascending)
    assert df2["total"].iloc[0] <= df2["total"].iloc[1]
    
    # Test 3: Limit with offset and predefined measures
    result3 = (
        sem.group_by("category")
        .aggregate(total=lambda t: t.total_value)
        .order_by("category")
        .limit(2, offset=1)
    )
    df3 = result3.execute()
    
    assert len(df3) == 2  # Should get 2 rows after skipping first
    assert "total" in df3.columns
    assert "category" in df3.columns
    
    # Test 4: Complex chain with filter, aggregate, and limit
    result4 = (
        sem.filter(lambda t: t.value >= 20)
        .group_by("priority")  
        .aggregate(
            total=lambda t: t.total_value,
            count=lambda t: t.count_rows
        )
        .limit(1)
    )
    df4 = result4.execute()
    
    assert len(df4) == 1
    assert "total" in df4.columns
    assert "count" in df4.columns
