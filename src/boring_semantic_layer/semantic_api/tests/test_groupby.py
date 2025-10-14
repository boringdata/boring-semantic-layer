import ibis
import pytest
from boring_semantic_layer.semantic_api.api import to_semantic_table


# Common test data for all scenarios
TEST_DATA = {
    "arr_time": [
        "2020-01-15 10:30:00",
        "2020-02-20 14:45:00",
        "2021-03-25 09:15:00",
        "2021-04-10 16:20:00",
    ],
    "category": ["A", "A", "B", "B"],
    "value": [10, 20, 30, 40],
}


def setup_semantic_table():
    """Common setup function for all tests."""
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    tbl = tbl.mutate(arr_time=tbl.arr_time.cast("timestamp"))
    return to_semantic_table(tbl)


# Test scenarios using common data and setup
INLINE_GROUPBY_TEST_CASES = [
    # Basic inline group_by - original failing pattern
    (
        lambda sem: sem.group_by(day=lambda t: t.arr_time.day()).aggregate(
            flight_count=lambda t: t.count()
        ),
        lambda result: (
            len(result) == 4
            and set(result["day"]) == {10, 15, 20, 25}
            and all(result["flight_count"] == 1)
        ),
    ),
    # Multiple inline dimensions
    (
        lambda sem: sem.group_by(
            year=lambda t: t.arr_time.year(), month=lambda t: t.arr_time.month()
        ).aggregate(total_value=lambda t: t.value.sum()),
        lambda result: (
            len(result) == 4
            and set(result["year"]) == {2020, 2021}
            and set(result["month"]) == {1, 2, 3, 4}
        ),
    ),
    # Mixed string keys with inline dimensions
    (
        lambda sem: sem.with_dimensions(year=lambda t: t.arr_time.year())
        .group_by("category", "year", day=lambda t: t.arr_time.day())
        .aggregate(total_value=lambda t: t.value.sum()),
        lambda result: (
            len(result) == 4
            and set(result["category"]) == {"A", "B"}
            and set(result["year"]) == {2020, 2021}
            and set(result["day"]) == {10, 15, 20, 25}
        ),
    ),
    # Inline group_by with post-aggregation filter
    (
        lambda sem: sem.group_by(year=lambda t: t.arr_time.year())
        .aggregate(total_value=lambda t: t.value.sum())
        .filter(lambda t: t.total_value > 30),
        lambda result: (
            len(result) == 1
            and result["year"].iloc[0] == 2021
            and result["total_value"].iloc[0] == 70
        ),
    ),
    # Complex inline expression
    (
        lambda sem: sem.group_by(quarter=lambda t: t.arr_time.quarter()).aggregate(
            avg_value=lambda t: t.value.mean()
        ),
        lambda result: (len(result) == 2 and set(result["quarter"]) == {1, 2}),
    ),
]


@pytest.mark.parametrize("query_fn,expected_fn", INLINE_GROUPBY_TEST_CASES)
def test_inline_groupby_scenarios(query_fn, expected_fn):
    """Parametrized test for all inline group_by scenarios."""
    # Setup semantic table
    sem = setup_semantic_table()

    # Execute query
    query = query_fn(sem)
    result = query.execute()

    # Validate result
    assert expected_fn(result), "Inline group_by test failed"


def test_inline_group_by_returns_semantic_table():
    """Test that inline group_by returns a proper SemanticTableExpr."""
    sem = setup_semantic_table()

    # Should return SemanticTableExpr, not a wrapper
    result = sem.group_by(day=lambda t: t.arr_time.day())

    # Should have semantic methods available
    assert hasattr(result, "aggregate")
    assert hasattr(result, "filter")
    assert hasattr(result, "execute")

    # Should be able to chain operations
    final_result = result.aggregate(count=lambda t: t.count()).execute()
    assert len(final_result) == 4
    assert all(final_result["count"] == 1)


def test_groupby_with_predefined_measures():
    """Test group_by operations with predefined measures."""
    tbl = ibis.memtable(TEST_DATA, name="test_tbl")
    tbl = tbl.mutate(arr_time=tbl.arr_time.cast("timestamp"))
    
    # Create semantic table with predefined measures
    sem = (
        to_semantic_table(tbl)
        .with_dimensions(category=lambda t: t.category)
        .with_measures(
            total_value=lambda t: t.value.sum(),
            count_rows=lambda t: t.count(),
            avg_value=lambda t: t.value.mean()
        )
    )
    
    # Test 1: Use predefined measure in group_by aggregation
    result1 = sem.group_by("category").aggregate(lambda t: t.total_value)
    df1 = result1.execute()
    
    assert len(df1) == 2  # Two categories
    assert "category" in df1.columns
    assert "total_value" in df1.columns
    
    # Check the aggregated values
    category_totals = df1.set_index("category")["total_value"].to_dict()
    assert category_totals["A"] == 30  # 10 + 20
    assert category_totals["B"] == 70  # 30 + 40
    
    # Test 2: Use multiple predefined measures
    result2 = sem.group_by("category").aggregate(
        total=lambda t: t.total_value,
        count=lambda t: t.count_rows,
        average=lambda t: t.avg_value
    )
    df2 = result2.execute()
    
    assert len(df2) == 2
    assert "total" in df2.columns
    assert "count" in df2.columns
    assert "average" in df2.columns
    
    # Test 3: Group by inline dimension with predefined measure
    result3 = sem.group_by(
        year=lambda t: t.arr_time.year()
    ).aggregate(lambda t: t.total_value)
    df3 = result3.execute()
    
    assert len(df3) == 2  # Two years: 2020, 2021
    assert "year" in df3.columns
    assert "total_value" in df3.columns
    
    # Check year totals
    year_totals = df3.set_index("year")["total_value"].to_dict()
    assert year_totals[2020] == 30  # Jan + Feb 2020
    assert year_totals[2021] == 70  # Mar + Apr 2021
