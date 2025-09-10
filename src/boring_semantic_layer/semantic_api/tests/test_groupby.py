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
