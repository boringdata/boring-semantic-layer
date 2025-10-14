import ibis
from ibis.expr.sql import convert  # lowering is registered in semantic_api.lower
from boring_semantic_layer.semantic_api.api import (
    order_by_,
    select_,
    to_semantic_table,
    where_,
    with_dimensions,
    with_measures,
)


def test_semantic_select_and_filter_basic():
    data = {"origin": ["A", "B", "A"], "value": [1, 2, 3]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    sem = with_dimensions(sem, origin=lambda t: t.origin)
    sem = with_measures(sem, total=lambda t: t.value.sum())

    q = select_(where_(sem, lambda t: t.origin == "A"), "origin", "total")

    expr = convert(q, catalog={})
    expected = (
        tbl.filter(tbl.origin == "A")
        .group_by([tbl.origin])
        .aggregate(total=tbl.value.sum())
    )
    assert repr(expr) == repr(expected)


def test_bare_table_has_no_semantic_methods():
    tbl = ibis.memtable({"x": [1, 2, 3]}, name="tbl")
    # Only semantic-DSL methods (beyond Ibis's core API) should be missing
    for method in (
        "with_dimensions",
        "with_measures",
        "join_one",
        "join_many",
        "join_cross",
    ):
        assert not hasattr(tbl, method), f"Bare table unexpectedly has {method}"


def test_to_semantic_table_binds_methods():
    tbl = ibis.memtable({"x": [1, 2, 3]}, name="tbl")
    sem = to_semantic_table(tbl)
    for method in (
        "with_dimensions",
        "with_measures",
        "group_by",
        "aggregate",
        "mutate",
        "order_by",
    ):
        assert hasattr(sem, method), f"Semantic table missing {method}"
    # Methods should be callable and return a TableExpr
    sem2 = sem.with_dimensions(x=lambda t: t.x)
    assert isinstance(sem2, ibis.expr.types.Table)


def test_order_by_basic():
    """Test basic order_by functionality with single column."""
    data = {"name": ["Charlie", "Alice", "Bob"], "value": [3, 1, 2]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    ordered = order_by_(sem, "name")

    # Check that order_by returns a semantic table expression
    assert hasattr(ordered, "execute")
    assert hasattr(ordered, "order_by")

    # Execute and verify ordering
    result = ordered.execute()
    expected_names = ["Alice", "Bob", "Charlie"]  # alphabetical order
    assert list(result["name"]) == expected_names


def test_order_by_multiple_columns():
    """Test order_by with multiple columns."""
    data = {
        "category": ["A", "A", "B", "B"],
        "priority": [2, 1, 2, 1],
        "value": [10, 20, 30, 40],
    }
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    ordered = order_by_(sem, "category", "priority")

    result = ordered.execute()

    # Should be ordered by category first, then priority within each category
    expected_categories = ["A", "A", "B", "B"]
    expected_priorities = [1, 2, 1, 2]  # Within each category, priority ascending

    assert list(result["category"]) == expected_categories
    assert list(result["priority"]) == expected_priorities


def test_order_by_with_aggregation():
    """Test order_by after group_by and aggregate operations."""
    data = {"group": ["X", "Y", "X", "Y", "X"], "value": [1, 2, 3, 4, 5]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    result_expr = (
        sem.group_by("group").aggregate(total=lambda t: t.value.sum()).order_by("group")
    )

    result = result_expr.execute()

    # Should be ordered by group (X, Y)
    assert list(result["group"]) == ["X", "Y"]
    assert list(result["total"]) == [9, 6]  # X: 1+3+5=9, Y: 2+4=6


def test_order_by_with_semantic_dimensions():
    """Test order_by with semantic dimensions."""
    data = {"date_str": ["2023-01-15", "2023-01-10", "2023-01-20"], "value": [1, 2, 3]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    sem = with_dimensions(
        sem,
        day=lambda t: t.date_str,  # Simple dimension for testing
    )

    result_expr = (
        sem.group_by("day").aggregate(total=lambda t: t.value.sum()).order_by("day")
    )

    result = result_expr.execute()

    # Should be ordered by day dimension
    expected_days = ["2023-01-10", "2023-01-15", "2023-01-20"]
    assert list(result["day"]) == expected_days


def test_order_by_method_chaining():
    """Test that order_by method is available and chainable on semantic table."""
    data = {"x": [3, 1, 2], "y": [30, 10, 20]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)

    # Test method chaining
    result_expr = sem.order_by("x").order_by(
        "y"
    )  # Second order_by should override first
    result = result_expr.execute()

    # Should be ordered by y (the last order_by)
    expected_y = [10, 20, 30]
    assert list(result["y"]) == expected_y


def test_order_by_converts_to_ibis():
    """Test that order_by operation converts properly to Ibis operations."""
    data = {"a": [3, 1, 2], "b": [30, 10, 20]}
    tbl = ibis.memtable(data, name="tbl")

    sem = to_semantic_table(tbl)
    ordered = order_by_(sem, "a")

    # Convert to Ibis and check it's an OrderBy operation
    ibis_expr = convert(ordered, catalog={})

    # The converted expression should be equivalent to tbl.order_by("a")
    expected = tbl.order_by("a")

    # Both should execute to the same result
    result1 = ibis_expr.execute()
    result2 = expected.execute()

    assert list(result1["a"]) == list(result2["a"])
    assert list(result1["b"]) == list(result2["b"])


def test_aggregate_with_predefined_measures():
    """Test aggregating with predefined measures (new functionality)."""
    data = {"group": ["X", "Y", "X", "Y"], "value": [1, 2, 3, 4]}
    tbl = ibis.memtable(data, name="tbl")

    # Create semantic table with predefined measures
    sem = (
        to_semantic_table(tbl)
        .with_dimensions(group=lambda t: t.group)
        .with_measures(
            total_value=lambda t: t.value.sum(),
            avg_value=lambda t: t.value.mean(),
        )
    )

    # Test 1: Reference existing measure in aggregation
    result1 = sem.group_by("group").aggregate(lambda t: t.total_value)
    df1 = result1.execute()
    
    assert len(df1) == 2  # Two groups
    assert "group" in df1.columns
    assert "total_value" in df1.columns
    assert sorted(df1["total_value"].tolist()) == [4, 6]  # X: 1+3=4, Y: 2+4=6

    # Test 2: Reference multiple existing measures
    result2 = sem.group_by("group").aggregate(
        total=lambda t: t.total_value,
        average=lambda t: t.avg_value
    )
    df2 = result2.execute()
    
    assert len(df2) == 2
    assert "total" in df2.columns
    assert "average" in df2.columns

    # Test 3: Mix predefined measure with new aggregation
    result3 = sem.group_by("group").aggregate(
        existing_total=lambda t: t.total_value,
        count=lambda t: t.value.count()  # New aggregation
    )
    df3 = result3.execute()
    
    assert len(df3) == 2
    assert "existing_total" in df3.columns
    assert "count" in df3.columns
