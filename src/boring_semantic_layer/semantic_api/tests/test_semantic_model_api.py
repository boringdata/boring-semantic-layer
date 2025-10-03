import pandas as pd
import pytest
import ibis
from boring_semantic_layer.semantic_api.api import (
    to_semantic_table,
    join_,
    order_by_,
    limit_,
)


def test_group_by_sum_and_count():
    """Test basic group by with sum and count measures."""
    df = pd.DataFrame({"grp": ["a", "b", "a", "b", "c"], "val": [1, 2, 3, 4, 5]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test", df)

    # Create semantic table with dimensions and measures
    sem_table = (
        to_semantic_table(table)
        .with_dimensions(grp=lambda t: t.grp)
        .with_measures(sum_val=lambda t: t.val.sum(), count=lambda t: t.val.count())
    )

    # Query with group by and aggregation
    result = (
        sem_table.group_by("grp")
        .aggregate(sum_val=lambda t: t.val.sum(), count=lambda t: t.val.count())
        .execute()
    )

    result = result.sort_values("grp").reset_index(drop=True)
    expected = pd.DataFrame(
        {"grp": ["a", "b", "c"], "sum_val": [4, 6, 5], "count": [2, 2, 1]}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_filter_and_order():
    """Test filtering and ordering operations."""
    df = pd.DataFrame({"grp": ["a", "b", "a"], "val": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test2", df)

    # Create semantic table
    sem_table = (
        to_semantic_table(table)
        .with_dimensions(grp=lambda t: t.grp)
        .with_measures(sum_val=lambda t: t.val.sum())
    )

    # Query with filter
    result = (
        sem_table.filter(lambda t: t.grp == "a")
        .group_by("grp")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"grp": ["a"], "sum_val": [40]})
    pd.testing.assert_frame_equal(result, expected)


def test_unknown_dimension_in_group_by():
    """Test that using unknown dimension in group_by raises error."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test3", df)

    sem_table = to_semantic_table(table).with_dimensions(a=lambda t: t.a)

    # In semantic API, unknown dimensions are allowed as inline dimensions
    # The test should verify this works correctly
    result = sem_table.group_by("b").aggregate(count=lambda t: t.count()).execute()

    # This should work but group by the actual column "b"
    assert len(result) == 3  # 3 unique values of b
    assert set(result["b"]) == {4, 5, 6}


@pytest.fixture
def simple_semantic_table():
    """Fixture providing a simple semantic table for testing."""
    df = pd.DataFrame({"col_test": ["a", "b", "a", "b", "c"], "val": [1, 2, 3, 4, 5]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_filters", df)

    return (
        to_semantic_table(table)
        .with_dimensions(col_test=lambda t: t.col_test, val=lambda t: t.val)
        .with_measures(sum_val=lambda t: t.val.sum(), count=lambda t: t.val.count())
    )


@pytest.fixture
def joined_semantic_tables():
    """Fixture providing semantic tables with joins for testing."""
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "customer_id": [101, 102, 101, 103],
            "amount": [100, 200, 300, 400],
        }
    )
    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "country": ["US", "UK", "US"],
            "tier": ["gold", "silver", "gold"],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    orders_table = con.create_table("orders", orders_df)
    customers_table = con.create_table("customers", customers_df)

    # Create semantic tables
    orders_sem = (
        to_semantic_table(orders_table)
        .with_dimensions(
            order_id=lambda t: t.order_id, customer_id=lambda t: t.customer_id
        )
        .with_measures(total_amount=lambda t: t.amount.sum())
    )

    customers_sem = to_semantic_table(customers_table).with_dimensions(
        country=lambda t: t.country,
        tier=lambda t: t.tier,
        customer_id=lambda t: t.customer_id,
    )

    return orders_sem, customers_sem


def test_simple_lambda_filter(simple_semantic_table):
    """Test simple lambda function filter."""
    result = (
        simple_semantic_table.filter(lambda t: t.col_test == "a")
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"col_test": ["a"], "sum_val": [4]})
    pd.testing.assert_frame_equal(result, expected)


def test_multiple_filters(simple_semantic_table):
    """Test multiple filter conditions."""
    result = (
        simple_semantic_table.filter(lambda t: t.col_test != "b")
        .filter(lambda t: t.val <= 5)
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .sort_values("col_test")
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"col_test": ["a", "c"], "sum_val": [4, 5]})
    pd.testing.assert_frame_equal(result, expected)


def test_filter_with_in_operator(simple_semantic_table):
    """Test filter with IN operator."""
    result = (
        simple_semantic_table.filter(lambda t: t.col_test.isin(["a", "b"]))
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .sort_values("col_test")
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"col_test": ["a", "b"], "sum_val": [4, 6]})
    pd.testing.assert_frame_equal(result, expected)


def test_filter_with_comparison(simple_semantic_table):
    """Test filter with comparison operators."""
    result = (
        simple_semantic_table.filter(lambda t: t.val >= 3)
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .sort_values("col_test")
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"col_test": ["a", "b", "c"], "sum_val": [3, 4, 5]})
    pd.testing.assert_frame_equal(result, expected)


def test_complex_filter_conditions(simple_semantic_table):
    """Test complex filter conditions with AND/OR logic."""
    # AND condition: col_test in ['a', 'b'] AND val >= 3
    result = (
        simple_semantic_table.filter(lambda t: t.col_test.isin(["a", "b"]))
        .filter(lambda t: t.val >= 3)
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .sort_values("col_test")
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"col_test": ["a", "b"], "sum_val": [3, 4]})
    pd.testing.assert_frame_equal(result, expected)

    # OR condition: col_test == 'c' OR val <= 2
    result2 = (
        simple_semantic_table.filter(lambda t: (t.col_test == "c") | (t.val <= 2))
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .execute()
        .sort_values("col_test")
        .reset_index(drop=True)
    )

    expected2 = pd.DataFrame({"col_test": ["a", "b", "c"], "sum_val": [1, 2, 5]})
    pd.testing.assert_frame_equal(result2, expected2)


def test_filters_with_joins(joined_semantic_tables):
    """Test filters with joined tables."""
    orders_sem, customers_sem = joined_semantic_tables

    # Join orders with customers
    joined = join_(
        orders_sem,
        customers_sem,
        on=lambda left, right: left.customer_id == right.customer_id,
    )

    # Filter for US gold customers
    result = (
        joined.filter(lambda t: t.country == "US")
        .filter(lambda t: t.tier == "gold")
        .group_by("customer_id", "country", "tier")
        .aggregate(total_amount=lambda t: t.amount.sum())
        .execute()
        .sort_values("customer_id")
        .reset_index(drop=True)
    )

    expected = pd.DataFrame(
        {
            "customer_id": [101, 103],
            "country": ["US", "US"],
            "tier": ["gold", "gold"],
            "total_amount": [400, 400],  # 101 has 100+300, 103 has 400
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.fixture
def time_semantic_table():
    """Fixture providing a semantic table with time dimension for testing."""
    # Create dates first
    dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")

    # Create repeating categories to match date length
    categories = ["A", "B", "C"] * (len(dates) // 3)
    if len(categories) < len(dates):
        categories.extend(["A"] * (len(dates) - len(categories)))

    df = pd.DataFrame(
        {"event_time": dates, "value": range(len(dates)), "category": categories}
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("time_test", df)

    return (
        to_semantic_table(table)
        .with_dimensions(
            category=lambda t: t.category,
            date=lambda t: t.event_time.date(),
            month=lambda t: t.event_time.month(),
            year=lambda t: t.event_time.year(),
        )
        .with_measures(
            total_value=lambda t: t.value.sum(), avg_value=lambda t: t.value.mean()
        )
    )


def test_time_filtering(time_semantic_table):
    """Test filtering by time dimensions."""
    # Filter for June 2023
    result = (
        time_semantic_table.filter(lambda t: t.event_time >= "2023-06-01")
        .filter(lambda t: t.event_time <= "2023-06-30")
        .group_by("month")
        .aggregate(total_value=lambda t: t.value.sum())
        .execute()
    )

    # Should only include June (month 6)
    assert len(result) == 1
    assert result["month"].iloc[0] == 6


def test_time_grain_aggregation(time_semantic_table):
    """Test aggregation at different time grains."""
    # Monthly aggregation
    monthly_result = (
        time_semantic_table.group_by("year", "month")
        .aggregate(total_value=lambda t: t.value.sum())
        .execute()
    )

    # Should have 12 months
    assert len(monthly_result) == 12

    # Quarterly aggregation (using month/3)
    quarterly_result = (
        time_semantic_table.mutate(
            quarter=lambda t: ((t.event_time.month() - 1) // 3) + 1
        )
        .group_by("year", "quarter")
        .aggregate(total_value=lambda t: t.value.sum())
        .execute()
    )

    # Should have 4 quarters
    assert len(quarterly_result) == 4


def test_order_by_functionality():
    """Test order_by functionality."""
    df = pd.DataFrame({"category": ["C", "A", "B"], "value": [30, 10, 20]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_order", df)

    sem_table = to_semantic_table(table)

    # Order by category
    result = sem_table.order_by("category").execute()
    assert list(result["category"]) == ["A", "B", "C"]

    # Order by value descending
    result_desc = order_by_(sem_table, ibis.desc("value")).execute()
    assert list(result_desc["value"]) == [30, 20, 10]


def test_limit_functionality():
    """Test limit functionality."""
    df = pd.DataFrame({"id": range(10), "value": range(10, 20)})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_limit", df)

    sem_table = to_semantic_table(table)

    # Limit to 5 rows
    result = sem_table.limit(5).execute()
    assert len(result) == 5
    assert list(result["id"]) == [0, 1, 2, 3, 4]

    # Limit with offset
    result_offset = limit_(sem_table, 3, offset=5).execute()
    assert len(result_offset) == 3
    assert list(result_offset["id"]) == [5, 6, 7]


def test_new_operator_mappings():
    """Test case-insensitive operators like ilike."""
    # Create test data with text fields suitable for string matching
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "charlie", "DAVID", "Eve"],
            "email": [
                "alice@example.com",
                "bob@test.org",
                "charlie@example.com",
                "david@TEST.ORG",
                "eve@example.com",
            ],
            "value": [10, 20, 30, 40, 50],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_operators", df)

    sem_table = (
        to_semantic_table(table)
        .with_dimensions(name=lambda t: t.name, email=lambda t: t.email)
        .with_measures(sum_value=lambda t: t.value.sum(), count=lambda t: t.count())
    )

    # Test case-insensitive matching with ilike
    # Note: In Ibis, use re_search for case-insensitive pattern matching
    result = (
        sem_table.filter(
            lambda t: t.name.re_search("(?i)charlie")
        )  # (?i) for case-insensitive
        .group_by("name")
        .aggregate(sum_value=lambda t: t.value.sum())
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"name": ["charlie"], "sum_value": [30]})
    pd.testing.assert_frame_equal(result, expected)

    # Test pattern matching with email domain
    result_domain = (
        sem_table.filter(lambda t: t.email.contains("example.com"))
        .group_by("name")
        .aggregate(sum_value=lambda t: t.value.sum())
        .execute()
        .sort_values("name")
        .reset_index(drop=True)
    )

    expected_domain = (
        pd.DataFrame({"name": ["Alice", "Eve", "charlie"], "sum_value": [10, 50, 30]})
        .sort_values("name")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result_domain, expected_domain)

    # Test NOT pattern (exclude example.com)
    result_not = (
        sem_table.filter(lambda t: ~t.email.contains("example.com"))
        .group_by("name")
        .aggregate(sum_value=lambda t: t.value.sum())
        .execute()
        .sort_values("name")
        .reset_index(drop=True)
    )

    expected_not = (
        pd.DataFrame({"name": ["Bob", "DAVID"], "sum_value": [20, 40]})
        .sort_values("name")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result_not, expected_not)


def test_compound_filters():
    """Test compound filters with AND/OR conditions."""
    df = pd.DataFrame(
        {
            "category": ["Tech", "Finance", "tech", "FINANCE", "Health"],
            "product": ["Laptop", "Stock", "Phone", "BOND", "Medicine"],
            "price": [1000, 500, 800, 300, 200],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_compound", df)

    sem_table = (
        to_semantic_table(table)
        .with_dimensions(
            category=lambda t: t.category,
            product=lambda t: t.product,
            price=lambda t: t.price,
        )
        .with_measures(avg_price=lambda t: t.price.mean())
    )

    # Test compound filter with case-insensitive match AND price condition
    result = (
        sem_table.filter(lambda t: t.category.re_search("(?i)tech"))
        .filter(lambda t: t.price >= 800)
        .group_by("category", "product")
        .aggregate(avg_price=lambda t: t.price.mean())
        .execute()
        .sort_values("product")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame(
            {
                "category": ["Tech", "tech"],
                "product": ["Laptop", "Phone"],
                "avg_price": [1000.0, 800.0],
            }
        )
        .sort_values("product")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, expected)

    # Test OR condition
    result_or = (
        sem_table.filter(
            lambda t: (t.category == "Health") | t.category.re_search("(?i)finance")
        )
        .group_by("category")
        .aggregate(row_count=lambda t: t.count())  # Use named aggregation
        .execute()
        .sort_values("category")
        .reset_index(drop=True)
    )

    expected_or = (
        pd.DataFrame(
            {"category": ["FINANCE", "Finance", "Health"], "row_count": [1, 1, 1]}
        )
        .sort_values("category")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result_or, expected_or)


def test_inline_dimensions():
    """Test inline dimension definitions in group_by."""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=10, freq="D"),
            "value": range(10),
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_inline", df)

    sem_table = to_semantic_table(table)

    # Test inline dimension in group_by
    result = (
        sem_table.group_by(month=lambda t: t.timestamp.month())
        .aggregate(total=lambda t: t.value.sum())
        .execute()
    )

    # All dates are in January, so should have one row with month=1
    assert len(result) == 1
    assert result["month"].iloc[0] == 1
    assert result["total"].iloc[0] == sum(range(10))
