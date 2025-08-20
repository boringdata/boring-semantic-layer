from ibis.expr.api import desc
import pandas as pd
from pandas._config import reset_option
import pytest
import ibis
from boring_semantic_layer import SemanticModel, Join, DimensionSpec, MeasureSpec


def test_group_by_sum_and_count():
    df = pd.DataFrame({"grp": ["a", "b", "a", "b", "c"], "val": [1, 2, 3, 4, 5]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test", df)
    model = SemanticModel(
        table=table,
        dimensions={"grp": lambda t: t.grp},
        measures={"sum_val": lambda t: t.val.sum(), "count": lambda t: t.val.count()},
    )
    expr = model.query(dimensions=["grp"], measures=["sum_val", "count"])
    result = expr.execute()
    result = result.sort_values("grp").reset_index(drop=True)
    expected = pd.DataFrame(
        {"grp": ["a", "b", "c"], "sum_val": [4, 6, 5], "count": [2, 2, 1]}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_filter_and_order():
    df = pd.DataFrame({"grp": ["a", "b", "a"], "val": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test2", df)
    model = SemanticModel(
        table=table,
        dimensions={"grp": lambda t: t.grp},
        measures={"sum_val": lambda t: t.val.sum()},
    )
    expr = model.query(
        dimensions=["grp"], measures=["sum_val"], filters=lambda t: t.grp == "a"
    )
    result = expr.execute().reset_index(drop=True)
    expected = pd.DataFrame({"grp": ["a"], "sum_val": [40]})
    pd.testing.assert_frame_equal(result, expected)


def test_unknown_dimension_raises():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test3", df)
    model = SemanticModel(table=table, dimensions={"a": lambda t: t.a}, measures={})
    with pytest.raises(KeyError):
        _ = model.query(dimensions=["b"], measures=[])


@pytest.fixture
def simple_model():
    """Fixture providing a simple model for testing."""
    df = pd.DataFrame({"col_test": ["a", "b", "a", "b", "c"], "val": [1, 2, 3, 4, 5]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_filters", df)
    return SemanticModel(
        table=table,
        dimensions={"col_test": lambda t: t.col_test, "val": lambda t: t.val},
        measures={"sum_val": lambda t: t.val.sum(), "count": lambda t: t.val.count()},
    )


@pytest.fixture
def joined_model():
    """Fixture providing a model with joins for testing."""
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

    customers_model = SemanticModel(
        table=customers_table,
        dimensions={
            "country": lambda t: t.country,
            "tier": lambda t: t.tier,
            "customer_id": lambda t: t.customer_id,
        },
        measures={},
    )

    return SemanticModel(
        table=orders_table,
        dimensions={
            "order_id": lambda t: t.order_id,
            "customer_id": lambda t: t.customer_id,
        },
        measures={"total_amount": lambda t: t.amount.sum()},
        joins={
            "customer": Join(
                alias="customer",
                model=customers_model,
                on=lambda t, j: t.customer_id == j.customer_id,
            )
        },
    )


@pytest.mark.parametrize(
    "test_id,model_fixture,filters,dimensions,measures,expected_data,expected_error",
    [
        # Test 1: Simple lambda function filter
        (
            "lambda_simple",
            "simple_model",
            lambda t: t.col_test == "a",
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a"], "sum_val": [4]}),
            None,
        ),
        # Test 2: Multiple lambda filters
        (
            "lambda_multiple",
            "simple_model",
            [lambda t: t.col_test != "b", lambda t: t.val <= 5],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a", "c"], "sum_val": [4, 5]}),
            None,
        ),
        # Test 3: String filter (evaluated ibis expression)
        (
            "string_eval",
            "simple_model",
            ["lambda t: t.col_test == 'a'"],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a"], "sum_val": [4]}),
            None,
        ),
        # Test 4: Unbound ibis expression
        (
            "ibis_unbound",
            "simple_model",
            lambda t: t.col_test == "a",  # Changed from list to single expression
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a"], "sum_val": [4]}),
            None,
        ),
        # Test 5: Simple JSON filter (equals)
        (
            "json_equals",
            "simple_model",
            [{"field": "col_test", "operator": "=", "value": "a"}],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a"], "sum_val": [4]}),
            None,
        ),
        # Test 6: JSON filter with IN operator
        (
            "json_in",
            "simple_model",
            [{"field": "col_test", "operator": "in", "values": ["a", "b"]}],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a", "b"], "sum_val": [4, 6]}),
            None,
        ),
        # Test 7: JSON filter with comparison operators
        (
            "json_comparison",
            "simple_model",
            [{"field": "val", "operator": ">=", "value": 3}],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a", "b", "c"], "sum_val": [3, 4, 5]}),
            None,
        ),
        # Test 8: JSON filter with AND condition
        (
            "json_and",
            "simple_model",
            [
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "col_test", "operator": "in", "values": ["a", "b"]},
                        {"field": "val", "operator": ">=", "value": 3},
                    ],
                }
            ],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a", "b"], "sum_val": [3, 4]}),
            None,
        ),
        # Test 9: JSON filter with OR condition
        (
            "json_or",
            "simple_model",
            [
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": "col_test", "operator": "=", "value": "c"},
                        {"field": "val", "operator": "<=", "value": 2},
                    ],
                }
            ],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a", "b", "c"], "sum_val": [1, 2, 5]}),
            None,
        ),
        # Test 10: Complex nested JSON filter
        (
            "json_nested",
            "simple_model",
            [
                {
                    "operator": "OR",
                    "conditions": [
                        {
                            "operator": "AND",
                            "conditions": [
                                {"field": "col_test", "operator": "=", "value": "a"},
                                {"field": "val", "operator": ">", "value": 2},
                            ],
                        },
                        {"field": "col_test", "operator": "=", "value": "c"},
                    ],
                }
            ],
            ["col_test"],
            ["sum_val"],
            pd.DataFrame({"col_test": ["a", "c"], "sum_val": [3, 5]}),
            None,
        ),
        # Test 11: JSON filter with joins
        (
            "json_joins",
            "joined_model",
            [
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "customer.country", "operator": "=", "value": "US"},
                        {"field": "customer.tier", "operator": "=", "value": "gold"},
                    ],
                }
            ],
            ["customer_id", "customer.country", "customer.tier"],
            ["total_amount"],
            pd.DataFrame(
                {
                    "customer_id": [101, 103],
                    "customer_country": ["US", "US"],
                    "customer_tier": ["gold", "gold"],
                    "total_amount": [
                        400,
                        400,
                    ],  # 400 for customer 101 (100+300) and 400 for customer 103
                }
            )
            .rename(
                columns={
                    "customer_country": "customer_country",
                    "customer_tier": "customer_tier",
                }
            )
            .sort_values("customer_id")
            .reset_index(drop=True),
            None,
        ),
        # Test 12: Invalid operator error
        (
            "error_invalid_operator",
            "simple_model",
            [{"field": "col_test", "operator": "invalid", "value": "a"}],
            ["col_test"],
            ["sum_val"],
            None,
            ValueError,
        ),
        # Test 13: Unknown field error
        (
            "error_unknown_field",
            "simple_model",
            [{"field": "unknown", "operator": "=", "value": "a"}],
            ["col_test"],
            ["sum_val"],
            None,
            KeyError,
        ),
        # Test 14: Invalid join reference error
        (
            "error_invalid_join",
            "simple_model",
            [{"field": "unknown.field", "operator": "=", "value": "a"}],
            ["col_test"],
            ["sum_val"],
            None,
            KeyError,
        ),
        # Test 15: Missing required keys error
        (
            "error_missing_keys",
            "simple_model",
            [{"operator": "=", "value": "a"}],  # Missing "field"
            ["col_test"],
            ["sum_val"],
            None,
            KeyError,
        ),
    ],
)
def test_filters(
    test_id,
    model_fixture,
    filters,
    dimensions,
    measures,
    expected_data,
    expected_error,
    request,
):
    """
    Comprehensive test for all filter types: lambda functions, strings, ibis expressions, and JSON filters.

    Args:
        test_id: Unique identifier for the test case
        model_fixture: Name of the fixture providing the model
        filters: Filter specification (lambda, string, ibis expression, or JSON)
        dimensions: Dimensions to group by
        measures: Measures to compute
        expected_data: Expected DataFrame result
        expected_error: Expected error type if the test should raise an error
        request: pytest fixture for accessing other fixtures
    """
    model = request.getfixturevalue(model_fixture)

    if expected_error is not None:
        with pytest.raises(expected_error):
            model.query(dimensions=dimensions, measures=measures, filters=filters)
    else:
        expr = model.query(dimensions=dimensions, measures=measures, filters=filters)
        result = (
            expr.execute()
            .sort_values([dim.replace(".", "_") for dim in dimensions])
            .reset_index(drop=True)
        )
        expected = expected_data.sort_values(
            [dim.replace(".", "_") for dim in dimensions]
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)


def test_json_filters_with_joins():
    """Test JSON filters with joined tables."""
    # Create main table
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [101, 102, 101, 103, 104],
            "amount": [100, 200, 300, 400, 500],
        }
    )

    # Create customer table for joining
    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103, 104],
            "country": ["US", "UK", "US", "US"],
            "tier": ["gold", "silver", "silver", "gold"],
        }
    )
    con = ibis.duckdb.connect(":memory:")
    orders_table = con.create_table("orders", orders_df)
    customers_table = con.create_table("customers", customers_df)

    # Create customer model
    customers_model = SemanticModel(
        table=customers_table,
        dimensions={
            "country": lambda t: t.country,
            "tier": lambda t: t.tier,
            "customer_id": lambda t: t.customer_id,
        },
        measures={},
    )

    # Create orders model with join to customers
    orders_model = SemanticModel(
        table=orders_table,
        dimensions={
            "order_id": lambda t: t.order_id,
            "customer_id": lambda t: t.customer_id,
        },
        measures={"total_amount": lambda t: t.amount.sum()},
        joins={
            "customer": Join(
                alias="customer",
                model=customers_model,
                on=lambda t, j: t.customer_id == j.customer_id,
            )
        },
    )

    # Test filter using joined fields
    json_filter = {
        "operator": "AND",
        "conditions": [
            {"field": "customer.country", "operator": "=", "value": "US"},
            {"field": "customer.tier", "operator": "=", "value": "gold"},
        ],
    }

    expr = orders_model.query(
        dimensions=["customer_id", "customer.country", "customer.tier"],
        measures=["total_amount"],
        filters=[json_filter],
    )

    result = expr.execute()
    assert len(result) == 2  # Should only get US gold customers
    assert (
        int(result["total_amount"].sum()) == 900
    )  # Sum of orders for US gold customers


def test_json_filter_errors():
    """Test error cases for JSON filters."""
    df = pd.DataFrame({"grp": ["a", "b"], "val": [1, 2]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_errors", df)
    model = SemanticModel(
        table=table,
        dimensions={"grp": lambda t: t.grp},
        measures={"sum_val": lambda t: t.val.sum()},
    )

    # Test invalid operator
    with pytest.raises(ValueError, match="Unsupported operator"):
        model.query(
            dimensions=["grp"],
            measures=["sum_val"],
            filters=[{"field": "grp", "operator": "invalid", "value": "a"}],
        )

    # Test unknown field
    with pytest.raises(KeyError, match="Unknown field"):
        model.query(
            dimensions=["grp"],
            measures=["sum_val"],
            filters=[{"field": "unknown", "operator": "=", "value": "a"}],
        )

    # Test invalid join reference
    with pytest.raises(KeyError, match="Unknown join alias"):
        model.query(
            dimensions=["grp"],
            measures=["sum_val"],
            filters=[{"field": "unknown.field", "operator": "=", "value": "a"}],
        )

    # Test missing required keys in filter dict
    with pytest.raises(KeyError):
        model.query(
            dimensions=["grp"],
            measures=["sum_val"],
            filters=[{"operator": "=", "value": "a"}],  # Missing "field"
        )


@pytest.fixture
def time_model():
    """Fixture providing a model with time dimension for testing."""
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
    return SemanticModel(
        table=table,
        dimensions={
            "category": lambda t: t.category,
            "date": lambda t: t.event_time.date(),
        },
        measures={
            "total_value": lambda t: t.value.sum(),
            "avg_value": lambda t: t.value.mean(),
        },
        time_dimension="date",
        smallest_time_grain="TIME_GRAIN_DAY",
    )


def test_time_range_filtering():
    """Test filtering by time range."""
    df = pd.DataFrame(
        {
            "event_time": pd.date_range(start="2023-01-01", end="2023-12-31", freq="D"),
            "value": range(365),
        }
    )
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("time_test", df)
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time.date(),
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
        time_dimension="date",
        smallest_time_grain="TIME_GRAIN_DAY",
    )

    # Test with time range
    expr = model.query(
        dimensions=["date"],
        measures=["total_value"],
        time_range={"start": "2023-06-01T00:00:00Z", "end": "2023-06-30T23:59:59Z"},
    )
    result = expr.execute()

    # Should only include June dates
    assert len(result) == 30
    assert min(result["date"]).strftime("%Y-%m-%d") == "2023-06-01"
    assert max(result["date"]).strftime("%Y-%m-%d") == "2023-06-30"


def test_time_range_with_other_filters(time_model):
    """Test combining time range with other filters."""
    # First get the total for category A without time filter
    total_expr = time_model.query(
        dimensions=["category"],
        measures=["total_value"],
        filters=[{"field": "category", "operator": "=", "value": "A"}],
    )
    total_result = total_expr.execute()
    total_value = total_result["total_value"].iloc[0]

    # Then get filtered result
    expr = time_model.query(
        dimensions=["category"],
        measures=["total_value"],
        filters=[{"field": "category", "operator": "=", "value": "A"}],
        time_range={"start": "2023-03-01T00:00:00Z", "end": "2023-03-31T23:59:59Z"},
    )
    result = expr.execute()

    # Should only include category A in March
    assert len(result) == 1
    assert result["category"].iloc[0] == "A"
    assert (
        result["total_value"].iloc[0] < total_value
    )  # March value should be less than total


def test_invalid_time_range():
    """Test error handling for invalid time range format."""
    df = pd.DataFrame(
        {
            "event_time": pd.date_range(start="2023-01-01", end="2023-12-31", freq="D"),
            "value": range(365),
        }
    )
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("time_test", df)
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time.date(),
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
        time_dimension="date",
        smallest_time_grain="TIME_GRAIN_DAY",
    )

    # Test missing start
    with pytest.raises(
        ValueError, match="time_range must be a dictionary with 'start' and 'end' keys"
    ):
        model.query(
            dimensions=["date"],
            measures=["total_value"],
            time_range={"end": "2023-12-31T23:59:59Z"},
        )

    # Test missing end
    with pytest.raises(
        ValueError, match="time_range must be a dictionary with 'start' and 'end' keys"
    ):
        model.query(
            dimensions=["date"],
            measures=["total_value"],
            time_range={"start": "2023-01-01T00:00:00Z"},
        )

    # Test invalid format
    with pytest.raises(
        ValueError, match="time_range must be a dictionary with 'start' and 'end' keys"
    ):
        model.query(
            dimensions=["date"],
            measures=["total_value"],
            time_range="2023-01-01/2023-12-31",
        )


def test_time_dimension_validation():
    """Test validation of time dimension configuration."""
    df = pd.DataFrame(
        {
            "event_time": pd.date_range(start="2023-01-01", end="2023-12-31", freq="D"),
            "value": range(365),
        }
    )
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("time_test", df)

    # Test with invalid time grain
    with pytest.raises(ValueError, match="Invalid smallest_time_grain"):
        SemanticModel(
            table=table,
            dimensions={
                "date": lambda t: t.event_time.date(),
            },
            measures={
                "total_value": lambda t: t.value.sum(),
            },
            time_dimension="date",
            smallest_time_grain="INVALID_GRAIN",
        )

    # Test with time range but no time dimension
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time.date(),
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
    )

    # Should not raise error, just ignore time range
    expr = model.query(
        dimensions=["date"],
        measures=["total_value"],
        time_range={"start": "2023-01-01T00:00:00Z", "end": "2023-12-31T23:59:59Z"},
    )
    result = expr.execute()
    assert len(result) == 365  # All dates included since time range was ignored


def test_time_grain_transformation(time_model):
    """Test time grain transformation functionality."""
    # Test monthly aggregation
    expr = time_model.query(
        dimensions=["date"], measures=["total_value"], time_grain="TIME_GRAIN_MONTH"
    )
    result = expr.execute()

    # Should have 12 months
    assert len(result) == 12

    # Test quarterly aggregation
    expr = time_model.query(
        dimensions=["date"], measures=["total_value"], time_grain="TIME_GRAIN_QUARTER"
    )
    result = expr.execute()

    # Should have 4 quarters
    assert len(result) == 4


def test_time_grain_with_time_range(time_model):
    """Test combining time grain with time range."""
    expr = time_model.query(
        dimensions=["date"],
        measures=["total_value"],
        time_grain="TIME_GRAIN_MONTH",
        time_range={"start": "2023-04-01T00:00:00Z", "end": "2023-06-30T23:59:59Z"},
    )
    result = expr.execute()

    # Should have 3 months (April, May, June)
    assert len(result) == 3


def test_time_grain_validation():
    """Test time grain validation."""
    df = pd.DataFrame(
        {
            "event_time": pd.date_range(start="2023-01-01", end="2023-12-31", freq="D"),
            "value": range(365),
        }
    )
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("time_test", df)

    # Create model with DAY as smallest grain
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time,
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
        time_dimension="date",
        smallest_time_grain="TIME_GRAIN_DAY",
    )

    # Test valid time grains
    for grain in [
        "TIME_GRAIN_DAY",
        "TIME_GRAIN_MONTH",
        "TIME_GRAIN_QUARTER",
        "TIME_GRAIN_YEAR",
    ]:
        expr = model.query(
            dimensions=["date"], measures=["total_value"], time_grain=grain
        )
        result = expr.execute()
        assert len(result) > 0

    # Test invalid (too fine) time grains
    for grain in ["TIME_GRAIN_HOUR", "TIME_GRAIN_MINUTE", "TIME_GRAIN_SECOND"]:
        with pytest.raises(
            ValueError, match="is finer than the smallest allowed grain"
        ):
            model.query(dimensions=["date"], measures=["total_value"], time_grain=grain)


def test_time_grain_with_other_dimensions(time_model):
    """Test time grain aggregation with other dimensions."""
    expr = time_model.query(
        dimensions=["date", "category"],
        measures=["total_value"],
        time_grain="TIME_GRAIN_MONTH",
    )
    result = expr.execute()

    # Should have 12 months * 3 categories
    assert len(result) == 36

    # Each month should have all categories
    # Convert date to month number for grouping
    result_df = pd.DataFrame(result)
    monthly_categories = result_df.groupby(pd.to_datetime(result_df["date"]).dt.month)[
        "category"
    ].nunique()
    assert all(count == 3 for count in monthly_categories)


def test_get_time_range(time_model):
    """Test getting the available time range from a model with time dimension."""
    time_range = time_model.get_time_range()

    # Should return start and end dates
    assert "start" in time_range
    assert "end" in time_range

    # Dates should be in ISO format
    assert time_range["start"].startswith("2023-01-01T00:00:00")
    assert time_range["end"].startswith("2023-12-31T00:00:00")


def test_get_time_range_no_time_dimension():
    """Test getting time range from a model without time dimension."""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [1, 2, 3]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("no_time", df)
    model = SemanticModel(
        table=table,
        dimensions={
            "category": lambda t: t.category,
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
    )

    time_range = model.get_time_range()
    assert "error" in time_range
    assert time_range["error"] == "Model does not have a time dimension"


def test_get_time_range_with_null_dates():
    """Test getting time range from a model with null dates."""
    # Create DataFrame with some null dates
    dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D").tolist()
    dates.extend([None] * 5)  # Add some nulls
    df = pd.DataFrame({"event_time": dates, "value": range(len(dates))})

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("null_dates", df)
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time,
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
        time_dimension="date",
    )

    time_range = model.get_time_range()

    # Should ignore nulls and return valid range
    assert "start" in time_range
    assert "end" in time_range
    assert time_range["start"].startswith("2023-01-01T00:00:00")
    assert time_range["end"].startswith("2023-12-31T00:00:00")


def test_get_time_range_single_date():
    """Test getting time range when all records have the same date."""
    single_date = pd.Timestamp("2023-01-01")
    df = pd.DataFrame({"event_time": [single_date], "value": [1]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("single_date", df)
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time,
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
        time_dimension="date",
    )

    time_range = model.get_time_range()

    # Start and end should be the same
    assert time_range["start"] == time_range["end"]
    assert time_range["start"].startswith("2023-01-01T00:00:00")


def test_get_time_range_empty_table():
    """Test getting time range from an empty table."""
    df = pd.DataFrame(
        {
            "event_time": pd.Series([], dtype="datetime64[ns]"),
            "value": pd.Series([], dtype="int64"),
        }
    )
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("empty", df)
    model = SemanticModel(
        table=table,
        dimensions={
            "date": lambda t: t.event_time,
        },
        measures={
            "total_value": lambda t: t.value.sum(),
        },
        time_dimension="date",
    )

    time_range = model.get_time_range()

    # Should handle null results
    assert "start" in time_range
    assert "end" in time_range

    # Accept both None and pandas NaT (Not-a-Time)
    def is_null_or_nat(val):
        return (
            val is None
            or (isinstance(val, str) and val == "NaT")
            or (hasattr(pd, "isna") and pd.isna(val))
        )

    assert is_null_or_nat(time_range["start"])
    assert is_null_or_nat(time_range["end"])


def test_query_with_chart_specification():
    """Test creating a query with chart specification."""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("chart_test", df)
    model = SemanticModel(
        table=table,
        dimensions={"category": lambda t: t.category},
        measures={"total_value": lambda t: t.value.sum()},
    )

    chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "category", "type": "nominal"},
            "y": {"field": "total_value", "type": "quantitative"},
        },
    }

    # Create query with chart specification
    expr = model.query(dimensions=["category"], measures=["total_value"])

    # Check that chart() method accepts spec and returns Altair chart
    chart = expr.chart(spec=chart_spec)
    assert hasattr(chart, "mark_bar")


def test_query_chart_auto_detection():
    """Test automatic chart type detection."""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("chart_test", df)
    model = SemanticModel(
        table=table,
        dimensions={"category": lambda t: t.category},
        measures={"total_value": lambda t: t.value.sum()},
    )

    # Create query without chart specification
    expr = model.query(dimensions=["category"], measures=["total_value"])

    # Call chart() with auto_detect=True (default)
    # Should auto-detect bar chart for categorical dimension + measure
    chart = expr.chart()
    assert hasattr(chart, "mark_bar")


def test_query_chart_with_time_series():
    """Test chart auto-detection with time series data."""
    dates = pd.date_range(start="2023-01-01", end="2023-01-31", freq="D")
    df = pd.DataFrame({"date": dates, "sales": range(31)})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("time_chart_test", df)
    model = SemanticModel(
        table=table,
        dimensions={"date": lambda t: t.date},
        measures={"total_sales": lambda t: t.sales.sum()},
        time_dimension="date",
    )

    # Create query
    expr = model.query(dimensions=["date"], measures=["total_sales"])

    # Call chart() - should detect line chart for time series
    chart = expr.chart()
    # Should auto-detect line chart for time series
    assert hasattr(chart, "mark_line")


def test_query_chart_field_validation():
    """Test chart field validation against query results."""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("chart_test", df)
    model = SemanticModel(
        table=table,
        dimensions={"category": lambda t: t.category},
        measures={"total_value": lambda t: t.value.sum()},
    )

    # Create chart spec referencing a field not in the query
    invalid_chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "category", "type": "nominal"},
            "y": {"field": "missing_field", "type": "quantitative"},
        },
    }

    expr = model.query(dimensions=["category"], measures=["total_value"])

    # Altair will handle the validation when the chart is displayed
    # We just verify that a chart object is created
    chart = expr.chart(spec=invalid_chart_spec)
    assert hasattr(chart, "mark_bar")


def test_query_chart_with_joins():
    """Test chart functionality with joined data."""
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 101, 102],
            "amount": [100, 200, 300],
        }
    )
    customers_df = pd.DataFrame({"customer_id": [101, 102], "country": ["US", "UK"]})

    con = ibis.duckdb.connect(":memory:")
    orders_table = con.create_table("orders", orders_df)
    customers_table = con.create_table("customers", customers_df)

    customers_model = SemanticModel(
        table=customers_table,
        dimensions={"country": lambda t: t.country},
        measures={},
        primary_key="customer_id",
    )

    orders_model = SemanticModel(
        table=orders_table,
        dimensions={"customer_id": lambda t: t.customer_id},
        measures={"total_amount": lambda t: t.amount.sum()},
        joins={
            "customer": Join.one("customer", customers_model, lambda t: t.customer_id)
        },
    )

    # Query with joined dimension
    expr = orders_model.query(
        dimensions=["customer.country"],
        measures=["total_amount"],
    )

    chart = expr.chart(
        spec={
            "mark": "bar",
            "encoding": {
                "x": {"field": "customer_country", "type": "nominal"},
                "y": {"field": "total_amount", "type": "quantitative"},
            },
        }
    )
    # Verify we get an Altair chart object
    assert hasattr(chart, "mark_bar")


def test_query_render_requires_altair():
    """Test that render() method requires Altair."""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("chart_test", df)
    model = SemanticModel(
        table=table,
        dimensions={"category": lambda t: t.category},
        measures={"total_value": lambda t: t.value.sum()},
    )

    expr = model.query(dimensions=["category"], measures=["total_value"])

    # Try to render without Altair installed
    try:
        import altair  # noqa: F401

        # If Altair is installed, this test won't work as expected
        # But we can still check that render() returns an Altair chart
        # Test chart with spec
        chart = expr.chart(spec={"mark": "bar"})
        assert hasattr(chart, "mark_bar")  # Altair charts have mark methods
    except ImportError:
        # If Altair is not installed, should raise helpful error
        with pytest.raises(ImportError, match="Altair is required for chart creation"):
            expr.chart()


def test_query_chart_output_formats():
    """Test different output formats for chart() method."""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("chart_test", df)
    model = SemanticModel(
        table=table,
        dimensions={"category": lambda t: t.category},
        measures={"total_value": lambda t: t.value.sum()},
    )

    expr = model.query(
        dimensions=["category"],
        measures=["total_value"],
    )

    chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "category", "type": "nominal"},
            "y": {"field": "total_value", "type": "quantitative"},
        },
    }

    try:
        import altair as alt  # noqa: F401

        # Test default format (altair)
        default_chart = expr.chart(spec=chart_spec)
        assert hasattr(default_chart, "mark_bar")

        # Test interactive format
        interactive_chart = expr.chart(spec=chart_spec, format="interactive")
        assert hasattr(interactive_chart, "mark_bar")
        # Interactive charts should have interactive method called

        # Test JSON format
        json_spec = expr.chart(spec=chart_spec, format="json")
        assert isinstance(json_spec, dict)
        assert "mark" in json_spec
        # Altair may convert mark string to object
        assert json_spec["mark"] == "bar" or json_spec["mark"] == {"type": "bar"}

        # Test invalid format
        with pytest.raises(ValueError, match="Unsupported format"):
            expr.chart(spec=chart_spec, format="invalid")

        # Test PNG/SVG formats (may fail if dependencies not installed)
        try:
            png_data = expr.chart(spec=chart_spec, format="png")
            assert isinstance(png_data, bytes)
        except ImportError:
            # Expected if vl-convert not installed
            pass

        try:
            svg_data = expr.chart(spec=chart_spec, format="svg")
            assert isinstance(svg_data, str)
            assert svg_data.startswith("<svg") or svg_data.startswith("<?xml")
        except ImportError:
            # Expected if vl-convert not installed
            pass

    except ImportError:
        # Altair not installed
        pass


def test_new_operator_mappings():
    """Test the new operator mappings: eq, equals, ilike, not ilike"""
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

    model = SemanticModel(
        table=table,
        dimensions={"name": lambda t: t.name, "email": lambda t: t.email},
        measures={"sum_value": lambda t: t.value.sum(), "count": lambda t: t.count()},
    )

    # Test "eq" operator (should work same as "=")
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "eq", "value": "Alice"}],
        )
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"name": ["Alice"], "sum_value": [10]})
    pd.testing.assert_frame_equal(result, expected)

    # Test "equals" operator (should work same as "=")
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "equals", "value": "Bob"}],
        )
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"name": ["Bob"], "sum_value": [20]})
    pd.testing.assert_frame_equal(result, expected)

    # Test "ilike" operator (case-insensitive LIKE)
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "ilike", "value": "charlie"}],
        )
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"name": ["charlie"], "sum_value": [30]})
    pd.testing.assert_frame_equal(result, expected)

    # Test "ilike" with pattern matching (case-insensitive)
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "ilike", "value": "david"}],
        )
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"name": ["DAVID"], "sum_value": [40]})
    pd.testing.assert_frame_equal(result, expected)

    # Test "not ilike" operator (negated case-insensitive LIKE)
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "not ilike", "value": "alice"}],
        )
        .execute()
        .sort_values("name")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame(
            {"name": ["Bob", "DAVID", "Eve", "charlie"], "sum_value": [20, 40, 50, 30]}
        )
        .sort_values("name")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, expected)

    # Test "ilike" with email domain pattern
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "email", "operator": "ilike", "value": "%example.com"}],
        )
        .execute()
        .sort_values("name")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame({"name": ["Alice", "Eve", "charlie"], "sum_value": [10, 50, 30]})
        .sort_values("name")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, expected)

    # Test "not ilike" with pattern
    result = (
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[
                {"field": "email", "operator": "not ilike", "value": "%example.com"}
            ],
        )
        .execute()
        .sort_values("name")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame({"name": ["Bob", "DAVID"], "sum_value": [20, 40]})
        .sort_values("name")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, expected)


def test_new_operators_in_compound_filters():
    """Test new operators work correctly in compound (AND/OR) filters"""
    df = pd.DataFrame(
        {
            "category": ["Tech", "Finance", "tech", "FINANCE", "Health"],
            "product": ["Laptop", "Stock", "Phone", "BOND", "Medicine"],
            "price": [1000, 500, 800, 300, 200],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_compound", df)

    model = SemanticModel(
        table=table,
        dimensions={
            "category": lambda t: t.category,
            "product": lambda t: t.product,
            "price": lambda t: t.price,  # Add price as a dimension for filtering
        },
        measures={"avg_price": lambda t: t.price.mean(), "count": lambda t: t.count()},
    )

    # Test compound filter with "eq" and "ilike"
    result = (
        model.query(
            dimensions=["category", "product"],
            measures=["avg_price"],
            filters=[
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "category", "operator": "ilike", "value": "tech"},
                        {"field": "price", "operator": ">=", "value": 800},
                    ],
                }
            ],
        )
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

    # Test compound filter with "equals" in OR condition
    result = (
        model.query(
            dimensions=["category"],
            measures=["count"],
            filters=[
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": "category", "operator": "equals", "value": "Health"},
                        {"field": "category", "operator": "ilike", "value": "finance"},
                    ],
                }
            ],
        )
        .execute()
        .sort_values("category")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame({"category": ["FINANCE", "Finance", "Health"], "count": [1, 1, 1]})
        .sort_values("category")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, expected)

    # Test "not ilike" in compound filter
    result = (
        model.query(
            dimensions=["category"],
            measures=["count"],
            filters=[
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "category", "operator": "not ilike", "value": "tech"},
                        {"field": "price", "operator": "<", "value": 400},
                    ],
                }
            ],
        )
        .execute()
        .sort_values("category")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame({"category": ["FINANCE", "Health"], "count": [1, 1]})
        .sort_values("category")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, expected)


def test_operator_mapping_completeness():
    """Test that all new operators are properly registered in OPERATOR_MAPPING"""
    from boring_semantic_layer.filters import OPERATOR_MAPPING

    # Check that new operators exist
    assert "eq" in OPERATOR_MAPPING
    assert "equals" in OPERATOR_MAPPING
    assert "ilike" in OPERATOR_MAPPING
    assert "not ilike" in OPERATOR_MAPPING

    # Test that they return callable functions
    assert callable(OPERATOR_MAPPING["eq"])
    assert callable(OPERATOR_MAPPING["equals"])
    assert callable(OPERATOR_MAPPING["ilike"])
    assert callable(OPERATOR_MAPPING["not ilike"])


def test_new_operators_error_handling():
    """Test error handling for new operators with invalid usage"""
    df = pd.DataFrame({"name": ["Alice", "Bob"], "value": [10, 20]})

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_errors", df)

    model = SemanticModel(
        table=table,
        dimensions={"name": lambda t: t.name},
        measures={"sum_value": lambda t: t.value.sum()},
    )

    # Test that "ilike" still requires a value
    with pytest.raises(ValueError, match="requires 'value' field"):
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "ilike"}],  # Missing value
        ).execute()

    # Test that "not ilike" still requires a value
    with pytest.raises(ValueError, match="requires 'value' field"):
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "not ilike"}],  # Missing value
        ).execute()

    # Test that "eq" still requires a value
    with pytest.raises(ValueError, match="requires 'value' field"):
        model.query(
            dimensions=["name"],
            measures=["sum_value"],
            filters=[{"field": "name", "operator": "eq"}],  # Missing value
        ).execute()


@pytest.mark.parametrize(
    "operator,expected_count",
    [
        ("=", 1),
        ("eq", 1),
        ("equals", 1),
    ],
)
def test_equality_operators_equivalence(operator, expected_count):
    """Test that =, eq, and equals operators produce identical results"""
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_equality", df)

    model = SemanticModel(
        table=table,
        dimensions={"category": lambda t: t.category},
        measures={"count": lambda t: t.count()},
    )

    result = (
        model.query(
            dimensions=["category"],
            measures=["count"],
            filters=[{"field": "category", "operator": operator, "value": "A"}],
        )
        .execute()
        .reset_index(drop=True)
    )

    expected = pd.DataFrame({"category": ["A"], "count": [expected_count]})
    pd.testing.assert_frame_equal(result, expected)


def test_measure_filtering_user_example():
    """Test the user's original example that was failing before - now should work."""
    # User's original data structure
    df = pd.DataFrame(
        {
            "company_name": ["A", "B", "A", "B", "C"],
            "transaction_type": ["buy", "sell", "buy", "sell", "buy"],
            "transaction_status": [
                "complete",
                "pending",
                "complete",
                "complete",
                "pending",
            ],
            "asset_code": ["BTC", "ETH", "BTC", "ETH", "BTC"],
            "amount_eur": [100000, 50000, 200000, 150000, 300000],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("transactions_test", df)

    # User's original semantic model
    model = SemanticModel(
        name="transactions",
        table=table,
        dimensions={
            "company_name": lambda t: t.company_name,
            "transaction_type": lambda t: t.transaction_type,
            "status": lambda t: t.transaction_status,
            "asset_code": lambda t: t.asset_code,
        },
        measures={
            "total_transactions": lambda t: t.count(),
            "total_amount_eur": lambda t: t.amount_eur.sum(),
        },
    )

    # User's original query that was failing with "Unknown dimension: total_amount_eur"
    # This should now work without errors
    result = model.query(
        dimensions=["company_name"],
        measures=["total_amount_eur", "total_transactions"],
        filters=[{"field": "total_amount_eur", "operator": "<", "value": 500000}],
        order_by=[("total_amount_eur", "desc")],
        limit=10,
    ).execute()

    # Verify the query executes successfully and returns expected structure
    assert isinstance(result, pd.DataFrame)
    assert "company_name" in result.columns
    assert "total_amount_eur" in result.columns
    assert "total_transactions" in result.columns

    # Since all filters are applied to input table before aggregation,
    # and the measure filter evaluates the aggregate function on the raw data,
    # the result should be empty (no individual rows pass the filter condition)
    assert len(result) == 0


def test_measure_filtering_with_real_row_level_filter():
    """Test measure filtering that actually filters at row level."""
    # Create data where individual amounts can be filtered meaningfully
    df = pd.DataFrame(
        {
            "company": ["A", "B", "A", "B", "C"],
            "individual_amount": [100, 500, 200, 600, 300],
            "category": ["small", "large", "small", "large", "medium"],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_row_filter", df)

    model = SemanticModel(
        table=table,
        dimensions={"company": lambda t: t.company, "category": lambda t: t.category},
        measures={
            "total_amount": lambda t: t.individual_amount.sum(),
            "avg_amount": lambda t: t.individual_amount.mean(),
            "count": lambda t: t.count(),
        },
    )

    # Filter by individual_amount (this makes sense at row level)
    result = (
        model.query(
            dimensions=["company"],
            measures=["total_amount", "count"],
            filters=[{"field": "individual_amount", "operator": ">=", "value": 300}],
        )
        .execute()
        .sort_values("company")
        .reset_index(drop=True)
    )

    # Should only include rows where individual_amount >= 300 (B: 500+600=1100, C: 300)
    expected = pd.DataFrame(
        {"company": ["B", "C"], "total_amount": [1100, 300], "count": [2, 1]}
    )

    pd.testing.assert_frame_equal(result, expected)


def test_mixed_dimension_and_measure_field_filters():
    """Test filtering on both dimension fields and measure-related fields."""
    df = pd.DataFrame(
        {
            "company": ["A", "B", "A", "B", "C", "C"],
            "region": ["US", "EU", "US", "EU", "US", "EU"],
            "amount": [100, 200, 150, 250, 300, 400],
        }
    )

    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_mixed_filters", df)

    model = SemanticModel(
        table=table,
        dimensions={"company": lambda t: t.company, "region": lambda t: t.region},
        measures={
            "total_amount": lambda t: t.amount.sum(),
            "avg_amount": lambda t: t.amount.mean(),
        },
    )

    # Mix dimension filter (region) with raw column filter (amount)
    result = (
        model.query(
            dimensions=["company"],
            measures=["total_amount"],
            filters=[
                {"field": "region", "operator": "=", "value": "US"},  # Dimension filter
                {
                    "field": "amount",
                    "operator": ">=",
                    "value": 200,
                },  # Raw column filter
            ],
        )
        .execute()
        .sort_values("company")
        .reset_index(drop=True)
    )

    # Should only include US region rows with amount >= 200
    # A: US + amount=100,150 -> neither >= 200 -> excluded
    # C: US + amount=300 -> passes -> 300
    expected = pd.DataFrame({"company": ["C"], "total_amount": [300]})

    pd.testing.assert_frame_equal(result, expected)

def test_backwards_compatibility_with_mixed_spec_classes():
    """Test that old lambda functions work alongside new DimensionSpec/MeasureSpec classes."""
    df = pd.DataFrame({
        "category": ["A","B","A","B"],
        "value": [1, 2, 3, 4],
        "count": [1, 1, 1, 1]
    })
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_mixed_spec", df)

    model = SemanticModel(
        table=table,
        dimensions={
            "category": lambda t: t.category,
            "category_upper": DimensionSpec(expr=lambda t: t.category.upper(), description="Category in uppercase")
        },
        measures={
            "sum_value": lambda t: t.value.sum(),
            "avg_value": MeasureSpec(expr=lambda t: t.value.mean(), description="Average value per group"),
        },
        description="Test model with old and new style definitions"
    )

    expr = model.query(
        dimensions=["category", "category_upper"],
        measures=["sum_value", "avg_value"]
    )
    result = expr.execute().sort_values("category").reset_index(drop=True)

    expected = pd.DataFrame({
        "category": ["A", "B"],
        "category_upper": ["A", "B"],
        "sum_value": [4, 6],
        "avg_value": [2.0, 3.0]
    })

    pd.testing.assert_frame_equal(result, expected)

def test_json_definition_with_descriptions():
    """Test that json_definition correctly handles descriptions for both old and new style definitions"""
    df = pd.DataFrame({"x":[1,2], "y":[3,4]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("desc_test", df)

    model = SemanticModel(
        table=table,
        dimensions={
            "x": lambda t: t.x,
            "x_doubled": DimensionSpec(
                expr=lambda t: t.x * 2,
                description="X value doubled"
            )
        },
        measures={
            "sum_y": lambda t: t.y.sum(),
            "max_y": MeasureSpec(
                expr=lambda t: t.y.max(),
                description="Maximum Y coordinate"
            )
        },
        description="Test model with old and new style definitions"
    )

    json_def = model.json_definition

    #Verify model description
    assert json_def["description"] == "Test model with old and new style definitions"

    #Verify dimension descriptions
    assert json_def["dimensions"]["x"]["description"] == ""
    assert json_def["dimensions"]["x_doubled"]["description"] == "X value doubled"

    #Verify measure descriptions
    assert json_def["measures"]["sum_y"]["description"] == ""
    assert json_def["measures"]["max_y"]["description"] == "Maximum Y coordinate"

    # Verify basic structure
    assert "desc_test" in json_def["name"]
    assert "x" in json_def["dimensions"]
    assert "x_doubled" in json_def["dimensions"]
    assert "sum_y" in json_def["measures"]
    assert "max_y" in json_def["measures"]
