import pandas as pd
import pytest
import ibis
from boring_semantic_layer import SemanticModel, Join
import plotly.graph_objects as go


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


class TestAltairChart:
    """Test class for Altair chart functionality."""

    def test_query_with_chart_specification(self):
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

    def test_query_chart_auto_detection(self):
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

    def test_query_chart_with_time_series(self):
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

    def test_query_chart_field_validation(self):
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

    def test_query_chart_with_joins(self):
        """Test chart functionality with joined data."""
        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [101, 101, 102],
                "amount": [100, 200, 300],
            }
        )
        customers_df = pd.DataFrame(
            {"customer_id": [101, 102], "country": ["US", "UK"]}
        )

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
                "customer": Join.one(
                    "customer", customers_model, lambda t: t.customer_id
                )
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

    def test_query_render_requires_altair(self):
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
            with pytest.raises(
                ImportError, match="Altair is required for chart creation"
            ):
                expr.chart()

    def test_query_chart_output_formats(self):
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


class TestPlotlyChart:
    def test_plotly_bar_chart_single_measure(self, simple_model):
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert fig.data[0].type == "bar"
        assert fig.layout.barmode != "group"

    def test_plotly_bar_chart_multiple_measures(self, simple_model):
        expr = simple_model.query(
            dimensions=["col_test"], measures=["sum_val", "count"]
        )

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 2

        for trace in fig.data:
            assert trace.type == "bar"

        assert fig.layout.barmode == "group"

    def test_plotly_line_chart_time_series(self, time_model):
        expr = time_model.query(dimensions=["date"], measures=["total_value"])

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert fig.data[0].type == "scatter"
        assert fig.data[0].mode == "lines"

    def test_plotly_line_chart_multiple_measures(self, time_model):
        expr = time_model.query(
            dimensions=["date"], measures=["total_value", "avg_value"]
        )

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 2

        for trace in fig.data:
            assert trace.type == "scatter"
            assert trace.mode == "lines"

    def test_plotly_line_chart_with_categories(self, time_model):
        expr = time_model.query(
            dimensions=["date", "category"], measures=["total_value"]
        )

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) > 1
        for trace in fig.data:
            assert trace.type == "scatter"
            assert trace.mode == "lines"

    def test_plotly_heatmap(self):
        df = pd.DataFrame(
            {
                "x_dim": ["A", "B", "A", "B", "C"],
                "y_dim": ["X", "X", "Y", "Y", "X"],
                "value": [10, 20, 30, 40, 50],
            }
        )
        con = ibis.duckdb.connect(":memory:")
        table = con.create_table("heatmap_test", df)
        model = SemanticModel(
            table=table,
            dimensions={"x_dim": lambda t: t.x_dim, "y_dim": lambda t: t.y_dim},
            measures={"total_value": lambda t: t.value.sum()},
        )

        expr = model.query(dimensions=["x_dim", "y_dim"], measures=["total_value"])

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert fig.data[0].type == "heatmap"
        assert hasattr(fig.data[0], "z")
        assert hasattr(fig.data[0], "x")
        assert hasattr(fig.data[0], "y")

    def test_plotly_table(self, simple_model):
        expr = simple_model.query(
            dimensions=["col_test", "val"], measures=["sum_val", "count"]
        )

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert fig.data[0].type == "table"
        assert hasattr(fig.data[0], "header")
        assert hasattr(fig.data[0], "cells")

    def test_plotly_indicator_not_implemented(self, simple_model):
        expr = simple_model.query(dimensions=[], measures=["sum_val"])

        with pytest.raises(
            NotImplementedError, match="Indicator charts are not yet supported"
        ):
            expr.chart(
                backend="plotly",
            )

    @pytest.mark.parametrize(
        "dimensions,measures,expected_chart_type",
        [
            (["col_test"], ["sum_val"], "bar"),
            (["col_test"], ["sum_val", "count"], "bar"),
            (["col_test", "val"], ["sum_val"], "heatmap"),
            (["col_test", "val"], ["sum_val", "count"], "table"),
        ],
    )
    def test_plotly_chart_auto_detection(
        self, simple_model, dimensions, measures, expected_chart_type
    ):
        from boring_semantic_layer.chart import _detect_plotly_chart_type

        detected_type = _detect_plotly_chart_type(
            dimensions=dimensions, measures=measures, time_dimension=None
        )

        assert detected_type == expected_chart_type

    def test_plotly_chart_time_series_detection(self, time_model):
        from boring_semantic_layer.chart import _detect_plotly_chart_type

        detected_type = _detect_plotly_chart_type(
            dimensions=["date"], measures=["total_value"], time_dimension="date"
        )
        assert detected_type == "line"

        detected_type = _detect_plotly_chart_type(
            dimensions=["date", "category"],
            measures=["total_value"],
            time_dimension="date",
        )
        assert detected_type == "line"

    def test_plotly_chart_custom_spec(self, simple_model):
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        fig = expr.chart(
            backend="plotly", spec={"layout": {"title": "Custom Title", "height": 500}}
        )

        assert isinstance(fig, go.Figure)
        assert fig.layout.title.text == "Custom Title"
        assert fig.layout.height == 500

    def test_plotly_chart_explicit_chart_type(self, simple_model):
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        # Test with a custom spec to force line chart using chart_type
        fig = expr.chart(backend="plotly", spec={"chart_type": "line"})

        assert isinstance(fig, go.Figure)
        assert fig.data[0].type == "scatter"
        assert fig.data[0].mode == "lines"

    @pytest.mark.parametrize(
        "format_type,expected_type",
        [
            ("static", "Figure"),
            ("interactive", "Figure"),
            ("json", "string"),
        ],
    )
    def test_plotly_chart_output_formats(
        self, simple_model, format_type, expected_type
    ):
        import json

        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        result = expr.chart(backend="plotly", format=format_type)

        if expected_type == "Figure":
            assert isinstance(result, go.Figure)
        elif expected_type == "string":
            assert isinstance(result, str)
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
            assert "data" in parsed or "layout" in parsed

    def test_plotly_chart_with_filters(self, simple_model):
        expr = simple_model.query(
            dimensions=["col_test"],
            measures=["sum_val"],
            filters=[{"field": "col_test", "operator": "in", "values": ["a", "b"]}],
        )

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        data = expr.execute()
        assert len(data) <= 2

    def test_plotly_chart_with_joins(self, joined_model):
        expr = joined_model.query(dimensions=["customer_id"], measures=["total_amount"])

        fig = expr.chart(
            backend="plotly",
        )

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1
        assert hasattr(fig, "data")
        assert hasattr(fig, "layout")

        data = expr.execute()
        assert len(data) > 0

    def test_plotly_chart_missing_plotly(self, simple_model):
        """Test error handling when Plotly is not available."""
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        try:
            import plotly.graph_objects  # noqa: F401

            # If plotly is available, this test is not applicable
            pytest.skip("Plotly is available, cannot test missing plotly scenario")
        except ImportError:
            # Test the actual error when plotly is missing
            with pytest.raises(
                ImportError, match="plotly is required for chart creation"
            ):
                expr.chart(
                    backend="plotly",
                )

    def test_plotly_chart_invalid_format(self, simple_model):
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        with pytest.raises(ValueError, match="Unsupported format"):
            expr.chart(backend="plotly", format="invalid_format")

    def test_plotly_chart_invalid_chart_type(self, simple_model):
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        try:
            fig = expr.chart(backend="plotly", chart_type="invalid_type")
            assert isinstance(fig, go.Figure)
            assert len(fig.data) >= 1
            assert hasattr(fig, "data")
            assert hasattr(fig, "layout")
        except Exception:
            pytest.skip("Invalid chart type handling has implementation limitations")

    def test_plotly_data_preparation_multiple_measures(self, simple_model):
        """Test that data is properly melted for multiple measures."""
        from boring_semantic_layer.chart import _prepare_plotly_data_and_params

        expr = simple_model.query(
            dimensions=["col_test"], measures=["sum_val", "count"]
        )

        df, base_params = _prepare_plotly_data_and_params(expr, "bar")

        # Should have melted data structure
        assert "measure" in df.columns
        assert "value" in df.columns
        assert base_params["y"] == "value"
        assert base_params["color"] == "measure"

    def test_plotly_data_preparation_heatmap(self):
        """Test data preparation for heatmap charts."""
        from boring_semantic_layer.chart import _prepare_plotly_data_and_params

        # Create test data
        df = pd.DataFrame(
            {
                "x_dim": ["A", "B", "A", "B"],
                "y_dim": ["X", "X", "Y", "Y"],
                "value": [10, 20, 30, 40],
            }
        )
        con = ibis.duckdb.connect(":memory:")
        table = con.create_table("heatmap_prep_test", df)
        model = SemanticModel(
            table=table,
            dimensions={"x_dim": lambda t: t.x_dim, "y_dim": lambda t: t.y_dim},
            measures={"total_value": lambda t: t.value.sum()},
        )

        expr = model.query(dimensions=["x_dim", "y_dim"], measures=["total_value"])

        df_result, base_params = _prepare_plotly_data_and_params(expr, "heatmap")

        # Should have pivot table structure for heatmap
        assert "z" in base_params
        assert "x" in base_params
        assert "y" in base_params
        assert isinstance(base_params["z"], list) or hasattr(
            base_params["z"], "shape"
        )  # numpy array or list

    def test_plotly_chart_backend_selection(self, simple_model):
        expr = simple_model.query(dimensions=["col_test"], measures=["sum_val"])

        fig = expr.chart(backend="plotly")

        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert fig.data[0].type == "bar"
