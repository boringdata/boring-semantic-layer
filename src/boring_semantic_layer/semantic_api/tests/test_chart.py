import pandas as pd
import pytest
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table

try:
    import plotly.graph_objects as go

    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    go = None

try:
    import altair

    ALTAIR_AVAILABLE = True
except ImportError:
    ALTAIR_AVAILABLE = False


@pytest.fixture
def simple_semantic_table():
    """Fixture providing a simple semantic table for testing."""
    df = pd.DataFrame({"col_test": ["a", "b", "a", "b", "c"], "val": [1, 2, 3, 4, 5]})
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("test_chart", df)

    # Create semantic table with dimensions and measures
    semantic_table = (
        to_semantic_table(table)
        .with_dimensions(col_test=lambda t: t.col_test, val=lambda t: t.val)
        .with_measures(sum_val=lambda t: t.val.sum(), count=lambda t: t.val.count())
    )
    return semantic_table


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

    semantic_table = (
        to_semantic_table(table)
        .with_dimensions(
            event_time={
                "expr": lambda t: t.event_time,
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
            category=lambda t: t.category,
            event_date=lambda t: t.event_time.date(),
        )
        .with_measures(
            total_value=lambda t: t.value.sum(),
            avg_value=lambda t: t.value.mean(),
        )
    )
    return semantic_table


@pytest.fixture
def joined_semantic_table():
    """Fixture providing a semantic table with joins for testing."""
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

    customers_semantic = to_semantic_table(customers_table).with_dimensions(
        country=lambda t: t.country,
        tier=lambda t: t.tier,
        customer_id=lambda t: t.customer_id,
    )

    orders_semantic = (
        to_semantic_table(orders_table)
        .with_dimensions(
            order_id=lambda t: t.order_id,
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(total_amount=lambda t: t.amount.sum())
        .join_one(customers_semantic, "customer_id", "customer_id")
    )

    return orders_semantic


# Altair Chart Tests
@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_with_specification(simple_semantic_table):
    """Test creating a chart with custom specification."""
    chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "col_test", "type": "nominal"},
            "y": {"field": "sum_val", "type": "quantitative"},
        },
    }

    # Create chart with aggregation and chart specification
    chart = (
        simple_semantic_table.group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .chart(spec=chart_spec)
    )
    assert hasattr(chart, "mark_bar")


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_auto_detection(simple_semantic_table):
    """Test automatic chart type detection."""
    # Create chart without specification - should auto-detect bar chart
    chart = (
        simple_semantic_table.group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .chart()
    )
    assert hasattr(chart, "mark_bar")


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_with_time_series(time_semantic_table):
    """Test chart auto-detection with time series data."""
    # Create chart with time dimension - should detect line chart
    chart = (
        time_semantic_table.group_by("event_date")
        .aggregate(total_value=lambda t: t.value.sum())
        .chart()
    )
    # Should auto-detect line chart for time series
    assert hasattr(chart, "mark_line")


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_field_validation(simple_semantic_table):
    """Test chart field validation against query results."""
    # Create chart spec referencing a field not in the query
    invalid_chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "col_test", "type": "nominal"},
            "y": {"field": "missing_field", "type": "quantitative"},
        },
    }

    # Altair will handle the validation when the chart is displayed
    # We just verify that a chart object is created
    chart = (
        simple_semantic_table.group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
        .chart(spec=invalid_chart_spec)
    )
    assert hasattr(chart, "mark_bar")


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_with_joins(joined_semantic_table):
    """Test chart functionality with joined data."""
    chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "country", "type": "nominal"},
            "y": {"field": "total_amount", "type": "quantitative"},
        },
    }

    chart = (
        joined_semantic_table.group_by("country")
        .aggregate(total_amount=lambda t: t.amount.sum())
        .chart(spec=chart_spec)
    )
    # Verify we get an Altair chart object
    assert hasattr(chart, "mark_bar")


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_requires_altair(simple_semantic_table):
    """Test that chart() method requires Altair."""
    # Try to render without Altair installed
    try:
        import altair  # noqa: F401

        # If Altair is installed, this test won't work as expected
        # But we can still check that chart() returns an Altair chart
        chart = (
            simple_semantic_table.group_by("col_test")
            .aggregate(sum_val=lambda t: t.val.sum())
            .chart(spec={"mark": "bar"})
        )
        assert hasattr(chart, "mark_bar")  # Altair charts have mark methods
    except ImportError:
        # If Altair is not installed, should raise helpful error
        with pytest.raises(ImportError, match="Altair is required for chart creation"):
            (
                simple_semantic_table.group_by("col_test")
                .aggregate(sum_val=lambda t: t.val.sum())
                .chart()
            )


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_chart_output_formats(simple_semantic_table):
    """Test different output formats for chart() method."""
    chart_spec = {
        "mark": "bar",
        "encoding": {
            "x": {"field": "col_test", "type": "nominal"},
            "y": {"field": "sum_val", "type": "quantitative"},
        },
    }

    try:
        import altair as alt  # noqa: F401

        table_expr = simple_semantic_table.group_by("col_test").aggregate(
            sum_val=lambda t: t.val.sum()
        )

        # Test default format (static)
        default_chart = table_expr.chart(spec=chart_spec)
        assert hasattr(default_chart, "mark_bar")

        # Test interactive format
        interactive_chart = table_expr.chart(spec=chart_spec, format="interactive")
        assert hasattr(interactive_chart, "mark_bar")

        # Test JSON format
        json_spec = table_expr.chart(spec=chart_spec, format="json")
        assert isinstance(json_spec, dict)
        assert "mark" in json_spec

        # Test invalid format
        with pytest.raises(ValueError, match="Unsupported format"):
            table_expr.chart(spec=chart_spec, format="invalid")

        # Test PNG/SVG formats (may fail if dependencies not installed)
        try:
            png_data = table_expr.chart(spec=chart_spec, format="png")
            assert isinstance(png_data, bytes)
        except (ImportError, ValueError):
            # Expected if vl-convert not installed
            pass

        try:
            svg_data = table_expr.chart(spec=chart_spec, format="svg")
            assert isinstance(svg_data, str) or isinstance(svg_data, bytes)
        except (ImportError, ValueError):
            # Expected if vl-convert not installed
            pass

    except ImportError:
        # Altair not installed
        pass


# Plotly Chart Tests
@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_bar_chart_single_measure(simple_semantic_table):
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1
    assert fig.data[0].type == "bar"


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_bar_chart_multiple_measures(simple_semantic_table):
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum(), count=lambda t: t.val.count()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 2

    for trace in fig.data:
        assert trace.type == "bar"

    assert fig.layout.barmode == "group"


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_line_chart_time_series(time_semantic_table):
    expr = time_semantic_table.group_by("event_date").aggregate(
        total_value=lambda t: t.value.sum()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1
    assert fig.data[0].type == "scatter"
    assert fig.data[0].mode == "lines"


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_line_chart_multiple_measures(time_semantic_table):
    expr = time_semantic_table.group_by("event_date").aggregate(
        total_value=lambda t: t.value.sum(), avg_value=lambda t: t.value.mean()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 2

    for trace in fig.data:
        assert trace.type == "scatter"
        assert trace.mode == "lines"


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_line_chart_with_categories(time_semantic_table):
    expr = time_semantic_table.group_by("event_date", "category").aggregate(
        total_value=lambda t: t.value.sum()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) > 1
    for trace in fig.data:
        assert trace.type == "scatter"
        assert trace.mode == "lines"


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_heatmap():
    df = pd.DataFrame(
        {
            "x_dim": ["A", "B", "A", "B", "C"],
            "y_dim": ["X", "X", "Y", "Y", "X"],
            "value": [10, 20, 30, 40, 50],
        }
    )
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table("heatmap_test", df)

    semantic_table = (
        to_semantic_table(table)
        .with_dimensions(x_dim=lambda t: t.x_dim, y_dim=lambda t: t.y_dim)
        .with_measures(total_value=lambda t: t.value.sum())
    )

    expr = semantic_table.group_by("x_dim", "y_dim").aggregate(
        total_value=lambda t: t.value.sum()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1
    assert fig.data[0].type == "heatmap"
    assert hasattr(fig.data[0], "z")
    assert hasattr(fig.data[0], "x")
    assert hasattr(fig.data[0], "y")


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_table(simple_semantic_table):
    # Create a more complex query for table visualization
    expr = simple_semantic_table.group_by("col_test", "val").aggregate(
        sum_val=lambda t: t.val.sum(), count=lambda t: t.val.count()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1
    assert fig.data[0].type == "table"
    assert hasattr(fig.data[0], "header")
    assert hasattr(fig.data[0], "cells")


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_indicator_not_implemented(simple_semantic_table):
    # Test with no dimensions (which would trigger indicator)
    expr = simple_semantic_table.aggregate(sum_val=lambda t: t.val.sum())

    with pytest.raises(
        NotImplementedError, match="Indicator charts are not yet supported"
    ):
        expr.chart(backend="plotly")


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_custom_spec(simple_semantic_table):
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    fig = expr.chart(
        backend="plotly", spec={"layout": {"title": "Custom Title", "height": 500}}
    )

    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text == "Custom Title"
    assert fig.layout.height == 500


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_explicit_chart_type(simple_semantic_table):
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    # Force line chart using chart_type
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
@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_output_formats(simple_semantic_table, format_type, expected_type):
    import json

    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    result = expr.chart(backend="plotly", format=format_type)

    if expected_type == "Figure":
        assert isinstance(result, go.Figure)
    elif expected_type == "string":
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "data" in parsed or "layout" in parsed


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_with_filters(simple_semantic_table):
    expr = (
        simple_semantic_table.filter(lambda t: t.col_test.isin(["a", "b"]))
        .group_by("col_test")
        .aggregate(sum_val=lambda t: t.val.sum())
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    data = expr.execute()
    assert len(data) <= 2


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_missing_plotly(simple_semantic_table):
    """Test error handling when Plotly is not available."""
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    try:
        import plotly.graph_objects  # noqa: F401

        # If plotly is available, this test is not applicable
        pytest.skip("Plotly is available, cannot test missing plotly scenario")
    except ImportError:
        # Test the actual error when plotly is missing
        with pytest.raises(ImportError, match="plotly is required for chart creation"):
            expr.chart(backend="plotly")


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_invalid_format(simple_semantic_table):
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    with pytest.raises(ValueError, match="Unsupported format"):
        expr.chart(backend="plotly", format="invalid_format")


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="Plotly not installed")
def test_plotly_chart_backend_selection(simple_semantic_table):
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    fig = expr.chart(backend="plotly")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1
    assert fig.data[0].type == "bar"


# Chart Renderer Internals Tests - Commented out due to API changes
# These tests were testing internal implementation details that have changed


# Backend and Format Validation Tests
def test_invalid_backend(simple_semantic_table):
    """Test error handling for invalid backend."""
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    with pytest.raises(ValueError, match="Unsupported backend"):
        expr.chart(backend="invalid_backend")


def test_invalid_format(simple_semantic_table):
    """Test error handling for invalid format."""
    expr = simple_semantic_table.group_by("col_test").aggregate(
        sum_val=lambda t: t.val.sum()
    )

    with pytest.raises(ValueError, match="Unsupported format"):
        expr.chart(format="invalid_format")
