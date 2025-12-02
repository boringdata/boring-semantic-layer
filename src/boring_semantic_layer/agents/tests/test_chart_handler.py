"""Tests for chart_handler utility functions."""

import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data


@pytest.fixture
def mock_query_result():
    """Create a mock query result with execute() method."""
    mock_result = Mock()
    df = pd.DataFrame({"origin": ["ATL", "ORD", "DFW"], "flight_count": [414513, 350380, 281281]})
    mock_result.execute.return_value = df
    mock_result.chart = Mock(return_value='{"spec": "chart_data"}')
    return mock_result


def test_show_chart_false_returns_only_data_json_mode(mock_query_result):
    """Test that show_chart=false returns only data in JSON mode."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        chart_spec={"show_chart": False, "show_table": True},
        default_backend="plotext",
        return_json=True,
    )

    # Should return JSON with records but no chart
    result_dict = json.loads(result)
    assert "records" in result_dict
    assert "chart" not in result_dict
    assert len(result_dict["records"]) == 3
    assert result_dict["records"][0]["origin"] == "ATL"

    # Chart method should not have been called
    mock_query_result.chart.assert_not_called()


def test_show_chart_false_prints_table_cli_mode(mock_query_result, capsys):
    """Test that show_chart=false prints table in CLI mode."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        chart_spec={"show_chart": False, "show_table": True},
        default_backend="plotext",
        return_json=False,
    )

    # Should return success message
    assert "Query executed successfully" in result
    assert "3 rows" in result

    # Should have printed the table
    captured = capsys.readouterr()
    assert "ATL" in captured.out
    assert "414513" in captured.out

    # Chart method should not have been called
    mock_query_result.chart.assert_not_called()


def test_show_chart_false_show_table_false_cli_mode(mock_query_result, capsys):
    """Test that show_chart=false and show_table=false doesn't print anything."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        chart_spec={"show_chart": False, "show_table": False},
        default_backend="plotext",
        return_json=False,
    )

    # Should return success message
    assert "Query executed successfully" in result

    # Should NOT have printed the table
    captured = capsys.readouterr()
    assert "ATL" not in captured.out

    # Chart method should not have been called
    mock_query_result.chart.assert_not_called()


def test_show_chart_true_calls_chart_method(mock_query_result):
    """Test that show_chart=true (default) calls chart method."""
    with patch.object(mock_query_result, "chart", return_value='{"spec": "chart_data"}'):
        result = generate_chart_with_data(
            query_result=mock_query_result,
            chart_spec={"show_chart": True, "backend": "plotext", "format": "json"},
            default_backend="plotext",
            return_json=True,
        )

        # Should return JSON with both records and chart
        result_dict = json.loads(result)
        assert "records" in result_dict
        assert "chart" in result_dict

        # Chart method should have been called
        mock_query_result.chart.assert_called_once()


def test_no_chart_spec_uses_defaults_json_mode(mock_query_result):
    """Test that no chart_spec in JSON mode returns records only (no chart)."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        chart_spec=None,
        default_backend="plotext",
        return_json=True,
    )

    # In JSON mode with no chart_spec, show_chart defaults to False
    result_dict = json.loads(result)
    assert "records" in result_dict
    assert "chart" not in result_dict

    # Chart method should NOT have been called
    mock_query_result.chart.assert_not_called()


def test_table_limit_parameter(mock_query_result, capsys):
    """Test that table_limit limits rows displayed."""
    # Create a larger dataframe
    df = pd.DataFrame(
        {"origin": [f"AIRPORT_{i}" for i in range(20)], "flight_count": list(range(20))}
    )
    mock_query_result.execute.return_value = df

    result = generate_chart_with_data(
        query_result=mock_query_result,
        chart_spec={"show_chart": False, "show_table": True, "table_limit": 5},
        default_backend="plotext",
        return_json=False,
    )

    # Should return success message with full row count
    assert "Query executed successfully" in result
    assert "20 rows" in result

    # Table should only show 5 rows
    captured = capsys.readouterr()
    assert "AIRPORT_0" in captured.out
    assert "AIRPORT_4" in captured.out
    # Row 5+ should not be in output (table limited to 5)


def test_single_row_result_hides_chart():
    """Test that single-row results automatically hide the chart."""
    # Create a mock with single-row result (e.g., aggregate total)
    mock_result = Mock()
    df = pd.DataFrame({"total_flights": [58635]})  # Single aggregate value
    mock_result.execute.return_value = df
    mock_result.chart = Mock(return_value='{"spec": "chart_data"}')

    # Even with show_chart=True (default), chart should be hidden for single row
    result = generate_chart_with_data(
        query_result=mock_result,
        chart_spec={"show_chart": True, "backend": "plotext", "format": "json"},
        default_backend="plotext",
        return_json=True,
    )

    # Should return JSON with records but NO chart (auto-hidden)
    result_dict = json.loads(result)
    assert "records" in result_dict
    assert "chart" not in result_dict
    assert len(result_dict["records"]) == 1
    assert result_dict["records"][0]["total_flights"] == 58635

    # Chart method should NOT have been called
    mock_result.chart.assert_not_called()


def test_two_row_result_shows_chart():
    """Test that two-row results still show the chart."""
    mock_result = Mock()
    df = pd.DataFrame({"category": ["A", "B"], "count": [100, 200]})  # Two rows
    mock_result.execute.return_value = df
    mock_result.chart = Mock(return_value='{"spec": "chart_data"}')

    result = generate_chart_with_data(
        query_result=mock_result,
        chart_spec={"show_chart": True, "backend": "plotext", "format": "json"},
        default_backend="plotext",
        return_json=True,
    )

    # Should return JSON with both records and chart
    result_dict = json.loads(result)
    assert "records" in result_dict
    assert "chart" in result_dict
    assert len(result_dict["records"]) == 2

    # Chart method SHOULD have been called
    mock_result.chart.assert_called_once()


def test_ibis_available_in_context():
    """Test that ibis module is available in safe_eval context."""
    from pathlib import Path

    import ibis

    from boring_semantic_layer import from_yaml
    from boring_semantic_layer.utils import safe_eval

    # Load models
    models = from_yaml(
        str(Path("examples/flights.yml")),
        profile_path=str(Path("examples/profiles.yml")),
    )

    # Test query with ibis.desc()
    query_str = 'flights.group_by("carrier").aggregate("flight_count").order_by(ibis.desc("flight_count")).limit(5)'

    result = safe_eval(query_str, context={**models, "ibis": ibis})

    # Should succeed (no AttributeError)
    assert result is not None

    # If it's a Result type, unwrap it
    if hasattr(result, "unwrap"):
        result = result.unwrap()

    # Should be able to execute
    df = result.execute()
    assert len(df) == 5
    # Should be sorted descending by flight_count
    assert df["flight_count"].iloc[0] >= df["flight_count"].iloc[1]
