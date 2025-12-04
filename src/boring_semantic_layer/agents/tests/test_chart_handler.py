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


def test_get_chart_false_returns_only_data_json_mode(mock_query_result):
    """Test that get_chart=false returns only data in JSON mode."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=False,
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


def test_cli_mode_auto_shows_table_when_get_records_true(mock_query_result, capsys):
    """Test that CLI mode auto-shows table when get_records=True (default)."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=False,  # No chart - just table
        default_backend="plotext",
        return_json=False,
    )

    # Should return JSON with insight for LLM
    result_dict = json.loads(result)
    assert result_dict["total_rows"] == 3
    assert "origin" in result_dict["columns"]
    assert "records" in result_dict

    # Should have printed the table (auto-shown because get_records=True)
    captured = capsys.readouterr()
    assert "ATL" in captured.out
    assert "414513" in captured.out

    # Chart method should not have been called (get_chart=False)
    mock_query_result.chart.assert_not_called()


def test_cli_mode_hides_table_when_get_records_false(mock_query_result, capsys):
    """Test that CLI mode hides table when get_records=False (display-only)."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_records=False,
        get_chart=False,
        default_backend="plotext",
        return_json=False,
    )

    # Should return JSON with metadata only (no records)
    result_dict = json.loads(result)
    assert result_dict["total_rows"] == 3
    assert "origin" in result_dict["columns"]
    assert "records" not in result_dict
    assert "note" in result_dict

    # Should NOT have printed the table (get_records=False)
    captured = capsys.readouterr()
    assert "ATL" not in captured.out

    # Chart method should not have been called (get_chart=False)
    mock_query_result.chart.assert_not_called()


def test_get_chart_true_calls_chart_method(mock_query_result):
    """Test that get_chart=true (default) calls chart method."""
    with patch.object(mock_query_result, "chart", return_value='{"spec": "chart_data"}'):
        result = generate_chart_with_data(
            query_result=mock_query_result,
            get_chart=True,
            chart_backend="plotext",
            chart_format="json",
            default_backend="plotext",
            return_json=True,
        )

        # Should return JSON with both records and chart
        result_dict = json.loads(result)
        assert "records" in result_dict
        assert "chart" in result_dict
        # New response format includes backend/format/data
        assert result_dict["chart"]["backend"] == "plotext"
        assert result_dict["chart"]["format"] == "json"

        # Chart method should have been called
        mock_query_result.chart.assert_called_once()


def test_defaults_json_mode(mock_query_result):
    """Test that defaults in JSON mode work correctly."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        default_backend="plotext",
        return_json=True,
    )

    # With defaults: get_records=True, get_chart=True (but chart is called with format=json)
    result_dict = json.loads(result)
    assert "records" in result_dict
    # Chart should be generated (default get_chart=True)
    assert "chart" in result_dict

    # Chart method should have been called
    mock_query_result.chart.assert_called_once()


def test_records_limit_cli_mode(mock_query_result, capsys):
    """Test that records_limit limits rows displayed in CLI mode."""
    # Create a larger dataframe
    df = pd.DataFrame(
        {"origin": [f"AIRPORT_{i}" for i in range(20)], "flight_count": list(range(20))}
    )
    mock_query_result.execute.return_value = df

    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=False,
        records_limit=5,
        default_backend="plotext",
        return_json=False,
    )

    # Should return JSON with insight for LLM
    result_dict = json.loads(result)
    assert result_dict["total_rows"] == 20
    assert "origin" in result_dict["columns"]
    assert len(result_dict["records"]) == 5  # Limited to 5

    # Table should only show 5 rows (same as records returned to LLM)
    captured = capsys.readouterr()
    assert "AIRPORT_0" in captured.out
    assert "AIRPORT_4" in captured.out
    # Row 5+ should not be in output (limited to 5)


def test_single_row_result_hides_chart():
    """Test that single-row results automatically hide the chart."""
    # Create a mock with single-row result (e.g., aggregate total)
    mock_result = Mock()
    df = pd.DataFrame({"total_flights": [58635]})  # Single aggregate value
    mock_result.execute.return_value = df
    mock_result.chart = Mock(return_value='{"spec": "chart_data"}')

    # Even with get_chart=True (default), chart should be hidden for single row
    result = generate_chart_with_data(
        query_result=mock_result,
        get_chart=True,
        chart_backend="plotext",
        chart_format="json",
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
        get_chart=True,
        chart_backend="plotext",
        chart_format="json",
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


def test_get_records_false_cli_mode(mock_query_result, capsys):
    """Test that get_records=false in CLI mode returns only metadata and hides table."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_records=False,
        get_chart=False,
        default_backend="plotext",
        return_json=False,
    )

    # Should return JSON with metadata but no records
    result_dict = json.loads(result)
    assert result_dict["total_rows"] == 3
    assert "origin" in result_dict["columns"]
    assert "records" not in result_dict
    assert "note" in result_dict
    assert "get_records=false" in result_dict["note"]

    # Table should NOT have been displayed (get_records=False hides table)
    captured = capsys.readouterr()
    assert "ATL" not in captured.out


def test_records_limit_truncation_message(mock_query_result):
    """Test that records_limit shows truncation message when data is truncated."""
    # Create a larger dataframe
    df = pd.DataFrame(
        {"origin": [f"AIRPORT_{i}" for i in range(20)], "flight_count": list(range(20))}
    )
    mock_query_result.execute.return_value = df

    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=False,
        records_limit=5,
        default_backend="plotext",
        return_json=True,
    )

    # Should return JSON with truncation note
    result_dict = json.loads(result)
    assert result_dict["total_rows"] == 20
    assert result_dict["returned_rows"] == 5
    assert len(result_dict["records"]) == 5
    assert "note" in result_dict
    assert "5 of 20" in result_dict["note"]
    assert "records_limit" in result_dict["note"]


def test_no_truncation_message_when_all_returned(mock_query_result):
    """Test that no truncation message when all records returned."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=False,
        records_limit=10,  # More than 3 rows
        default_backend="plotext",
        return_json=True,
    )

    # Should return JSON without truncation note
    result_dict = json.loads(result)
    assert result_dict["total_rows"] == 3
    assert "returned_rows" not in result_dict  # No truncation
    assert "note" not in result_dict  # No note needed


def test_columns_included_in_response(mock_query_result):
    """Test that columns are always included in response."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=False,
        default_backend="plotext",
        return_json=True,
    )

    result_dict = json.loads(result)
    assert "columns" in result_dict
    assert result_dict["columns"] == ["origin", "flight_count"]


def test_chart_response_includes_backend_and_format(mock_query_result):
    """Test that chart response includes backend, format, and data fields."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_backend="plotext",
        chart_format="json",
        default_backend="altair",
        return_json=True,
    )

    result_dict = json.loads(result)
    assert "chart" in result_dict
    assert result_dict["chart"]["backend"] == "plotext"
    assert result_dict["chart"]["format"] == "json"
    assert "data" in result_dict["chart"]


def test_cli_mode_with_chart_includes_chart_info(mock_query_result, capsys):
    """Test that CLI mode with chart includes chart info in response."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        default_backend="plotext",
        return_json=False,
    )

    result_dict = json.loads(result)
    assert "chart" in result_dict
    assert result_dict["chart"]["backend"] == "plotext"
    assert result_dict["chart"]["format"] == "static"
    assert result_dict["chart"]["displayed"] is True


def test_chart_backend_override(mock_query_result):
    """Test that chart_backend parameter overrides default_backend."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_backend="altair",
        chart_format="json",
        default_backend="plotext",
        return_json=True,
    )

    result_dict = json.loads(result)
    assert "chart" in result_dict
    assert result_dict["chart"]["backend"] == "altair"

    # Verify chart was called with altair backend
    mock_query_result.chart.assert_called_with(spec=None, backend="altair", format="json")


def test_static_format_message_in_api_mode():
    """Test that static format with non-plotext backend returns message in API mode."""
    mock_result = Mock()
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    mock_result.execute.return_value = df

    result = generate_chart_with_data(
        query_result=mock_result,
        get_chart=True,
        chart_backend="altair",
        chart_format="static",
        return_json=True,
    )

    result_dict = json.loads(result)
    assert "records" in result_dict
    assert "chart" in result_dict
    assert "message" in result_dict["chart"]
    assert "Use format='json'" in result_dict["chart"]["message"]


def test_chart_spec_passed_to_chart_method(mock_query_result):
    """Test that chart_spec is correctly passed to chart method."""
    custom_spec = {"chart_type": "bar", "theme": "dark"}

    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_spec={"spec": custom_spec},
        chart_backend="altair",
        chart_format="json",
        return_json=True,
    )

    result_dict = json.loads(result)
    assert "chart" in result_dict

    # Verify chart was called with the custom spec
    mock_query_result.chart.assert_called_with(spec=custom_spec, backend="altair", format="json")


def test_error_callback_called_on_chart_error(capsys):
    """Test that error_callback is called when chart generation fails."""
    mock_result = Mock()
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    mock_result.execute.return_value = df
    mock_result.chart = Mock(side_effect=Exception("Chart generation failed"))

    errors = []

    def error_callback(msg):
        errors.append(msg)

    generate_chart_with_data(
        query_result=mock_result,
        get_chart=True,
        default_backend="plotext",
        return_json=False,
        error_callback=error_callback,
    )

    # Error callback should have been called
    assert len(errors) == 1
    assert "Chart generation failed" in errors[0]


def test_query_execution_error_json_mode():
    """Test that query execution errors are handled in JSON mode."""
    mock_result = Mock()
    mock_result.execute = Mock(side_effect=Exception("Database connection failed"))

    result = generate_chart_with_data(
        query_result=mock_result,
        return_json=True,
    )

    result_dict = json.loads(result)
    assert "error" in result_dict
    assert "Database connection failed" in result_dict["error"]


def test_query_execution_error_cli_mode(capsys):
    """Test that query execution errors are handled in CLI mode."""
    mock_result = Mock()
    mock_result.execute = Mock(side_effect=Exception("Database connection failed"))

    errors = []

    def error_callback(msg):
        errors.append(msg)

    generate_chart_with_data(
        query_result=mock_result,
        return_json=False,
        error_callback=error_callback,
    )

    # Error callback should have been called
    assert len(errors) == 1
    assert "Database connection failed" in errors[0]


def test_chart_error_returns_records_with_error(mock_query_result):
    """Test that chart errors don't prevent records from being returned."""
    mock_query_result.chart = Mock(side_effect=Exception("Chart rendering failed"))

    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        return_json=True,
    )

    result_dict = json.loads(result)
    # Records should still be returned
    assert "records" in result_dict
    assert len(result_dict["records"]) == 3
    # Chart error should be reported
    assert "chart_error" in result_dict
    assert "Chart rendering failed" in result_dict["chart_error"]


def test_default_backend_used_when_chart_backend_none(mock_query_result):
    """Test that default_backend is used when chart_backend is None."""
    result = generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_backend=None,
        chart_format="json",
        default_backend="altair",
        return_json=True,
    )

    result_dict = json.loads(result)
    assert result_dict["chart"]["backend"] == "altair"

    # Verify chart was called with default backend
    mock_query_result.chart.assert_called_with(spec=None, backend="altair", format="json")


def test_cli_mode_opens_altair_in_browser(mock_query_result):
    """Test that CLI mode opens altair charts in browser."""

    generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_backend="altair",
        return_json=False,  # CLI mode
    )

    # Chart should be called with altair and format="static" to get chart object
    mock_query_result.chart.assert_called_with(spec=None, backend="altair", format="static")


def test_cli_mode_opens_plotly_in_browser(mock_query_result):
    """Test that CLI mode opens plotly charts in browser."""
    generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_backend="plotly",
        return_json=False,  # CLI mode
    )

    # Chart should be called with plotly and format="static" to get chart object
    mock_query_result.chart.assert_called_with(spec=None, backend="plotly", format="static")


def test_cli_mode_allows_plotext_backend(mock_query_result):
    """Test that CLI mode works fine when plotext is explicitly requested."""
    warnings = []

    def capture_warning(msg):
        warnings.append(msg)

    generate_chart_with_data(
        query_result=mock_query_result,
        get_chart=True,
        chart_backend="plotext",  # Should work fine
        return_json=False,  # CLI mode
        error_callback=capture_warning,
    )

    # No warnings should be issued
    assert len(warnings) == 0

    # Chart should be called with plotext
    mock_query_result.chart.assert_called_with(spec=None, backend="plotext", format="static")


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
