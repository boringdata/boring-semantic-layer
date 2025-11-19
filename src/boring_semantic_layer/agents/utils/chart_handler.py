"""Utilities for handling chart generation in agent backends."""

import json
import traceback
from collections.abc import Callable
from typing import Any

from returns.result import Result, Success


def _format_error(error: Exception | str) -> str:
    """Extract a meaningful error string from an exception or string."""
    error_detail = str(error) if str(error) else repr(error)
    if not error_detail or error_detail == "":
        error_detail = traceback.format_exc()
    return error_detail


def _handle_error(
    error_detail: str,
    error_type: str,
    return_json: bool,
    error_callback: Callable[[str], None] | None = None,
) -> str:
    """Handle error by formatting and returning appropriate response."""
    error_msg = f"❌ {error_type}: {error_detail}"
    if not return_json:
        if error_callback:
            error_callback(error_msg)
        else:
            print(f"\n{error_msg}\n")
    return json.dumps({"error": error_detail}) if return_json else error_msg


def generate_chart_with_data(
    query_result: Any,
    chart_spec: dict[str, Any] | None,
    default_backend: str = "altair",
    return_json: bool = True,
    error_callback: Callable[[str], None] | None = None,
) -> str:
    """
    Generate chart and return data in appropriate format.

    This is the common chart generation logic for both MCP and LangChain backends.

    Args:
        query_result: The query result object with chart() method, or a Result type
        chart_spec: Chart specification dict or None
        default_backend: Default backend to use if not specified in chart_spec
        return_json: If True, always return JSON. If False, render locally (for CLI).
        error_callback: Optional callback(message: str) for displaying errors in CLI mode

    Returns:
        - JSON string with records and chart data if return_json=True
        - Success message if return_json=False (after rendering)
    """
    # Handle Result type from safe_eval
    if isinstance(query_result, Result):
        if isinstance(query_result, Success):
            actual_result = query_result.unwrap()
        else:
            # It's a Failure - extract the underlying exception
            try:
                failure_value = query_result.failure()
                error_detail = _format_error(failure_value)
            except Exception as unwrap_error:
                # Get the original exception from __cause__ or __context__
                original_error = unwrap_error.__cause__ or unwrap_error.__context__ or unwrap_error
                error_detail = _format_error(original_error)

            return _handle_error(error_detail, "Query Error", return_json, error_callback)
    else:
        actual_result = query_result

    try:
        result_df = actual_result.execute()
    except Exception as e:
        error_detail = _format_error(e)
        return _handle_error(error_detail, "Query Execution Error", return_json, error_callback)

    # Early return for no-chart case
    if chart_spec is None:
        if return_json:
            records = json.loads(result_df.to_json(orient="records", date_format="iso"))
            return json.dumps({"records": records})
        return f"Query executed successfully. Returned {len(result_df)} rows."

    # Extract chart parameters
    backend = chart_spec.get("backend", default_backend)
    spec = chart_spec.get("spec")
    show_chart = chart_spec.get("show_chart", True)
    show_table = chart_spec.get("show_table", True)
    format_type = chart_spec.get(
        "format", "json" if return_json else ("static" if backend == "plotext" else "json")
    )

    # If show_chart is False, just display table
    if not show_chart:
        if return_json:
            records = json.loads(result_df.to_json(orient="records", date_format="iso"))
            return json.dumps({"records": records})
        else:
            # CLI mode - display table using Rich (from plotext module)
            if show_table:
                from boring_semantic_layer.chart.plotext_chart import display_table

                display_table(result_df, limit=len(result_df))
            return f"Query executed successfully. Returned {len(result_df)} rows."

    # Generate chart
    if return_json:
        try:
            chart_result = actual_result.chart(spec=spec, backend=backend, format=format_type)
            records = json.loads(result_df.to_json(orient="records", date_format="iso"))

            # Handle different format types
            if format_type == "json":
                # Parse chart data based on backend
                chart_data = (
                    chart_result
                    if backend == "altair"
                    else (
                        json.loads(chart_result) if isinstance(chart_result, str) else chart_result
                    )
                )
                return json.dumps({"records": records, "chart": chart_data})
            else:
                # Non-JSON formats can't be serialized directly
                return json.dumps(
                    {
                        "records": records,
                        "chart": {
                            "backend": backend,
                            "format": format_type,
                            "message": f"Chart generated as {format_type} format. Use format='json' for serializable output.",
                        },
                    }
                )
        except Exception as chart_error:
            # If chart fails in JSON mode, return records with error
            records = json.loads(result_df.to_json(orient="records", date_format="iso"))
            return json.dumps({"records": records, "chart_error": str(chart_error)})
    else:
        # Local rendering mode (CLI)
        try:
            actual_result.chart(spec=spec, backend=backend, format=format_type)
        except Exception as chart_error:
            error_msg = f"⚠️  Chart generation failed: {chart_error}"
            if error_callback:
                error_callback(error_msg)
            else:
                print(f"\n{error_msg}")

        return f"Query executed successfully. Returned {len(result_df)} rows."
