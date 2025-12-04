"""Utilities for handling chart generation in agent backends."""

import json
import tempfile
import webbrowser
from collections.abc import Callable
from pathlib import Path
from typing import Any


def _open_chart_in_browser(chart_obj: Any, backend: str) -> bool:
    """Open an altair or plotly chart in the default browser.

    Args:
        chart_obj: The chart object (Altair Chart or Plotly Figure)
        backend: Either "altair" or "plotly"

    Returns:
        True if successfully opened, False otherwise
    """
    try:
        if backend == "altair":
            # Altair 5.3+ has chart.show() but we use HTML for compatibility
            html_content = chart_obj.to_html()
            with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False) as f:
                f.write(html_content)
                temp_path = f.name
            webbrowser.open(f"file://{temp_path}")
            return True
        elif backend == "plotly":
            # Plotly can write HTML and auto-open
            temp_path = Path(tempfile.gettempdir()) / "bsl_chart.html"
            chart_obj.write_html(str(temp_path), auto_open=True)
            return True
    except Exception:
        return False
    return False


def generate_chart_with_data(
    query_result: Any,
    get_records: bool | dict[str, Any] | None = True,
    records_limit: int | None = None,
    get_chart: bool = True,
    chart_backend: str | None = None,
    chart_format: str | None = None,
    chart_spec: dict[str, Any] | None = None,
    default_backend: str = "altair",
    return_json: bool = True,
    error_callback: Callable[[str], None] | None = None,
) -> str:
    """Generate chart from query result with explicit control over records and chart output.

    Args:
        query_result: BSL query result object with execute() and chart() methods
        get_records: Return data records to LLM (default: True). For backward compatibility,
                     can also be a dict (old chart_spec style) which will be handled.
        records_limit: Max records returned to LLM (default: None = all)
        get_chart: Generate chart visualization (default: True)
        chart_backend: Override backend ("plotext", "altair", "plotly") or None for default
        chart_format: Override format ("json", "static", "string") or None for auto
        chart_spec: Backend-specific customization (chart_type, theme, etc.)
        default_backend: Fallback backend when chart_backend is None
        return_json: True for API/MCP mode, False for CLI mode
        error_callback: Optional callback for error messages

    Returns:
        JSON string with query results and optional chart data
    """
    try:
        result_df = query_result.execute()
    except Exception as e:
        error_msg = f"❌ Query Execution Error: {e}"
        if not return_json:
            error_callback(error_msg) if error_callback else print(f"\n{error_msg}\n")
        return json.dumps({"error": str(e)}) if return_json else error_msg

    total_rows = len(result_df)
    columns = list(result_df.columns)

    # Detect if chart would be meaningful (need at least 2 rows for visualization)
    chart_is_meaningful = total_rows >= 2

    # Resolve chart parameters
    backend = chart_backend or default_backend
    # In CLI mode with altair/plotly, we'll open in browser instead of terminal display
    open_in_browser = not return_json and backend in ("altair", "plotly")

    spec = chart_spec.get("spec") if chart_spec else None
    format_type = chart_format or (
        "json" if return_json else ("static" if backend == "plotext" else "json")
    )

    # Check for non-serializable format in API mode
    non_serializable_format = return_json and format_type == "static" and backend != "plotext"

    # Determine if chart should be shown
    show_chart = get_chart
    # Override: don't show chart if it wouldn't be meaningful (single row result)
    if show_chart and not chart_is_meaningful:
        show_chart = False

    # Generate records only if needed
    returned_rows = None
    if return_json and get_records:
        all_records = json.loads(result_df.to_json(orient="records", date_format="iso"))
        records = all_records[:records_limit] if records_limit else all_records
        returned_rows = len(records)
    else:
        records = None

    if not return_json:
        # CLI mode (plotext): auto-show table when get_records=True
        # This way the user sees what the LLM sees
        if get_records:
            from boring_semantic_layer.chart.plotext_chart import display_table

            all_records = json.loads(result_df.to_json(orient="records", date_format="iso"))
            limited_records = all_records[:records_limit] if records_limit else all_records
            cli_returned_rows = len(limited_records)

            # Show table with the same records that go to LLM
            display_table(result_df, limit=cli_returned_rows)

            # Render chart if requested (and meaningful)
            if show_chart:
                try:
                    if open_in_browser:
                        # Get chart object and open in browser for altair/plotly
                        chart_obj = query_result.chart(spec=spec, backend=backend, format="static")
                        if _open_chart_in_browser(chart_obj, backend):
                            print(f"\n📊 Chart opened in browser ({backend})")
                        else:
                            msg = f"⚠️  Could not open {backend} chart in browser"
                            error_callback(msg) if error_callback else print(f"\n{msg}")
                    else:
                        # Plotext renders directly in terminal
                        query_result.chart(spec=spec, backend=backend, format=format_type)
                except Exception as e:
                    msg = f"⚠️  Chart generation failed: {e}"
                    error_callback(msg) if error_callback else print(f"\n{msg}")

            # Return records to LLM
            cli_response: dict[str, Any] = {
                "total_rows": total_rows,
                "columns": columns,
                "records": limited_records,
            }
            if cli_returned_rows < total_rows:
                cli_response["returned_rows"] = cli_returned_rows
                cli_response["note"] = (
                    f"Showing {cli_returned_rows} of {total_rows} rows. Use records_limit to see more."
                )
            # Add chart info
            if show_chart:
                cli_response["chart"] = {
                    "backend": backend,
                    "format": format_type,
                    "displayed": True,
                }
            return json.dumps(cli_response)
        else:
            # get_records=False: final display-only query, just show chart
            if show_chart:
                try:
                    if open_in_browser:
                        # Get chart object and open in browser for altair/plotly
                        chart_obj = query_result.chart(spec=spec, backend=backend, format="static")
                        if _open_chart_in_browser(chart_obj, backend):
                            print(f"\n📊 Chart opened in browser ({backend})")
                        else:
                            msg = f"⚠️  Could not open {backend} chart in browser"
                            error_callback(msg) if error_callback else print(f"\n{msg}")
                    else:
                        # Plotext renders directly in terminal
                        query_result.chart(spec=spec, backend=backend, format=format_type)
                except Exception as e:
                    msg = f"⚠️  Chart generation failed: {e}"
                    error_callback(msg) if error_callback else print(f"\n{msg}")

            response: dict[str, Any] = {
                "total_rows": total_rows,
                "columns": columns,
                "note": "Records not returned (get_records=false). Data displayed to user.",
            }
            if show_chart:
                response["chart"] = {
                    "backend": backend,
                    "format": format_type,
                    "displayed": True,
                }
            return json.dumps(response)

    # Build response dict with insight for LLM
    def build_response(**kwargs: Any) -> str:
        resp: dict[str, Any] = {
            "total_rows": total_rows,
            "columns": columns,
        }
        # Add insight about records
        if get_records and returned_rows is not None:
            if returned_rows < total_rows:
                resp["returned_rows"] = returned_rows
                resp["note"] = (
                    f"Showing {returned_rows} of {total_rows} rows. Use records_limit to see more."
                )
        elif not get_records:
            resp["note"] = "Records not returned (get_records=false). Data displayed to user."
        # Add any additional kwargs
        for k, v in kwargs.items():
            if v is not None:
                resp[k] = v
        return json.dumps(resp)

    # JSON mode - no chart requested
    if not show_chart:
        return build_response(records=records)

    # JSON mode with non-serializable format - return message instead of error
    if non_serializable_format:
        return build_response(
            records=records,
            chart={
                "backend": backend,
                "format": format_type,
                "message": "Use format='json' for serializable output.",
            },
        )

    # JSON mode with chart
    try:
        chart_result = query_result.chart(spec=spec, backend=backend, format=format_type)
        if format_type == "json":
            chart_data = (
                chart_result
                if backend == "altair"
                else (json.loads(chart_result) if isinstance(chart_result, str) else chart_result)
            )
            return build_response(
                records=records,
                chart={
                    "backend": backend,
                    "format": format_type,
                    "data": chart_data,
                },
            )
        # Non-json format (e.g., string for plotext)
        return build_response(
            records=records,
            chart={
                "backend": backend,
                "format": format_type,
                "data": chart_result if format_type == "string" else None,
                "displayed": format_type == "static",
            },
        )
    except Exception as e:
        return build_response(records=records, chart_error=str(e))
