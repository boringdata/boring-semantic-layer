"""Utilities for handling chart generation in agent backends."""

import json
import tempfile
import webbrowser
from collections.abc import Callable
from pathlib import Path
from typing import Any


def _open_chart_in_browser(chart_obj: Any, backend: str) -> str | None:
    """Open an altair or plotly chart in the default browser."""
    try:
        if backend == "altair":
            html_content = chart_obj.to_html()
            with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False) as f:
                f.write(html_content)
                temp_path = f.name
            file_url = f"file://{temp_path}"
            webbrowser.open(file_url)
            return file_url
        elif backend == "plotly":
            temp_path = Path(tempfile.gettempdir()) / "bsl_chart.html"
            chart_obj.write_html(str(temp_path), auto_open=True)
            return f"file://{temp_path}"
    except Exception:
        return None
    return None


def _render_cli_chart(
    query_result: Any,
    spec: Any,
    backend: str,
    format_type: str,
    error_callback: Callable[[str], None] | None,
) -> None:
    """Render chart in CLI mode (terminal or browser)."""
    open_in_browser = backend in ("altair", "plotly")
    try:
        if open_in_browser:
            chart_obj = query_result.chart(spec=spec, backend=backend, format="static")
            chart_url = _open_chart_in_browser(chart_obj, backend)
            if chart_url:
                print(f"\n📊 Chart opened in browser ({backend}): {chart_url}")
            else:
                msg = f"⚠️  Could not open {backend} chart in browser"
                error_callback(msg) if error_callback else print(f"\n{msg}")
        else:
            query_result.chart(spec=spec, backend=backend, format=format_type)
    except Exception as e:
        msg = f"⚠️  Chart generation failed: {e}"
        error_callback(msg) if error_callback else print(f"\n{msg}")


def generate_chart_with_data(
    query_result: Any,
    get_records: bool = True,
    records_limit: int | None = None,
    records_displayed_limit: int | None = None,
    get_chart: bool = True,
    chart_backend: str | None = None,
    chart_format: str | None = None,
    chart_spec: dict[str, Any] | None = None,
    default_backend: str = "altair",
    return_json: bool = True,
    error_callback: Callable[[str], None] | None = None,
) -> str:
    """Generate chart from query result with control over records and chart output."""
    try:
        result_df = query_result.execute()
    except Exception as e:
        error_msg = f"❌ Query Execution Error: {e}"
        if not return_json:
            error_callback(error_msg) if error_callback else print(f"\n{error_msg}\n")
        return json.dumps({"error": str(e)}) if return_json else error_msg

    total_rows = len(result_df)
    columns = list(result_df.columns)
    backend = chart_backend or default_backend
    spec = chart_spec.get("spec") if chart_spec else None
    format_type = chart_format or (
        "json" if return_json else ("static" if backend == "plotext" else "json")
    )
    show_chart = get_chart and total_rows >= 2

    # CLI mode
    if not return_json:
        all_records = json.loads(result_df.to_json(orient="records", date_format="iso"))

        if get_records:
            from boring_semantic_layer.chart.plotext_chart import display_table

            llm_records = all_records[:records_limit] if records_limit else all_records
            display_limit = records_displayed_limit if records_displayed_limit is not None else 10
            display_table(result_df, limit=display_limit)

            if show_chart:
                _render_cli_chart(query_result, spec, backend, format_type, error_callback)

            response: dict[str, Any] = {
                "total_rows": total_rows,
                "columns": columns,
                "records": llm_records,
            }
            if len(llm_records) < total_rows:
                response["returned_rows"] = len(llm_records)
            if show_chart:
                response["chart"] = {"backend": backend, "displayed": True}
            return json.dumps(response)
        else:
            if show_chart:
                _render_cli_chart(query_result, spec, backend, format_type, error_callback)
            response = {
                "total_rows": total_rows,
                "columns": columns,
                "note": "Records not returned.",
            }
            if show_chart:
                response["chart"] = {"backend": backend, "displayed": True}
            return json.dumps(response)

    # JSON/API mode
    records = None
    if get_records:
        all_records = json.loads(result_df.to_json(orient="records", date_format="iso"))
        records = all_records[:records_limit] if records_limit else all_records

    def build_response(**kwargs: Any) -> str:
        resp: dict[str, Any] = {"total_rows": total_rows, "columns": columns}
        if get_records and records is not None and len(records) < total_rows:
            resp["returned_rows"] = len(records)
        for k, v in kwargs.items():
            if v is not None:
                resp[k] = v
        return json.dumps(resp)

    if not show_chart:
        return build_response(records=records)

    # Non-serializable format check
    if return_json and format_type == "static" and backend != "plotext":
        return build_response(
            records=records,
            chart={
                "backend": backend,
                "format": format_type,
                "message": "Use format='json' for serializable output.",
            },
        )

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
                chart={"backend": backend, "format": format_type, "data": chart_data},
            )
        return build_response(
            records=records,
            chart={
                "backend": backend,
                "format": format_type,
                "data": chart_result if format_type == "string" else None,
            },
        )
    except Exception as e:
        return build_response(records=records, chart_error=str(e))
