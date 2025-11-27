"""Utilities for handling chart generation in agent backends."""

import json
from collections.abc import Callable
from typing import Any


def generate_chart_with_data(
    query_result: Any,
    chart_spec: dict[str, Any] | None,
    default_backend: str = "altair",
    return_json: bool = True,
    error_callback: Callable[[str], None] | None = None,
) -> str:
    """Generate chart from query result. Returns JSON if return_json=True, else renders locally."""
    try:
        result_df = query_result.execute()
    except Exception as e:
        error_msg = f"❌ Query Execution Error: {e}"
        if not return_json:
            error_callback(error_msg) if error_callback else print(f"\n{error_msg}\n")
        return json.dumps({"error": str(e)}) if return_json else error_msg

    success_msg = f"Query executed successfully. Returned {len(result_df)} rows."
    records = (
        json.loads(result_df.to_json(orient="records", date_format="iso")) if return_json else None
    )

    # No chart requested
    if chart_spec is None or not chart_spec.get("show_chart", True):
        if return_json:
            return json.dumps({"records": records})
        if chart_spec and chart_spec.get("show_table", True):
            from boring_semantic_layer.chart.plotext_chart import display_table

            display_table(result_df, limit=len(result_df))
        return success_msg

    # Extract chart parameters
    backend = chart_spec.get("backend", default_backend)
    spec = chart_spec.get("spec")
    format_type = chart_spec.get(
        "format", "json" if return_json else ("static" if backend == "plotext" else "json")
    )

    if not return_json:
        try:
            query_result.chart(spec=spec, backend=backend, format=format_type)
        except Exception as e:
            msg = f"⚠️  Chart generation failed: {e}"
            error_callback(msg) if error_callback else print(f"\n{msg}")
        return success_msg

    # JSON mode
    try:
        chart_result = query_result.chart(spec=spec, backend=backend, format=format_type)
        if format_type == "json":
            chart_data = (
                chart_result
                if backend == "altair"
                else (json.loads(chart_result) if isinstance(chart_result, str) else chart_result)
            )
            return json.dumps({"records": records, "chart": chart_data})
        return json.dumps(
            {
                "records": records,
                "chart": {
                    "backend": backend,
                    "format": format_type,
                    "message": "Use format='json' for serializable output.",
                },
            }
        )
    except Exception as e:
        return json.dumps({"records": records, "chart_error": str(e)})
