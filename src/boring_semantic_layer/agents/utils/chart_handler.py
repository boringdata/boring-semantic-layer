"""Utilities for handling chart generation in agent backends."""

import json
from typing import Any


def generate_chart_with_data(
    query_result: Any,
    chart_spec: dict[str, Any] | None,
    default_backend: str = "altair",
    return_json: bool = True,
) -> str:
    """
    Generate chart and return data in appropriate format.

    This is the common chart generation logic for both MCP and LangChain backends.

    Args:
        query_result: The query result object with chart() method
        chart_spec: Chart specification dict or None
        default_backend: Default backend to use if not specified in chart_spec
        return_json: If True, always return JSON. If False, render locally (for CLI).

    Returns:
        - JSON string with records and chart data if return_json=True
        - Success message if return_json=False (after rendering)
    """
    result_df = query_result.execute()

    # Early return for no-chart case
    if chart_spec is None:
        if return_json:
            records = json.loads(result_df.to_json(orient="records", date_format="iso"))
            return json.dumps({"records": records})
        return f"Query executed successfully. Returned {len(result_df)} rows."

    # Extract chart parameters once
    backend = chart_spec.get("backend", default_backend)
    spec = chart_spec.get("spec")
    format_type = chart_spec.get(
        "format", "json" if return_json else ("static" if backend == "plotext" else "json")
    )

    # Generate chart
    if return_json:
        try:
            chart_result = query_result.chart(spec=spec, backend=backend, format=format_type)
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
            query_result.chart(spec=spec, backend=backend, format=format_type)
        except Exception as chart_error:
            print(f"\n⚠️  Chart generation failed: {chart_error}")

        return f"Query executed successfully. Returned {len(result_df)} rows."
