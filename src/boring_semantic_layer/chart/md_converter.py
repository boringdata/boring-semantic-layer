"""Convert BSL query results to various output formats."""

import contextlib
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal and datetime objects."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime | date | pd.Timestamp):
            return str(obj)
        return super().default(obj)


class ResultConverter:
    """Convert BSL query results to structured data formats."""

    @staticmethod
    def convert_bsl_result(
        result: Any, code: str, context: dict[str, Any], is_chart_only: bool = False
    ) -> dict[str, Any]:
        """Convert executed BSL query to structured result dictionary."""
        df = result.execute()

        result_data = {
            "code": code,
            "sql": ResultConverter._extract_sql(result),
            "plan": ResultConverter._extract_query_plan(result),
            "table": ResultConverter._convert_dataframe(df),
        }

        chart_data = ResultConverter._extract_chart(result, code, context, is_chart_only)
        if chart_data:
            result_data["chart"] = chart_data

        return result_data, context

    @staticmethod
    def _extract_sql(result: Any) -> str | None:
        """Extract SQL query from BSL result."""
        try:
            if hasattr(result, "sql"):
                return result.sql()
        except Exception as e:
            return f"Error generating SQL: {str(e)}"
        return None

    @staticmethod
    def _extract_query_plan(result: Any) -> str | None:
        """Extract query execution plan from BSL result."""
        try:
            return str(result.expr) if hasattr(result, "expr") else str(result)
        except Exception:
            return None

    @staticmethod
    def _convert_dataframe(df: pd.DataFrame) -> dict[str, Any]:
        """Convert DataFrame to JSON-serializable dict format."""
        df_copy = df.copy()

        # Convert datetime and Decimal columns
        for col in df_copy.columns:
            if df_copy[col].dtype.name.startswith("datetime"):
                df_copy[col] = df_copy[col].astype(str)
            elif df_copy[col].dtype == "object" and len(df_copy) > 0:
                first_val = df_copy[col].iloc[0]
                if isinstance(first_val, pd.Timestamp | datetime | date):
                    df_copy[col] = df_copy[col].astype(str)
                elif isinstance(first_val, Decimal):
                    df_copy[col] = df_copy[col].apply(
                        lambda x: float(x) if isinstance(x, Decimal) else x
                    )

        # Replace NaN with None
        df_copy = df_copy.replace({float("nan"): None})

        return {"columns": list(df_copy.columns), "data": df_copy.values.tolist()}

    @staticmethod
    def _extract_chart(
        result: Any, code: str, context: dict[str, Any], is_chart_only: bool
    ) -> dict[str, Any] | None:
        """Extract chart specification from BSL query result."""
        if not hasattr(result, "chart"):
            return None

        use_plotly = ResultConverter._should_use_plotly(code)
        chart_spec_param = ResultConverter._find_chart_spec_param(code, context)

        if use_plotly:
            chart_data = ResultConverter._try_plotly_chart(result, chart_spec_param, is_chart_only)
            if chart_data:
                return chart_data

        chart_data = ResultConverter._try_altair_chart(result, chart_spec_param, is_chart_only)
        if chart_data:
            return chart_data

        # Fallback to Plotly if Altair failed
        if not use_plotly:
            return ResultConverter._try_plotly_chart(result, chart_spec_param, is_chart_only)

        return None

    @staticmethod
    def _should_use_plotly(code: str) -> bool:
        """Check if Plotly backend is requested in code."""
        return any(
            marker in code for marker in ["# USE_PLOTLY", 'backend="plotly"', "backend='plotly'"]
        )

    @staticmethod
    def _find_chart_spec_param(code: str, context: dict[str, Any]) -> Any:
        """Find chart_spec parameter from code or context."""
        if "chart_spec" in context:
            return context["chart_spec"]

        match = re.search(r"\.chart\([^)]*spec=([^,)]+)", code)
        if match:
            spec_expr = match.group(1).strip()
            with contextlib.suppress(Exception):
                return eval(spec_expr, context)

        return None

    @staticmethod
    def _try_plotly_chart(
        result: Any, chart_spec_param: Any, is_chart_only: bool
    ) -> dict[str, Any] | None:
        """Try to generate Plotly chart."""
        try:
            import plotly.graph_objects as go

            chart_obj = (
                result.chart(spec=chart_spec_param, backend="plotly")
                if chart_spec_param
                else result.chart(backend="plotly")
            )

            if isinstance(chart_obj, go.Figure):
                plotly_json = chart_obj.to_json(engine="json")
                if is_chart_only:
                    return {"chart_spec": plotly_json, "chart_type": "plotly"}
                return {"type": "plotly", "spec": plotly_json}
        except Exception:
            pass

        return None

    @staticmethod
    def _try_altair_chart(
        result: Any, chart_spec_param: Any, is_chart_only: bool
    ) -> dict[str, Any] | None:
        """Try to generate Altair/Vega-Lite chart."""
        try:
            chart_obj = (
                result.chart(spec=chart_spec_param, backend="altair")
                if chart_spec_param
                else result.chart(backend="altair")
            )

            if hasattr(chart_obj, "properties"):
                chart_obj = chart_obj.properties(width=700, height=400)

            vega_spec = None
            if hasattr(chart_obj, "to_dict"):
                vega_spec = chart_obj.to_dict()
            elif hasattr(chart_obj, "spec"):
                vega_spec = chart_obj.spec
            elif isinstance(chart_obj, dict):
                vega_spec = chart_obj

            if vega_spec:
                if is_chart_only:
                    return {"chart_spec": vega_spec}
                return {"type": "vega", "spec": vega_spec}
        except Exception:
            pass

        return None
