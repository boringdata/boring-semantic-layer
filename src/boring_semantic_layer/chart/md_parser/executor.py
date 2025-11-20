"""BSL query execution engine with context management."""

import io
import sys
from typing import Any

import ibis

from boring_semantic_layer import to_semantic_table


class QueryExecutor:
    """Execute BSL queries and manage execution context."""

    RESULT_VAR_NAMES = ("result", "q", "query")

    def __init__(self, capture_output: bool = True):
        """Initialize executor with output capture settings."""
        self.capture_output = capture_output
        self.context: dict[str, Any] = {}

    def execute(self, code: str, is_chart_only: bool = False) -> dict[str, Any]:
        """Execute BSL query code and return structured results."""
        try:
            output = self._execute_code(code)
            result = self._find_result()

            if result is None:
                if output:
                    return {"output": self._format_output(output)}
                return {"error": "No result found in query"}

            return self._process_result(result, code, output, is_chart_only)

        except Exception as e:
            import traceback

            return {"error": str(e), "traceback": traceback.format_exc()}

    def _execute_code(self, code: str) -> Any:
        """Execute code and capture output, returns last expression or output."""
        captured = io.StringIO()
        old_stdout = sys.stdout
        if self.capture_output:
            sys.stdout = captured

        try:
            namespace = {"ibis": ibis, "to_semantic_table": to_semantic_table, **self.context}
            last_expr = self._eval_last_expression(code, namespace)

            # Update context with new variables
            for key, val in namespace.items():
                if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                    self.context[key] = val

            output = captured.getvalue() if self.capture_output else ""
            return self._combine_output(output, last_expr)

        finally:
            if self.capture_output:
                sys.stdout = old_stdout

    def _eval_last_expression(self, code: str, namespace: dict) -> Any:
        """Try to evaluate last line as expression, fallback to exec."""
        lines = code.strip().split("\n")
        non_empty = [line for line in lines if line.strip() and not line.strip().startswith("#")]

        if not non_empty:
            exec(code, namespace)
            return None

        last_line = non_empty[-1].strip()

        if not self._is_simple_expression(last_line):
            exec(code, namespace)
            return None

        # Check for unclosed brackets
        code_without_last = "\n".join(lines[:-1])
        if self._has_unclosed_brackets(code_without_last):
            exec(code, namespace)
            return None

        # Try eval, fallback to exec
        if code_without_last.strip():
            exec(code_without_last, namespace)

        try:
            result = eval(last_line, namespace)
            return (result, "," in last_line)
        except Exception:
            exec(last_line, namespace)
            return None

    def _is_simple_expression(self, line: str) -> bool:
        """Check if line is a simple expression (not a statement)."""
        keywords = [
            "print",
            "if",
            "for",
            "while",
            "def",
            "class",
            "import",
            "from",
            "with",
            "try",
            "except",
            "finally",
            "raise",
            "return",
            "yield",
            "pass",
            "break",
            "continue",
        ]
        return (
            line
            and not any(line.startswith(kw) for kw in keywords)
            and "=" not in line.split(".")[0]
            and not line.endswith(":")
        )

    def _has_unclosed_brackets(self, code: str) -> bool:
        """Check if code has unclosed parentheses, brackets, or braces."""
        return any(
            [
                code.count("(") != code.count(")"),
                code.count("[") != code.count("]"),
                code.count("{") != code.count("}"),
            ]
        )

    def _combine_output(self, output: str, last_expr: Any) -> Any:
        """Combine print output with last expression result."""
        if last_expr is None:
            return output

        result, has_comma = last_expr
        if isinstance(result, tuple) and has_comma and len(result) > 1:
            return [str(item) for item in result]

        return output + str(result) if output else result

    def _find_result(self) -> Any:
        """Find result in context by checking known variable names."""
        for var_name in self.RESULT_VAR_NAMES:
            if var_name in self.context:
                return self.context[var_name]

        # Look for new variables
        new_vars = [
            v
            for k, v in self.context.items()
            if not k.startswith("_") and k not in ["ibis", "to_semantic_table"]
        ]
        return new_vars[-1] if new_vars else None

    def _format_output(self, output: Any) -> str | list[str]:
        """Format output for display."""
        if isinstance(output, list):
            return output
        if isinstance(output, str):
            return output.strip()
        return str(output)

    def _process_result(
        self, result: Any, code: str, output: Any, is_chart_only: bool
    ) -> dict[str, Any]:
        """Process execution result based on type."""
        # Chart-only mode (for altairchart components)
        if is_chart_only and hasattr(result, "to_dict"):
            return self._extract_chart_spec(result)

        # Semantic table definition
        if hasattr(result, "group_by") and not hasattr(result, "execute"):
            return {
                "semantic_table": True,
                "name": getattr(result, "name", "unknown"),
                "info": "Semantic table definition stored in context",
            }

        # BSL query with execute method
        if hasattr(result, "execute"):
            from .converter import ResultConverter

            result_data, _ = ResultConverter.convert_bsl_result(
                result, code, self.context, is_chart_only
            )
            return result_data

        # Pandas-like object
        if hasattr(result, "to_pandas"):
            df = result.to_pandas()
            return {"table": {"columns": list(df.columns), "data": df.values.tolist()}}

        # String result
        if isinstance(result, str):
            return {"output": result}

        # Print output with no clear result
        if output:
            return {"output": self._format_output(output)}

        return {"error": "Unknown result type"}

    def _extract_chart_spec(self, chart_obj: Any) -> dict[str, Any]:
        """Extract chart specification from Altair chart object."""
        try:
            if hasattr(chart_obj, "properties"):
                chart_obj = chart_obj.properties(width=700, height=400)
            return {"chart_spec": chart_obj.to_dict(), "code": ""}
        except Exception as e:
            return {"error": f"Could not extract chart spec: {e}"}
