"""Core markdown rendering utilities for BSL queries.

This module provides shared functionality for parsing and executing BSL queries
embedded in markdown documents. It is used by both the standalone md_renderer
and the documentation builder.
"""

import contextlib
import io
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import ibis
import pandas as pd

from boring_semantic_layer import to_semantic_table


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal and datetime objects."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime | date | pd.Timestamp):
            return str(obj)
        return super().default(obj)


class QueryParser:
    """Parse markdown content and extract BSL query blocks."""

    # Standard language names to skip
    STANDARD_LANGUAGES = {
        "python",
        "sql",
        "bash",
        "javascript",
        "typescript",
        "js",
        "ts",
        "yaml",
        "yml",
        "json",
        "toml",
        "text",
        "sh",
    }

    @classmethod
    def parse_markdown_with_queries(
        cls, content: str, include_hidden: bool = False
    ) -> tuple[str, dict[str, str], dict[str, str]]:
        """
        Parse markdown content and extract BSL query blocks.

        Args:
            content: Markdown content to parse
            include_hidden: If True, also extract queries from HTML comments

        Returns:
            - Modified markdown (hidden blocks removed if include_hidden=True)
            - Dictionary of query_name -> code
            - Dictionary of query_name -> component_type (empty if not tracking)
        """
        queries = {}
        component_types = {}
        modified_md = content

        # Handle hidden code blocks in HTML comments
        if include_hidden:
            hidden_pattern = r"<!--\s*\n```(\w+)\n(.*?)\n```\s*\n-->"

            def extract_hidden_query(match):
                query_name = match.group(1)
                query_code = match.group(2).strip()
                if query_name.lower() not in cls.STANDARD_LANGUAGES:
                    queries[query_name] = query_code
                return ""

            modified_md = re.sub(hidden_pattern, extract_hidden_query, modified_md, flags=re.DOTALL)

        # Handle visible code blocks
        pattern = r"```(\w+)\n(.*?)\n```"

        def extract_query(match):
            query_name = match.group(1)
            query_code = match.group(2).strip()

            # Skip if it's a standard language
            if query_name.lower() in cls.STANDARD_LANGUAGES:
                return match.group(0)

            # Store the query
            queries[query_name] = query_code

            # Keep the code block in markdown
            return match.group(0)

        modified_md = re.sub(pattern, extract_query, modified_md, flags=re.DOTALL)

        return modified_md, queries, component_types

    @classmethod
    def find_component_types(cls, content: str) -> dict[str, str]:
        """
        Find component type annotations in markdown.

        Args:
            content: Markdown content to scan

        Returns:
            Dictionary of query_name -> component_type
        """
        component_types = {}
        component_patterns = {
            "altairchart": r'<altairchart[^>]+code-block="(\w+)"',
            "bslquery": r'<bslquery[^>]+code-block="(\w+)"',
            "regularoutput": r'<regularoutput[^>]+code-block="(\w+)"',
            "collapsedcodeblock": r'<collapsedcodeblock[^>]+code-block="(\w+)"',
        }

        for comp_type, pattern in component_patterns.items():
            for match in re.finditer(pattern, content):
                block_name = match.group(1)
                if block_name not in component_types:
                    component_types[block_name] = comp_type

        return component_types

    @classmethod
    def resolve_file_includes(cls, content: str, content_dir: Path) -> tuple[str, dict[str, str]]:
        """
        Resolve file includes in markdown content.

        Syntax: <yamlcontent path="filename.yaml"></yamlcontent>

        Args:
            content: Markdown content
            content_dir: Directory containing the markdown file

        Returns:
            - Modified markdown content
            - Dictionary of file_path -> file_content
        """
        files = {}
        pattern = r'<yamlcontent\s+path="([^"]+)"(?:\s*/)?></yamlcontent>'

        def extract_file(match):
            file_path = match.group(1).strip()
            full_path = content_dir / file_path
            if not full_path.exists():
                return f"<!-- Error: File not found: {file_path} -->"
            files[file_path] = full_path.read_text()
            return match.group(0)

        modified = re.sub(pattern, extract_file, content)
        return modified, files


class QueryExecutor:
    """Execute BSL queries and return results."""

    @staticmethod
    def execute_bsl_query(
        query_code: str,
        context: dict[str, Any],
        is_chart_only: bool = False,
        capture_output: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Execute BSL query code and return results.

        Args:
            query_code: The Python code to execute
            context: The execution context with previous variables
            is_chart_only: If True, only return chart spec (for altairchart components)
            capture_output: If True, capture print statements

        Returns:
            (result_data, updated_context)
        """
        try:
            # Setup output capture
            captured_output = io.StringIO()
            old_stdout = sys.stdout
            if capture_output:
                sys.stdout = captured_output

            # Create namespace with ibis and BSL imports
            namespace = {"ibis": ibis, "to_semantic_table": to_semantic_table, **context}

            # Execute code and capture last expression
            try:
                code_lines = query_code.strip().split("\n")
                non_empty_lines = [
                    line for line in code_lines if line.strip() and not line.strip().startswith("#")
                ]
                last_line = non_empty_lines[-1].strip() if non_empty_lines else ""
                last_expr_result = None
                has_comma_in_expr = False

                # Check if last line is a simple expression (not a statement)
                is_simple_expression = (
                    last_line
                    and not any(
                        last_line.startswith(kw)
                        for kw in [
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
                    )
                    and "=" not in last_line.split(".")[0]
                    and not last_line.endswith((":",))
                )

                # Check for unclosed brackets/parens
                if is_simple_expression:
                    code_without_last = "\n".join(code_lines[:-1])
                    paren_count = code_without_last.count("(") - code_without_last.count(")")
                    bracket_count = code_without_last.count("[") - code_without_last.count("]")
                    brace_count = code_without_last.count("{") - code_without_last.count("}")
                    is_simple_expression = (
                        paren_count == 0 and bracket_count == 0 and brace_count == 0
                    )

                # Try to eval last line if it's a simple expression
                if is_simple_expression:
                    code_without_last = "\n".join(code_lines[:-1])
                    if code_without_last.strip():
                        exec(code_without_last, namespace)
                    try:
                        last_expr_result = eval(last_line, namespace)
                        has_comma_in_expr = "," in last_line
                    except Exception:
                        exec(last_line, namespace)
                        has_comma_in_expr = False
                else:
                    exec(query_code, namespace)
            finally:
                if capture_output:
                    sys.stdout = old_stdout

            output = captured_output.getvalue() if capture_output else ""

            # Update context with all new variables
            updated_context = {**context}
            for key, val in namespace.items():
                if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                    updated_context[key] = val

            # For chart-only mode (altairchart components)
            if (
                is_chart_only
                and last_expr_result is not None
                and hasattr(last_expr_result, "to_dict")
            ):
                try:
                    if hasattr(last_expr_result, "properties"):
                        last_expr_result = last_expr_result.properties(width=700, height=400)
                    vega_spec = last_expr_result.to_dict()
                    return {"chart_spec": vega_spec, "code": query_code}, updated_context
                except Exception as e:
                    print(f"    Warning: Could not extract chart spec: {e}")

            # Handle last expression result (for print/tuple output)
            if last_expr_result is not None and capture_output:
                if (
                    isinstance(last_expr_result, tuple)
                    and has_comma_in_expr
                    and len(last_expr_result) > 1
                ):
                    output = [str(item) for item in last_expr_result]
                else:
                    output += str(last_expr_result)

            # Check for print output
            has_output = (isinstance(output, list) and len(output) > 0) or (
                isinstance(output, str) and len(output.strip()) > 0
            )
            if has_output and capture_output:
                result = None
                for var_name in ["result", "q", "query"]:
                    if var_name in namespace:
                        result = namespace[var_name]
                        break

                if result is None:
                    output_data = output if isinstance(output, list) else output.strip()
                    return {"output": output_data}, updated_context

            # Get the result variable
            result = None
            for var_name in ["result", "q", "query"]:
                if var_name in namespace:
                    result = namespace[var_name]
                    break

            if result is None:
                # Look for new variables
                new_vars = {
                    k: v
                    for k, v in namespace.items()
                    if not k.startswith("_")
                    and k not in ["ibis", "to_semantic_table"]
                    and k not in context
                }
                if new_vars:
                    result = list(new_vars.values())[-1]

            if result is None and not output:
                return {"error": "No result found in query"}, context

            # Check if it's a semantic table definition (don't execute)
            if hasattr(result, "group_by") and not hasattr(result, "execute"):
                return {
                    "semantic_table": True,
                    "name": getattr(result, "name", "unknown"),
                    "info": "Semantic table definition stored in context",
                }, updated_context

            # Check if it's a BSL query object (has .execute() method)
            if hasattr(result, "execute"):
                return QueryExecutor._execute_bsl_result(
                    result, query_code, namespace, updated_context, is_chart_only
                )

            # Convert to dataframe if possible
            if hasattr(result, "to_pandas"):
                df = result.to_pandas()
                return {
                    "table": {"columns": list(df.columns), "data": df.values.tolist()}
                }, updated_context

            # String results
            if isinstance(result, str):
                return {"output": result}, updated_context

            return {"error": "Unknown result type"}, context

        except Exception as e:
            import traceback

            return {"error": str(e), "traceback": traceback.format_exc()}, context

    @staticmethod
    def _execute_bsl_result(
        result: Any,
        query_code: str,
        namespace: dict[str, Any],
        context: dict[str, Any],
        is_chart_only: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Execute a BSL query result and extract data, SQL, and charts."""
        # Execute query to get dataframe
        df = result.execute()

        # Get SQL query
        sql_query = None
        try:
            if hasattr(result, "sql"):
                sql_query = result.sql()
        except Exception as e:
            sql_query = f"Error generating SQL: {str(e)}"

        # Get query plan
        query_plan = None
        try:
            query_plan = str(result.expr) if hasattr(result, "expr") else str(result)
        except Exception as e:
            print(f"    Warning: Could not generate query plan: {str(e)}")

        # Get chart spec (Altair/Vega-Lite or Plotly)
        chart_data = None
        try:
            if hasattr(result, "chart"):
                chart_data = QueryExecutor._extract_chart(
                    result, query_code, namespace, is_chart_only, context
                )
        except Exception as e:
            print(f"    Warning: Could not generate chart: {str(e)}")

        # Convert DataFrame to dict format
        df_copy = df.copy()

        # Handle datetime and Decimal columns
        for col in df_copy.columns:
            if df_copy[col].dtype == "datetime64[ns]" or df_copy[col].dtype.name.startswith(
                "datetime"
            ):
                df_copy[col] = df_copy[col].astype(str)
            elif df_copy[col].dtype == "object":
                try:
                    if len(df_copy) > 0:
                        first_val = df_copy[col].iloc[0]
                        if isinstance(first_val, pd.Timestamp | datetime | date):
                            df_copy[col] = df_copy[col].astype(str)
                        elif isinstance(first_val, Decimal):
                            df_copy[col] = df_copy[col].apply(
                                lambda x: float(x) if isinstance(x, Decimal) else x
                            )
                except Exception:
                    pass

        # Replace NaN with None
        df_copy = df_copy.replace({float("nan"): None})

        result_data = {
            "code": query_code,
            "sql": sql_query,
            "plan": query_plan,
            "table": {"columns": list(df_copy.columns), "data": df_copy.values.tolist()},
        }

        if chart_data:
            result_data["chart"] = chart_data

        return result_data, context

    @staticmethod
    def _extract_chart(
        result: Any,
        query_code: str,
        namespace: dict[str, Any],
        is_chart_only: bool,
        context: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Extract chart specification from a BSL query result."""
        # Check if Plotly is requested
        use_plotly = (
            "# USE_PLOTLY" in query_code
            or 'backend="plotly"' in query_code
            or "backend='plotly'" in query_code
        )

        # Try to extract chart_spec parameter from code
        chart_spec_param = None
        if "chart_spec" in namespace:
            chart_spec_param = namespace["chart_spec"]
        else:
            spec_match = re.search(r"\.chart\([^)]*spec=([^,)]+)", query_code)
            if spec_match:
                spec_expr = spec_match.group(1).strip()
                with contextlib.suppress(Exception):
                    chart_spec_param = eval(spec_expr, namespace)

        # Try Plotly first if requested
        if use_plotly:
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
            except Exception as plotly_err:
                print(f"    Warning: Plotly chart failed: {plotly_err}")

        # Try Altair/Vega-Lite (default)
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
        except Exception as altair_err:
            # Fallback to Plotly if Altair fails
            if not use_plotly:
                try:
                    import plotly.graph_objects as go

                    chart_obj = (
                        result.chart(spec=chart_spec_param, backend="plotly")
                        if chart_spec_param
                        else result.chart(backend="plotly")
                    )
                    if isinstance(chart_obj, go.Figure):
                        plotly_json = chart_obj.to_json(engine="json")
                        return {"type": "plotly", "spec": plotly_json}
                except Exception as plotly_err:
                    print(
                        f"    Warning: Both chart backends failed. Altair: {altair_err}, Plotly: {plotly_err}"
                    )

        return None
