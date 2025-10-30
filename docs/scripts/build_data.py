#!/usr/bin/env python3
"""
Build script to parse markdown files, execute BSL queries, and generate JSON data.
Evidence-style: finds code blocks with names, executes them, and makes results available.
"""

import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

# Add parent directory to path to import boring_semantic_layer
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import contextlib

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


def resolve_file_includes(content: str, content_dir: Path) -> tuple[str, dict[str, str]]:
    """
    Resolve file includes in markdown content.

    Syntax: <yamlcontent path="filename.yaml"></yamlcontent>

    This will be kept in the markdown and the file content will be stored
    separately in the "files" dict for the React component to access.

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

        # Read and store file content
        file_content = full_path.read_text()
        files[file_path] = file_content

        # Keep the tag in markdown
        return match.group(0)

    modified = re.sub(pattern, extract_file, content)
    return modified, files


def parse_markdown_with_queries(content: str) -> tuple[str, dict[str, str], dict[str, str]]:
    """
    Parse markdown content and extract BSL query blocks.

    Syntax: ```query_name
            <BSL query code>
            ```

    Or hidden: <!--
               ```query_name
               <BSL query code>
               ```
               -->

    Returns:
        - Modified markdown (with hidden blocks removed)
        - Dictionary of query_name -> code
        - Dictionary of query_name -> component_type (e.g., 'altairchart', 'bslquery')
    """
    queries = {}
    component_types = {}

    # First, handle hidden code blocks in HTML comments
    hidden_pattern = r"<!--\s*\n```(\w+)\n(.*?)\n```\s*\n-->"

    def extract_hidden_query(match):
        query_name = match.group(1)
        query_code = match.group(2).strip()

        # Skip if it's a language like python, sql, bash, yaml, etc.
        if query_name.lower() not in [
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
        ]:
            queries[query_name] = query_code

        # Remove the comment block from markdown
        return ""

    modified_md = re.sub(hidden_pattern, extract_hidden_query, content, flags=re.DOTALL)

    # Then handle visible code blocks
    pattern = r"```(\w+)\n(.*?)\n```"

    def replace_query(match):
        query_name = match.group(1)
        query_code = match.group(2).strip()

        # Skip if it's a language like python, sql, bash, yaml, etc.
        if query_name.lower() in [
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
        ]:
            return match.group(0)  # Return original

        # This is a BSL query - store it but don't replace
        queries[query_name] = query_code

        # Keep the code block in markdown (don't replace)
        return match.group(0)

    modified_md = re.sub(pattern, replace_query, modified_md, flags=re.DOTALL)

    # Find component types by looking for component tags
    component_patterns = {
        "altairchart": r'<altairchart[^>]+code-block="(\w+)"',
        "bslquery": r'<bslquery[^>]+code-block="(\w+)"',
        "regularoutput": r'<regularoutput[^>]+code-block="(\w+)"',
        "collapsedcodeblock": r'<collapsedcodeblock[^>]+code-block="(\w+)"',
    }

    for comp_type, pattern in component_patterns.items():
        for match in re.finditer(pattern, modified_md):
            block_name = match.group(1)
            if block_name not in component_types:
                component_types[block_name] = comp_type

    return modified_md, queries, component_types


def execute_bsl_query(
    query_code: str, context: dict[str, Any], is_chart_only: bool = False
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Execute BSL query code and return results in a structured format.
    Returns: (result_data, updated_context)

    Args:
        query_code: The Python code to execute
        context: The execution context with previous variables
        is_chart_only: If True, only return chart spec (for altair_chart blocks)
    """
    try:
        # Capture print output
        import io
        import sys

        captured_output = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured_output

        # Create a namespace with ibis and BSL imports
        namespace = {
            "ibis": ibis,
            "to_semantic_table": to_semantic_table,
            **context,  # Include any existing semantic tables
        }

        # Execute the code and capture last expression
        try:
            # Split code into lines to check if last line is an expression
            code_lines = query_code.strip().split("\n")
            non_empty_lines = [
                line for line in code_lines if line.strip() and not line.strip().startswith("#")
            ]
            last_line = non_empty_lines[-1].strip() if non_empty_lines else ""
            last_expr_result = None
            has_comma_in_expr = False

            # Check if the last line is a simple expression (not an assignment or statement)
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
                and "="
                not in last_line.split(".")[0]  # Check assignment only in first part before dots
                and not last_line.endswith((":",))  # Don't treat block starters as expressions
            )

            # Check if parentheses are balanced before the last line
            if is_simple_expression:
                code_without_last = "\n".join(code_lines[:-1])
                paren_count = code_without_last.count("(") - code_without_last.count(")")
                bracket_count = code_without_last.count("[") - code_without_last.count("]")
                brace_count = code_without_last.count("{") - code_without_last.count("}")

                # Only treat as expression if parentheses are balanced
                is_simple_expression = paren_count == 0 and bracket_count == 0 and brace_count == 0

            if is_simple_expression:
                # Last line looks like a standalone expression, evaluate it separately
                code_without_last = "\n".join(code_lines[:-1])

                # Execute all code except last line
                if code_without_last.strip():
                    exec(code_without_last, namespace)

                # Evaluate last line as expression
                try:
                    last_expr_result = eval(last_line, namespace)
                    # Mark if this was a comma-separated expression (tuple literal)
                    has_comma_in_expr = "," in last_line
                except Exception:
                    # If eval fails, just execute it normally
                    exec(last_line, namespace)
                    has_comma_in_expr = False
            else:
                # Execute all code normally
                exec(query_code, namespace)
        finally:
            # Restore stdout
            sys.stdout = old_stdout

        # Get captured output
        output = captured_output.getvalue()

        # For chart-only mode, check if last_expr_result is a chart object
        if is_chart_only and last_expr_result is not None and hasattr(last_expr_result, "to_dict"):
            # Check if it's an Altair Chart object
            try:
                if hasattr(last_expr_result, "properties"):
                    last_expr_result = last_expr_result.properties(width=700, height=400)
                vega_spec = last_expr_result.to_dict()
                # Update context with variables
                updated_context = {**context}
                for key, val in namespace.items():
                    if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                        updated_context[key] = val
                # Also store the code so it can be displayed
                return {"chart_spec": vega_spec, "code": query_code}, updated_context
            except Exception as e:
                print(f"    Warning: Could not extract chart spec from last expression: {e}")
                import traceback

                traceback.print_exc()

        # If we captured a last expression result, convert it to string output
        if last_expr_result is not None:
            # Only split if it's a tuple AND the last line had a comma (multiple expressions)
            # This way, object properties that return tuples won't be split
            # Example: "var_a, var_b" → split into rows
            # Example: "obj.dimensions" → single output (even if it returns a tuple)
            if (
                isinstance(last_expr_result, tuple)
                and has_comma_in_expr
                and len(last_expr_result) > 1
            ):
                # Store as array of outputs for display in separate rows
                output = [str(item) for item in last_expr_result]
            else:
                output += str(last_expr_result)

        # If there's print output and no result variable, return the output
        has_output = (isinstance(output, list) and len(output) > 0) or (
            isinstance(output, str) and len(output.strip()) > 0
        )
        if has_output:
            # Check if there's also a result to execute
            result = None
            for var_name in ["result", "q", "query"]:
                if var_name in namespace:
                    result = namespace[var_name]
                    break

            # If no result but we have output, return just the output
            if result is None:
                # Update context with all new variables
                updated_context = {**context}
                for key, val in namespace.items():
                    if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                        updated_context[key] = val

                # Return output as-is (could be string or list)
                output_data = output if isinstance(output, list) else output.strip()
                return {"output": output_data}, updated_context

        # Get the result (assume last expression or stored in 'result' variable)
        result = None
        for var_name in ["result", "q", "query"]:
            if var_name in namespace:
                result = namespace[var_name]
                break

        # If no explicit result, look for new variables
        if result is None:
            new_vars = {
                k: v
                for k, v in namespace.items()
                if not k.startswith("_")
                and k not in ["ibis", "to_semantic_table"]
                and k not in context
            }
            if new_vars:
                # Get the last defined variable
                result = list(new_vars.values())[-1]

        if result is None and not output:
            return {"error": "No result found in query"}, context

        # Update context with all new variables (for next queries)
        updated_context = {**context}
        for key, val in namespace.items():
            if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                updated_context[key] = val

        # Check if it's a BSL query object (has .execute() method)
        if hasattr(result, "execute"):
            # Execute query to get dataframe
            df = result.execute()

            # Get SQL query
            sql_query = None
            try:
                if hasattr(result, "sql"):
                    sql_query = result.sql()
            except Exception as e:
                sql_query = f"Error generating SQL: {str(e)}"

            # Get chart spec (supports both Altair/Vega-Lite and Plotly)
            chart_data = None
            try:
                if hasattr(result, "chart"):
                    # Check if code explicitly requests Plotly backend
                    use_plotly = (
                        "# USE_PLOTLY" in query_code
                        or 'backend="plotly"' in query_code
                        or "backend='plotly'" in query_code
                    )

                    # Extract spec parameter from code if present
                    # Look for spec={...} or spec=variable_name in the .chart() call
                    chart_spec_param = None

                    # First check if there's a chart_spec variable in namespace
                    if "chart_spec" in namespace:
                        chart_spec_param = namespace["chart_spec"]
                    else:
                        # Try to extract spec from .chart(spec=...) call
                        import re

                        spec_match = re.search(r"\.chart\([^)]*spec=([^,)]+)", query_code)
                        if spec_match:
                            spec_expr = spec_match.group(1).strip()
                            # Try to evaluate the spec expression
                            with contextlib.suppress(Exception):
                                chart_spec_param = eval(spec_expr, namespace)

                    if use_plotly:
                        # Generate Plotly chart
                        try:
                            import plotly.graph_objects as go

                            if chart_spec_param:
                                chart_obj = result.chart(spec=chart_spec_param, backend="plotly")
                            else:
                                chart_obj = result.chart(backend="plotly")

                            # Convert Plotly Figure to JSON using standard JSON encoding (not binary)
                            if isinstance(chart_obj, go.Figure):
                                # Use engine='json' to avoid binary encoding
                                plotly_json = chart_obj.to_json(engine="json")
                                chart_data = {"type": "plotly", "spec": plotly_json}

                                # If this is a chart-only block, return just the chart spec
                                if is_chart_only:
                                    return {
                                        "chart_spec": plotly_json,
                                        "chart_type": "plotly",
                                    }, updated_context
                        except Exception as plotly_err:
                            print(f"    Warning: Plotly chart generation failed: {plotly_err}")
                    else:
                        # Try Altair backend first (default)
                        try:
                            if chart_spec_param:
                                chart_obj = result.chart(spec=chart_spec_param, backend="altair")
                            else:
                                chart_obj = result.chart(backend="altair")
                            # BSL's .chart() returns an Altair Chart object
                            # Set width and height properties on the chart (max-w-4xl = ~896px, leave margin)
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
                                chart_data = {"type": "vega", "spec": vega_spec}

                                # If this is a chart-only block, return just the chart spec
                                if is_chart_only:
                                    return {"chart_spec": vega_spec}, updated_context
                        except Exception:
                            # If Altair fails, try Plotly as fallback
                            try:
                                import plotly.graph_objects as go

                                if chart_spec_param:
                                    chart_obj = result.chart(
                                        spec=chart_spec_param, backend="plotly"
                                    )
                                else:
                                    chart_obj = result.chart(backend="plotly")

                                # Convert Plotly Figure to JSON using standard JSON encoding (not binary)
                                if isinstance(chart_obj, go.Figure):
                                    # Use engine='json' to avoid binary encoding
                                    plotly_json = chart_obj.to_json(engine="json")
                                    chart_data = {"type": "plotly", "spec": plotly_json}
                            except Exception as plotly_err:
                                print(
                                    f"    Warning: Both Altair and Plotly chart generation failed: {plotly_err}"
                                )
            except Exception as e:
                print(f"    Warning: Could not generate chart: {str(e)}")

            # Convert to dict format
            # Convert DataFrame to JSON-serializable format
            # Convert datetime and Decimal columns to avoid JSON serialization issues
            df_copy = df.copy()
            for col in df_copy.columns:
                if df_copy[col].dtype == "datetime64[ns]" or df_copy[col].dtype.name.startswith(
                    "datetime"
                ):
                    df_copy[col] = df_copy[col].astype(str)
                elif df_copy[col].dtype == "object":
                    # Check if any value is a date/datetime/Decimal object
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

            # Convert to values.tolist() after type conversions
            # Replace NaN with None for valid JSON serialization
            df_copy = df_copy.replace({float("nan"): None})
            result_data = {
                "code": query_code,  # The original BSL query code
                "sql": sql_query,
                "table": {"columns": list(df_copy.columns), "data": df_copy.values.tolist()},
            }

            # Add chart if available
            if chart_data:
                result_data["chart"] = chart_data

            return result_data, updated_context

        # Check if it's a semantic table definition
        if hasattr(result, "group_by"):
            # It's a semantic table - don't execute, just store
            return {
                "semantic_table": True,
                "name": getattr(result, "name", "unknown"),
                "info": "Semantic table definition stored in context",
            }, updated_context

        # For other results (like raw tables)
        # Try to convert to dataframe-like structure
        if hasattr(result, "to_pandas"):
            df = result.to_pandas()
            return {
                "table": {"columns": list(df.columns), "data": df.values.tolist()}
            }, updated_context

        # Handle string results (for regularoutput component)
        if isinstance(result, str):
            return {"output": result}, updated_context

        return {"error": "Unknown result type"}, context

    except Exception as e:
        import traceback

        return {"error": str(e), "traceback": traceback.format_exc()}, context


def process_markdown_file(md_path: Path, output_dir: Path) -> bool:
    """
    Process a single markdown file and generate JSON data.
    Returns True if successful, False if any queries failed.
    """
    print(f"Processing {md_path.name}...")

    # Read markdown content
    content = md_path.read_text()

    # Resolve file includes first
    content_dir = md_path.parent
    content, files = resolve_file_includes(content, content_dir)

    # Parse and extract queries
    modified_md, queries, component_types = parse_markdown_with_queries(content)

    if not queries:
        print(f"  No BSL queries found in {md_path.name}")
        # Still save markdown-only pages
        output_file = output_dir / f"{md_path.stem}.json"
        output_data = {"markdown": modified_md, "queries": {}, "files": files}
        output_file.write_text(json.dumps(output_data, indent=2, cls=CustomJSONEncoder))
        print(f"  Saved markdown-only page to {output_file}")
        return True

    print(f"  Found {len(queries)} queries: {list(queries.keys())}")

    # Execute queries and collect results
    results = {}
    context = {}  # Shared context for semantic tables and variables
    has_errors = False

    for query_name, query_code in queries.items():
        print(f"  Executing query: {query_name}")
        # Check if this is a chart-only block based on component type
        is_chart_only = component_types.get(query_name) == "altairchart"
        # Pass query_code as the code parameter
        result, context = execute_bsl_query(query_code, context, is_chart_only=is_chart_only)

        # Only store results for blocks that have a component tag
        if query_name in component_types:
            results[query_name] = result
        else:
            print("    (executed for context, no output component)")

        # Check if query failed
        if "error" in result:
            has_errors = True
            print(f"  ❌ ERROR in query '{query_name}': {result['error']}")
            if "traceback" in result:
                print(f"  Traceback:\n{result['traceback']}")

    # Save to JSON even if there are errors (for debugging)
    output_file = output_dir / f"{md_path.stem}.json"
    output_data = {"markdown": modified_md, "queries": results, "files": files}

    output_file.write_text(json.dumps(output_data, indent=2, cls=CustomJSONEncoder))

    if has_errors:
        print(f"  ⚠️  Saved to {output_file} (with errors)")
        return False
    else:
        print(f"  ✅ Saved to {output_file}")
        return True


def main():
    """Main build script."""
    project_root = Path(__file__).parent.parent
    content_dir = project_root / "content"
    output_dir = project_root / "public" / "bsl-data"

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all markdown files
    md_files = list(content_dir.glob("*.md"))

    if not md_files:
        print(f"No markdown files found in {content_dir}")
        sys.exit(1)

    print(f"Found {len(md_files)} markdown files")

    # Process each file and track failures
    failed_files = []
    for md_file in md_files:
        success = process_markdown_file(md_file, output_dir)
        if not success:
            failed_files.append(md_file.name)

    # Generate pages.json (list of available pages)
    pages = [f.stem for f in md_files]
    pages_file = project_root / "public" / "pages.json"
    pages_file.write_text(json.dumps(pages))

    # Print summary and exit with appropriate code
    if failed_files:
        print(f"\n❌ Build completed with ERRORS in {len(failed_files)} file(s):")
        for filename in failed_files:
            print(f"  - {filename}")
        print(f"\nGenerated data for {len(pages)} pages, but some queries failed.")
        sys.exit(1)
    else:
        print(f"\n✅ Build complete! Generated data for {len(pages)} pages.")
        sys.exit(0)


if __name__ == "__main__":
    main()
