#!/usr/bin/env python3
"""
Documentation Builder - Parse markdown files, execute BSL queries, generate JSON data.

This script processes markdown documentation files, executes embedded BSL queries,
and generates JSON data files for the web documentation site.

Usage:
    python docs_builder.py
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

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from boring_semantic_layer import to_semantic_table


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal and datetime objects."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime | date | pd.Timestamp):
            return str(obj)
        return super().default(obj)


class DocBuilder:
    """Build documentation JSON from markdown files with BSL queries."""

    def __init__(self, docs_dir: Path | None = None):
        """
        Initialize the documentation builder.

        Args:
            docs_dir: Path to the docs directory. If None, uses parent of this script.
        """
        if docs_dir is None:
            # Script is in docs/md/, so parent.parent is project root
            docs_dir = Path(__file__).parent.parent
        self.docs_dir = docs_dir
        self.content_dir = docs_dir / "md" / "doc"
        self.output_dir = docs_dir / "web" / "public" / "bsl-data"
        self.pages_file = docs_dir / "web" / "public" / "pages.json"

    def resolve_file_includes(self, content: str, content_dir: Path) -> tuple[str, dict[str, str]]:
        """
        Resolve file includes in markdown content.

        Syntax: <yamlcontent path="filename.yaml"></yamlcontent>

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

    def parse_markdown_with_queries(
        self, content: str
    ) -> tuple[str, dict[str, str], dict[str, str]]:
        """
        Parse markdown and extract BSL query blocks.

        Returns:
            - Modified markdown (hidden blocks removed)
            - Dictionary of query_name -> code
            - Dictionary of query_name -> component_type
        """
        queries = {}
        component_types = {}

        # Handle hidden code blocks in HTML comments
        hidden_pattern = r"<!--\s*\n```(\w+)\n(.*?)\n```\s*\n-->"

        def extract_hidden_query(match):
            query_name = match.group(1)
            query_code = match.group(2).strip()
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
            return ""

        modified_md = re.sub(hidden_pattern, extract_hidden_query, content, flags=re.DOTALL)

        # Handle visible code blocks
        pattern = r"```(\w+)\n(.*?)\n```"

        def replace_query(match):
            query_name = match.group(1)
            query_code = match.group(2).strip()
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
                return match.group(0)
            queries[query_name] = query_code
            return match.group(0)

        modified_md = re.sub(pattern, replace_query, modified_md, flags=re.DOTALL)

        # Find component types
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
        self, query_code: str, context: dict[str, Any], is_chart_only: bool = False
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Execute BSL query and return results."""
        try:
            # Capture print output
            captured_output = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output

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

                if is_simple_expression:
                    code_without_last = "\n".join(code_lines[:-1])
                    paren_count = code_without_last.count("(") - code_without_last.count(")")
                    bracket_count = code_without_last.count("[") - code_without_last.count("]")
                    brace_count = code_without_last.count("{") - code_without_last.count("}")
                    is_simple_expression = (
                        paren_count == 0 and bracket_count == 0 and brace_count == 0
                    )

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
                sys.stdout = old_stdout

            output = captured_output.getvalue()

            # For chart-only mode
            if (
                is_chart_only
                and last_expr_result is not None
                and hasattr(last_expr_result, "to_dict")
            ):
                try:
                    if hasattr(last_expr_result, "properties"):
                        last_expr_result = last_expr_result.properties(width=700, height=400)
                    vega_spec = last_expr_result.to_dict()
                    updated_context = {**context}
                    for key, val in namespace.items():
                        if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                            updated_context[key] = val
                    return {"chart_spec": vega_spec, "code": query_code}, updated_context
                except Exception as e:
                    print(f"    Warning: Could not extract chart spec: {e}")

            # Handle last expression result
            if last_expr_result is not None:
                if (
                    isinstance(last_expr_result, tuple)
                    and has_comma_in_expr
                    and len(last_expr_result) > 1
                ):
                    output = [str(item) for item in last_expr_result]
                else:
                    output += str(last_expr_result)

            # Check for output
            has_output = (isinstance(output, list) and len(output) > 0) or (
                isinstance(output, str) and len(output.strip()) > 0
            )
            if has_output:
                result = None
                for var_name in ["result", "q", "query"]:
                    if var_name in namespace:
                        result = namespace[var_name]
                        break

                if result is None:
                    updated_context = {**context}
                    for key, val in namespace.items():
                        if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                            updated_context[key] = val
                    output_data = output if isinstance(output, list) else output.strip()
                    return {"output": output_data}, updated_context

            # Get result
            result = None
            for var_name in ["result", "q", "query"]:
                if var_name in namespace:
                    result = namespace[var_name]
                    break

            if result is None:
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

            # Update context
            updated_context = {**context}
            for key, val in namespace.items():
                if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                    updated_context[key] = val

            # Execute BSL query
            if hasattr(result, "execute"):
                df = result.execute()

                # Get SQL
                sql_query = None
                try:
                    if hasattr(result, "sql"):
                        sql_query = result.sql()
                except Exception as e:
                    sql_query = f"Error generating SQL: {str(e)}"

                # Get chart spec
                chart_data = None
                try:
                    if hasattr(result, "chart"):
                        use_plotly = (
                            "# USE_PLOTLY" in query_code
                            or 'backend="plotly"' in query_code
                            or "backend='plotly'" in query_code
                        )

                        chart_spec_param = None
                        if "chart_spec" in namespace:
                            chart_spec_param = namespace["chart_spec"]
                        else:
                            spec_match = re.search(r"\.chart\([^)]*spec=([^,)]+)", query_code)
                            if spec_match:
                                spec_expr = spec_match.group(1).strip()
                                with contextlib.suppress(Exception):
                                    chart_spec_param = eval(spec_expr, namespace)

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
                                    chart_data = {"type": "plotly", "spec": plotly_json}
                                    if is_chart_only:
                                        return {
                                            "chart_spec": plotly_json,
                                            "chart_type": "plotly",
                                        }, updated_context
                            except Exception as plotly_err:
                                print(f"    Warning: Plotly chart failed: {plotly_err}")
                        else:
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
                                    chart_data = {"type": "vega", "spec": vega_spec}
                                    if is_chart_only:
                                        return {"chart_spec": vega_spec}, updated_context
                            except Exception:
                                # Fallback to Plotly
                                try:
                                    import plotly.graph_objects as go

                                    chart_obj = (
                                        result.chart(spec=chart_spec_param, backend="plotly")
                                        if chart_spec_param
                                        else result.chart(backend="plotly")
                                    )
                                    if isinstance(chart_obj, go.Figure):
                                        plotly_json = chart_obj.to_json(engine="json")
                                        chart_data = {"type": "plotly", "spec": plotly_json}
                                except Exception as plotly_err:
                                    print(f"    Warning: Both chart backends failed: {plotly_err}")
                except Exception as e:
                    print(f"    Warning: Could not generate chart: {str(e)}")

                # Convert DataFrame
                df_copy = df.copy()
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

                df_copy = df_copy.replace({float("nan"): None})

                # Get query plan
                query_plan = None
                try:
                    query_plan = str(result.expr) if hasattr(result, "expr") else str(result)
                except Exception as e:
                    print(f"    Warning: Could not generate query plan: {str(e)}")

                result_data = {
                    "code": query_code,
                    "sql": sql_query,
                    "plan": query_plan,
                    "table": {"columns": list(df_copy.columns), "data": df_copy.values.tolist()},
                }

                if chart_data:
                    result_data["chart"] = chart_data

                return result_data, updated_context

            # Semantic table definition
            if hasattr(result, "group_by"):
                return {
                    "semantic_table": True,
                    "name": getattr(result, "name", "unknown"),
                    "info": "Semantic table definition stored in context",
                }, updated_context

            # Convert to dataframe
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

    def process_markdown_file(self, md_path: Path) -> bool:
        """Process a markdown file and generate JSON. Returns True if successful."""
        print(f"Processing {md_path.name}...")

        content = md_path.read_text()
        content, files = self.resolve_file_includes(content, md_path.parent)
        modified_md, queries, component_types = self.parse_markdown_with_queries(content)

        if not queries:
            print(f"  No BSL queries found in {md_path.name}")
            output_file = self.output_dir / f"{md_path.stem}.json"
            output_data = {"markdown": modified_md, "queries": {}, "files": files}
            output_file.write_text(json.dumps(output_data, indent=2, cls=CustomJSONEncoder) + "\n")
            print(f"  Saved markdown-only page to {output_file}")
            return True

        print(f"  Found {len(queries)} queries: {list(queries.keys())}")

        results = {}
        context = {}
        has_errors = False

        # Change to content directory so relative file paths work
        import os

        original_cwd = os.getcwd()
        os.chdir(md_path.parent)

        try:
            for query_name, query_code in queries.items():
                print(f"  Executing query: {query_name}")
                is_chart_only = component_types.get(query_name) == "altairchart"
                result, context = self.execute_bsl_query(
                    query_code, context, is_chart_only=is_chart_only
                )

                if query_name in component_types:
                    results[query_name] = result
                else:
                    print("    (executed for context, no output component)")

                if "error" in result:
                    has_errors = True
                    print(f"  ❌ ERROR in query '{query_name}': {result['error']}")
                    if "traceback" in result:
                        print(f"  Traceback:\n{result['traceback']}")

            output_file = self.output_dir / f"{md_path.stem}.json"
            output_data = {"markdown": modified_md, "queries": results, "files": files}
            output_file.write_text(json.dumps(output_data, indent=2, cls=CustomJSONEncoder) + "\n")

            if has_errors:
                print(f"  ⚠️  Saved to {output_file} (with errors)")
                return False
            else:
                print(f"  ✅ Saved to {output_file}")
                return True
        finally:
            # Restore original working directory
            os.chdir(original_cwd)

    def build(self) -> bool:
        """Build all documentation. Returns True if successful."""
        print("Building Documentation")
        print("=" * 60)

        self.output_dir.mkdir(parents=True, exist_ok=True)

        md_files = list(self.content_dir.glob("*.md"))
        if not md_files:
            print(f"No markdown files found in {self.content_dir}")
            return False

        print(f"Found {len(md_files)} markdown files\n")

        failed_files = []
        for md_file in md_files:
            success = self.process_markdown_file(md_file)
            if not success:
                failed_files.append(md_file.name)

        # Generate pages.json
        pages = sorted([f.stem for f in md_files])
        self.pages_file.write_text(json.dumps(pages, indent=2) + "\n")

        if failed_files:
            print(f"\n❌ Documentation build completed with ERRORS in {len(failed_files)} file(s):")
            for filename in failed_files:
                print(f"  - {filename}")
            return False
        else:
            print(f"\n✅ Documentation build complete! Generated {len(pages)} pages.")
            return True


def main():
    """Main build script."""
    import subprocess

    # First, build the documentation
    builder = DocBuilder()
    success = builder.build()

    if not success:
        return 1

    # Then validate all internal links
    print("\n🔗 Validating internal links...")
    print("=" * 60)
    validate_script = Path(__file__).parent / "validate_links.py"

    if not validate_script.exists():
        print("⚠️  Link validation script not found, skipping validation")
        return 0

    result = subprocess.run([sys.executable, str(validate_script)], capture_output=False)

    if result.returncode != 0:
        print("\n❌ Link validation failed!")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
