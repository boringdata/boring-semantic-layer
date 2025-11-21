"""Markdown rendering with BSL query execution.

This module provides functionality to render markdown files containing BSL queries
into HTML with embedded visualizations or markdown with image exports.
"""

import json
import re
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from xorq.vendor import ibis
import markdown
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


def parse_markdown_with_queries(content: str) -> tuple[str, dict[str, str]]:
    """
    Parse markdown content and extract BSL query blocks.

    Syntax: ```query_name
            <BSL query code>
            ```

    Returns:
        - Modified markdown (queries remain for display)
        - Dictionary of query_name -> code
    """
    queries = {}

    # Find code blocks with custom names (not standard language names)
    pattern = r"```(\w+)\n(.*?)\n```"

    def extract_query(match):
        query_name = match.group(1)
        query_code = match.group(2).strip()

        # Skip if it's a standard language
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
            "text",
            "sh",
        ]:
            return match.group(0)  # Return original

        # Store the query
        queries[query_name] = query_code

        # Keep the code block in markdown
        return match.group(0)

    modified_md = re.sub(pattern, extract_query, content, flags=re.DOTALL)

    return modified_md, queries


def execute_bsl_query(
    query_code: str, context: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Execute BSL query code and return results.

    Args:
        query_code: The Python code to execute
        context: The execution context with previous variables

    Returns:
        (result_data, updated_context)
    """
    try:
        # Create a namespace with ibis and BSL imports
        namespace = {
            "ibis": ibis,
            "to_semantic_table": to_semantic_table,
            **context,
        }

        # Execute the code
        exec(query_code, namespace)

        # Update context with all new variables
        updated_context = {**context}
        for key, val in namespace.items():
            if not key.startswith("_") and key not in ["ibis", "to_semantic_table"]:
                updated_context[key] = val

        # Get the result
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

        if result is None:
            return {"error": "No result found in query"}, context

        # Check if it's a semantic table definition (don't execute)
        if hasattr(result, "group_by") and not hasattr(result, "execute"):
            return {
                "semantic_table": True,
                "name": getattr(result, "name", "unknown"),
            }, updated_context

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

            # Get chart spec (Altair/Vega-Lite)
            chart_spec = None
            try:
                if hasattr(result, "chart"):
                    chart_obj = result.chart(backend="altair")
                    # Set reasonable dimensions
                    if hasattr(chart_obj, "properties"):
                        chart_obj = chart_obj.properties(width=700, height=400)

                    if hasattr(chart_obj, "to_dict"):
                        chart_spec = chart_obj.to_dict()
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
                "table": {"columns": list(df_copy.columns), "data": df_copy.values.tolist()},
            }

            if chart_spec:
                result_data["chart"] = chart_spec

            return result_data, updated_context

        return {"error": "Unknown result type"}, context

    except Exception as e:
        import traceback

        return {"error": str(e), "traceback": traceback.format_exc()}, context


def render_table_html(table_data: dict) -> str:
    """Render a table as HTML."""
    columns = table_data["columns"]
    rows = table_data["data"]

    html = ['<div class="bsl-table">']
    html.append("<table>")
    html.append("<thead><tr>")
    html.append("<th>#</th>")
    for col in columns:
        html.append(f"<th>{col}</th>")
    html.append("</tr></thead>")
    html.append("<tbody>")

    for idx, row in enumerate(rows):
        html.append("<tr>")
        html.append(f'<td class="row-number">{idx}</td>')
        for cell in row:
            # Format cell value
            if cell is None:
                cell_str = "null"
            elif isinstance(cell, int | float):
                cell_str = f"{cell:,}" if isinstance(cell, int) else f"{cell:.2f}"
            else:
                cell_str = str(cell)
            html.append(f"<td>{cell_str}</td>")
        html.append("</tr>")

    html.append("</tbody>")
    html.append("</table>")
    html.append("</div>")

    return "\n".join(html)


def render_chart_html(chart_spec: dict) -> str:
    """Render a Vega-Lite chart as HTML with vega-embed."""
    # Embed the chart using vega-embed
    chart_json = json.dumps(chart_spec, cls=CustomJSONEncoder)
    chart_id = f"chart_{abs(hash(chart_json)) % 10000}"

    html = [
        f'<div id="{chart_id}" class="bsl-chart"></div>',
        "<script>",
        f'vegaEmbed("#{chart_id}", {chart_json}, {{actions: false, renderer: "svg"}});',
        "</script>",
    ]

    return "\n".join(html)


def export_chart_as_image(chart_spec: dict, output_path: Path) -> bool:
    """
    Export a Vega-Lite chart as an image.

    Requires selenium and chromedriver for rendering.
    Falls back to saving the spec as JSON if rendering fails.

    Returns:
        True if successful, False otherwise
    """
    try:
        import altair as alt

        # Create Altair chart from spec
        chart = alt.Chart.from_dict(chart_spec)

        # Try to save as PNG
        try:
            chart.save(str(output_path))
            return True
        except Exception as e:
            print(f"    Warning: Could not save chart as image: {e}")
            print("    Tip: Install altair_saver with selenium and chromedriver")

            # Fallback: save spec as JSON
            json_path = output_path.with_suffix(".json")
            with open(json_path, "w") as f:
                json.dump(chart_spec, f, indent=2, cls=CustomJSONEncoder)
            print(f"    Saved chart spec to {json_path}")
            return False

    except Exception as e:
        print(f"    Error exporting chart: {e}")
        return False


def render_to_html(md_path: Path, output_path: Path) -> bool:
    """
    Render markdown file with BSL queries to standalone HTML.

    Args:
        md_path: Path to input markdown file
        output_path: Path to output HTML file

    Returns:
        True if successful, False if errors occurred
    """
    print(f"Rendering {md_path} to HTML...")

    # Read markdown content
    content = md_path.read_text()

    # Parse and extract queries
    modified_md, queries = parse_markdown_with_queries(content)

    if not queries:
        print(f"  No BSL queries found in {md_path.name}")
        # Just render markdown to HTML
        html_content = markdown.markdown(modified_md, extensions=["fenced_code", "tables"])

        # Create full HTML page
        full_html = create_html_page(html_content, {})
        output_path.write_text(full_html)
        print(f"  ✅ Saved to {output_path}")
        return True

    print(f"  Found {len(queries)} queries: {list(queries.keys())}")

    # Execute queries
    results = {}
    context = {}
    has_errors = False

    for query_name, query_code in queries.items():
        print(f"  Executing query: {query_name}")
        result, context = execute_bsl_query(query_code, context)
        results[query_name] = result

        if "error" in result:
            has_errors = True
            print(f"  ❌ ERROR in query '{query_name}': {result['error']}")
            if "traceback" in result:
                print(f"  Traceback:\n{result['traceback']}")

    # Replace <bslquery> tags with rendered content
    def replace_bslquery(match):
        code_block = match.group(1)
        if code_block in results:
            result = results[code_block]

            if "error" in result:
                return f'<div class="error">Error in query {code_block}: {result["error"]}</div>'

            if "semantic_table" in result:
                return f'<div class="info">Semantic table {result["name"]} defined</div>'

            html_parts = []

            # Add table
            if "table" in result:
                html_parts.append('<div class="query-result">')
                html_parts.append(f"<h4>Query: {code_block}</h4>")
                html_parts.append(render_table_html(result["table"]))
                html_parts.append("</div>")

            # Add chart
            if "chart" in result:
                html_parts.append('<div class="query-chart">')
                html_parts.append(render_chart_html(result["chart"]))
                html_parts.append("</div>")

            return "\n".join(html_parts)

        return match.group(0)

    # Replace bslquery tags
    pattern = r'<bslquery\s+code-block="([^"]+)"\s*/?>'
    modified_md = re.sub(pattern, replace_bslquery, modified_md)

    # Convert markdown to HTML
    html_content = markdown.markdown(modified_md, extensions=["fenced_code", "tables"])

    # Create full HTML page with styles and vega-embed
    full_html = create_html_page(html_content, results)

    # Write to file
    output_path.write_text(full_html)

    if has_errors:
        print(f"  ⚠️  Saved to {output_path} (with errors)")
        return False
    else:
        print(f"  ✅ Saved to {output_path}")
        return True


def render_to_markdown(md_path: Path, output_path: Path, images_dir: Path) -> bool:
    """
    Render markdown file with BSL queries to markdown with exported images.

    Args:
        md_path: Path to input markdown file
        output_path: Path to output markdown file
        images_dir: Directory to save exported images

    Returns:
        True if successful, False if errors occurred
    """
    print(f"Rendering {md_path} to markdown with images...")

    # Create images directory
    images_dir.mkdir(parents=True, exist_ok=True)

    # Read markdown content
    content = md_path.read_text()

    # Parse and extract queries
    modified_md, queries = parse_markdown_with_queries(content)

    if not queries:
        print(f"  No BSL queries found in {md_path.name}")
        output_path.write_text(content)
        print(f"  ✅ Saved to {output_path}")
        return True

    print(f"  Found {len(queries)} queries: {list(queries.keys())}")

    # Execute queries
    results = {}
    context = {}
    has_errors = False

    for query_name, query_code in queries.items():
        print(f"  Executing query: {query_name}")
        result, context = execute_bsl_query(query_code, context)
        results[query_name] = result

        if "error" in result:
            has_errors = True
            print(f"  ❌ ERROR in query '{query_name}': {result['error']}")
            if "traceback" in result:
                print(f"  Traceback:\n{result['traceback']}")

    # Replace <bslquery> tags with markdown tables/images
    def replace_bslquery(match):
        code_block = match.group(1)
        if code_block in results:
            result = results[code_block]

            if "error" in result:
                return f"**Error in query {code_block}:** {result['error']}"

            if "semantic_table" in result:
                return f"_Semantic table {result['name']} defined_"

            md_parts = []

            # Add table as markdown
            if "table" in result:
                table_data = result["table"]
                columns = table_data["columns"]
                rows = table_data["data"]

                # Create markdown table
                md_parts.append(f"### Query Result: {code_block}\n")
                md_parts.append("| # | " + " | ".join(columns) + " |")
                md_parts.append("|---" * (len(columns) + 1) + "|")

                for idx, row in enumerate(rows):
                    cells = [str(idx)]
                    for cell in row:
                        if cell is None:
                            cells.append("null")
                        elif isinstance(cell, float):
                            cells.append(f"{cell:.2f}")
                        else:
                            cells.append(str(cell))
                    md_parts.append("| " + " | ".join(cells) + " |")

                md_parts.append("")

            # Export chart as image
            if "chart" in result:
                image_path = images_dir / f"{code_block}.png"
                success = export_chart_as_image(result["chart"], image_path)

                if success:
                    # Use relative path
                    rel_path = image_path.relative_to(output_path.parent)
                    md_parts.append(f"![Chart: {code_block}]({rel_path})\n")
                else:
                    md_parts.append(f"_Chart for {code_block} (see {code_block}.json)_\n")

            return "\n".join(md_parts)

        return match.group(0)

    # Replace bslquery tags
    pattern = r'<bslquery\s+code-block="([^"]+)"\s*/?>'
    modified_md = re.sub(pattern, replace_bslquery, modified_md)

    # Write to file
    output_path.write_text(modified_md)

    if has_errors:
        print(f"  ⚠️  Saved to {output_path} (with errors)")
        return False
    else:
        print(f"  ✅ Saved to {output_path}")
        return True


def create_html_page(content: str, results: dict) -> str:
    """Create a complete HTML page with styles and scripts."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BSL Query Results</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
            color: #333;
            background: #f9fafb;
        }}

        h1, h2, h3, h4 {{
            color: #111;
            margin-top: 1.5em;
        }}

        code {{
            background: #f3f4f6;
            padding: 0.2em 0.4em;
            border-radius: 3px;
            font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
            font-size: 0.9em;
        }}

        pre {{
            background: #1f2937;
            color: #f9fafb;
            padding: 1rem;
            border-radius: 6px;
            overflow-x: auto;
        }}

        pre code {{
            background: none;
            color: inherit;
            padding: 0;
        }}

        .bsl-table {{
            margin: 2rem 0;
            overflow-x: auto;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
        }}

        th {{
            background: #f3f4f6;
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid #e5e7eb;
        }}

        td {{
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #e5e7eb;
        }}

        tr:hover {{
            background: #f9fafb;
        }}

        .row-number {{
            color: #9ca3af;
            font-weight: 500;
        }}

        .bsl-chart {{
            margin: 2rem 0;
            background: white;
            padding: 1rem;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        .query-result {{
            margin: 2rem 0;
        }}

        .error {{
            background: #fee;
            border: 1px solid #fcc;
            color: #c33;
            padding: 1rem;
            border-radius: 6px;
            margin: 1rem 0;
        }}

        .info {{
            background: #e0f2fe;
            border: 1px solid #bae6fd;
            color: #0c4a6e;
            padding: 1rem;
            border-radius: 6px;
            margin: 1rem 0;
        }}
    </style>
</head>
<body>
{content}
</body>
</html>
"""


def cmd_render(
    md_path: Path,
    output: Path | None = None,
    format: str = "html",
    images_dir: Path | None = None,
    watch: bool = False,
) -> bool:
    """
    Render a markdown file with BSL queries.

    Args:
        md_path: Path to input markdown file
        output: Path to output file (default: same name with .html or .md extension)
        format: Output format ('html' or 'markdown')
        images_dir: Directory for exported images (markdown format only)
        watch: Watch for file changes and auto-regenerate

    Returns:
        True if successful, False if errors occurred
    """
    if not md_path.exists():
        print(f"❌ Error: File not found: {md_path}", file=sys.stderr)
        return False

    # Determine output path
    if output is None:
        if format == "html":
            output = md_path.with_suffix(".html")
        else:
            output = md_path.with_name(f"{md_path.stem}_rendered.md")

    # Determine images directory for markdown format
    if format == "markdown" and images_dir is None:
        images_dir = output.parent / f"{output.stem}_images"

    # Render based on format
    def do_render():
        if format == "html":
            return render_to_html(md_path, output)
        elif format == "markdown":
            return render_to_markdown(md_path, output, images_dir)
        else:
            print(f"❌ Error: Unknown format: {format}", file=sys.stderr)
            return False

    # Initial render
    success = do_render()

    # Watch mode
    if watch:
        try:
            from watchdog.events import FileSystemEventHandler
            from watchdog.observers import Observer
        except ImportError:
            print("❌ Error: watchdog package required for --watch mode", file=sys.stderr)
            print("Install with: uv pip install watchdog", file=sys.stderr)
            return False

        class MarkdownChangeHandler(FileSystemEventHandler):
            def __init__(self, target_path: Path):
                self.target_path = target_path.resolve()
                self.last_modified = time.time()

            def on_modified(self, event):
                if event.is_directory:
                    return

                # Check if it's our target file
                event_path = Path(event.src_path).resolve()
                if event_path != self.target_path:
                    return

                # Debounce: ignore if modified less than 0.5 seconds ago
                current_time = time.time()
                if current_time - self.last_modified < 0.5:
                    return

                self.last_modified = current_time
                print("\n📝 File changed, regenerating...")
                do_render()

        print(f"\n👀 Watching {md_path} for changes... (Press Ctrl+C to stop)")
        print(f"📄 Output: {output}")

        event_handler = MarkdownChangeHandler(md_path)
        observer = Observer()
        observer.schedule(event_handler, str(md_path.parent), recursive=False)
        observer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n👋 Stopping watch mode...")
            observer.stop()
            observer.join()
            return True

    return success
