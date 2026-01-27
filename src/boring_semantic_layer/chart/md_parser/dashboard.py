"""Dashboard rendering with 16-column CSS grid layout.

Renders BSL markdown files as interactive dashboards with:
- 16-column responsive grid (mviz-style)
- Auto-detecting chart types from query results
- KPI cards for single-value aggregations
- Tables and charts for multi-row results
- CSS theming via YAML frontmatter
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, NamedTuple

import yaml

from .converter import CustomJSONEncoder
from .executor import QueryExecutor
from .parser import DashboardBlock, MarkdownParser

logger = logging.getLogger(__name__)


class FrontmatterResult(NamedTuple):
    """Parsed frontmatter data from markdown file."""

    content: str  # Markdown content without frontmatter
    style_path: str | None  # Path to external CSS file
    inline_css: str | None  # Inline CSS from css: key


def parse_frontmatter(content: str) -> FrontmatterResult:
    """
    Parse YAML frontmatter from markdown content.

    Frontmatter format:
        ---
        style: ./themes/editorial.css
        css: |
          :root { --bg: #fffff8; }
        ---

    Args:
        content: Full markdown content

    Returns:
        FrontmatterResult with parsed data and remaining content
    """
    # Match frontmatter between --- markers at start of file
    pattern = r"^---\s*\n(.*?)\n---\s*\n"
    match = re.match(pattern, content, re.DOTALL)

    if not match:
        return FrontmatterResult(content=content, style_path=None, inline_css=None)

    frontmatter_text = match.group(1)
    remaining_content = content[match.end() :]

    try:
        data = yaml.safe_load(frontmatter_text) or {}
    except yaml.YAMLError as e:
        logger.warning(f"Failed to parse frontmatter YAML: {e}")
        return FrontmatterResult(content=content, style_path=None, inline_css=None)

    style_path = data.get("style")
    inline_css = data.get("css")

    return FrontmatterResult(
        content=remaining_content,
        style_path=style_path,
        inline_css=inline_css,
    )


def load_css_file(style_path: str, md_path: Path) -> str | None:
    """
    Load CSS file relative to the markdown file location.

    Args:
        style_path: Relative or absolute path to CSS file
        md_path: Path to the markdown file (for relative resolution)

    Returns:
        CSS content as string, or None if file not found
    """
    css_path = Path(style_path)

    # Resolve relative paths against markdown file directory
    if not css_path.is_absolute():
        css_path = md_path.parent / css_path

    css_path = css_path.resolve()

    if not css_path.exists():
        logger.warning(f"CSS file not found: {css_path}")
        return None

    if not css_path.is_file():
        logger.warning(f"CSS path is not a file: {css_path}")
        return None

    try:
        return css_path.read_text()
    except OSError as e:
        logger.warning(f"Failed to read CSS file {css_path}: {e}")
        return None


# Base CSS: Grid system, resets, and structural styles only
BASE_CSS = """
:root {
    --grid-cols: 16;
    --gap: 16px;
}

* {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    padding: var(--gap);
    min-height: 100vh;
}

.dashboard-grid {
    display: grid;
    grid-template-columns: repeat(var(--grid-cols), 1fr);
    gap: var(--gap);
    max-width: 1600px;
    margin: 0 auto;
}

.grid-cell {
    border-radius: 8px;
    padding: 16px;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}
"""

# Default theme CSS: Colors, typography, component styles
DEFAULT_THEME_CSS = """
:root {
    /* boring-bi design tokens */
    --bbi-font-heading: 'Palatino', Georgia, serif;
    --bbi-font-body: 'Source Sans Pro', system-ui, sans-serif;
    --bbi-color-primary: #4F46E5;
    --bbi-color-background: #FFFFF8;
    --bbi-color-surface: #FFFFFF;
    --bbi-color-border: #E5E7EB;
    --bbi-color-text: #1F2937;
    --bbi-color-text-muted: #6B7280;

    /* Legacy variable mappings for backward compatibility */
    --bg: var(--bbi-color-background);
    --card-bg: var(--bbi-color-surface);
    --text: var(--bbi-color-text);
    --text-muted: var(--bbi-color-text-muted);
    --border: var(--bbi-color-border);
    --accent: var(--bbi-color-primary);
}

body {
    font-family: var(--bbi-font-body);
    background: var(--bbi-color-background);
    color: var(--bbi-color-text);
}

h1 {
    font-family: var(--bbi-font-heading);
    font-size: 1.5rem;
    font-weight: 600;
    margin-bottom: var(--gap);
    color: var(--bbi-color-text);
}

.grid-cell {
    background: var(--bbi-color-surface);
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

/* KPI Component */
.kpi {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    height: 100%;
    text-align: center;
}

.kpi-value {
    font-size: 2.5rem;
    font-weight: 700;
    color: var(--bbi-color-text);
    line-height: 1.2;
}

.kpi-label {
    font-size: 0.875rem;
    color: var(--bbi-color-text-muted);
    margin-top: 4px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* KPI Row */
.kpi-row {
    display: flex;
    justify-content: space-around;
    align-items: center;
    height: 100%;
}

.kpi-row-item {
    text-align: center;
    padding: 0 16px;
}

.kpi-row-item .kpi-value {
    font-size: 1.75rem;
}

/* Chart */
.chart-container {
    flex: 1;
    min-height: 0;
    display: flex;
    align-items: center;
    justify-content: center;
}

.chart-container > div {
    width: 100%;
    height: 100%;
}

/* Table */
.table-container {
    overflow: auto;
    flex: 1;
}

table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.875rem;
}

th, td {
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid var(--bbi-color-border);
}

th {
    font-weight: 600;
    color: var(--bbi-color-text-muted);
    text-transform: uppercase;
    font-size: 0.75rem;
    letter-spacing: 0.05em;
}

tr:hover {
    background: var(--bbi-color-background);
}

/* Error */
.error {
    background: #fef2f2;
    border: 1px solid #fecaca;
    color: #dc2626;
    padding: 16px;
    border-radius: 6px;
}

/* Info */
.info {
    background: #f0f9ff;
    border: 1px solid #bae6fd;
    color: #0369a1;
    padding: 16px;
    border-radius: 6px;
}

/* Dark mode support */
@media (prefers-color-scheme: dark) {
    :root {
        --bbi-color-background: #0f172a;
        --bbi-color-surface: #1e293b;
        --bbi-color-text: #f1f5f9;
        --bbi-color-text-muted: #94a3b8;
        --bbi-color-border: #334155;
    }
}
"""


def render_dashboard(md_path: Path, output_path: Path) -> bool:
    """
    Render a markdown file with BSL queries as an interactive dashboard.

    Args:
        md_path: Path to input markdown file
        output_path: Path to output HTML file

    Returns:
        True if successful, False if errors occurred
    """
    print(f"Rendering dashboard: {md_path}")

    raw_content = md_path.read_text()

    # Parse frontmatter for CSS theming
    frontmatter = parse_frontmatter(raw_content)
    content = frontmatter.content

    # Determine user CSS (inline css takes precedence over style file)
    user_css = None
    if frontmatter.inline_css:
        user_css = frontmatter.inline_css
    elif frontmatter.style_path:
        user_css = load_css_file(frontmatter.style_path, md_path)

    # Extract title from first H1
    title_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
    title = title_match.group(1) if title_match else "Dashboard"

    # Parse dashboard blocks
    blocks = MarkdownParser.extract_dashboard_blocks(content)

    if not blocks:
        print(f"  No BSL blocks found in {md_path.name}")
        return False

    print(f"  Found {len(blocks)} dashboard blocks")

    # Execute queries and build components
    executor = QueryExecutor(capture_output=True)
    components = []
    has_errors = False

    for i, block in enumerate(blocks):
        print(f"  Executing block {i + 1}: {block.code[:50]}...")
        result = executor.execute(block.code, is_chart_only=False)

        if "error" in result:
            has_errors = True
            print(f"  ERROR in block {i + 1}: {result['error']}")
            components.append(
                {
                    "type": "error",
                    "error": result["error"],
                    "size": block.size,
                    "row_group": block.row_group,
                }
            )
        else:
            component = _result_to_component(result, block)
            components.append(component)

    # Generate HTML
    html = _generate_dashboard_html(title, components, user_css=user_css)
    output_path.write_text(html)

    if has_errors:
        print(f"  Saved to {output_path} (with errors)")
        return False
    else:
        print(f"  Saved to {output_path}")
        return True


def _result_to_component(result: dict[str, Any], block: DashboardBlock) -> dict[str, Any]:
    """Convert query result to dashboard component."""
    component = {
        "size": block.size,
        "row_group": block.row_group,
    }

    # Check for table data
    if "table" in result:
        table = result["table"]
        columns = table["columns"]
        data = table["data"]

        # Single value = KPI
        if len(data) == 1 and len(columns) == 1:
            component["type"] = "kpi"
            component["value"] = data[0][0]
            component["label"] = columns[0]
        # Single row with few columns = KPI row
        elif len(data) == 1 and len(columns) <= 4:
            component["type"] = "kpi_row"
            component["items"] = [
                {"label": col, "value": data[0][i]} for i, col in enumerate(columns)
            ]
        # Multiple rows = chart or table
        else:
            component["type"] = "chart"
            component["table"] = table

            # Include chart spec if available
            if "chart" in result:
                component["chart_spec"] = result["chart"]

    elif "chart" in result:
        component["type"] = "chart"
        component["chart_spec"] = result["chart"]

    elif "semantic_table" in result:
        component["type"] = "info"
        component["message"] = f"Semantic table '{result.get('name', 'unknown')}' defined"

    else:
        component["type"] = "empty"

    return component


def _generate_dashboard_html(
    title: str, components: list[dict], user_css: str | None = None
) -> str:
    """
    Generate complete dashboard HTML with 16-column grid.

    CSS injection order:
    1. BASE_CSS - Grid system and structural styles
    2. DEFAULT_THEME_CSS - Default theme (colors, typography, components)
    3. user_css - User-provided CSS (overrides defaults)

    Args:
        title: Dashboard title
        components: List of dashboard components
        user_css: Optional user-provided CSS to inject after defaults
    """
    # Group components by row
    rows: dict[int, list[dict]] = {}
    for comp in components:
        row_group = comp["row_group"]
        if row_group not in rows:
            rows[row_group] = []
        rows[row_group].append(comp)

    # Build grid content
    grid_html = []
    for row_idx in sorted(rows.keys()):
        row_comps = rows[row_idx]
        for comp in row_comps:
            cols, row_height = comp["size"]
            comp_html = _render_component(comp)
            grid_html.append(
                f'<div class="grid-cell" style="grid-column: span {cols}; grid-row: span {row_height};">'
                f"{comp_html}</div>"
            )

    # Build CSS: base -> default theme -> user overrides
    css_parts = [BASE_CSS, DEFAULT_THEME_CSS]
    if user_css:
        css_parts.append(f"/* User CSS */\n{user_css}")

    combined_css = "\n".join(css_parts)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
{combined_css}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <div class="dashboard-grid">
        {"".join(grid_html)}
    </div>
</body>
</html>
"""


def _render_component(comp: dict) -> str:
    """Render a single dashboard component to HTML."""
    comp_type = comp.get("type", "empty")

    if comp_type == "kpi":
        value = _format_value(comp["value"])
        label = _clean_label(comp["label"])
        return f"""<div class="kpi">
            <div class="kpi-value">{value}</div>
            <div class="kpi-label">{label}</div>
        </div>"""

    elif comp_type == "kpi_row":
        items_html = "".join(
            f"""<div class="kpi-row-item">
                <div class="kpi-value">{_format_value(item["value"])}</div>
                <div class="kpi-label">{_clean_label(item["label"])}</div>
            </div>"""
            for item in comp["items"]
        )
        return f'<div class="kpi-row">{items_html}</div>'

    elif comp_type == "chart":
        if "chart_spec" in comp:
            return _render_vega_chart(comp["chart_spec"])
        elif "table" in comp:
            return _render_table(comp["table"])
        return '<div class="info">No data to display</div>'

    elif comp_type == "error":
        return f'<div class="error">Error: {comp["error"]}</div>'

    elif comp_type == "info":
        return f'<div class="info">{comp["message"]}</div>'

    return '<div class="info">Empty component</div>'


def _render_vega_chart(spec: dict) -> str:
    """Render a Vega-Lite chart."""
    # Unwrap nested spec if present (converter returns {"type": "vega", "spec": {...}})
    if "spec" in spec and isinstance(spec.get("spec"), dict):
        spec = spec["spec"]
    chart_id = f"chart_{abs(hash(json.dumps(spec, cls=CustomJSONEncoder))) % 100000}"
    chart_json = json.dumps(spec, cls=CustomJSONEncoder)

    return f"""<div class="chart-container">
        <div id="{chart_id}"></div>
    </div>
    <script>
        vegaEmbed("#{chart_id}", {chart_json}, {{
            actions: false,
            renderer: "svg",
            config: {{
                autosize: {{type: "fit", contains: "padding"}}
            }}
        }});
    </script>"""


def _render_table(table: dict) -> str:
    """Render a data table."""
    columns = table["columns"]
    data = table["data"]

    header = "".join(f"<th>{_clean_label(col)}</th>" for col in columns)
    rows = "".join(
        "<tr>" + "".join(f"<td>{_format_value(cell)}</td>" for cell in row) + "</tr>"
        for row in data[:50]  # Limit to 50 rows
    )

    truncated = ""
    if len(data) > 50:
        truncated = f'<tr><td colspan="{len(columns)}" style="text-align:center;color:var(--text-muted)">... and {len(data) - 50} more rows</td></tr>'

    return f"""<div class="table-container">
        <table>
            <thead><tr>{header}</tr></thead>
            <tbody>{rows}{truncated}</tbody>
        </table>
    </div>"""


def _format_value(value: Any) -> str:
    """Format a value for display."""
    if value is None:
        return "-"
    if isinstance(value, float):
        if abs(value) >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if abs(value) >= 1_000:
            return f"{value / 1_000:.1f}K"
        return f"{value:,.2f}"
    if isinstance(value, int):
        if abs(value) >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if abs(value) >= 1_000:
            return f"{value / 1_000:.1f}K"
        return f"{value:,}"
    return str(value)


def _clean_label(label: str) -> str:
    """Clean up a label for display."""
    # Replace underscores with spaces, title case
    return label.replace("_", " ").title()
