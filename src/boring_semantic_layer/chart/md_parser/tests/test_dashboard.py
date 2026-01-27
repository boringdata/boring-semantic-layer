"""Tests for dashboard parsing and rendering."""

from pathlib import Path
from unittest.mock import patch

from boring_semantic_layer.chart.md_parser.dashboard import (
    _clean_label,
    _format_value,
    _generate_dashboard_html,
    _result_to_component,
    load_css_file,
    parse_frontmatter,
    BASE_CSS,
    DEFAULT_THEME_CSS,
)
from boring_semantic_layer.chart.md_parser.parser import DashboardBlock, MarkdownParser


class TestMarkdownParser:
    """Tests for dashboard block extraction."""

    def test_extract_single_block(self):
        """Extract a single BSL block with size."""
        md = """
# Dashboard

```bsl size=[8,6]
model.group_by("x").aggregate("y")
```
"""
        blocks = MarkdownParser.extract_dashboard_blocks(md)
        assert len(blocks) == 1
        assert blocks[0].code == 'model.group_by("x").aggregate("y")'
        assert blocks[0].size == (8, 6)
        assert blocks[0].row_group == 0

    def test_extract_default_size(self):
        """Block without size gets default [8,5]."""
        md = """
```bsl
query_here
```
"""
        blocks = MarkdownParser.extract_dashboard_blocks(md)
        assert len(blocks) == 1
        assert blocks[0].size == (8, 5)

    def test_consecutive_blocks_same_row(self):
        """Consecutive blocks (no blank line) share row_group."""
        md = """
```bsl size=[4,2]
query1
```
```bsl size=[4,2]
query2
```
```bsl size=[4,2]
query3
```
"""
        blocks = MarkdownParser.extract_dashboard_blocks(md)
        assert len(blocks) == 3
        assert all(b.row_group == 0 for b in blocks)

    def test_blank_line_creates_new_row(self):
        """Blank line between blocks creates new row_group."""
        md = """
```bsl size=[8,4]
row1_query
```

```bsl size=[8,4]
row2_query
```
"""
        blocks = MarkdownParser.extract_dashboard_blocks(md)
        assert len(blocks) == 2
        assert blocks[0].row_group == 0
        assert blocks[1].row_group == 1

    def test_mixed_row_groups(self):
        """Complex layout with multiple rows."""
        md = """
```bsl size=[4,2]
kpi1
```
```bsl size=[4,2]
kpi2
```
```bsl size=[4,2]
kpi3
```
```bsl size=[4,2]
kpi4
```

```bsl size=[8,6]
chart1
```
```bsl size=[8,6]
chart2
```

```bsl size=[16,4]
full_width_table
```
"""
        blocks = MarkdownParser.extract_dashboard_blocks(md)
        assert len(blocks) == 7
        # First 4 KPIs in row 0
        assert blocks[0].row_group == 0
        assert blocks[3].row_group == 0
        # 2 charts in row 1
        assert blocks[4].row_group == 1
        assert blocks[5].row_group == 1
        # Table in row 2
        assert blocks[6].row_group == 2

    def test_parse_size_from_info(self):
        """Parse size from info string."""
        assert MarkdownParser.parse_size_from_info("bsl size=[8,6]") == (8, 6)
        assert MarkdownParser.parse_size_from_info("bsl size=[16, 4]") == (16, 4)
        assert MarkdownParser.parse_size_from_info("bsl") is None
        assert MarkdownParser.parse_size_from_info("python") is None


class TestFormatters:
    """Tests for value and label formatting."""

    def test_format_value_integers(self):
        """Format integers with K/M suffixes."""
        assert _format_value(500) == "500"
        assert _format_value(1500) == "1.5K"
        assert _format_value(1_500_000) == "1.5M"

    def test_format_value_floats(self):
        """Format floats with K/M suffixes."""
        assert _format_value(123.456) == "123.46"
        assert _format_value(1234.5) == "1.2K"
        assert _format_value(1_234_567.89) == "1.2M"

    def test_format_value_none(self):
        """None formats as dash."""
        assert _format_value(None) == "-"

    def test_format_value_string(self):
        """Strings pass through."""
        assert _format_value("hello") == "hello"

    def test_clean_label(self):
        """Labels get title-cased with underscores replaced."""
        assert _clean_label("flight_count") == "Flight Count"
        assert _clean_label("total_distance") == "Total Distance"
        assert _clean_label("revenue") == "Revenue"


class TestResultToComponent:
    """Tests for converting query results to dashboard components."""

    def test_single_value_kpi(self):
        """Single value result becomes KPI."""
        result = {"table": {"columns": ["total"], "data": [[12345]]}}
        block = DashboardBlock(code="", size=(4, 2), row_group=0)

        comp = _result_to_component(result, block)

        assert comp["type"] == "kpi"
        assert comp["value"] == 12345
        assert comp["label"] == "total"

    def test_single_row_multi_col_kpi_row(self):
        """Single row with few columns becomes KPI row."""
        result = {
            "table": {
                "columns": ["revenue", "orders", "customers"],
                "data": [[100000, 500, 200]],
            }
        }
        block = DashboardBlock(code="", size=(12, 2), row_group=0)

        comp = _result_to_component(result, block)

        assert comp["type"] == "kpi_row"
        assert len(comp["items"]) == 3
        assert comp["items"][0]["label"] == "revenue"
        assert comp["items"][0]["value"] == 100000

    def test_multi_row_becomes_chart(self):
        """Multi-row result becomes chart/table."""
        result = {
            "table": {
                "columns": ["region", "sales"],
                "data": [["East", 100], ["West", 200], ["North", 150]],
            }
        }
        block = DashboardBlock(code="", size=(8, 6), row_group=0)

        comp = _result_to_component(result, block)

        assert comp["type"] == "chart"
        assert "table" in comp

    def test_semantic_table_info(self):
        """Semantic table definition returns info component."""
        result = {"semantic_table": True, "name": "flights"}
        block = DashboardBlock(code="", size=(16, 1), row_group=0)

        comp = _result_to_component(result, block)

        assert comp["type"] == "info"
        assert "flights" in comp["message"]


class TestFrontmatterParsing:
    """Tests for YAML frontmatter parsing."""

    def test_parse_frontmatter_with_style(self):
        """Parse frontmatter with style path."""
        md = """---
style: ./themes/editorial.css
---
# Dashboard

Content here
"""
        result = parse_frontmatter(md)

        assert result.style_path == "./themes/editorial.css"
        assert result.inline_css is None
        assert result.content.startswith("# Dashboard")

    def test_parse_frontmatter_with_inline_css(self):
        """Parse frontmatter with inline CSS."""
        md = """---
css: |
  :root { --bg: #fffff8; }
  body { color: red; }
---
# Dashboard
"""
        result = parse_frontmatter(md)

        assert result.style_path is None
        assert ":root { --bg: #fffff8; }" in result.inline_css
        assert "body { color: red; }" in result.inline_css
        assert result.content.startswith("# Dashboard")

    def test_parse_frontmatter_with_both(self):
        """Parse frontmatter with both style and inline CSS."""
        md = """---
style: ./theme.css
css: |
  :root { --bg: #000; }
---
# Dashboard
"""
        result = parse_frontmatter(md)

        assert result.style_path == "./theme.css"
        assert result.inline_css is not None
        assert "--bg: #000" in result.inline_css

    def test_parse_frontmatter_empty(self):
        """Parse frontmatter when none present."""
        md = """# Dashboard

No frontmatter here.
"""
        result = parse_frontmatter(md)

        assert result.style_path is None
        assert result.inline_css is None
        assert result.content == md

    def test_parse_frontmatter_invalid_yaml(self):
        """Invalid YAML returns original content."""
        md = """---
style: [invalid: yaml: here
---
# Dashboard
"""
        result = parse_frontmatter(md)

        # Should fall back gracefully
        assert result.style_path is None
        assert result.inline_css is None
        assert result.content == md

    def test_parse_frontmatter_preserves_content(self):
        """Content after frontmatter is preserved exactly."""
        md = """---
style: ./test.css
---
# My Dashboard

```bsl size=[8,6]
query_here
```
"""
        result = parse_frontmatter(md)

        assert "# My Dashboard" in result.content
        assert "```bsl size=[8,6]" in result.content
        assert "query_here" in result.content


class TestCssFileLoading:
    """Tests for CSS file loading."""

    def test_load_css_file_relative(self, tmp_path):
        """Load CSS file with relative path."""
        # Create markdown file and CSS file
        md_file = tmp_path / "dashboard.md"
        css_file = tmp_path / "theme.css"
        css_content = ":root { --bg: #fff; }"
        css_file.write_text(css_content)

        result = load_css_file("theme.css", md_file)

        assert result == css_content

    def test_load_css_file_nested_relative(self, tmp_path):
        """Load CSS file with nested relative path."""
        # Create directory structure
        themes_dir = tmp_path / "themes"
        themes_dir.mkdir()
        md_file = tmp_path / "dashboard.md"
        css_file = themes_dir / "editorial.css"
        css_content = ":root { --bbi-font-heading: Georgia; }"
        css_file.write_text(css_content)

        result = load_css_file("./themes/editorial.css", md_file)

        assert result == css_content

    def test_load_css_file_not_found(self, tmp_path):
        """Missing CSS file returns None."""
        md_file = tmp_path / "dashboard.md"

        result = load_css_file("nonexistent.css", md_file)

        assert result is None

    def test_load_css_file_absolute_path(self, tmp_path):
        """Load CSS file with absolute path."""
        css_file = tmp_path / "absolute.css"
        css_content = "body { margin: 0; }"
        css_file.write_text(css_content)
        md_file = Path("/some/other/place/dashboard.md")

        result = load_css_file(str(css_file), md_file)

        assert result == css_content


class TestCssInjection:
    """Tests for CSS injection in HTML generation."""

    def test_base_css_included(self):
        """BASE_CSS is always included in output."""
        html = _generate_dashboard_html("Test", [])

        assert "--grid-cols: 16" in html
        assert "display: grid" in html

    def test_default_theme_included(self):
        """DEFAULT_THEME_CSS is always included in output."""
        html = _generate_dashboard_html("Test", [])

        assert "--bbi-font-heading" in html
        assert "--bbi-color-primary" in html
        assert "--bbi-color-background" in html

    def test_user_css_injected(self):
        """User CSS is injected after defaults."""
        user_css = ":root { --custom-color: #123456; }"
        html = _generate_dashboard_html("Test", [], user_css=user_css)

        assert "--custom-color: #123456" in html
        assert "/* User CSS */" in html
        # User CSS should come after base and default theme
        base_pos = html.find("--grid-cols")
        theme_pos = html.find("--bbi-font-heading")
        user_pos = html.find("--custom-color")
        assert base_pos < theme_pos < user_pos

    def test_user_css_none_no_comment(self):
        """No user CSS comment when user_css is None."""
        html = _generate_dashboard_html("Test", [])

        assert "/* User CSS */" not in html

    def test_design_tokens_in_default_theme(self):
        """Default theme includes all boring-bi design tokens."""
        assert "--bbi-font-heading" in DEFAULT_THEME_CSS
        assert "--bbi-font-body" in DEFAULT_THEME_CSS
        assert "--bbi-color-primary" in DEFAULT_THEME_CSS
        assert "--bbi-color-background" in DEFAULT_THEME_CSS
        assert "--bbi-color-surface" in DEFAULT_THEME_CSS
        assert "--bbi-color-border" in DEFAULT_THEME_CSS
        assert "--bbi-color-text" in DEFAULT_THEME_CSS
        assert "--bbi-color-text-muted" in DEFAULT_THEME_CSS

    def test_base_css_minimal(self):
        """BASE_CSS contains only structural styles (~30 lines)."""
        lines = [l for l in BASE_CSS.strip().split("\n") if l.strip()]
        # Should be roughly 30 lines of actual CSS
        assert len(lines) < 40
        # Should not contain theme colors
        assert "#4F46E5" not in BASE_CSS  # primary color
        assert "font-family" not in BASE_CSS  # typography
