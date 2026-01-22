"""Tests for dashboard parsing and rendering."""

from boring_semantic_layer.chart.md_parser.dashboard import (
    _clean_label,
    _format_value,
    _result_to_component,
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
