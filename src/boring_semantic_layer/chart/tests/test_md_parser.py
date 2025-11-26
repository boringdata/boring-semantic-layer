"""Unit tests for markdown parser module."""

import tempfile
from pathlib import Path

from boring_semantic_layer.chart.md_parser import MarkdownParser


class TestMarkdownParser:
    """Test MarkdownParser functionality."""

    def test_extract_visible_queries(self):
        """Test extraction of visible code blocks."""
        content = """
# Test

```my_query
flights.group_by("origin")
```

```python
print("not a query")
```
"""
        modified, queries = MarkdownParser.extract_queries(content, include_hidden=False)
        assert "my_query" in queries
        assert queries["my_query"] == 'flights.group_by("origin")'
        assert "python" not in queries

    def test_extract_hidden_queries(self):
        """Test extraction of hidden queries from HTML comments."""
        content = """
<!--
```setup
t = ibis.table([("a", "int64")])
```
-->

```visible
t.filter(_.a > 5)
```
"""
        modified, queries = MarkdownParser.extract_queries(content, include_hidden=True)
        assert "setup" in queries
        assert "visible" in queries
        assert "<!--" not in modified

    def test_ignore_standard_languages(self):
        """Test that standard languages are not extracted."""
        content = """
```python
print("hello")
```

```sql
SELECT * FROM table
```

```bsl_query
flights.agg(count=_.count())
```
"""
        modified, queries = MarkdownParser.extract_queries(content)
        assert "python" not in queries
        assert "sql" not in queries
        assert "bsl_query" in queries

    def test_find_component_types(self):
        """Test component type detection."""
        content = """
<bslquery code-block="query1"/>
<altairchart code-block="query2"/>
<regularoutput code-block="query3"/>
<collapsedcodeblock code-block="query4"/>
"""
        types = MarkdownParser.find_component_types(content)
        assert types["query1"] == "bslquery"
        assert types["query2"] == "altairchart"
        assert types["query3"] == "regularoutput"
        assert types["query4"] == "collapsedcodeblock"

    def test_resolve_file_includes(self):
        """Test file inclusion resolution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            config_file = tmp_path / "config.yaml"
            config_file.write_text("key: value\n")

            content = """
Some markdown
<yamlcontent path="config.yaml"></yamlcontent>
More text
"""
            modified, files = MarkdownParser.resolve_file_includes(content, tmp_path)
            assert "config.yaml" in files
            assert files["config.yaml"] == "key: value\n"

    def test_resolve_missing_file(self):
        """Test handling of missing file includes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            content = '<yamlcontent path="missing.yaml"></yamlcontent>'
            modified, files = MarkdownParser.resolve_file_includes(content, tmp_path)
            assert "missing.yaml" not in files
            assert "Error: File not found" in modified

    def test_multiline_queries(self):
        """Test extraction of multiline queries."""
        content = """
```complex_query
flights
  .filter(_.year == 2023)
  .group_by("origin", "dest")
  .agg(
      total_flights=_.count(),
      avg_delay=_.arr_delay.mean()
  )
```
"""
        modified, queries = MarkdownParser.extract_queries(content)
        assert "complex_query" in queries
        assert "filter" in queries["complex_query"]
        assert "group_by" in queries["complex_query"]
        assert "agg" in queries["complex_query"]
