#!/usr/bin/env python3
"""Tests for documentation builder and link validation.

Tests cover:
- Markdown link extraction
- Route extraction from App.tsx
- Link validation logic
- Broken link detection
- Redirect handling
- Dynamic route matching
"""

# Import from docs builder's validate_links module
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "docs" / "md"))
from validate_links import (
    extract_markdown_links,
    extract_routes_from_app_tsx,
    validate_links,
)


class TestExtractMarkdownLinks:
    """Test markdown link extraction."""

    def test_extract_simple_link(self, tmp_path):
        """Test extracting a simple internal link."""
        md_file = tmp_path / "test.md"
        md_file.write_text("See [this page](/some/path) for more info.")

        links = extract_markdown_links(tmp_path)
        assert links == {"/some/path"}

    def test_extract_multiple_links(self, tmp_path):
        """Test extracting multiple links."""
        md_file = tmp_path / "test.md"
        md_file.write_text(
            """
            - [Link 1](/path/one)
            - [Link 2](/path/two)
            - [Link 3](/path/three)
            """
        )

        links = extract_markdown_links(tmp_path)
        assert links == {"/path/one", "/path/two", "/path/three"}

    def test_extract_link_with_anchor(self, tmp_path):
        """Test that anchors are removed from links."""
        md_file = tmp_path / "test.md"
        md_file.write_text("See [section](/page#section) for details.")

        links = extract_markdown_links(tmp_path)
        assert links == {"/page"}

    def test_ignore_external_links(self, tmp_path):
        """Test that external links are ignored."""
        md_file = tmp_path / "test.md"
        md_file.write_text(
            """
            - [Internal](/internal/path)
            - [External](https://example.com)
            - [Also external](http://example.com)
            """
        )

        links = extract_markdown_links(tmp_path)
        assert links == {"/internal/path"}

    def test_deduplicate_links(self, tmp_path):
        """Test that duplicate links are deduplicated."""
        md_file = tmp_path / "test.md"
        md_file.write_text(
            """
            - [Link 1](/path)
            - [Link 2](/path)
            - [Link 3](/path#anchor1)
            - [Link 4](/path#anchor2)
            """
        )

        links = extract_markdown_links(tmp_path)
        assert links == {"/path"}

    def test_multiple_files(self, tmp_path):
        """Test extracting links from multiple markdown files."""
        (tmp_path / "file1.md").write_text("[Link 1](/path/one)")
        (tmp_path / "file2.md").write_text("[Link 2](/path/two)")

        links = extract_markdown_links(tmp_path)
        assert links == {"/path/one", "/path/two"}

    def test_nested_directories(self, tmp_path):
        """Test extracting links from nested directories."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested.md").write_text("[Nested](/path/nested)")
        (tmp_path / "root.md").write_text("[Root](/path/root)")

        links = extract_markdown_links(tmp_path)
        assert links == {"/path/nested", "/path/root"}


class TestExtractRoutesFromAppTsx:
    """Test route extraction from App.tsx."""

    def test_extract_simple_route(self, tmp_path):
        """Test extracting a simple route."""
        app_tsx = tmp_path / "App.tsx"
        app_tsx.write_text('<Route path="/some/path" element={<Component />} />')

        routes, redirects = extract_routes_from_app_tsx(app_tsx)
        assert routes == {"/some/path"}
        assert redirects == {}

    def test_extract_multiple_routes(self, tmp_path):
        """Test extracting multiple routes."""
        app_tsx = tmp_path / "App.tsx"
        app_tsx.write_text(
            """
            <Route path="/path/one" element={<ComponentOne />} />
            <Route path="/path/two" element={<ComponentTwo />} />
            <Route path="/path/three" element={<ComponentThree />} />
            """
        )

        routes, redirects = extract_routes_from_app_tsx(app_tsx)
        assert routes == {"/path/one", "/path/two", "/path/three"}

    def test_ignore_wildcard_and_root(self, tmp_path):
        """Test that wildcard and root routes are ignored."""
        app_tsx = tmp_path / "App.tsx"
        app_tsx.write_text(
            """
            <Route path="/" element={<Home />} />
            <Route path="/valid" element={<Valid />} />
            <Route path="*" element={<NotFound />} />
            """
        )

        routes, redirects = extract_routes_from_app_tsx(app_tsx)
        assert routes == {"/valid"}

    def test_extract_redirects(self, tmp_path):
        """Test extracting redirect routes."""
        app_tsx = tmp_path / "App.tsx"
        app_tsx.write_text(
            """
            <Route path="/old/path" element={<Navigate to="/new/path" replace />} />
            <Route path="/another/old" element={<Navigate to="/another/new" replace />} />
            """
        )

        routes, redirects = extract_routes_from_app_tsx(app_tsx)
        assert redirects == {
            "/old/path": "/new/path",
            "/another/old": "/another/new",
        }

    def test_dynamic_routes(self, tmp_path):
        """Test extracting dynamic routes with parameters."""
        app_tsx = tmp_path / "App.tsx"
        app_tsx.write_text('<Route path="/examples/:slug" element={<Example />} />')

        routes, redirects = extract_routes_from_app_tsx(app_tsx)
        assert "/examples/:slug" in routes


class TestValidateLinks:
    """Test link validation logic."""

    def test_valid_links(self):
        """Test that valid links pass validation."""
        links = {"/path/one", "/path/two"}
        routes = {"/path/one", "/path/two", "/path/three"}
        redirects = {}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == []
        assert redirect_list == []

    def test_broken_links(self):
        """Test that broken links are detected."""
        links = {"/path/one", "/path/broken", "/path/two"}
        routes = {"/path/one", "/path/two"}
        redirects = {}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == ["/path/broken"]

    def test_redirect_links(self):
        """Test that redirected links are identified."""
        links = {"/old/path", "/valid/path"}
        routes = {"/valid/path", "/new/path"}
        redirects = {"/old/path": "/new/path"}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == []
        assert redirect_list == [("/old/path", "/new/path")]

    def test_dynamic_route_matching(self):
        """Test that dynamic routes match correctly."""
        links = {"/examples/getting-started", "/examples/advanced"}
        routes = {"/examples/:slug"}
        redirects = {}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == []

    def test_dynamic_route_no_match(self):
        """Test that non-matching paths are caught as broken."""
        links = {"/examples/getting-started", "/other/path"}
        routes = {"/examples/:slug"}
        redirects = {}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == ["/other/path"]

    def test_multiple_dynamic_segments(self):
        """Test routes with multiple dynamic segments."""
        links = {"/category/123/item/456"}
        routes = {"/category/:catId/item/:itemId"}
        redirects = {}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == []

    def test_mixed_scenarios(self):
        """Test a mix of valid, broken, and redirected links."""
        links = {
            "/valid/path",
            "/broken/path",
            "/old/redirect",
            "/examples/some-slug",
        }
        routes = {"/valid/path", "/new/redirect", "/examples/:slug"}
        redirects = {"/old/redirect": "/new/redirect"}

        broken, redirect_list = validate_links(links, routes, redirects)
        assert broken == ["/broken/path"]
        assert ("/old/redirect", "/new/redirect") in redirect_list


class TestIntegration:
    """Integration tests with real project structure."""

    def test_real_markdown_and_routes(self):
        """Test with a realistic scenario."""
        # Create temporary structure
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # Create markdown files
            doc_dir = tmp_path / "docs"
            doc_dir.mkdir()
            (doc_dir / "page1.md").write_text(
                """
                # Page 1
                See [Page 2](/page/two) for more.
                Also check [Advanced](/advanced/topic).
                """
            )

            # Create App.tsx
            (tmp_path / "App.tsx").write_text(
                """
                <Route path="/page/two" element={<PageTwo />} />
                <Route path="/advanced/topic" element={<Advanced />} />
                """
            )

            # Extract and validate
            links = extract_markdown_links(doc_dir)
            routes, redirects = extract_routes_from_app_tsx(tmp_path / "App.tsx")
            broken, redirect_list = validate_links(links, routes, redirects)

            assert broken == []
            assert links == {"/page/two", "/advanced/topic"}
            assert routes == {"/page/two", "/advanced/topic"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
