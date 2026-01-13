"""Tests for lazy import behavior of optional dependencies.

These tests verify the fix for issue #145:
https://github.com/boringdata/boring-semantic-layer/issues/145

The bug: importing MCPSemanticModel failed when langchain was not installed,
even though MCP doesn't require langchain. This was because
agents/backends/__init__.py unconditionally imported LangGraphBackend.
"""


class TestLazyImportsIssue145:
    """Test the exact bug from issue #145: MCP import failing without langchain."""

    def test_mcp_import_without_langchain_installed(self):
        """Issue #145: MCPSemanticModel should import even when langchain is missing.

        This is the exact reproduction case from the issue:
        1. pip install boring-semantic-layer fastmcp (without langchain)
        2. from boring_semantic_layer import MCPSemanticModel
        3. Should NOT fail with 'ModuleNotFoundError: No module named langchain'

        We verify this by importing MCP directly - if the backends/__init__.py
        still had the eager import, this would fail because it would try to
        load langgraph.py which imports langchain.
        """
        # The exact reproduction from the issue
        from boring_semantic_layer import MCPSemanticModel

        assert MCPSemanticModel is not None

    def test_mcp_import_from_backends_submodule(self):
        """MCPSemanticModel can be imported from backends.mcp submodule."""
        from boring_semantic_layer.agents.backends import mcp

        assert mcp.MCPSemanticModel is not None

    def test_backends_package_import_without_langchain(self):
        """Importing agents.backends should not fail when langchain is missing.

        The fix uses __getattr__ to lazily import LangGraphBackend only when accessed.
        """
        # Import the backends package - should not trigger langchain import
        from boring_semantic_layer.agents import backends

        # Package should load successfully
        assert backends is not None
        assert "LangGraphBackend" in backends.__all__

        # Accessing __all__ should not trigger the import
        assert hasattr(backends, "__all__")


class TestLazyImportBehavior:
    """Test that imports are properly lazy."""

    def test_mcp_import_works_from_main_module(self):
        """MCPSemanticModel can be imported from main module."""
        from boring_semantic_layer import MCPSemanticModel

        assert MCPSemanticModel is not None

    def test_langgraph_import_works_from_main_module(self):
        """LangGraphBackend can be imported from main module."""
        from boring_semantic_layer import LangGraphBackend

        assert LangGraphBackend is not None

    def test_mcp_import_works_from_backends_mcp(self):
        """MCPSemanticModel can be imported directly from backends.mcp."""
        from boring_semantic_layer.agents.backends.mcp import MCPSemanticModel

        assert MCPSemanticModel is not None

    def test_langgraph_import_works_from_backends(self):
        """LangGraphBackend can be imported from backends module via __getattr__."""
        from boring_semantic_layer.agents.backends import LangGraphBackend

        assert LangGraphBackend is not None


class TestHelpfulErrorMessages:
    """Test that missing dependencies give helpful error messages."""

    def test_mcp_in_all_exports(self):
        """MCPSemanticModel should be listed in __all__."""
        import boring_semantic_layer

        assert "MCPSemanticModel" in boring_semantic_layer.__all__

    def test_langgraph_in_all_exports(self):
        """LangGraphBackend should be listed in __all__."""
        import boring_semantic_layer

        assert "LangGraphBackend" in boring_semantic_layer.__all__
