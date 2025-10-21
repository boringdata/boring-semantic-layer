"""Boring Semantic Layer core package."""

__all__ = []


def __getattr__(name):
    if name == "MCPSemanticModel" and not _MCP_AVAILABLE:
        raise ImportError(
            "MCPSemanticModel requires the 'mcp' optional dependencies. "
            "Install with: pip install 'boring-semantic-layer[mcp]'"
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
