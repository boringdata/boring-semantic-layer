"""Boring Semantic Layer core package."""

__all__ = []

# Check if MCP dependencies are available
try:
    from .semantic_api.mcp import MCPSemanticModel  # noqa: F401
    _MCP_AVAILABLE = True
    __all__.append("MCPSemanticModel")
except ImportError:
    _MCP_AVAILABLE = False
    MCPSemanticModel = None


def __getattr__(name):
    if name == "MCPSemanticModel":
        if not _MCP_AVAILABLE:
            raise ImportError(
                "MCPSemanticModel requires the 'mcp' optional dependencies. "
                "Install with: pip install 'boring-semantic-layer[mcp]'"
            )
        from .semantic_api.mcp import MCPSemanticModel
        return MCPSemanticModel
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
