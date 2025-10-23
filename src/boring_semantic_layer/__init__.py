"""Boring Semantic Layer core package."""

try:
    from .semantic_api.mcp import create_mcp_server, MCPSemanticModel
    __all__ = ["create_mcp_server", "MCPSemanticModel"]
except ImportError:
    # MCP dependencies not installed
    __all__ = []
