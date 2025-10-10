from .semantic_model import SemanticModel
from .semantic_model import Join, Filter, QueryExpr, DimensionSpec, MeasureSpec

__all__ = [
    "SemanticModel",
    "Join",
    "Filter",
    "QueryExpr",
    "DimensionSpec",
    "MeasureSpec",
]

# Import MCP functionality from separate module if available
try:
    from .mcp import MCPSemanticModel  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False

__all__.append("MCPSemanticModel")


def __getattr__(name):
    if name == "MCPSemanticModel" and not _MCP_AVAILABLE:
        raise ImportError(
            "MCPSemanticModel requires the 'fastmcp' optional dependencies. "
            "Install with: pip install 'boring-semantic-layer[fastmcp]'"
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
