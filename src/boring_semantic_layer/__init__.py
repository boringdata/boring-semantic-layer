from .semantic_model import SemanticModel
from .semantic_model import Join, Filter, QueryExpr

__all__ = [
    "SemanticModel",
    "Join",
    "Filter",
    "QueryExpr",
]

# Conditional import for MCP functionality
try:
    from .semantic_model import MCPSemanticModel

    __all__.append("MCPSemanticModel")
except ImportError:
    # Define a placeholder that raises a helpful error when accessed
    class MCPSemanticModel:
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "MCP functionality requires additional dependencies. "
                "Please install with: pip install 'boring_semantic_layer[mcp]'"
            )

        def __new__(cls, *args, **kwargs):
            raise ImportError(
                "MCP functionality requires additional dependencies. "
                "Please install with: pip install 'boring_semantic_layer[mcp]'"
            )
