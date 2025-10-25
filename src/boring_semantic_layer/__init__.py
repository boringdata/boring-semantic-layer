"""
Semantic API layer on top of external ibis.
"""

# Import convert and format to register dispatch handlers for semantic operations
from . import convert  # noqa: F401
from . import format  # noqa: F401

# Main API exports
from .api import (
    to_semantic_table,
)
from .expr import (
    SemanticModel,
    SemanticTable,
    to_ibis,
)
from .ops import (
    Dimension,
    Measure,
)
from .yaml import (
    from_yaml,
)

__all__ = [
    "to_semantic_table",
    "to_ibis",
    "SemanticModel",
    "SemanticTable",
    "Dimension",
    "Measure",
    "from_yaml",
    "MCPSemanticModel",
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
