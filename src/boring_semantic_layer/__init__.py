"""
Semantic API layer on top of external ibis.
"""

# Import convert and format to register dispatch handlers for semantic operations
from . import (
    convert,  # noqa: F401
    format,  # noqa: F401
)

# Main API exports
from .api import (
    to_semantic_table,
)
from .config import (
    options,
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
from .profile import (
    ProfileLoader,
    loader,
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
    "options",
    "to_xorq",
    "from_xorq",
    "ProfileLoader",
    "loader",
]

# Import MCP functionality if available
try:
    from .mcp import MCPSemanticModel  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False

# Import xorq conversion functionality
from .xorq_convert import from_xorq, to_xorq  # noqa: F401


def __getattr__(name):
    if name == "MCPSemanticModel" and not _MCP_AVAILABLE:
        raise ImportError(
            "MCPSemanticModel requires the 'fastmcp' optional dependencies. "
            "Install with: pip install 'boring-semantic-layer[fastmcp]'"
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
