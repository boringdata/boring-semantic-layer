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
]

# Import MCP functionality from separate module if available
try:
    from .mcp import MCPSemanticModel  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False

# Import xorq conversion functionality if xorq is available
try:
    from .xorq_convert import from_xorq, to_xorq  # noqa: F401

    _XORQ_AVAILABLE = True
except ImportError:
    _XORQ_AVAILABLE = False


def __getattr__(name):
    if name == "MCPSemanticModel" and not _MCP_AVAILABLE:
        raise ImportError(
            "MCPSemanticModel requires the 'fastmcp' optional dependencies. "
            "Install with: pip install 'boring-semantic-layer[fastmcp]'"
        )
    if name in ("to_xorq", "from_xorq") and not _XORQ_AVAILABLE:
        raise ImportError(
            "Xorq conversion requires the 'xorq' optional dependency. "
            "Install with: pip install 'boring-semantic-layer[xorq]'"
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
