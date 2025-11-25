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
from .graph_utils import (
    graph_bfs,
    graph_invert,
    graph_predecessors,
    graph_successors,
    graph_to_dict,
)
from .ops import (
    Dimension,
    Measure,
)
from .profile import (
    ProfileError,
    get_connection,
    get_tables,
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
    "to_tagged",
    "from_tagged",
    "to_xorq",
    "from_xorq",
    "ProfileError",
    "get_connection",
    "get_tables",
    "graph_predecessors",
    "graph_successors",
    "graph_bfs",
    "graph_invert",
    "graph_to_dict",
]

# Import MCP functionality if available
try:
    from .mcp import MCPSemanticModel  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False

# Import xorq conversion functionality
from .xorq_convert import from_tagged, to_tagged  # noqa: F401

# Backward compatibility aliases
to_xorq = to_tagged
from_xorq = from_tagged

# Import graph utilities
from .graph_utils import (  # noqa: F401
    bfs as graph_bfs,
    invert as graph_invert,
    predecessors as graph_predecessors,
    successors as graph_successors,
    to_dict as graph_to_dict,
)

# Install window compatibility if xorq is available
# This allows users to use `import ibis` seamlessly with xorq backend
try:
    from .window_compat import install_window_compatibility

    install_window_compatibility()

    _XORQ_AVAILABLE = True
except ImportError:
    _XORQ_AVAILABLE = False


def __getattr__(name):
    if name == "MCPSemanticModel" and not _MCP_AVAILABLE:
        raise ImportError(
            "MCPSemanticModel requires the 'fastmcp' optional dependencies. "
            "Install with: pip install 'boring-semantic-layer[fastmcp]'"
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
