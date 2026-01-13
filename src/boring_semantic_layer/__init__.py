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
    entity_dimension,
    time_dimension,
    to_semantic_table,
)
from .config import (
    options,
)
from .expr import (
    SemanticModel,
    SemanticTable,
    to_untagged,
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
)
from .yaml import (
    from_config,
    from_yaml,
)

__all__ = [
    "to_semantic_table",
    "to_untagged",
    "entity_dimension",
    "time_dimension",
    "SemanticModel",
    "SemanticTable",
    "Dimension",
    "Measure",
    "from_config",
    "from_yaml",
    "MCPSemanticModel",
    "LangGraphBackend",
    "options",
    "graph_bfs",
    "graph_invert",
    "graph_predecessors",
    "graph_successors",
    "graph_to_dict",
    "ProfileError",
    "get_connection",
]


def __getattr__(name):
    """Lazy imports for optional dependencies."""
    if name == "MCPSemanticModel":
        try:
            from .agents.backends.mcp import MCPSemanticModel

            return MCPSemanticModel
        except ImportError:
            raise ImportError(
                "MCPSemanticModel requires the 'fastmcp' optional dependencies. "
                "Install with: pip install 'boring-semantic-layer[fastmcp]'"
            ) from None
    if name == "LangGraphBackend":
        try:
            from .agents.backends.langgraph import LangGraphBackend

            return LangGraphBackend
        except ImportError:
            raise ImportError(
                "LangGraphBackend requires the 'langgraph' optional dependencies. "
                "Install with: pip install 'boring-semantic-layer[langgraph]'"
            ) from None
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
