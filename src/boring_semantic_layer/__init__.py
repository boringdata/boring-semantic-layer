"""
Semantic API layer on top of external ibis.
"""

# Import lower to register convert dispatch handlers for semantic operations
from . import lower  # noqa: F401

# Main API exports
from .api import (
    to_semantic_table,
)
from .ops import (
    SemanticTable,
    Dimension,
    Measure,
)
from .yaml import (
    from_yaml,
)
from .mcp import (
    MCPSemanticModel,
)


__all__ = [
    "to_semantic_table",
    "SemanticTable",
    "Dimension",
    "Measure",
    "from_yaml",
    "MCPSemanticModel",
]
