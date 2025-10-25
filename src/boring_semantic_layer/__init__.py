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
from .mcp import (
    MCPSemanticModel,
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
