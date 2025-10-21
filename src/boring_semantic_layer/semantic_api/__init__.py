"""
Semantic API layer on top of external ibis.
"""

# Import lower to register convert dispatch handlers for semantic operations
from . import lower  # noqa: F401

# Main API exports
from .table import (
    to_semantic_table,
    SemanticTable,
)
from .ops import (
    Dimension,
    Measure,
)

__all__ = [
    "to_semantic_table",
    "SemanticTable",
    "Dimension",
    "Measure",
]
