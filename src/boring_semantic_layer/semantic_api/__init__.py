"""
Semantic API layer on top of external ibis.
"""

# Import lower to register convert dispatch handlers for semantic operations
from . import lower  # noqa: F401

# Main API exports
from .api import (
    to_semantic_table,
    with_dimensions,
    with_measures,
    SemanticTableExpr,
)

from .ops import (
    Dimension,
    Measure,
    SemanticTable,
)

__all__ = [
    "to_semantic_table",
    "with_dimensions",
    "with_measures",
    "SemanticTableExpr",
    "Dimension",
    "Measure",
    "SemanticTable",
]
