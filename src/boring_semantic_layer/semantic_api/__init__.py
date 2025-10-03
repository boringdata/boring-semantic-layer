"""
Semantic API layer on top of external ibis.
"""

# Indicates that this package implements a standalone semantic API
__all__ = []

# Import lower to register convert dispatch handlers for semantic operations
from . import lower  # noqa: F401