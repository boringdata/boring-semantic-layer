"""Shared serialization helpers."""

from __future__ import annotations

from typing import Any

from .freeze import list_to_tuple


def extract_simple_column_name(expr) -> str | None:
    """Extract column name from a simple Deferred like ``_.col_name``.

    Returns the column name string if the expression is a simple column access,
    or None if it requires structured serialization.
    """
    from ..ops import _CallableWrapper, _is_deferred

    if isinstance(expr, _CallableWrapper):
        expr = expr._fn

    if not _is_deferred(expr):
        return None

    resolver = expr._resolver
    if type(resolver).__name__ != "Attr":
        return None

    if type(resolver.obj).__name__ != "Variable":
        return None

    name_resolver = resolver.name
    if type(name_resolver).__name__ != "Just":
        return None

    value = name_resolver.value
    return value if isinstance(value, str) else None


def deserialize_structured(struct_data: Any, context: str) -> Any:
    """Deserialize a structured expression, raising on failure.

    Args:
        struct_data: Tuple or list of structured expression data.
        context: Human-readable label for error messages.

    Returns:
        Deserialized callable/deferred expression.

    Raises:
        ValueError: If deserialization fails or no data provided.
    """
    from ..utils import structured_to_expr

    if isinstance(struct_data, tuple | list):
        data = list_to_tuple(struct_data) if isinstance(struct_data, list) else struct_data
        result = structured_to_expr(data).value_or(None)
        if result is None:
            raise ValueError(f"{context}: failed to deserialize struct")
        return result
    raise ValueError(f"{context}: no structured data")
