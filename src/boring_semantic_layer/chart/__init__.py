"""
Chart functionality for semantic API.

Provides multiple backends for visualization:
- Altair: Vega-Lite based declarative charts
- Plotly: Interactive web-based charts
- Plotext: Terminal-based charts

Auto-detect chart specifications based on query dimensions and measures.
"""

from typing import Any

from .base import ChartBackend

# Backend registry
_BACKENDS: dict[str, type[ChartBackend]] = {}


def register_backend(name: str, backend_class: type[ChartBackend]) -> None:
    """
    Register a chart backend.

    Args:
        name: Backend name (e.g., "altair", "plotly", "plotext")
        backend_class: Backend class implementing ChartBackend interface
    """
    _BACKENDS[name] = backend_class


def get_backend(name: str) -> ChartBackend:
    """
    Get a chart backend instance by name.

    Args:
        name: Backend name (e.g., "altair", "plotly", "plotext")

    Returns:
        Instance of the requested backend

    Raises:
        ValueError: If backend is not registered
    """
    if name not in _BACKENDS:
        raise ValueError(
            f"Unsupported backend: {name}. Available backends: {', '.join(_BACKENDS.keys())}"
        )
    return _BACKENDS[name]()


def list_backends() -> list[str]:
    """
    List all registered backend names.

    Returns:
        List of backend names
    """
    return list(_BACKENDS.keys())


def chart(
    semantic_aggregate: Any,
    spec: dict[str, Any] | None = None,
    backend: str = "altair",
    format: str = "static",
) -> Any:
    """
    Generate a chart visualization for semantic aggregate query results.

    Args:
        semantic_aggregate: The SemanticAggregate object to visualize
        spec: Optional chart specification dict (backend-specific format).
              If partial spec is provided (e.g., only "mark" or only "encoding"),
              missing parts will be auto-detected and merged.
        backend: Visualization backend ("altair", "plotly", or "plotext")
        format: Output format (backend-specific, typically "static", "interactive", "json")

    Returns:
        Chart object or formatted output (type depends on backend and format)

    Examples:
        # Auto-detect chart type with Altair
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result)

        # Use Plotly backend
        result = flights.group_by("dep_month").aggregate("flight_count")
        chart(result, backend="plotly")

        # Use Plotext for terminal output
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, backend="plotext")

        # Custom specification with auto-detection
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, spec={"mark": "line"})

        # Export as JSON
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, format="json")
    """
    backend_instance = get_backend(backend)
    return backend_instance.render(semantic_aggregate, spec, format)


# Register built-in backends
def _register_builtin_backends():
    """Register all built-in chart backends."""
    try:
        from .altair_backend import AltairBackend

        register_backend("altair", AltairBackend)
    except ImportError:
        pass  # Altair not installed

    try:
        from .plotly_backend import PlotlyBackend

        register_backend("plotly", PlotlyBackend)
    except ImportError:
        pass  # Plotly not installed

    try:
        from .plotext_backend import PlotextBackend

        register_backend("plotext", PlotextBackend)
    except ImportError:
        pass  # Plotext not installed


# Auto-register backends on module import
_register_builtin_backends()


__all__ = [
    "ChartBackend",
    "chart",
    "register_backend",
    "get_backend",
    "list_backends",
]
