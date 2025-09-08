"""
Chart functionality for Semantic API.
"""

from typing import Any


def create_chart(
    semantic_expr: "SemanticTableExpr",
    spec: dict[str, Any] | None = None,
    format: str = "altair",
) -> Any:
    """
    Create a chart from the semantic table using native Ibis-Altair integration.

    Args:
        semantic_expr: The SemanticTableExpr to create a chart from
        spec: Optional Vega-Lite specification for the chart.
              If not provided, will auto-detect chart type based on query.
              If partial spec is provided (e.g., only encoding or only mark),
              missing parts will be auto-detected and merged.
        format: The output format of the chart:
            - "altair" (default): Returns Altair Chart object
            - "interactive": Returns interactive Altair Chart with tooltip
            - "json": Returns Vega-Lite JSON specification
            - "png": Returns PNG image bytes
            - "svg": Returns SVG string

    Returns:
        Chart in the requested format:
            - altair/interactive: Altair Chart object
            - json: Dict containing Vega-Lite specification
            - png: bytes of PNG image
            - svg: str containing SVG markup

    Raises:
        ImportError: If Altair is not installed
        ValueError: If an unsupported format is specified
    """
    try:
        import altair as alt
    except ImportError:
        raise ImportError(
            "Altair is required for chart creation. "
            "Install it with: pip install 'boring-semantic-layer[visualization]'"
        )

    from ...chart import _detect_chart_spec

    # For the semantic API, we need to infer dimensions and measures from the query operations
    # This is a simplified approach - in practice would need more sophisticated introspection
    dimensions = []
    measures = []
    time_dimension = None
    time_grain = None

    # Try to extract information from the semantic table operations
    node = semantic_expr._node
    if hasattr(node, "keys"):
        dimensions = list(node.keys)
    if hasattr(node, "aggs"):
        measures = list(node.aggs.keys())

    # Auto-detect chart spec based on available information
    base_spec = _detect_chart_spec(
        dimensions=dimensions,
        measures=measures,
        time_dimension=time_dimension,
        time_grain=time_grain,
    )

    if spec is None:
        spec = base_spec
    else:
        if "mark" not in spec.keys():
            spec["mark"] = base_spec.get("mark", "point")

        if "encoding" not in spec.keys():
            spec["encoding"] = base_spec.get("encoding", {})

        if "transform" not in spec.keys():
            spec["transform"] = base_spec.get("transform", [])

    chart = alt.Chart(semantic_expr.to_ibis(), **spec)

    # Handle different output formats
    if format == "altair":
        return chart
    elif format == "interactive":
        return chart.interactive()
    elif format == "json":
        return chart.to_dict()
    elif format in ["png", "svg"]:
        try:
            import io

            buffer = io.BytesIO()
            chart.save(buffer, format=format)
            return buffer.getvalue()
        except Exception as e:
            raise ImportError(
                f"{format} export requires additional dependencies: {e}. "
                "Install with: pip install 'altair[all]' or pip install vl-convert-python"
            )
    else:
        raise ValueError(
            f"Unsupported format: {format}. "
            "Supported formats: 'altair', 'interactive', 'json', 'png', 'svg'"
        )
