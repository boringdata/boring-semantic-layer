"""
Chart functionality for semantic API.

Provides chart() method for SemanticAggregate results.
"""

from typing import Any, Optional


def chart(
    semantic_aggregate: Any,
    backend: str = "altair",
    chart_type: Optional[str] = None
):
    """
    Generate a chart visualization for semantic aggregate query results.

    Args:
        semantic_aggregate: The SemanticAggregate object to visualize
        backend: Visualization backend ("altair" or "plotly")
        chart_type: Optional manual chart type override

    Returns:
        Chart object (altair.Chart or plotly Figure)

    Examples:
        # Auto-detect chart type with Altair
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result)

        # Use Plotly backend
        result = flights.group_by("dep_month").aggregate("flight_count")
        chart(result, backend="plotly")

        # Override chart type
        result = flights.group_by("carrier").aggregate("flight_count")
        chart(result, chart_type="bar")
    """
    from ..chart import (
        _detect_altair_spec,
        _detect_plotly_chart_type,
        _prepare_plotly_data_and_params
    )
    from .ops import _find_all_root_models, _get_merged_fields

    # Get dimensions and measures
    dimensions = list(semantic_aggregate.keys)
    measures = list(semantic_aggregate.aggs.keys())

    # Try to detect time dimension from source
    time_dimension = None
    time_grain = None
    all_roots = _find_all_root_models(semantic_aggregate.source)
    if all_roots:
        dims_dict = _get_merged_fields(all_roots, 'dims')
        for dim_name in dimensions:
            if dim_name in dims_dict:
                dim_obj = dims_dict[dim_name]
                if hasattr(dim_obj, 'is_time_dimension') and dim_obj.is_time_dimension:
                    time_dimension = dim_name
                    break

    if backend == "altair":
        import altair as alt

        # Execute query to get data
        df = semantic_aggregate.execute()

        # Get chart spec
        if chart_type:
            # Manual override - create basic spec
            spec = {"mark": chart_type, "encoding": {}}
            if dimensions:
                spec["encoding"]["x"] = {"field": dimensions[0], "type": "ordinal"}
            if measures:
                spec["encoding"]["y"] = {"field": measures[0], "type": "quantitative"}
        else:
            # Auto-detect
            spec = _detect_altair_spec(dimensions, measures, time_dimension, time_grain)

        # Create and return Altair chart
        chart_obj = alt.Chart(df)

        # Apply mark type
        mark = spec.get("mark")
        if isinstance(mark, str):
            chart_obj = getattr(chart_obj, f"mark_{mark}")()
        elif isinstance(mark, dict):
            mark_type = mark.get("type", "bar")
            chart_obj = getattr(chart_obj, f"mark_{mark_type}")(**{k: v for k, v in mark.items() if k != "type"})

        # Apply encoding
        encoding = spec.get("encoding", {})
        if encoding:
            chart_obj = chart_obj.encode(**encoding)

        # Apply transform if present
        if "transform" in spec:
            for transform in spec["transform"]:
                if "fold" in transform:
                    chart_obj = chart_obj.transform_fold(
                        transform["fold"],
                        as_=transform.get("as", ["key", "value"])
                    )

        return chart_obj

    elif backend == "plotly":
        import plotly.express as px
        import plotly.graph_objects as go

        # Detect chart type
        if not chart_type:
            chart_type = _detect_plotly_chart_type(dimensions, measures, time_dimension)

        # Execute query and prepare parameters
        df = semantic_aggregate.execute()

        # Create a minimal query expression object for _prepare_plotly_data_and_params
        class QueryExpr:
            def __init__(self, dimensions, measures, time_dimension, df):
                self.dimensions = dimensions
                self.measures = measures
                self.df = df

                class Model:
                    pass
                self.model = Model()
                self.model.time_dimension = time_dimension
                self.order_by = None

            def execute(self):
                return self.df

        query_expr = QueryExpr(dimensions, measures, time_dimension, df)
        df, base_params = _prepare_plotly_data_and_params(query_expr, chart_type)

        # Create chart based on type
        if chart_type == "bar":
            return px.bar(**base_params)
        elif chart_type == "line":
            return px.line(**base_params)
        elif chart_type == "scatter":
            return px.scatter(**base_params)
        elif chart_type == "heatmap":
            return go.Figure(data=go.Heatmap(**base_params))
        elif chart_type == "indicator":
            value = df[measures[0]].iloc[0] if measures else 0
            return go.Figure(go.Indicator(mode="number", value=value))
        else:
            # Default to table
            return go.Figure(data=[go.Table(
                header=dict(values=list(df.columns)),
                cells=dict(values=[df[col] for col in df.columns])
            )])
    else:
        raise ValueError(f"Unsupported backend: {backend}. Use 'altair' or 'plotly'")
