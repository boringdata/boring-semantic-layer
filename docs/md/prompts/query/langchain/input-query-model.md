Execute a semantic model query and return results with optional chart.

Args:
    query: BSL query string, e.g.:
           model_name.group_by("dim1", "dim2").aggregate("measure1", "measure2")
    chart_spec: Optional chart specification dict. Keys depend on backend:
               - plotext: chart_type, theme, height, show_chart, show_table, etc.
               - altair: Vega-Lite spec (mark, encoding, etc.)
               - plotly: Plotly spec (type, mode, etc.)
               For details: get_documentation(topic="plotext"|"altair"|"plotly")
    chart_backend: Backend to use: "plotext" (terminal), "altair" (web), "plotly" (web).
                   Defaults to agent's configured backend.
    limit: Max rows to display in table (plotext only). Default: 10, use 0 for all.
           NOTE: Only affects display, not query execution.

Returns:
    Query results with optional chart/table visualization.
