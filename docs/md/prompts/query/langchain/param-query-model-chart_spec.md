Optional dictionary for backend-specific chart customization. Use top-level params (`get_chart`, `chart_backend`, `chart_format`) for common options.

**Note:** Charts are automatically hidden for single-row results (e.g., a single aggregate value).

## Plotext Backend (Terminal Charts)

Additional options:
- `chart_type`: `"bar"` (categorical), `"line"` (trends/time), `"scatter"` (correlations)
- `theme`: `"pro"` (default), `"dark"`, `"clear"`, `"default"`
- `height`: Chart height in terminal rows (default: 40)
- `grid`: `true/false` - Show grid lines (default: true)
- `title`: Custom chart title (string)
- `marker`: `"dot"`, `"small"`, `"medium"`, `"large"` (line charts only)

Examples:
```json
{"chart_type": "bar", "theme": "dark", "height": 50}
{"chart_type": "line", "marker": "dot", "title": "Trends"}
```

**Need more details?** Call `get_documentation(topic="plotext")` for complete specs.

## Altair Backend (Vega-Lite)

Provide Vega-Lite JSON spec. Common patterns:
```json
{
  "mark": "bar",
  "encoding": {
    "x": {"field": "category", "type": "nominal"},
    "y": {"field": "value", "type": "quantitative"}
  }
}
```

**Need more details?** Call `get_documentation(topic="altair")` for complete specs.

## Plotly Backend (Interactive)

Provide Plotly spec. Example:
```json
{
  "type": "scatter",
  "mode": "lines+markers"
}
```

**Need more details?** Call `get_documentation(topic="plotly")` for complete specs.
