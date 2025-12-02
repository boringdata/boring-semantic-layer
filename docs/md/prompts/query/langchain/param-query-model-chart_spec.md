Optional dictionary that customizes the chart. Provide only the keys relevant to your backend.

## Plotext Backend (Terminal Charts)

Common options:
- `chart_type`: `"bar"` (categorical), `"line"` (trends/time), `"scatter"` (correlations)
- `theme`: `"pro"` (default), `"dark"`, `"clear"`, `"default"`
- `height`: Chart height in terminal rows (default: 40)
- `grid`: `true/false` - Show grid lines (default: true)
- `title`: Custom chart title (string)
- `marker`: `"dot"`, `"small"`, `"medium"`, `"large"` (line charts only)
- `show_chart`: `true/false` - Show chart visualization (default: true)
- `show_table`: `true/false` - Show tabular output (default: true)
- `table_limit`: Number of rows to display in table (default: 10)

**IMPORTANT: To display raw dataframe without chart visualization, use:**
```json
{"show_chart": false, "show_table": true}
```
This will show only the tabular data, which is useful when the user asks to "show the dataframe" or "display the table".

Examples:
```json
{"chart_type": "bar", "theme": "dark", "height": 50}
{"chart_type": "line", "marker": "dot", "title": "Trends"}
{"show_chart": true, "show_table": false}
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
