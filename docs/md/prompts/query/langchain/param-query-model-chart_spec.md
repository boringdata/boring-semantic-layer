Optional dictionary that customizes the chart. Provide only the keys relevant to your backend.

**Note:** Charts are automatically hidden for single-row results (e.g., a single aggregate value), since these don't benefit from visualization.

## Common Options (All Backends)

- `show_chart`: `true/false` - Show chart visualization (default: true, auto-disabled for single-row results)
- `show_table`: `true/false` - Show tabular output (default: false in API mode, true in CLI)
- `table_limit`: Number of rows to display in table (default: 10)
- `return_records`: `true/false` - Return data records to LLM (default: true). **Set to `false` for final queries to save tokens.**
- `records_limit`: Maximum records to return to LLM. **Use for discovery queries (e.g., 20-50) to save tokens.**

**Token-saving patterns:**
```json
// Discovery query - limit records, hide from user
{"show_chart": false, "show_table": false, "records_limit": 50}

// Final query - display to user, don't return records
{"return_records": false}

// Display table only (no chart)
{"show_chart": false, "show_table": true, "return_records": false}
```

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
