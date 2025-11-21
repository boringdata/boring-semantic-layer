Chart specification dictionary for generating visualizations.

Format: `{"backend": "altair"|"plotly"|"plotext", "spec": {...}, "format": "json"|"static"|"interactive"}`

When provided, returns both data and chart: `{"records": [...], "chart": {...}}`
When None, returns only data: `{"records": [...]}`

Backend options:
- "altair": Vega-Lite charts (default, works everywhere)
- "plotly": Plotly charts (interactive, rich features)
- "plotext": Terminal charts (ASCII, for CLI display)

Format options:
- "json": Returns chart specification as JSON (serializable)
- "static": Returns static image (PNG/SVG)
- "interactive": Returns interactive chart object

Spec examples:
- `{"mark": "bar"}` - Bar chart
- `{"mark": "line"}` - Line chart
- `{"mark": "point"}` - Scatter plot
- `{"title": "My Chart"}` - Add title
- `{"width": 600, "height": 400}` - Set size

Complete example:
```json
{
    "backend": "altair",
    "spec": {"mark": "line", "title": "Monthly Trends"},
    "format": "json"
}
```
