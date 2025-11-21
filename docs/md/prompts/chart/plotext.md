# Plotext Chart Specifications

Terminal-based charts for CLI environments. Renders directly in terminal as ASCII/Unicode art.

## Basic Spec

```json
{
  "chart_type": "bar",     // "bar", "line", "scatter"
  "theme": "pro",          // "pro", "clear", "dark", "default"
  "height": 40,            // Chart height in rows (default: 40)
  "grid": true,            // Show grid lines (default: true)
  "title": "Custom Title", // Optional chart title
  "marker": "dot"          // Line marker style (line charts only)
}
```

## Chart Types

### bar - Categorical comparisons
- Best for: Comparing categories (carriers, destinations)
- Auto-selected when: Single categorical dimension

### line - Trends over time
- Best for: Time series, continuous data
- Auto-selected when: Time-based dimension detected
- Supports `marker`: `"dot"`, `"small"`, `"medium"`, `"large"`

### scatter - Two-dimensional relationships
- Best for: Correlation, distribution patterns
- Auto-selected when: Two numeric measures

## Themes

- `"pro"` (default) - Professional, high contrast
- `"clear"` - Minimal, clean appearance
- `"dark"` - Dark mode optimized
- `"default"` - Standard colors

## Examples

```python
# Custom bar chart with dark theme
chart_spec = {"chart_type": "bar", "theme": "dark", "height": 50}

# Line chart with markers and title
chart_spec = {"chart_type": "line", "marker": "dot", "title": "Flight Trends", "height": 45}

# Tall scatter plot without grid
chart_spec = {"chart_type": "scatter", "height": 60, "grid": false}

# Minimal theme for clean look
chart_spec = {"theme": "clear", "grid": false}
```

## Display Control

Plotext backend supports toggling chart and table display with top-level flags:

```python
# Show chart only (no table)
query_model(query="...", show_chart=True, show_table=False)

# Show table only (no chart)
query_model(query="...", show_chart=False, show_table=True)

# Show both (default)
query_model(query="...", show_chart=True, show_table=True)
```

**Note**: Web backends (altair/plotly) ignore these flags and always generate charts.

## When to Use Plotext

✅ CLI/terminal environments
✅ SSH remote sessions
✅ Quick data previews
✅ No browser required
✅ Works everywhere

## Parameters Reference

### Display Flags (top-level)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `show_chart` | boolean | `true` | Display chart visualization |
| `show_table` | boolean | `true` | Display data table |
| `limit` | integer | `10` | Max table rows to display |

### Chart Spec Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `chart_type` | string | auto | `"bar"`, `"line"`, or `"scatter"` |
| `theme` | string | `"pro"` | `"pro"`, `"clear"`, `"dark"`, `"default"` |
| `height` | integer | `40` | Chart height in terminal rows |
| `grid` | boolean | `true` | Show grid lines |
| `title` | string | null | Optional chart title |
| `marker` | string | null | Marker style for line charts |
