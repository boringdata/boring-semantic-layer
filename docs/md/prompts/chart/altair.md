# Altair Chart Specifications

Vega-Lite declarative specifications for web-based interactive charts. Returns chart objects for browsers/notebooks.

## Basic Structure

```json
{
  "mark": "bar",
  "encoding": {
    "x": {"field": "column1", "type": "nominal"},
    "y": {"field": "column2", "type": "quantitative"}
  },
  "width": 700,
  "height": 400
}
```

## Field Types

- `"nominal"` - Categorical (unordered): carrier, origin, destination
- `"ordinal"` - Categorical (ordered): priority levels, ratings
- `"quantitative"` - Numeric: counts, averages, distances
- `"temporal"` - Time-based: dates, timestamps

## Common Specifications

### Bar Chart with Sorted Axis

```json
{
  "mark": "bar",
  "encoding": {
    "x": {"field": "carrier", "type": "nominal", "sort": "-y"},
    "y": {"field": "flight_count", "type": "quantitative"}
  }
}
```

`"sort": "-y"` sorts by y-axis values descending (largest first)

### Line Chart with Points

```json
{
  "mark": {"type": "line", "point": true},
  "encoding": {
    "x": {"field": "date", "type": "temporal"},
    "y": {"field": "count", "type": "quantitative"}
  }
}
```

### Interactive Scatter with Color

```json
{
  "mark": "circle",
  "encoding": {
    "x": {"field": "distance", "type": "quantitative"},
    "y": {"field": "delay", "type": "quantitative"},
    "color": {"field": "carrier", "type": "nominal"},
    "size": {"value": 60}
  }
}
```

### Stacked Area Chart

```json
{
  "mark": "area",
  "encoding": {
    "x": {"field": "date", "type": "temporal"},
    "y": {"field": "count", "type": "quantitative"},
    "color": {"field": "carrier", "type": "nominal"}
  }
}
```

### Grouped Bar Chart

```json
{
  "mark": "bar",
  "encoding": {
    "x": {"field": "month", "type": "ordinal"},
    "y": {"field": "count", "type": "quantitative"},
    "color": {"field": "carrier", "type": "nominal"},
    "xOffset": {"field": "carrier"}
  }
}
```

## Mark Types

- `"bar"` - Bar chart
- `"line"` - Line chart
- `"area"` - Area chart (filled line)
- `"point"` - Scatter plot points
- `"circle"` - Circular marks (scatter)
- `"square"` - Square marks
- `"tick"` - Tick marks
- `"rect"` - Rectangles (heatmaps)

## Customization

### Axis Labels

```json
{
  "encoding": {
    "x": {
      "field": "carrier",
      "type": "nominal",
      "axis": {"title": "Airline Carrier"}
    }
  }
}
```

### Color Schemes

```json
{
  "encoding": {
    "color": {
      "field": "carrier",
      "type": "nominal",
      "scale": {"scheme": "category10"}
    }
  }
}
```

Popular schemes: `"category10"`, `"tableau10"`, `"viridis"`, `"plasma"`

### Tooltips

```json
{
  "encoding": {
    "tooltip": [
      {"field": "carrier", "type": "nominal"},
      {"field": "count", "type": "quantitative"}
    ]
  }
}
```

## When to Use Altair

✅ Web dashboards
✅ Jupyter notebooks
✅ Declarative specifications
✅ Vega ecosystem integration
✅ Interactive exploration

## Resources

- [Altair Gallery](https://altair-viz.github.io/gallery/)
- [Vega-Lite Documentation](https://vega.github.io/vega-lite/)
