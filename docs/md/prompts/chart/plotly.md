# Plotly Chart Specifications

Plotly specifications for rich interactive charts. Returns chart objects for browsers/notebooks with advanced interactivity.

## Basic Structure

```json
{
  "type": "bar",
  "mode": "lines+markers",
  "marker": {"color": "blue", "size": 10}
}
```

## Chart Types

### Bar Chart

```json
{
  "type": "bar"
}
```

### Grouped Bar Chart

```json
{
  "type": "bar",
  "barmode": "group"
}
```

### Stacked Bar Chart

```json
{
  "type": "bar",
  "barmode": "stack"
}
```

### Line Chart with Markers

```json
{
  "type": "scatter",
  "mode": "lines+markers",
  "marker": {"size": 8}
}
```

### Scatter Plot

```json
{
  "type": "scatter",
  "mode": "markers",
  "marker": {
    "size": 12,
    "color": "red",
    "opacity": 0.7
  }
}
```

### Area Chart

```json
{
  "type": "scatter",
  "fill": "tozeroy",
  "mode": "lines"
}
```

## Advanced Charts

### 3D Scatter

```json
{
  "type": "scatter3d",
  "mode": "markers",
  "marker": {
    "size": 5,
    "color": "blue",
    "opacity": 0.8
  }
}
```

### Heatmap

```json
{
  "type": "heatmap",
  "colorscale": "Viridis"
}
```

### Box Plot

```json
{
  "type": "box",
  "boxmean": true
}
```

### Histogram

```json
{
  "type": "histogram",
  "nbinsx": 20
}
```

## Marker Customization

```json
{
  "marker": {
    "size": 10,
    "color": "rgba(255, 0, 0, 0.8)",
    "line": {
      "color": "black",
      "width": 1
    },
    "symbol": "circle"  // "circle", "square", "diamond", "cross", etc.
  }
}
```

## Line Customization

```json
{
  "line": {
    "color": "blue",
    "width": 2,
    "dash": "solid"  // "solid", "dot", "dash", "dashdot"
  }
}
```

## Layout Options

While `chart_spec` defines the trace, you can also customize layout:

```json
{
  "type": "bar",
  "layout": {
    "title": "Flight Analysis",
    "xaxis": {"title": "Carrier"},
    "yaxis": {"title": "Count"},
    "showlegend": true
  }
}
```

## Color Scales

For continuous data (heatmaps, choropleth):

- `"Viridis"`, `"Plasma"`, `"Inferno"`, `"Magma"`
- `"Blues"`, `"Reds"`, `"Greens"`
- `"RdBu"`, `"RdYlGn"` (diverging)

```json
{
  "type": "heatmap",
  "colorscale": "Viridis",
  "reversescale": false
}
```

## Mode Options

For `type: "scatter"`:

- `"lines"` - Line only
- `"markers"` - Points only
- `"lines+markers"` - Both
- `"lines+markers+text"` - With labels

## When to Use Plotly

✅ Web dashboards
✅ Jupyter notebooks
✅ Advanced interactivity needed
✅ 3D visualizations
✅ Complex chart types (heatmaps, box plots, etc.)
✅ Rich hover tooltips and zoom

## Resources

- [Plotly Python Graphing Library](https://plotly.com/python/)
- [Plotly Chart Types](https://plotly.com/python/basic-charts/)
