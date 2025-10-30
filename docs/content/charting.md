# Charting

BSL includes built-in support for generating data visualizations from your semantic queries. Create charts directly from query results with automatic chart type detection or full custom control.

## Installation

To use chart visualization, install with the appropriate backend:

```bash
# For Altair backend (default)
pip install 'boring-semantic-layer[viz-altair]'

# For Plotly backend
pip install 'boring-semantic-layer[viz-plotly]'
```

## Quick Start

Here's a simple example showing how to create a chart:

```setup_chart_data
import ibis
from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect(":memory:")
flights_data = ibis.memtable({
    "origin": ["JFK", "LAX", "SFO", "ORD", "DFW", "ATL", "DEN"],
    "flight_count": [150, 135, 89, 112, 98, 145, 78],
    "avg_distance": [2475, 1850, 1200, 950, 1100, 1650, 900]
})
flights_tbl = con.create_table("flights", flights_data)

flights_st = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        origin=lambda t: t.origin
    )
    .with_measures(
        flight_count=lambda t: t.flight_count.sum(),
        avg_distance=lambda t: t.avg_distance.mean()
    )
)
```

<collapsedcodeblock code-block="setup_chart_data" title="Setup: Create Sample Data"></collapsedcodeblock>

```query_basic_chart
# Query and chart in one fluent chain
result = (
    flights_st
    .group_by("origin")
    .aggregate("flight_count")
    .order_by(ibis.desc("flight_count"))
    .limit(5)
)

result.chart()
```

<altairchart code-block="query_basic_chart"></altairchart>

<note type="info">
The `.chart()` method is available on query results from `.aggregate()`, `.order_by()`, `.limit()`, and `.mutate()` operations.
</note>

## Backend Selection

BSL supports two charting backends with different strengths:

### Altair (Default)

**Best for:** Web-native interactive visualizations, declarative specifications, embedding in notebooks and web apps.

```python
# Use Altair backend (default)
chart = result.chart()
# or explicitly
chart = result.chart(backend="altair")
```

**Features:**
- Built on Vega-Lite grammar
- Declarative JSON specifications
- Great for interactive web visualizations
- Excellent notebook integration

### Plotly

**Best for:** Rich interactive dashboards, 3D visualizations, extensive chart types, business intelligence tools.

```python
# Use Plotly backend
chart = result.chart(backend="plotly")
```

**Features:**
- Extensive chart type library
- Rich interactivity out of the box
- Dashboard integration
- Export to static formats

## Auto-Detection

BSL automatically detects the appropriate chart type based on your query structure:

### Bar Chart (Categorical Data)

Single dimension + measure → Bar chart

```query_bar_chart
result = (
    flights_st
    .group_by("origin")
    .aggregate("flight_count")
    .order_by(ibis.desc("flight_count"))
)

result.chart()
```

<altairchart code-block="query_bar_chart"></altairchart>

**Auto-detected because:** Single categorical dimension (`origin`) with one measure (`flight_count`)

### Time Series (Temporal Data)

Time dimension + measure → Line chart with time-aware formatting

```setup_timeseries
import ibis
from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect(":memory:")
timeseries_data = ibis.memtable({
    "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06", "2024-01-07"],
    "flight_count": [145, 152, 148, 139, 156, 161, 143]
})
timeseries_tbl = con.create_table("daily_flights", timeseries_data)

daily_flights_st = (
    to_semantic_table(timeseries_tbl, name="daily_flights")
    .with_dimensions(
        date={
            "expr": lambda t: t.date.cast("date"),
            "is_time_dimension": True,
            "smallest_time_grain": "TIME_GRAIN_DAY"
        }
    )
    .with_measures(
        flight_count=lambda t: t.flight_count.sum()
    )
)
```

<collapsedcodeblock code-block="setup_timeseries" title="Setup: Create Time Series Data"></collapsedcodeblock>

```query_timeseries
result = (
    daily_flights_st
    .group_by("date")
    .aggregate("flight_count")
)
result.chart()
```

<altairchart code-block="query_timeseries"></altairchart>

**Auto-detected because:** Dimension marked as `is_time_dimension=True`

### Heatmap (Two Dimensions)

Two categorical dimensions + measure → Heatmap

```setup_heatmap
import ibis
from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect(":memory:")
route_data = ibis.memtable({
    "origin": ["JFK", "JFK", "LAX", "LAX", "SFO", "SFO"],
    "dest": ["LAX", "SFO", "JFK", "SFO", "JFK", "LAX"],
    "flight_count": [45, 32, 43, 28, 31, 27]
})
route_tbl = con.create_table("routes", route_data)

routes_st = (
    to_semantic_table(route_tbl, name="routes")
    .with_dimensions(
        origin=lambda t: t.origin,
        dest=lambda t: t.dest
    )
    .with_measures(
        flight_count=lambda t: t.flight_count.sum()
    )
)
```

<collapsedcodeblock code-block="setup_heatmap" title="Setup: Create Route Data"></collapsedcodeblock>

```query_heatmap
result = (
    routes_st
    .group_by("origin", "dest")
    .aggregate("flight_count")
)
result.chart()
```

<altairchart code-block="query_heatmap"></altairchart>

**Auto-detected because:** Two categorical dimensions with one measure

### Multi-Series Charts

Multiple measures → Grouped/overlaid visualization with color encoding

```query_multi_measure
result = (
    flights_st
    .group_by("origin")
    .aggregate("flight_count", "avg_distance")
    .limit(5)
)
result.chart()
```

<altairchart code-block="query_multi_measure"></altairchart>

**Auto-detected because:** Multiple measures trigger automatic color encoding by measure name


## Custom Specifications

Override auto-detection with custom specifications:

### Change Mark Type And Add Styling

Customize the mark type while providing explicit encodings:

```query_custom_mark
import ibis
# Create line chart with custom spec
result = (
    flights_st
    .group_by("origin")
    .aggregate("flight_count")
    .order_by(ibis.desc("flight_count"))
    .limit(5)
)
result.chart(spec={
    "mark": {"type": "line", "color": "#e74c3c"}
})
```

<altairchart code-block="query_custom_mark"></altairchart>

<note type="info">
You don't need to provide full vega spec: the spec object is merged with the BSL's default one.
</note>

## Export Formats

Export charts in various formats for different use cases:

```python
# Interactive chart object (default)
chart = result.chart()

# JSON specification for web embedding
json_spec = result.chart(format="json")

# PNG image (requires altair[all] or plotly)
png_bytes = result.chart(format="png")

# SVG markup (requires altair[all] or plotly)
svg_str = result.chart(format="svg")

# Save to file
with open("my_chart.png", "wb") as f:
    f.write(png_bytes)
```

**Available formats:**
- `"static"` or `"interactive"` - Chart object (default)
- `"json"` - JSON specification
- `"png"` - PNG image bytes
- `"svg"` - SVG markup string

## Next Steps

- Learn about [Query Methods](query-methods.md) to build complex queries
- Explore [YAML Configuration](yaml-config.md) for declarative semantic models
- See [Compose Models](compose.md) for joining semantic tables
