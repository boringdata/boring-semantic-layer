Execute BSL queries and visualize results. Returns query results with optional charts.

## Sorting and Limiting

Use `.order_by()` and `.limit()` to sort and restrict results.

**CRITICAL DISTINCTION:**
- `.limit()` in query → Limits data **before** calculations (breaks window functions!)
- `limit` parameter → Only limits **table display**, full dataset processed

**For descending order, use `ibis.desc("column")`:**
```python
flights.group_by("destination").aggregate("flight_count").order_by(ibis.desc("flight_count")).limit(10)
```

**⚠️ WARNING:** Do NOT use `.limit()` with window functions - use the `limit` parameter instead!

## Window Functions (.mutate)

Post-aggregation transformations using `.mutate()` - **MUST** come after `.order_by()`.

**Quick example - rolling average:**
```python
flights.group_by("arr_week").aggregate("flight_count").order_by("arr_week").mutate(
    rolling_avg=lambda t: t.flight_count.mean().over(ibis.window(rows=(-9, 0), order_by="arr_week"))
)
```

**When user asks to "show top N rows" with window functions:**
- Use `limit` parameter (e.g., `limit=100`) to control display
- DO NOT add `.limit()` to the query - this breaks calculations!

**For complete documentation:** `get_documentation(topic="windowing")` - includes cumulative sums, lag/lead, rankings, and more.

## Chart Specifications

Override auto-detection with `chart_spec`:
```json
{"chart_type": "bar"}  // or "line", "scatter"
```

**Backends:** `"plotext"` (terminal), `"altair"` (web/Vega-Lite), `"plotly"` (web/interactive)

**For detailed specs:** Use `get_documentation(topic="plotext")`, `get_documentation(topic="altair")`, or `get_documentation(topic="plotly")`