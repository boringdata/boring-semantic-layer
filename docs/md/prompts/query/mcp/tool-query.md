# query_model Tool

Query a semantic model with support for filters and time dimensions.

## Critical Usage Rules

1. **ALWAYS call get_model() first** to see available dimensions and measures
2. **Use correct dimension/measure names**:
   - For joined models, names MUST include table prefix (e.g., "flights.arr_time")
   - Check get_model() output - if you see dots in names, the model is joined
3. **When using time_grain, MUST include the time dimension in dimensions list**:
    CORRECT: dimensions=["flights.arr_time"], time_grain="TIME_GRAIN_YEAR"
   L WRONG: dimensions=[], time_grain="TIME_GRAIN_YEAR"  # Missing time dimension!
4. **For filters with lists, use "values" (plural), not "value"**:
    CORRECT: {"field": "carrier", "operator": "in", "values": ["AA", "UA"]}
   L WRONG: {"field": "carrier", "operator": "in", "value": ["AA", "UA"]}

## Arguments

- **model_name** (string, required): Name of the model to query
- **dimensions** (list[string], optional): List of dimension names to group by (e.g., ['flights.origin', 'flights.destination'])
- **measures** (list[string], optional): List of measure names to aggregate (e.g., ['flights.flight_count', 'flights.avg_distance'])
- **filters** (list[dict], optional): List of JSON filter objects (see Filter Structure below)
- **order_by** (list[list[string]], optional): List of [field, direction] pairs for sorting (e.g., [['flights.flight_count', 'desc']])
- **limit** (integer, optional): Maximum number of rows to return
- **time_grain** (string, optional): Time grain for aggregating time-based dimensions (e.g., "TIME_GRAIN_DAY", "TIME_GRAIN_MONTH")
- **time_range** (dict, optional): Time range filter with 'start' and 'end' keys (ISO 8601 format)
- **chart_spec** (dict, optional): Chart specification for generating visualizations (see Chart Specification below)

## Filter Structure

### Simple Filter

```json
{
    "field": "dimension_name",
    "operator": "=",
    "value": "single_value"
}
```

For 'in'/'not in' operators:
```json
{
    "field": "dimension_name",
    "operator": "in",
    "values": ["val1", "val2"]
}
```

### Important Operator Guidelines

- Equality: Use "=" (preferred), "eq", or "equals" - all work identically
- Text matching: Use "ilike" (case-insensitive) instead of "like" for better results
- List membership: "in" requires "values" field (array), NOT "value"
- Negated list: "not in" requires "values" field (array), NOT "value"
- Pattern matching: "ilike" and "not ilike" support wildcards (%, _)
- Null checks: "is null" and "is not null" need no value/values field

### Available Operators

- "=" / "eq" / "equals": exact match (use "value")
- "!=": not equal (use "value")
- ">", ">=", "<", "<=": comparisons (use "value")
- "in": value is in list (use "values" array)
- "not in": value not in list (use "values" array)
- "ilike": case-insensitive pattern match (use "value" with % wildcards)
- "not ilike": negated case-insensitive pattern (use "value" with % wildcards)
- "like": case-sensitive pattern match (use "value" with % wildcards)
- "not like": negated case-sensitive pattern (use "value" with % wildcards)
- "is null": field is null (no value/values needed)
- "is not null": field is not null (no value/values needed)

### Common Mistakes to Avoid

1. Don't use "value" with "in"/"not in" - use "values" array instead
2. Don't filter on measures - only filter on dimensions
3. Don't use .month(), .year() etc. - use time_grain parameter instead
4. For case-insensitive text search, prefer "ilike" over "like"

### Compound Filter (AND/OR)

```json
{
    "operator": "AND",
    "conditions": [
        {
            "field": "country",
            "operator": "equals",
            "value": "US"
        },
        {
            "field": "tier",
            "operator": "in",
            "values": ["gold", "platinum"]
        },
        {
            "field": "name",
            "operator": "ilike",
            "value": "%john%"
        }
    ]
}
```

### Filter Examples

Simple filters:
```json
[
    {"field": "status", "operator": "in", "values": ["active", "pending"]},
    {"field": "name", "operator": "ilike", "value": "%smith%"},
    {"field": "created_date", "operator": ">=", "value": "2024-01-01"},
    {"field": "email", "operator": "not ilike", "value": "%spam%"}
]
```

Complex nested filter with time ranges:
```json
[{
    "operator": "AND",
    "conditions": [
        {
            "operator": "AND",
            "conditions": [
                {"field": "flight_date", "operator": ">=", "value": "2024-01-01"},
                {"field": "flight_date", "operator": "<", "value": "2024-04-01"}
            ]
        },
        {"field": "carrier.country", "operator": "eq", "value": "US"}
    ]
}]
```

## Time Grain

Time grain for aggregating time-based dimensions.

IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
use this time_grain parameter to aggregate by time periods. The system will
automatically handle time dimension transformations.

### Available Time Grains

- TIME_GRAIN_YEAR
- TIME_GRAIN_QUARTER
- TIME_GRAIN_MONTH
- TIME_GRAIN_WEEK
- TIME_GRAIN_DAY
- TIME_GRAIN_HOUR
- TIME_GRAIN_MINUTE
- TIME_GRAIN_SECOND

### Examples

- For monthly data: time_grain="TIME_GRAIN_MONTH"
- For yearly data: time_grain="TIME_GRAIN_YEAR"
- For daily data: time_grain="TIME_GRAIN_DAY"

Then filter using the time_range parameter or regular date filters like:
```json
{"field": "date_column", "operator": ">=", "value": "2024-01-01"}
```

## Time Range

Optional time range filter with format:
```json
{
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z"
}
```

Using time_range is preferred over using filters for time-based filtering because:
1. It automatically applies to the model's primary time dimension
2. It ensures proper time zone handling with ISO 8601 format
3. It's more concise than creating complex filter conditions
4. It works seamlessly with time_grain parameter for time-based aggregations

## Chart Specification

Chart specification dictionary for generating visualizations.

Format: `{"backend": "altair"|"plotly"|"plotext", "spec": {...}, "format": "json"|"static"|"interactive"}`

When provided, returns both data and chart: `{"records": [...], "chart": {...}}`
When None, returns only data: `{"records": [...]}`

### Backend Options

- "altair": Vega-Lite charts (default, works everywhere)
- "plotly": Plotly charts (interactive, rich features)
- "plotext": Terminal charts (ASCII, for CLI display)

### Format Options

- "json": Returns chart specification as JSON (serializable)
- "static": Returns static image (PNG/SVG)
- "interactive": Returns interactive chart object

### Spec Examples

- `{"mark": "bar"}` - Bar chart
- `{"mark": "line"}` - Line chart
- `{"mark": "point"}` - Scatter plot
- `{"title": "My Chart"}` - Add title
- `{"width": 600, "height": 400}` - Set size

### Complete Example

```json
{
    "backend": "altair",
    "spec": {"mark": "line", "title": "Monthly Trends"},
    "format": "json"
}
```

## Common Query Patterns

### Simple Aggregation

```python
query_model(
    model_name="flights",
    dimensions=["flights.carrier"],
    measures=["flights.flight_count"]
)
```

### Time-based Aggregation

MUST include time dimension in dimensions list:
```python
query_model(
    model_name="flights",
    dimensions=["flights.arr_time"],  #  REQUIRED when using time_grain
    measures=["flights.flight_count"],
    time_grain="TIME_GRAIN_YEAR",
    order_by=[["flights.arr_time", "asc"]]
)
```

### With Filters

```python
query_model(
    model_name="flights",
    dimensions=["flights.origin"],
    measures=["flights.flight_count"],
    filters=[
        {"field": "flights.carrier", "operator": "in", "values": ["AA", "UA"]},
        {"field": "flights.distance", "operator": ">", "value": 1000}
    ]
)
```

## Returns

When chart_spec is None: Query results as JSON string `{"records": [...]}`
When chart_spec is provided: JSON with both records and chart `{"records": [...], "chart": {...}}`

## Common Errors and Solutions

- "'Table' object has no attribute 'column_name'": Check column names in get_model()
- "Column 'X' not found": You used unprefixed name in joined model (add table prefix)
- "time_range requires a time dimension": Add time dimension to dimensions parameter
