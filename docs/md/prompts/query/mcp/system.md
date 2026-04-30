# MCP Semantic Model Server

MCP server specialized for semantic models using SemanticTable.

This server provides a semantic layer for querying structured data with support for:
- Dimensions (columns to group by)
- Measures (metrics to aggregate)
- Time-based aggregations with configurable grains
- Filtering with various operators
- Joins across multiple tables

## Important Usage Guidelines for LLM

1. ALWAYS start by calling list_models() to see available models
2. ALWAYS call get_model(model_name) to understand dimensions and measures before querying
3. When using joined models (multiple tables), ALWAYS prefix dimension/measure names with table name
   Example: "orders.created_at" not just "created_at"
4. For time-based queries, use time_grain (e.g., "month", "year") to apply one grain to all time dimensions,
   or time_grains (e.g., {"order_date": "month", "ship_date": "quarter"}) for per-dimension grains
5. Time dimensions must be explicitly included in dimensions parameter when using time_grain/time_grains

## Common Mistakes to Avoid

- Using unprefixed names in joined models (will cause errors)
- Forgetting to include time dimension in dimensions list when using time_grain
- Using invalid time grain values (valid: second, minute, hour, day, week, month, quarter, year)

## Available Tools

- list_models: list all model names
- get_model: get model metadata (dimensions, measures, time dimensions)
- get_time_range: get available time range for time dimensions
- query_model: execute queries with time_grain, time_range, and chart_spec support
- compare_periods: compare two explicit time ranges and return current/previous/delta/pct_change columns
