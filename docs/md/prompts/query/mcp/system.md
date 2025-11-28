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
   Example: "flights.arr_time" not just "arr_time"
4. For time-based queries, use time_grain parameter (e.g., "TIME_GRAIN_YEAR", "TIME_GRAIN_MONTH")
5. Time dimensions must be explicitly included in dimensions parameter when using time_grain

## Common Mistakes to Avoid

- Using unprefixed names in joined models (will cause errors)
- Forgetting to include time dimension in dimensions list when using time_grain
- Using invalid time grain values (must be one of: TIME_GRAIN_SECOND, TIME_GRAIN_MINUTE,
  TIME_GRAIN_HOUR, TIME_GRAIN_DAY, TIME_GRAIN_WEEK, TIME_GRAIN_MONTH, TIME_GRAIN_QUARTER, TIME_GRAIN_YEAR)

## Available Tools

- list_models: list all model names
- get_model: get model metadata (dimensions, measures, time dimensions)
- get_time_range: get available time range for time dimensions
- query_model: execute queries with time_grain, time_range, and chart_spec support
