# get_model Tool

Get details about a specific semantic model including dimensions and measures.

## Usage

Call this BEFORE querying to understand what dimensions and measures are available.
Pay special attention to dimension names - if they contain a dot (e.g., "flights.arr_time"),
the model is joined and you MUST use the full prefixed names in your queries.

## Arguments

- **model_name** (string, required): Name of the model (get from list_models())

## Returns

Dictionary with:
- **dimensions**: Available grouping columns with metadata
- **measures**: Available metrics with descriptions
- **calculated_measures**: Derived metrics
- **is_time_dimension**: True for time-based dimensions
- **smallest_time_grain**: Minimum time granularity for time dimensions

## Important Notes

Check dimension names carefully!
- Simple model: dimensions like "carrier", "origin"
- Joined model: dimensions like "flights.carrier", "carriers.name"
   You MUST use the prefixed names in query_model()
