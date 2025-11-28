# get_time_range Tool

Get the available time range for a model's time dimension.

## Usage

Call this to understand the date/time range of data before filtering.
Useful for determining appropriate time_range filters in query_model().

## Arguments

- **model_name** (string, required): Name of the model (must have a time dimension)

## Returns

Dictionary with:
- **start**: Earliest timestamp in ISO format (e.g., "2000-01-01T00:00:00")
- **end**: Latest timestamp in ISO format (e.g., "2005-12-31T23:59:59")

## Example

```python
get_time_range("flights")
```

Returns:
```json
{
    "start": "2000-01-01T00:00:00",
    "end": "2005-12-31T23:06:00"
}
```

Then use in query_model with time_range:
```python
query_model(
    model_name="flights",
    dimensions=["flights.arr_time"],
    measures=["flights.flight_count"],
    time_grain="TIME_GRAIN_MONTH",
    time_range={"start": "2000-01-01", "end": "2000-12-31"}
)
```
