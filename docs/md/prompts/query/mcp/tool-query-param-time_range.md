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
