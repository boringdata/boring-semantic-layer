List of JSON filter objects with the following structure:

Simple Filter:
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

IMPORTANT OPERATOR GUIDELINES:
- Equality: Use "=" (preferred), "eq", or "equals" - all work identically
- Text matching: Use "ilike" (case-insensitive) instead of "like" for better results
- List membership: "in" requires "values" field (array), NOT "value"
- Negated list: "not in" requires "values" field (array), NOT "value"
- Pattern matching: "ilike" and "not ilike" support wildcards (%, _)
- Null checks: "is null" and "is not null" need no value/values field

Available operators:
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

COMMON MISTAKES TO AVOID:
1. Don't use "value" with "in"/"not in" - use "values" array instead
2. Don't filter on measures - only filter on dimensions
3. Don't use .month(), .year() etc. - use time_grain parameter instead
4. For case-insensitive text search, prefer "ilike" over "like"

Compound Filter (AND/OR):
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

Example filters:
```json
[
    {"field": "status", "operator": "in", "values": ["active", "pending"]},
    {"field": "name", "operator": "ilike", "value": "%smith%"},
    {"field": "created_date", "operator": ">=", "value": "2024-01-01"},
    {"field": "email", "operator": "not ilike", "value": "%spam%"}
]
```

Example of a complex nested filter with time ranges:
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
