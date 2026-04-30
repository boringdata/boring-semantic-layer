Time grain for aggregating time-based dimensions.

IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
use the time_grain or time_grains parameter to aggregate by time periods. The system
will automatically handle time dimension transformations.

## time_grain (string, optional)

Applies a single grain to ALL time dimensions in the query.
Both short names ("month") and long names ("TIME_GRAIN_MONTH") are accepted.

Examples:
- For monthly data: time_grain="month"
- For yearly data: time_grain="year"
- For daily data: time_grain="day"

## time_grains (dict, optional)

Applies a different grain to each time dimension independently.
Cannot be used together with time_grain.

Example — order date by month, ship date by quarter:
```json
{"time_grains": {"order_date": "month", "ship_date": "quarter"}}
```

## Available Time Grains

Short form (preferred) | Long form
-----------------------|-------------------
year                   | TIME_GRAIN_YEAR
quarter                | TIME_GRAIN_QUARTER
month                  | TIME_GRAIN_MONTH
week                   | TIME_GRAIN_WEEK
day                    | TIME_GRAIN_DAY
hour                   | TIME_GRAIN_HOUR
minute                 | TIME_GRAIN_MINUTE
second                 | TIME_GRAIN_SECOND

Then filter using the time_range parameter or regular date filters like:
{"field": "date_column", "operator": ">=", "value": "2024-01-01"}
