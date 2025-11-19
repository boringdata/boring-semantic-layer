Time grain for aggregating time-based dimensions (e.g., "TIME_GRAIN_DAY", "TIME_GRAIN_MONTH").

IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
use this time_grain parameter to aggregate by time periods. The system will
automatically handle time dimension transformations.

Available time grains:
- TIME_GRAIN_YEAR
- TIME_GRAIN_QUARTER
- TIME_GRAIN_MONTH
- TIME_GRAIN_WEEK
- TIME_GRAIN_DAY
- TIME_GRAIN_HOUR
- TIME_GRAIN_MINUTE
- TIME_GRAIN_SECOND

Examples:
- For monthly data: time_grain="TIME_GRAIN_MONTH"
- For yearly data: time_grain="TIME_GRAIN_YEAR"
- For daily data: time_grain="TIME_GRAIN_DAY"

Then filter using the time_range parameter or regular date filters like:
{"field": "date_column", "operator": ">=", "value": "2024-01-01"}
