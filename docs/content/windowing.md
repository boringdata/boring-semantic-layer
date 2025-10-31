# Window Functions

Perform calculations across ordered rows using window functions like running totals, moving averages, rank, lag/lead, and more. Window functions operate on query results after aggregation, enabling powerful comparative and analytical operations.

## Overview

Window functions allow you to:

- **Compare rows**: Calculate differences between current and previous rows (lag/lead)
- **Running calculations**: Compute cumulative sums and running averages
- **Ranking**: Assign ranks, row numbers, and percentiles
- **Moving windows**: Calculate metrics over sliding time windows

<note type="info">
Window functions in BSL are applied using Ibis window operations on aggregated results. They execute logically after the aggregation stage.
</note>

## Setup

Create a synthetic sales dataset with daily revenue data:

```setup_data
import ibis
from ibis import _
from datetime import datetime, timedelta
import random

# Create daily sales data spanning 90 days
start_date = datetime(2024, 1, 1)
dates = [start_date + timedelta(days=i) for i in range(90)]

# Generate synthetic revenue with upward trend and weekly patterns
random.seed(42)

revenue_values = []
for i, date in enumerate(dates):
    # Base trend: increasing over time
    base = 1000 + (i * 10)

    # Weekly pattern: weekends have higher sales
    weekday_multiplier = 1.3 if date.weekday() >= 5 else 1.0

    # Random variation
    noise = random.uniform(-100, 100)

    revenue = base * weekday_multiplier + noise
    revenue_values.append(round(revenue, 2))

# Create table
sales_data = ibis.memtable({
    "sale_date": dates,
    "revenue": revenue_values,
    "product_category": ["Electronics" if i % 3 == 0 else "Clothing" if i % 3 == 1 else "Home" for i in range(90)],
})
```

<collapsedcodeblock code-block="setup_data" title="Setup: Create Daily Sales Data"></collapsedcodeblock>

```setup_st
from boring_semantic_layer import to_semantic_table

# Create semantic table with measures
sales_st = to_semantic_table(
    sales_data,
    name="daily_sales"
).with_measures(
    total_revenue=lambda t: t.revenue.sum(),
    avg_revenue=lambda t: t.revenue.mean(),
    sale_count=lambda t: t.count(),
)
```

<collapsedcodeblock code-block="setup_st" title="Setup: Define Semantic Table"></collapsedcodeblock>

## Lag and Lead: Comparing to Previous/Next Rows

Calculate period-over-period changes by comparing current values to previous rows:

```query_lag_lead
from ibis import _

# Aggregate daily revenue
daily_revenue = (
    sales_st
    .group_by("sale_date")
    .aggregate("total_revenue")
    .order_by("sale_date")
)

# Add window functions for lag/lead
result = daily_revenue.mutate(
    prev_day_revenue=_.total_revenue.lag(),
    next_day_revenue=_.total_revenue.lead(),
    day_over_day_change=_.total_revenue - _.total_revenue.lag(),
    pct_change=((_.total_revenue - _.total_revenue.lag()) / _.total_revenue.lag() * 100).round(2)
).limit(10)
```

<bslquery code-block="query_lag_lead"></bslquery>

<note type="info">
`lag()` accesses the previous row's value, while `lead()` accesses the next row's value. The first row's lag and last row's lead will be null.
</note>

## Running Totals: Cumulative Calculations

Compute running sums to track cumulative metrics over time:

```query_running_total
from ibis import _

# Daily revenue with cumulative total
daily_revenue = (
    sales_st
    .group_by("sale_date")
    .aggregate("total_revenue")
    .order_by("sale_date")
)

# Calculate cumulative sum and running average
window_unbounded = ibis.window(rows=(None, 0), order_by="sale_date")

result = daily_revenue.mutate(
    cumulative_revenue=_.total_revenue.cumsum(),
    days_count=lambda t: t.count().over(window_unbounded),
    avg_daily_so_far=lambda t: (t.cumulative_revenue / t.days_count).round(2)
).limit(10)
```

<bslquery code-block="query_running_total"></bslquery>

## Moving Averages: Sliding Window Calculations

Calculate metrics over a rolling window of rows:

```query_moving_average
from ibis import _

# Daily revenue
daily_revenue = (
    sales_st
    .group_by("sale_date")
    .aggregate("total_revenue")
    .order_by("sale_date")
)

# 7-day moving average
window_7d = ibis.window(rows=(-6, 0), order_by="sale_date")

result = daily_revenue.mutate(
    ma_7day=_.total_revenue.mean().over(window_7d).round(2),
    ma_7day_sum=_.total_revenue.sum().over(window_7d).round(2),
).limit(10)
```

<bslquery code-block="query_moving_average"></bslquery>

<note type="info">
The window specification `rows=(-6, 0)` means "6 rows before the current row through the current row" (7 total rows). The moving average smooths out daily volatility.
</note>

## Ranking: Assign Positions

Rank rows based on values:

```query_ranking
from ibis import _

# Aggregate by product category
category_revenue = (
    sales_st
    .group_by("product_category")
    .aggregate("total_revenue", "sale_count")
    .order_by(_.total_revenue.desc())
)

# Add rank columns
result = category_revenue.mutate(
    rank=ibis.rank().over(ibis.window(order_by=_.total_revenue.desc())),
    dense_rank=ibis.dense_rank().over(ibis.window(order_by=_.total_revenue.desc())),
    row_number=ibis.row_number().over(ibis.window(order_by=_.total_revenue.desc())),
)
```

<bslquery code-block="query_ranking"></bslquery>

<note type="info">
`row_number()` assigns unique sequential numbers, `rank()` assigns the same rank to ties (skipping next ranks), and `dense_rank()` assigns the same rank to ties without gaps.
</note>

## Week-over-Week Comparison

Compare metrics across weekly periods:

```query_week_over_week
from ibis import _

# Aggregate by week
weekly_revenue = (
    sales_st
    .mutate(week_start=_.sale_date.truncate("W"))
    .group_by("week_start")
    .aggregate("total_revenue")
    .order_by("week_start")
)

# Calculate week-over-week changes
result = weekly_revenue.mutate(
    prev_week_revenue=_.total_revenue.lag(),
    wow_change=_.total_revenue - _.total_revenue.lag(),
    wow_pct_change=((_.total_revenue - _.total_revenue.lag()) / _.total_revenue.lag() * 100).round(2)
).limit(10)
```

<bslquery code-block="query_week_over_week"></bslquery>

## Percent of Running Total

Calculate each row's contribution to the cumulative total:

```query_pct_running
from ibis import _

# Top 10 days by revenue
top_days = (
    sales_st
    .group_by("sale_date")
    .aggregate("total_revenue")
    .order_by(_.total_revenue.desc())
    .limit(10)
)

# Calculate cumulative percentage
result = top_days.mutate(
    cumulative_revenue=_.total_revenue.cumsum(),
    total_top10=_.total_revenue.sum(),
    pct_of_top10=(_.total_revenue.cumsum() / _.total_revenue.sum() * 100).round(2)
)
```

<bslquery code-block="query_pct_running"></bslquery>

## Moving Window with Filters

Combine window functions with filtering for focused analysis:

```query_window_filter
from ibis import _

# Focus on weekends only
weekend_revenue = (
    sales_st
    .mutate(is_weekend=_.sale_date.day_of_week.index().isin([5, 6]))
    .filter(_.is_weekend)
    .group_by("sale_date")
    .aggregate("total_revenue")
    .order_by("sale_date")
)

# 3-weekend moving average
window_3 = ibis.window(rows=(-2, 0), order_by="sale_date")

result = weekend_revenue.mutate(
    ma_3weekend=_.total_revenue.mean().over(window_3).round(2),
    prev_weekend=_.total_revenue.lag(),
    weekend_change=_.total_revenue - _.total_revenue.lag()
).limit(10)
```

<bslquery code-block="query_window_filter"></bslquery>

## Key Takeaways

- **Window functions operate after aggregation**: They work on query results, not raw data
- **Order matters**: Most window functions require `order_by()` for meaningful results
- **Flexible windows**: Define windows by rows (`rows=(n, m)`) or ranges
- **Common patterns**:
  - `lag()/lead()` for period-over-period comparisons
  - `cumsum()` for running totals
  - `.over(window)` for moving averages
  - `rank()`, `row_number()` for ranking
- **Combine with filters**: Focus window calculations on specific subsets

## Next Steps

- Explore [Percentage of Total](/advanced/percentage-total) for ratio calculations
- Learn about [Nested Subtotals](/advanced/nested-subtotals) for hierarchical aggregations
- Check out [Nesting](/advanced/nesting) for complex data structures
