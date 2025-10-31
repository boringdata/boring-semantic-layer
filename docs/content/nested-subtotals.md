# Nested Subtotals

Create hierarchical aggregations with subtotals at multiple levels using the `nest` parameter. This pattern enables drill-down analysis where each row contains both summary metrics and nested breakdowns.

## Overview

The nested subtotals pattern allows you to:

- Generate subtotals at each level of a dimensional hierarchy in a single query
- Create nested structures where each parent row contains child breakdowns
- Avoid complex self-joins or ROLLUP queries
- Build hierarchical data suitable for tree views and drill-down UIs

## Setup

Create a sample order items dataset with temporal and categorical dimensions:

```setup_data
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table

# Create synthetic order items data
order_items_data = ibis.memtable({
    "order_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010,
                 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020,
                 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030],
    "sale_price": [45.99, 89.50, 120.00, 34.99, 67.80, 99.99, 54.50, 78.99, 150.00, 42.00,
                   55.99, 72.50, 88.80, 110.00, 39.99, 95.00, 62.50, 81.99, 125.00, 48.50,
                   66.99, 92.00, 105.50, 73.99, 58.80, 118.00, 84.50, 69.99, 135.00, 51.50],
    "status": ["shipped", "delivered", "shipped", "processing", "delivered",
               "shipped", "cancelled", "delivered", "shipped", "processing",
               "delivered", "shipped", "delivered", "processing", "shipped",
               "cancelled", "delivered", "shipped", "delivered", "processing",
               "shipped", "delivered", "shipped", "processing", "delivered",
               "shipped", "cancelled", "delivered", "shipped", "processing"],
    "created_year": [2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022,
                     2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023,
                     2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024],
    "created_month": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5,
                      1, 1, 2, 2, 3, 3, 4, 4, 5, 5,
                      1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
})

# Create semantic table with measures
order_items = to_semantic_table(
    order_items_data,
    name="order_items",
).with_measures(
    order_count=lambda t: t.count(),
    total_sales=lambda t: t.sale_price.sum(),
    avg_price=lambda t: t.sale_price.mean(),
)
```

<collapsedcodeblock code-block="setup_data" title="Setup: Create Order Items Data"></collapsedcodeblock>

## Year with Nested Month Subtotals

Create yearly totals with monthly breakdowns nested inside each year:

```query_year_with_months
from ibis import _

# First aggregate by year and month to get monthly subtotals
monthly_data = (
    order_items
    .group_by("created_year", "created_month")
    .aggregate("order_count", "total_sales")
)

# Then nest months within years
result = (
    monthly_data
    .group_by("created_year")
    .aggregate(
        year_order_count=lambda t: t.order_count.sum(),
        year_total_sales=lambda t: t.total_sales.sum(),
        nest={"by_month": lambda t: t.group_by(["created_month", "order_count", "total_sales"]).order_by("created_month")}
    )
    .order_by("created_year")
)
```

<bslquery code-block="query_year_with_months"></bslquery>

<note type="info">
Each year row contains a `by_month` array with all monthly subtotals for that year. The pattern is: aggregate at the finest level first, then nest at each parent level.
</note>

## Year with Nested Status Subtotals

Alternative breakdown: nest order status within each year:

```query_year_with_status
from ibis import _

# First aggregate by year and status
status_data = (
    order_items
    .group_by("created_year", "status")
    .aggregate("order_count", "total_sales", "avg_price")
)

# Then nest status within years
result = (
    status_data
    .group_by("created_year")
    .aggregate(
        year_order_count=lambda t: t.order_count.sum(),
        year_total_sales=lambda t: t.total_sales.sum(),
        nest={"by_status": lambda t: t.group_by(["status", "order_count", "total_sales", "avg_price"]).order_by(_.total_sales.desc())}
    )
    .order_by("created_year")
)
```

<bslquery code-block="query_year_with_status"></bslquery>

## Multi-Level Nesting: Year > Month > Status

Create three-level hierarchy with nested subtotals:

```query_multi_level
from ibis import _

# First aggregate at the finest level: year, month, status
detailed_data = (
    order_items
    .group_by("created_year", "created_month", "status")
    .aggregate("order_count", "total_sales")
)

# Second level: nest status within month
monthly_with_status = (
    detailed_data
    .group_by("created_year", "created_month")
    .aggregate(
        month_order_count=lambda t: t.order_count.sum(),
        month_total_sales=lambda t: t.total_sales.sum(),
        nest={"by_status": lambda t: t.group_by(["status", "order_count", "total_sales"])}
    )
)

# Top level: nest months within year
result = (
    monthly_with_status
    .group_by("created_year")
    .aggregate(
        year_order_count=lambda t: t.month_order_count.sum(),
        year_total_sales=lambda t: t.month_total_sales.sum(),
        nest={"by_month": lambda t: t.group_by(["created_month", "month_order_count", "month_total_sales", "by_status"]).order_by("created_month")}
    )
    .order_by("created_year")
    .limit(3)
)
```

<bslquery code-block="query_multi_level"></bslquery>

## Use Cases

**Financial Reporting**: Create income statements with nested line items - show total revenue with product categories nested inside, each containing individual products.

**Geographic Hierarchies**: Aggregate sales by region, with nested states, with nested cities, all in a single query result.

**Time-Based Drill-Downs**: Show yearly summaries with monthly breakdowns nested inside, perfect for dashboard drill-down interactions.

**Organizational Analysis**: Display department totals with nested team breakdowns, with nested individual employee details.

## Key Takeaways

- Use the `nest` parameter in `.aggregate()` to create hierarchical subtotals
- Each parent row contains an array column with child-level breakdowns
- Avoid complex SQL ROLLUP or self-join patterns
- Nest multiple levels deep for complex hierarchies
- Perfect for building tree views, expandable tables, and drill-down UIs

## Next Steps

- Learn about [Percentage of Total](/advanced/percentage-total) calculations
- Explore [Advanced Nesting](/advanced/nesting) for more complex hierarchical patterns
