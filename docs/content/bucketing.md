# Bucketing with 'Other'

Limit displayed group-by values while consolidating remaining items into an 'Other' category. This pattern maintains focus on top-performing segments while capturing complete data and handling long-tail distributions.

## Overview

The bucketing with 'Other' pattern allows you to:

- Focus on top N items while grouping the rest as 'Other'
- Use window functions to rank and identify top performers
- Create custom ranges for continuous values (e.g., age groups, price tiers)
- Consolidate low-frequency items into an "Other" category
- Maintain analytical clarity by reducing dimensional cardinality

## Setup

Let's create customer data with ages and purchase amounts:

```setup_raw_data
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table

# Create customer transaction data
customer_data = ibis.memtable({
    "customer_id": list(range(1, 21)),
    "age": [22, 28, 35, 42, 19, 55, 31, 67, 24, 38, 45, 29, 51, 33, 61, 26, 48, 36, 58, 41],
    "purchase_amount": [45, 120, 250, 180, 35, 520, 95, 850, 65, 310, 190, 78, 420, 145, 680, 88, 275, 165, 590, 225],
    "product_category": ["Electronics", "Clothing", "Electronics", "Home", "Clothing", "Electronics",
                        "Clothing", "Electronics", "Clothing", "Home", "Electronics", "Clothing",
                        "Home", "Clothing", "Electronics", "Clothing", "Home", "Electronics", "Electronics", "Home"]
})
```

<collapsedcodeblock code-block="setup_raw_data" title="Setup: Create Raw Customer Data"></collapsedcodeblock>

Now create a semantic table with dimensions and measures:

```semantic_table_def
from boring_semantic_layer import to_semantic_table

customer_st = (
    to_semantic_table(customer_data, name="customers")
    .with_dimensions(
        customer_id=lambda t: t.customer_id,
        age=lambda t: t.age,
        product_category=lambda t: t.product_category
    )
    .with_measures(
        customer_count=lambda t: t.count(),
        total_revenue=lambda t: t.purchase_amount.sum(),
        avg_purchase=lambda t: t.purchase_amount.mean().round(2)
    )
)
```

<collapsedcodeblock code-block="semantic_table_def" title="Setup: Define Semantic Table"></collapsedcodeblock>

## Top Categories with 'Other'

The most common bucketing pattern: show top N items by a metric, consolidate the rest as 'Other'. This uses a two-stage approach with window functions to rank items.

```query_top_categories
from ibis import _

# Two-stage pipeline: rank then consolidate
result = (
    customer_st
    .group_by("product_category")
    .aggregate("total_revenue", "customer_count")
    .mutate(
        # Rank categories by revenue
        rank=lambda t: ibis.row_number().over(
            ibis.window(order_by=t.total_revenue.desc())
        )
    )
    .mutate(
        # Replace non-top categories with "Other"
        category_display=lambda t: ibis.cases(
            (t.rank <= 2, t.product_category),
            else_="Other"
        ),
        # Keep original revenue for sorting (only for top categories)
        sort_value=lambda t: ibis.cases(
            (t.rank <= 2, t.total_revenue),
            else_=0
        )
    )
    .group_by("category_display")
    .aggregate(
        revenue=lambda t: t.total_revenue.sum(),
        customers=lambda t: t.customer_count.sum(),
        sort_helper=lambda t: t.sort_value.max()
    )
    .mutate(
        avg_per_customer=lambda t: (t.revenue / t.customers).round(2)
    )
    .order_by(_.sort_helper.desc())
)
```

<bslquery code-block="query_top_categories"></bslquery>

<note type="info">
The window function `row_number()` ranks categories by revenue. Non-top items are marked with `is_other`, then consolidated into a single 'Other' category. The `sort_helper` field ensures top categories appear first, sorted by their original revenue, with 'Other' at the end.
</note>

## Age Range Bucketing

Create age buckets using case expressions:

```query_age_buckets
from ibis import _
result = (
    customer_st
    .group_by("customer_id", "age", "product_category")
    .aggregate("total_revenue")
    .mutate(
        age_group=lambda t: ibis.cases(
            (t.age < 25, "18-24"),
            (t.age < 35, "25-34"),
            (t.age < 45, "35-44"),
            (t.age < 55, "45-54"),
            else_="55+"
        )
    )
    .group_by("age_group")
    .aggregate(
        customers=lambda t: t.count(),
        revenue=lambda t: t.total_revenue.sum()
    )
    .order_by(_.age_group)
)
```

<bslquery code-block="query_age_buckets" />

## Purchase Amount Tiers

Categorize purchases into value tiers:

```query_purchase_tiers
from ibis import _
result = (
    customer_st
    .group_by("customer_id")
    .aggregate("total_revenue")
    .mutate(
        tier=lambda t: ibis.cases(
            (t.total_revenue < 100, "Small ($0-99)"),
            (t.total_revenue < 250, "Medium ($100-249)"),
            (t.total_revenue < 500, "Large ($250-499)"),
            else_="Premium ($500+)"
        )
    )
    .group_by("tier")
    .aggregate(
        customer_count=lambda t: t.count(),
        total_value=lambda t: t.total_revenue.sum(),
        avg_value=lambda t: t.total_revenue.mean().round(2)
    )
    .order_by(_.total_value.desc())
)
```

<bslquery code-block="query_purchase_tiers" />

## Threshold-Based 'Other' Category

Instead of ranking, you can consolidate categories based on a threshold (e.g., minimum customer count):

```query_with_other
from ibis import _

result = (
    customer_st
    .group_by("product_category")
    .aggregate("total_revenue", "customer_count")
    .mutate(
        # Mark categories with less than 5 customers as "Other"
        category_grouped=lambda t: ibis.cases(
            (t.customer_count >= 5, t.product_category),
            else_="Other"
        )
    )
    .group_by("category_grouped")
    .aggregate(
        customers=lambda t: t.customer_count.sum(),
        revenue=lambda t: t.total_revenue.sum()
    )
    .mutate(
        avg_per_customer=lambda t: (t.revenue / t.customers).round(2)
    )
    .order_by(_.revenue.desc())
)
```

<bslquery code-block="query_with_other"></bslquery>

<note type="info">
This approach uses a fixed threshold rather than ranking. Categories with fewer than 5 customers are consolidated into 'Other'. This is simpler but less dynamic than the window function approach.
</note>

## Combined Bucketing

Combine age groups and purchase tiers for multi-dimensional segmentation:

```query_combined_buckets
from ibis import _
result = (
    customer_st
    .group_by("customer_id", "age")
    .aggregate("total_revenue")
    .mutate(
        age_group=lambda t: ibis.cases(
            (t.age < 30, "Young (18-29)"),
            (t.age < 50, "Middle (30-49)"),
            else_="Senior (50+)"
        ),
        value_tier=lambda t: ibis.cases(
            (t.total_revenue < 150, "Low Value"),
            (t.total_revenue < 350, "Mid Value"),
            else_="High Value"
        )
    )
    .group_by("age_group", "value_tier")
    .aggregate(
        customers=lambda t: t.count(),
        revenue=lambda t: t.total_revenue.sum()
    )
    .order_by(_.age_group, _.revenue.desc())
)
```

<bslquery code-block="query_combined_buckets" />

## Use Cases

**Focus on Top Performers**: Show top 10 products by revenue, consolidate the rest as 'Other' to highlight key items while maintaining complete totals.

**Long-Tail Distribution Management**: In e-commerce, display top categories while grouping niche categories as 'Other' to simplify reporting and dashboards.

**Threshold-Based Filtering**: Consolidate low-volume customer segments (< 100 customers) into 'Other' to focus on statistically significant groups.

**Age and Value Segmentation**: Create meaningful customer segments by combining age ranges (Young, Middle, Senior) with purchase tiers (Low, Mid, High).

## Key Takeaways

- Use window functions like `row_number()` to rank items for dynamic top-N selection
- Two-stage pattern: rank first, then consolidate and re-aggregate
- `ibis.cases((condition, value), ..., else_=default)` provides flexible bucketing logic
- Threshold-based 'Other' works well when you have a clear cutoff value
- Sort helper fields ensure 'Other' appears at the end of results
- 'Other' category maintains complete data while reducing cardinality

## Next Steps

- Learn about [Sessionized Data](/advanced/sessionized) for time-based grouping
- Explore [Indexing](/advanced/indexing) for baseline comparisons
