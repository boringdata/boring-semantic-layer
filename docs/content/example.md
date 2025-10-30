# Example: E-commerce Analytics

This example demonstrates how to use BSL for e-commerce data analysis.

## Setup Data

```orders_table
orders_tbl = ibis.memtable({
    "order_id": [1, 2, 3, 4, 5, 6, 7, 8],
    "customer": ["Alice", "Bob", "Alice", "Charlie", "Bob", "Alice", "David", "Charlie"],
    "product": ["Widget", "Gadget", "Widget", "Doohickey", "Widget", "Gadget", "Widget", "Gadget"],
    "amount": [100, 150, 100, 75, 100, 150, 100, 150],
    "quantity": [1, 2, 1, 3, 1, 2, 1, 2],
})

orders_st = (
    to_semantic_table(orders_tbl, name="orders")
    .with_dimensions(
        customer=lambda t: t.customer,
        product=lambda t: t.product,
    )
    .with_measures(
        total_orders=lambda t: t.count(),
        total_revenue=lambda t: t.amount.sum(),
        total_quantity=lambda t: t.quantity.sum(),
        avg_order_value=lambda t: t.amount.mean(),
    )
)
```

## Revenue by Customer

Let's see which customers generate the most revenue:

```revenue_by_customer
result = orders_st.group_by("customer").aggregate(
    "total_orders",
    "total_revenue",
    "avg_order_value"
)
```
<bslquery code-block="revenue_by_customer" />
Customer revenue analysis:

## Product Performance

Which products are selling best?

```product_performance
result = orders_st.group_by("product").aggregate(
    "total_orders",
    "total_quantity",
    "total_revenue"
)
```

Product performance metrics:

<bslquery code-block="product_performance" />

## Summary

This demonstrates how BSL makes it easy to:
- Define semantic models once
- Run multiple queries with different groupings
- Generate consistent metrics across analyses
