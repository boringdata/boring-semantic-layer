#!/usr/bin/env python3
"""Example showing nested joins with multiple tables in the ERD."""

import ibis

from boring_semantic_layer import SemanticModel

# Create sample tables
customers = ibis.table(
    [("customer_id", "int64"), ("name", "string"), ("country_id", "int64")], name="customers"
)

countries = ibis.table([("country_id", "int64"), ("country_name", "string")], name="countries")

orders = ibis.table(
    [("order_id", "int64"), ("customer_id", "int64"), ("amount", "float64")], name="orders"
)

# Create semantic models
customers_model = SemanticModel(
    table=customers,
    dimensions={
        "customer_id": lambda t: t.customer_id,
        "name": lambda t: t.name,
        "country_id": lambda t: t.country_id,
    },
    name="customers",
)

countries_model = SemanticModel(
    table=countries,
    dimensions={"country_id": lambda t: t.country_id, "country_name": lambda t: t.country_name},
    name="countries",
)

orders_model = SemanticModel(
    table=orders,
    dimensions={"order_id": lambda t: t.order_id, "customer_id": lambda t: t.customer_id},
    measures={"total_amount": lambda t: t.amount.sum(), "order_count": lambda t: t.order_id.count()},
    name="orders",
)

# Create a multi-table join: orders -> customers -> countries
joined_model = (
    orders_model.join_one(customers_model, on="customer_id").join_one(
        countries_model, on="country_id"
    )
)

print("=" * 100)
print("ERD: Multi-table Join (Orders -> Customers -> Countries)")
print("=" * 100)
print(joined_model.to_erd())
print()

print("=" * 100)
print("Explanation")
print("=" * 100)
print("This diagram shows a three-table join chain:")
print("  1. Orders joins to Customers on customer_id")
print("  2. Customers joins to Countries on country_id")
print()
print("The horizontal layout makes it easy to see the relationship flow:")
print("  orders ──── (customer_id = customer_id) ──── customers ──── (country_id = country_id) ──── countries")
