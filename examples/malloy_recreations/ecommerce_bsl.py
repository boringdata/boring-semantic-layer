#!/usr/bin/env python3
"""
BSL v2 Recreation of Malloy E-commerce Model
https://github.com/malloydata/malloy-samples/blob/main/ecommerce/ecommerce.malloy

Original Malloy Model Structure:
- users: User dimension table with full_name formatting
- product: Product catalog
- inventory_items: Inventory with product join and cost tracking
- user_order_facts: Pre-aggregated user metrics (lifetime orders, spend, tiers)
- order_items: Main fact table with multiple joins and 14+ analytical views

This BSL v2 implementation demonstrates:
- Multi-table semantic layer with joins
- Computed dimensions (full_name, gross_margin, customer tiers)
- Filtered measures (sales by year, status filters)
- Multiple analytical views (top N, dashboards, YoY comparisons)
- Complex aggregations with percent of total
"""

import pandas as pd
import ibis
from boring_semantic_layer.api import to_semantic_table


def create_sample_ecommerce_data(con):
    """
    Create sample e-commerce data matching Malloy structure.
    In production, you would load from Parquet files.
    """

    # Users table
    users_df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "first_name": ["john", "jane", "bob", "alice", "charlie", "diana", "eve", "frank", "grace", "henry"],
        "last_name": ["doe", "smith", "jones", "williams", "brown", "davis", "miller", "wilson", "moore", "taylor"],
        "email": [f"user{i}@example.com" for i in range(1, 11)],
        "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
                "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
        "state": ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"],
        "country": ["USA"] * 10,
    })

    # Products table
    products_df = pd.DataFrame({
        "id": list(range(1, 21)),
        "name": [
            "iPhone 13", "MacBook Pro", "iPad Air", "AirPods Pro", "Apple Watch",
            "Galaxy S21", "Galaxy Tab", "Galaxy Buds", "Galaxy Watch", "Pixel 6",
            "Pixel Buds", "Nest Hub", "Surface Pro", "Surface Laptop", "Xbox Series X",
            "PlayStation 5", "Nintendo Switch", "Kindle", "Echo Dot", "Fire TV"
        ],
        "brand": [
            "Apple", "Apple", "Apple", "Apple", "Apple",
            "Samsung", "Samsung", "Samsung", "Samsung", "Google",
            "Google", "Google", "Microsoft", "Microsoft", "Microsoft",
            "Sony", "Nintendo", "Amazon", "Amazon", "Amazon"
        ],
        "category": [
            "Electronics", "Electronics", "Electronics", "Electronics", "Electronics",
            "Electronics", "Electronics", "Electronics", "Electronics", "Electronics",
            "Electronics", "Electronics", "Electronics", "Electronics", "Electronics",
            "Electronics", "Electronics", "Electronics", "Electronics", "Electronics"
        ],
        "department": ["Tech"] * 20,
    })

    # Inventory items table
    inventory_items_df = pd.DataFrame({
        "id": list(range(1, 101)),
        "product_id": [i % 20 + 1 for i in range(100)],
        "cost": [round(50 + (i * 13.7) % 500, 2) for i in range(100)],
        "created_at": pd.date_range("2021-01-01", periods=100, freq="3D"),
    })

    # Order items table
    order_items_df = pd.DataFrame({
        "id": list(range(1, 201)),
        "order_id": [i // 2 + 1 for i in range(200)],  # 2 items per order
        "user_id": [(i % 10) + 1 for i in range(200)],
        "inventory_item_id": [(i % 100) + 1 for i in range(200)],
        "sale_price": [round(100 + (i * 17.3) % 700, 2) for i in range(200)],
        "status": ["Completed"] * 150 + ["Cancelled"] * 25 + ["Returned"] * 25,
        "created_at": pd.date_range("2021-01-01", periods=200, freq="D"),
    })

    # Create tables
    users_tbl = con.create_table("users", users_df)
    products_tbl = con.create_table("products", products_df)
    inventory_items_tbl = con.create_table("inventory_items", inventory_items_df)
    order_items_tbl = con.create_table("order_items", order_items_df)

    return users_tbl, products_tbl, inventory_items_tbl, order_items_tbl


def create_users_semantic_table(users_tbl):
    """
    Malloy source: users is users_table extend {
      primary_key: id
      dimension: full_name is concat(...)
      measure: user_count is count()
    }
    """
    users_st = (
        to_semantic_table(users_tbl, name="users")
        .with_dimensions(
            id=lambda t: t.id,
            first_name=lambda t: t.first_name,
            last_name=lambda t: t.last_name,
            # Malloy: full_name with title case formatting
            full_name=lambda t: (
                t.first_name.substr(0, 1).upper() + t.first_name.substr(1).lower() +
                " " +
                t.last_name.substr(0, 1).upper() + t.last_name.substr(1).lower()
            ),
            email=lambda t: t.email,
            city=lambda t: t.city,
            state=lambda t: t.state,
            country=lambda t: t.country,
        )
        .with_measures(
            user_count=lambda t: t.count()
        )
    )
    return users_st


def create_products_semantic_table(products_tbl):
    """
    Malloy source: product is product_table extend {
      primary_key: id
      measure: product_count is count()
    }
    """
    products_st = (
        to_semantic_table(products_tbl, name="products")
        .with_dimensions(
            id=lambda t: t.id,
            name=lambda t: t.name,
            brand=lambda t: t.brand,
            category=lambda t: t.category,
            department=lambda t: t.department,
        )
        .with_measures(
            product_count=lambda t: t.count()
        )
    )
    return products_st


def create_user_order_facts(order_items_tbl):
    """
    Malloy source: user_order_facts - pre-aggregated user metrics.

    In Malloy this is created via:
      source: user_order_facts is order_items_table -> {
        group_by: user_id
        aggregate: first_order, most_recent_order, lifetime_orders, lifetime_spend
      } extend {
        dimension: top_customer, lifetime_order_tier, spend_category
      }
    """
    # Pre-aggregate user facts
    user_facts = order_items_tbl.group_by("user_id").aggregate([
        order_items_tbl.created_at.min().name("first_order"),
        order_items_tbl.created_at.max().name("most_recent_order"),
        order_items_tbl.count().name("lifetime_orders"),
        # Malloy: lifetime_spend excludes Cancelled & Returned
        (order_items_tbl.sale_price * (
            (order_items_tbl.status != "Cancelled") &
            (order_items_tbl.status != "Returned")
        ).cast("int")).sum().name("lifetime_spend"),
    ])

    # Add computed dimension tiers
    user_facts_st = (
        to_semantic_table(user_facts, name="user_order_facts")
        .with_dimensions(
            user_id=lambda t: t.user_id,
            first_order=lambda t: t.first_order,
            most_recent_order=lambda t: t.most_recent_order,
            lifetime_orders=lambda t: t.lifetime_orders,
            lifetime_spend=lambda t: t.lifetime_spend,
            # Malloy: top_customer is lifetime_orders > 10
            top_customer=lambda t: t.lifetime_orders > 10,
            # Malloy: lifetime_order_tier with pick/when logic
            lifetime_order_tier=lambda t: (
                ibis.case()
                .when(t.lifetime_orders < 4, "1 to 3")
                .when(t.lifetime_orders < 7, "4 to 6")
                .when(t.lifetime_orders < 11, "7 to 10")
                .else_("11+")
                .end()
            ),
            # Malloy: spend_category
            spend_category=lambda t: (
                ibis.case()
                .when(t.lifetime_spend < 50, "Low Spend")
                .when(t.lifetime_spend < 500, "Mid Spend")
                .when(t.lifetime_spend < 1000, "High Spend")
                .when(t.lifetime_spend >= 1000, "VIP")
                .else_(None)
                .end()
            ),
        )
    )
    return user_facts_st


def create_order_items_semantic_table(con, order_items_tbl, users_tbl, products_tbl, inventory_items_tbl):
    """
    Main order_items semantic table with all joins and measures.

    Malloy source: order_items is order_items_table extend {
      primary_key: id
      join_one: users, inventory_items, user_order_facts
      dimension: gross_margin
      measure: order_count, total_sales, total_gross_margin, etc.
      view: top_categories, top_brands, ...
    }
    """
    # Create joins with explicit suffixes to avoid column name collisions
    # Join inventory_items -> products first
    inventory_with_product = inventory_items_tbl.join(
        products_tbl,
        inventory_items_tbl.product_id == products_tbl.id,
        how="left",
        lname="inv_{name}",
        rname="prod_{name}"
    )

    # Join order_items with all dimension tables
    order_items_enriched = (
        order_items_tbl
        .join(users_tbl, order_items_tbl.user_id == users_tbl.id, how="left", lname="{name}", rname="user_{name}")
        .join(inventory_with_product, order_items_tbl.inventory_item_id == inventory_with_product.inv_id, how="left", lname="{name}", rname="inventory_{name}")
    )

    # Create semantic table with prefixed columns from joins
    # Note: BSL v2 now supports bracket notation for joined columns
    order_items_st = (
        to_semantic_table(order_items_enriched, name="order_items")
        .with_dimensions(
            id=lambda t: t.id,
            order_id=lambda t: t.order_id,
            user_id=lambda t: t.user_id,
            status=lambda t: t.status,
            created_at=lambda t: t.created_at,
            sale_price=lambda t: t.sale_price,
            # User dimensions (from users join - not prefixed in result)
            user_full_name=lambda t: (
                t["first_name"].substr(0, 1).upper() + t["first_name"].substr(1).lower() +
                " " +
                t["last_name"].substr(0, 1).upper() + t["last_name"].substr(1).lower()
            ),
            user_state=lambda t: t.state,
            user_city=lambda t: t.city,
            # Product dimensions (from joined inventory table)
            product_id=lambda t: t.product_id,
            product_name=lambda t: t.name,
            product_brand=lambda t: t.brand,
            product_category=lambda t: t.category,
            # Inventory cost
            inventory_cost=lambda t: t.cost,
            # Malloy: gross_margin is sale_price - inventory_items.cost
            gross_margin=lambda t: t.sale_price - t.cost,
        )
        .with_measures(
            order_item_count=lambda t: t.count(),
            order_count=lambda t: t.order_id.nunique(),
            # Malloy: total_sales
            total_sales=lambda t: t.sale_price.sum(),
            # Malloy: percent_of_sales = total_sales / all(total_sales)
            # Note: This should be calculated in mutate() after aggregation
            # Malloy: total_gross_margin
            total_gross_margin=lambda t: (t.sale_price - t.cost).sum(),
            # Malloy: average_gross_margin
            average_gross_margin=lambda t: (t.sale_price - t.cost).mean(),
            product_count=lambda t: t.product_id.nunique(),
            user_count=lambda t: t.user_id.nunique(),
            # Malloy: filtered measures for YoY
            total_sales_2022=lambda t: (t.sale_price * (t.created_at.year() == 2022).cast("int")).sum(),
            total_sales_2021=lambda t: (t.sale_price * (t.created_at.year() == 2021).cast("int")).sum(),
        )
    )

    return order_items_st


# ============================================================================
# MALLOY VIEWS - Analytical Queries
# ============================================================================

def view_top_categories(order_items_st, limit=5):
    """
    Malloy view: top_categories is {
      top: 5
      group_by: inventory_items.product_category
      aggregate: total_sales
    }
    """
    return (
        order_items_st
        .group_by("product_category")
        .aggregate("total_sales")
        .order_by(ibis.desc("total_sales"))
        .limit(limit)
    )


def view_top_brands(order_items_st, limit=5):
    """
    Malloy view: top_brands is {
      top: 5
      group_by: inventory_items.product_brand
      aggregate: total_sales, percent_of_sales, total_gross_margin, order_item_count
    }
    """
    return (
        order_items_st
        .group_by("product_brand")
        .aggregate("total_sales", "total_gross_margin", "order_item_count")
        .mutate(
            percent_of_sales=lambda t: t["total_sales"] / t.all(t["total_sales"])
        )
        .order_by(ibis.desc("total_sales"))
        .limit(limit)
    )


def view_top_products(order_items_st, limit=5):
    """
    Malloy view: top_products is {
      top: 5
      group_by: inventory_items.product_name
      aggregate: total_sales, percent_of_sales, total_gross_margin, order_item_count
    }
    """
    return (
        order_items_st
        .group_by("product_name")
        .aggregate("total_sales", "total_gross_margin", "order_item_count")
        .mutate(
            percent_of_sales=lambda t: t["total_sales"] / t.all(t["total_sales"])
        )
        .order_by(ibis.desc("total_sales"))
        .limit(limit)
    )


def view_top_customers(order_items_st, limit=5):
    """
    Malloy view: top_customers is {
      top: 5
      group_by: users.full_name, user_id
      aggregate: total_sales, order_item_count
    }
    """
    return (
        order_items_st
        .group_by("user_full_name", "user_id")
        .aggregate("total_sales", "order_item_count")
        .order_by(ibis.desc("total_sales"))
        .limit(limit)
    )


def view_by_year(order_items_st):
    """
    Malloy view: by_year is {
      group_by: created_year is year(created_at)
      aggregate: total_sales
    }
    """
    # In BSL v2, we need to add the dimension first, then group by it
    st_with_year = order_items_st.with_dimensions(
        created_year=lambda t: t.created_at.year()
    )
    return (
        st_with_year
        .group_by("created_year")
        .aggregate("total_sales")
        .order_by("created_year")
    )


def view_by_month(order_items_st):
    """
    Malloy view: by_month is {
      group_by: created_month is created_at.month
      aggregate: total_sales
    }
    """
    # In BSL v2, we need to add the dimension first, then group by it
    st_with_month = order_items_st.with_dimensions(
        created_month=lambda t: t.created_at.month()
    )
    return (
        st_with_month
        .group_by("created_month")
        .aggregate("total_sales")
        .order_by("created_month")
    )


def view_sales_by_state(order_items_st):
    """
    Malloy view: sales_by_state is {
      group_by: users.state
      aggregate: total_sales
    }
    """
    return (
        order_items_st
        .group_by("user_state")
        .aggregate("total_sales")
        .order_by(ibis.desc("total_sales"))
    )


def view_orders_by_status(order_items_st):
    """
    Malloy view: orders_by_status is {
      group_by: status
      aggregate: order_count
    }
    """
    return (
        order_items_st
        .group_by("status")
        .aggregate("order_count")
    )


def view_frequent_returners(order_items_st, limit=10):
    """
    Malloy view: frequent_returners is {
      group_by: users.full_name, user_id
      aggregate:
        count_returns is order_item_count { where: status = 'Returned'}
        percent_purchases_returned is round(100.0 * ..., 0)
        value_returned is total_sales { where: status = 'Returned'}
    }
    """
    return (
        order_items_st
        .group_by("user_full_name", "user_id")
        .aggregate(
            total_items=lambda t: t.count(),
            count_returns=lambda t: (t.status == "Returned").sum(),
            value_returned=lambda t: (t.sale_price * (t.status == "Returned").cast("int")).sum(),
        )
        .mutate(
            percent_purchases_returned=lambda t: (100.0 * t["count_returns"] / t["total_items"]).round(0)
        )
        .filter(lambda t: t["count_returns"] > 0)
        .order_by(ibis.desc("count_returns"))
        .limit(limit)
    )


def view_sales_summary_yoy(order_items_st):
    """
    Malloy view: sales_summary_yoy is {
      aggregate:
        total_sales_2022
        sales_growth is total_sales_2022 - total_sales_2021
        sales_yoy is total_sales_2022 / nullif(total_sales_2021, 0) - 1
        growth_contribution is (total_sales_2022 - total_sales_2021) / all(...)
    }
    """
    # First get overall metrics
    overall = (
        order_items_st
        .aggregate("total_sales_2022", "total_sales_2021")
        .mutate(
            sales_growth=lambda t: t["total_sales_2022"] - t["total_sales_2021"],
            sales_yoy=lambda t: (t["total_sales_2022"] / t["total_sales_2021"].nullif(0)) - 1,
        )
    )
    return overall


def demo_ecommerce_model():
    """Run all e-commerce views and display results."""
    print("=" * 80)
    print("BSL v2 Recreation of Malloy E-commerce Model")
    print("=" * 80)

    # Setup
    con = ibis.duckdb.connect(":memory:")
    users_tbl, products_tbl, inventory_items_tbl, order_items_tbl = create_sample_ecommerce_data(con)

    # Create semantic tables
    print("\nCreating semantic tables...")
    users_st = create_users_semantic_table(users_tbl)
    products_st = create_products_semantic_table(products_tbl)
    order_items_st = create_order_items_semantic_table(
        con, order_items_tbl, users_tbl, products_tbl, inventory_items_tbl
    )

    # Run views
    print("\n" + "-" * 80)
    print("View: top_categories")
    print("-" * 80)
    print(view_top_categories(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: top_brands")
    print("-" * 80)
    print(view_top_brands(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: top_products")
    print("-" * 80)
    print(view_top_products(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: top_customers")
    print("-" * 80)
    print(view_top_customers(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: by_year")
    print("-" * 80)
    print(view_by_year(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: by_month")
    print("-" * 80)
    print(view_by_month(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: sales_by_state")
    print("-" * 80)
    print(view_sales_by_state(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: orders_by_status")
    print("-" * 80)
    print(view_orders_by_status(order_items_st).execute())

    print("\n" + "-" * 80)
    print("View: frequent_returners")
    print("-" * 80)
    result = view_frequent_returners(order_items_st).execute()
    if len(result) > 0:
        print(result)
    else:
        print("No frequent returners found in sample data")

    print("\n" + "-" * 80)
    print("View: sales_summary_yoy")
    print("-" * 80)
    print(view_sales_summary_yoy(order_items_st).execute())

    print("\n" + "=" * 80)
    print("Demo Complete!")
    print("=" * 80)
    print("\nKey BSL v2 Features Demonstrated:")
    print("  ✓ Multi-table joins with semantic tables")
    print("  ✓ Computed dimensions (full_name, gross_margin, customer tiers)")
    print("  ✓ Filtered measures (year-based, status-based)")
    print("  ✓ Percent of total calculations using t.all()")
    print("  ✓ Top N queries with ordering")
    print("  ✓ YoY growth analysis")
    print("  ✓ Bracket notation for joined columns")


if __name__ == "__main__":
    demo_ecommerce_model()
