#!/usr/bin/env python3
"""Nested Subtotals - Hierarchical Drill-Down Analysis.

Malloy: https://docs.malloydata.dev/documentation/patterns/nested_subtotals
"""
import ibis
from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")
    order_items_tbl = con.read_parquet(f"{BASE_URL}/order_items.parquet")

    order_items_with_dates = order_items_tbl.mutate(
        created_year=order_items_tbl.created_at.year(),
        created_month=order_items_tbl.created_at.month(),
    )

    order_items = to_semantic_table(order_items_with_dates, name="order_items").with_measures(
        order_count=lambda t: t.count(),
        total_sales=lambda t: t.sale_price.sum(),
        avg_price=lambda t: t.sale_price.mean(),
    )

    sales_by_year = (
        order_items.group_by("created_year")
        .aggregate("order_count", "total_sales")
        .order_by("created_year")
        .execute()
    )
    print("\nSales by year:")
    print(sales_by_year)

    sales_by_year_month = (
        order_items.group_by("created_year", "created_month")
        .aggregate("order_count", "total_sales")
        .order_by("created_year", "created_month")
        .limit(15)
        .execute()
    )
    print("\nSales by year and month:")
    print(sales_by_year_month)

    sales_by_year_status = (
        order_items.group_by("created_year", "status")
        .aggregate("order_count", "total_sales", "avg_price")
        .order_by("created_year", "total_sales")
        .limit(15)
        .execute()
    )
    print("\nSales by year and status:")
    print(sales_by_year_status)


if __name__ == "__main__":
    main()
