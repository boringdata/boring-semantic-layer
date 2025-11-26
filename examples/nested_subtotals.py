#!/usr/bin/env python3
"""Nested Subtotals - Hierarchical Drill-Down Analysis.

Malloy: https://docs.malloydata.dev/documentation/patterns/nested_subtotals

This example demonstrates hierarchical drill-down patterns using YAML-defined
semantic models. The semantic model (dimensions and measures) is defined in
order_items.yml, and this script focuses on query patterns.
"""

from pathlib import Path

from boring_semantic_layer import from_yaml


def main():
    # Load semantic model from YAML (includes profile connection)
    yaml_path = Path(__file__).parent / "order_items.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Get the order_items model (already has all dimensions and measures)
    order_items = models["order_items"]

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
