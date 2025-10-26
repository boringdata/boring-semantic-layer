#!/usr/bin/env python3
"""Working with Nested Data - Google Analytics Pattern (Malloy-style).

This example demonstrates the Malloy-inspired approach to nested data,
using column.unnest() for inline array expansion with automatic locality tracking.

Malloy Reference: https://docs.malloydata.dev/documentation/patterns/nested_data

Key Pattern:
- In Malloy: hits.count() automatically unnests and counts
- In BSL: t.hits.unnest().count() explicitly unnests and counts
- Locality is automatically inferred and tracked internally

Malloy Model (for comparison):
```malloy
source: ga_sessions is duckdb.table('../data/ga_sample.parquet') extend {
  measure:
    user_count is count(fullVisitorId)
    percent_of_users is user_count / all(user_count)
    session_count is count()
    total_visits is totals.visits.sum()
    total_hits is totals.hits.sum()
    total_page_views is totals.pageviews.sum()
    hits_count is hits.count()
}
```
"""

import time

import ibis
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    print("=" * 80)
    print("  Working with Nested Data - Malloy-style")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # ============================================================================
    # STEP 1: Load Google Analytics sample data
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 1: Load Google Analytics sample data")
    print("-" * 80)

    ga_sessions_raw = con.read_parquet(f"{BASE_URL}/ga_sample.parquet")

    print(f"✓ Loaded {ga_sessions_raw.count().execute()} sessions")
    print("\nNested structure:")
    print("  - totals (struct): visits, hits, pageviews, etc.")
    print("  - trafficSource (struct): source, medium, campaign")
    print("  - device (struct): browser, OS, device type")
    print("  - hits (array): page views and events")
    print("    - product (array): products in each hit")

    # ============================================================================
    # STEP 2: Define Semantic Model (Experimental Malloy-style)
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 2: Define semantic model with Malloy-style measures")
    print("-" * 80)
    print("⚠️  EXPERIMENTAL: Using automatic nested array access")
    print("   Syntax: lambda t: t.hits.count() (no explicit .unnest() needed)")

    # Define the semantic model matching the Malloy example
    # The framework will automatically detect and unnest array columns
    ga_sessions = (
        to_semantic_table(ga_sessions_raw, name="ga_sessions")
        .with_measures(
            # Session-level measures (no unnesting needed)
            user_count=lambda t: t.fullVisitorId.nunique(),
            session_count=lambda t: t.count(),
            total_visits=lambda t: t.totals.visits.sum(),
            total_hits=lambda t: t.totals.hits.sum(),
            total_page_views=lambda t: t.totals.pageviews.sum(),
            # Hit-level measures (automatic unnesting! ✨)
            # The framework detects t.hits is an array and automatically unnests
            hits_count=lambda t: t.hits.count(),
            # Product-level measures (double-nested automatic unnesting! ✨✨)
            # The framework detects t.hits.product needs double unnesting
            product_count=lambda t: t.hits.product.count(),
        )
        .with_measures(
            # Calculated measure using t.all() for percent of total
            percent_of_users=lambda t: (t.user_count / t.all(t.user_count)) * 100,
        )
    )

    print("✓ Semantic model defined")
    print(f"  Measures: {list(ga_sessions.measures)}")

    # ============================================================================
    # PART 1: Show Data by Traffic Source (matching Malloy query)
    # ============================================================================
    print("\n" + "-" * 80)
    print("PART 1: Show Data by Traffic Source")
    print("-" * 80)
    print("💡 Recreating the exact Malloy query from the docs")

    # Add the source dimension
    ga_with_source = ga_sessions.with_dimensions(
        source=lambda t: t.trafficSource.source,
    )

    # Build the query (compile step)
    compile_start = time.perf_counter()
    query = (
        ga_with_source.filter(lambda t: t.source != "(direct)")
        .group_by("source")
        .aggregate(
            "user_count",
            "percent_of_users",
            "hits_count",
            "total_visits",
            "session_count",
        )
        .order_by(_.user_count.desc())
        .limit(10)
    )
    compile_time = time.perf_counter() - compile_start

    # Execute the query
    execute_start = time.perf_counter()
    result = query.execute()
    execute_time = time.perf_counter() - execute_start

    print(f"\n⏱️  Compile time: {compile_time * 1000:.2f}ms")
    print(f"⏱️  Execute time: {execute_time * 1000:.2f}ms")
    print(f"⏱️  Total time:   {(compile_time + execute_time) * 1000:.2f}ms")

    print("\nTraffic source analysis (compare to Malloy results):")
    print(result)
    print("\nExpected top source: youtube.com with ~178 users")

    # ============================================================================
    # PART 2: Show Data by Browser (with multi-level aggregation)
    # ============================================================================
    print("\n" + "-" * 80)
    print("PART 2: Show Data by Browser (with multi-level aggregation)")
    print("-" * 80)

    # Add the browser dimension
    ga_with_browser = ga_sessions.with_dimensions(
        browser=lambda t: t.device.browser,
    )

    # Build the query (compile step)
    compile_start = time.perf_counter()
    query = (
        ga_with_browser.group_by("browser")
        .aggregate(
            "user_count",
            "percent_of_users",
            "total_visits",
            "total_hits",
            "total_page_views",
            "hits_count",
            "product_count",
        )
        .order_by(_.user_count.desc())
    )
    compile_time = time.perf_counter() - compile_start

    # Execute the query
    execute_start = time.perf_counter()
    result = query.execute()
    execute_time = time.perf_counter() - execute_start

    print(f"\n⏱️  Compile time: {compile_time * 1000:.2f}ms")
    print(f"⏱️  Execute time: {execute_time * 1000:.2f}ms")
    print(f"⏱️  Total time:   {(compile_time + execute_time) * 1000:.2f}ms")

    print("\nBrowser analysis (with multi-level measures):")
    print(result)

    print("\n" + "=" * 80)
    print("Comparison with Malloy results:")
    print("=" * 80)
    print("Chrome results:")
    print(
        f"  user_count: {result[result.browser == 'Chrome'].user_count.iloc[0]} (sessions with Chrome)"
    )
    print(
        f"  hits_count: {result[result.browser == 'Chrome'].hits_count.iloc[0]} (page views/events)"
    )
    print(
        f"  product_count: {result[result.browser == 'Chrome'].product_count.iloc[0]} (all products including impressions)"
    )
    print("\nMalloy's sold_count: 642")
    print("  This is NOT product_count (37,864)")
    print("  sold_count = distinct hits where products have quantity > 0")
    print("  Requires filtered aggregations (future enhancement)")

    # ============================================================================
    # Summary
    # ============================================================================
    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\n🎯 KEY TAKEAWAYS:")
    print("  ✓ EXPERIMENTAL: Automatic nested array access (Malloy-style)")
    print("  ✓ Session-level: lambda t: t.fullVisitorId.nunique()")
    print("  ✓ Hit-level: lambda t: t.hits.count()  # Automatic unnesting!")
    print("  ✓ Product-level: lambda t: t.hits.product.count()  # Double unnesting!")
    print("  ✓ Nested fields: lambda t: t.hits.pageTitle.nunique()")
    print("  ✓ t.all() works for percent-of-total calculations")
    print("  ✓ Mixed session/hit/product measures in same query!")

    print("\n📚 Malloy vs BSL (Experimental Feature):")
    print("  Malloy:           hits.count()")
    print("  BSL equivalent:   lambda t: t.hits.count()  # No .unnest() needed!")
    print("")
    print("  Malloy:           hits.product.count()")
    print("  BSL equivalent:   lambda t: t.hits.product.count()  # Double unnest!")
    print("\n  The framework automatically detects array columns and applies")
    print("  table-level unnesting (even nested arrays!) behind the scenes.")
    print()


if __name__ == "__main__":
    main()
