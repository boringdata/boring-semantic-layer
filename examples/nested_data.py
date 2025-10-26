#!/usr/bin/env python3
"""Working with Nested Data - Google Analytics Pattern.

Malloy Reference: https://docs.malloydata.dev/documentation/patterns/nested_data

Malloy Model:
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

import ibis
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    print("=" * 80)
    print("  Working with Nested Data - Malloy-style")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    ga_sessions_raw = con.read_parquet(f"{BASE_URL}/ga_sample.parquet")

    print("STEP 2: Define semantic model with Malloy-style measures")

    ga_sessions = (
        to_semantic_table(ga_sessions_raw, name="ga_sessions")
        .with_measures(
            user_count=lambda t: t.fullVisitorId.nunique(),
            session_count=lambda t: t.count(),
            total_visits=lambda t: t.totals.visits.sum(),
            total_hits=lambda t: t.totals.hits.sum(),
            total_page_views=lambda t: t.totals.pageviews.sum(),
            hits_count=lambda t: t.hits.count(),
            product_count=lambda t: t.hits.product.count(),
        )
        .with_measures(
            percent_of_users=lambda t: (t.user_count / t.all(t.user_count)) * 100,
        )
    )

    print(f"  Measures: {list(ga_sessions.measures)}")

    print("PART 1: Show Data by Traffic Source")

    ga_with_source = ga_sessions.with_dimensions(
        source=lambda t: t.trafficSource.source,
    )

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

    result = query.execute()

    print(result)

    print("PART 2: Show Data by Browser (with multi-level aggregation)")

    ga_with_browser = ga_sessions.with_dimensions(
        browser=lambda t: t.device.browser,
    )

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

    result = query.execute()

    print(result)


if __name__ == "__main__":
    main()
