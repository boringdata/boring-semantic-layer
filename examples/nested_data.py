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

from pathlib import Path

import ibis
from ibis import _

from boring_semantic_layer import from_yaml

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    print("=" * 80)
    print("  Working with Nested Data - Malloy-style")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Load the GA sample table
    ga_sample = con.read_parquet(f"{BASE_URL}/ga_sample.parquet")

    print("STEP 2: Load semantic model from YAML profile")

    # Load the profile from YAML
    yaml_path = Path(__file__).parent / "ga_sessions.yaml"
    models = from_yaml(str(yaml_path), tables={"ga_sample": ga_sample})
    ga_sessions = models["ga_sessions"]

    # Note: percent_of_users with .all() is not working with nested data structures
    # See issue for details

    print(f"  Measures: {list(ga_sessions.measures)}")

    print("PART 1: Show Data by Traffic Source")

    query = (
        ga_sessions.filter(lambda t: t.source != "(direct)")
        .group_by("source")
        .aggregate(
            "user_count",
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

    query = (
        ga_sessions.group_by("browser")
        .aggregate(
            "user_count",
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
