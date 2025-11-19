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

from ibis import _

from boring_semantic_layer import from_yaml


def main():
    print("=" * 80)
    print("  Working with Nested Data - Malloy-style")
    print("=" * 80)

    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    print("STEP 2: Use semantic model from YAML")

    # Use ga_sessions model from YAML (already has all dimensions and measures)
    ga_sessions = models["ga_sessions"]

    print(f"  Measures: {list(ga_sessions.measures)}")

    print("PART 1: Show Data by Traffic Source")

    query = (
        ga_sessions.filter(lambda t: t.source != "(direct)")
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
