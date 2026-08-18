#!/usr/bin/env python3
"""Nested Queries - Hierarchical Results with nest=.

Malloy Reference: https://docs.malloydata.dev/documentation/language/nesting

Replicates the canonical Malloy nesting example, producing one result set
with two levels of nested subtables (states -> top 5 counties -> facility
types):

```malloy
run: airports -> {
  group_by: state
  aggregate: airport_count
  nest: top_5_counties is {
    limit: 5
    group_by: county
    aggregate: airport_count
    nest: by_facility is {
      group_by: fac_type
      aggregate: airport_count
    }
  }
}
```

Each `nest:` block maps to a `nest={name: lambda t: ...}` entry whose lambda
is a full semantic pipeline evaluated at the enclosing group's grain, so
`order_by`/`limit` apply per group and nests compose recursively. Malloy
orders every level by the first aggregate descending by default; BSL spells
that out explicitly.
"""

import ibis
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")
    airports_tbl = con.read_parquet(f"{BASE_URL}/airports.parquet")

    airports = (
        to_semantic_table(airports_tbl, name="airports")
        .with_dimensions(
            state=_.state,
            county=_.county,
            fac_type=_.fac_type,
        )
        .with_measures(airport_count=_.count())
    )

    result = (
        airports.group_by("state")
        .aggregate(
            "airport_count",
            nest={
                "top_5_counties": lambda t: (
                    t.group_by("county")
                    .aggregate(
                        "airport_count",
                        nest={
                            "by_facility": lambda t: (
                                t.group_by("fac_type")
                                .aggregate("airport_count")
                                .order_by(lambda t: t.airport_count.desc())
                            )
                        },
                    )
                    .order_by(lambda t: t.airport_count.desc())
                    .limit(5)
                )
            },
        )
        .order_by(lambda t: t.airport_count.desc())
        .execute()
    )

    # Each row holds a list of county structs, each with its own nested
    # by_facility list -- render the hierarchy as an indented tree.
    for _idx, row in result.head(5).iterrows():
        print(f"{row['state']}  airport_count={row['airport_count']}")
        for county in row["top_5_counties"]:
            print(f"    {county['county']:<16} {county['airport_count']}")
            for fac in county["by_facility"]:
                print(f"        {fac['fac_type']:<18} {fac['airport_count']}")
    print(f"\n({len(result)} states total)")


if __name__ == "__main__":
    main()
