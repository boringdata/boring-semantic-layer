#!/usr/bin/env python3
"""Malloy -> BSL -> versioned xorq expression with deferred file access.

Run from the repository root with:

    uv run python examples/malloy_interop.py

No pre-existing data, external database, or Git repository is required.
"""

import tempfile
from pathlib import Path

import pandas as pd
from xorq.catalog.catalog import Catalog, CatalogAlias

from boring_semantic_layer import from_tagged, to_tagged
from boring_semantic_layer.malloy import (
    from_malloy,
    to_malloy,
    xorq_deferred_source_resolver,
)

MALLOY_MODEL = """
source: flights is duckdb.table('flights.csv') extend {
  dimension: is_long is distance >= 1000
  measure: flight_count is count()
  measure: total_distance is sum(distance)
}

run: flights -> {
  group_by: carrier
  aggregate: flight_count, total_distance
  order_by: total_distance desc
}
"""


def main() -> None:
    # Create sample input, then keep its Xorq read deferred. The temporary
    # directory only makes the example self-contained; real code can point the
    # Malloy source at an existing local path or URL.
    with tempfile.TemporaryDirectory(prefix="malloy-xorq-data-") as data_tmp:
        flights_path = Path(data_tmp) / "flights.csv"
        pd.DataFrame(
            {
                "carrier": ["AA", "AA", "UA", "UA", "DL"],
                "distance": [500, 1500, 800, 1200, 700],
            }
        ).to_csv(flights_path, index=False)

        # Malloy -> executable BSL chains backed by an Xorq Read expression.
        malloy_model = MALLOY_MODEL.replace("flights.csv", str(flights_path))
        document = from_malloy(
            malloy_model,
            source_resolver=xorq_deferred_source_resolver,
        )

        print("Malloy query executed through BSL/Xorq:")
        print(document.runs[0].execute())

        # BSL -> canonical Malloy. This also demonstrates a complete round trip.
        print("\nCanonical Malloy emitted from the BSL document:")
        print(to_malloy(document, table_paths={"flights": "flights.csv"}))

        # BSL -> xorq. The tag contains the semantic definitions as structured
        # metadata, while the expression itself remains executable by xorq.
        query_v1 = document.runs[0]
        tagged_v1 = to_tagged(query_v1)
        tagged_v2 = to_tagged(query_v1.limit(2))
        print("Tagged xorq expression:", type(tagged_v1).__name__)

        # Put two versions of the expression in a temporary Git-backed catalog.
        # Catalog entry names are derived from expression content. The stable
        # "carrier-stats" alias is first attached to v1, then advanced to v2.
        with tempfile.TemporaryDirectory(prefix="malloy-xorq-catalog-") as tmp:
            catalog = Catalog.from_repo_path(Path(tmp) / "catalog", init=True)
            project_path = Path(__file__).resolve().parents[1]

            v1 = catalog.add(
                tagged_v1,
                aliases=("carrier-stats",),
                project_path=project_path,
            )
            v1_commit = catalog.repo.head.commit

            v2 = catalog.add(tagged_v2, project_path=project_path)
            catalog.add_alias(v2.name, "carrier-stats")
            v2_commit = catalog.repo.head.commit

            print("\nGit-backed xorq catalog:")
            print(f"  v1 content id: {v1.name}")
            print(f"  v2 content id: {v2.name}")
            print(f"  current alias: carrier-stats -> {v2.name}")
            print("  commits:")
            for commit in catalog.repo.iter_commits(max_count=4):
                print(f"    {commit.hexsha[:8]} {commit.message.strip()}")

            changed = catalog.repo.git.diff(
                v1_commit.hexsha,
                v2_commit.hexsha,
                "--name-only",
            )
            print("  files changed between v1 and v2:")
            for name in changed.splitlines():
                print(f"    {name}")

            alias = CatalogAlias.from_name("carrier-stats", catalog)
            revisions = alias.list_revisions()
            print("  alias history:")
            for entry, commit in revisions:
                print(f"    {commit.hexsha[:8]} -> {entry.name}")

            # Loading through the stable alias returns a xorq expression. Restoring
            # its BSL tag gives us the semantic query, ready to execute again.
            current = catalog.get_catalog_entry("carrier-stats", maybe_alias=True)
            # Keep the loaded tagged expression alive while the restored semantic
            # wrapper executes; xorq ties extracted archive data to its lifetime.
            loaded_expr = current.expr
            restored = from_tagged(loaded_expr)
            print("\nResult loaded from the catalog alias:")
            print(restored.execute())


if __name__ == "__main__":
    main()
