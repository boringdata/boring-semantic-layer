"""Soundness regressions for automatic nested-array aggregation."""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.nested_compile import unnest_nested_arrays


def test_nested_array_path_preserves_declared_traversal_order():
    """Array names must not be alphabetized before they are unnested."""
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table(
        "nested_path_order",
        pd.DataFrame(
            {
                "session_id": [1, 2],
                # The child name sorts before the parent name.  Sorting the
                # path would try to unnest ``aproducts`` before ``zhits``.
                "zhits": [
                    [{"aproducts": [1, 2]}, {"aproducts": [3]}],
                    [{"aproducts": [4, 5]}],
                ],
            }
        ),
    )
    model = (
        to_semantic_table(table, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(product_count=lambda t: t.zhits.aproducts.count())
    )

    result = (
        model.group_by("session_id").aggregate("product_count").execute().set_index("session_id")
    )

    assert result.loc[1, "product_count"] == 3
    assert result.loc[2, "product_count"] == 2


def test_nested_array_path_ignores_colliding_top_level_child_name():
    """A nested child must not resolve to an unrelated top-level array."""
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table(
        "nested_path_collision",
        pd.DataFrame(
            {
                "session_id": [1, 2],
                # This sibling deliberately has the same name as the child of
                # ``events``. Its lengths differ from the nested ground truth.
                "products": [[900], [800, 801, 802]],
                "events": [
                    [{"products": [1, 2]}, {"products": [3]}],
                    [{"products": [4, 5]}],
                ],
            }
        ),
    )
    model = (
        to_semantic_table(table, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(product_count=lambda t: t.events.products.count())
    )

    result = (
        model.group_by("session_id")
        .aggregate("product_count")
        .execute()
        .sort_values("session_id")
    )

    assert result["product_count"].tolist() == [3, 2]


@pytest.mark.parametrize(
    ("measure_fn", "requested_path", "message"),
    [
        (
            lambda t: t.events.missing.count(),
            "events.missing",
            "does not exist",
        ),
        (
            lambda t: t.events.products.missing.count(),
            "events.products.missing",
            "does not exist",
        ),
        (
            lambda t: t.events.label.count(),
            "events.label",
            "is not an array",
        ),
    ],
    ids=["missing-child", "missing-deep-child", "non-array-child"],
)
def test_invalid_nested_array_child_fails_closed(
    measure_fn, requested_path, message
):
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table(
        "nested_invalid_child",
        pd.DataFrame(
            {
                "session_id": [1, 2],
                "events": [
                    [
                        {"products": [1, 2], "label": "a"},
                        {"products": [3], "label": "b"},
                    ],
                    [{"products": [4], "label": "c"}],
                ],
            }
        ),
    )
    model = (
        to_semantic_table(table, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(invalid_count=measure_fn)
    )

    with pytest.raises(ValueError) as exc_info:
        model.group_by("session_id").aggregate("invalid_count").execute()

    error = str(exc_info.value)
    assert requested_path in error
    assert message in error


@pytest.mark.parametrize(
    ("array_path", "message"),
    [(("missing",), "does not exist"), (("scalar",), "is not an array")],
)
def test_invalid_nested_array_root_fails_closed(array_path, message):
    table = ibis.memtable({"scalar": [1, 2]})

    with pytest.raises(ValueError) as exc_info:
        unnest_nested_arrays(table, array_path)

    error = str(exc_info.value)
    assert ".".join(array_path) in error
    assert message in error


def test_nested_only_query_retains_groups_with_empty_or_null_arrays():
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table(
        "nested_empty_groups",
        pd.DataFrame(
            {
                "category": ["filled", "empty", "null"],
                "hits": [[1, 2, 2], [], None],
            }
        ),
    )
    model = (
        to_semantic_table(table, name="sessions")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(
            hit_count=lambda t: t.hits.count(),
            unique_hits=lambda t: t.hits.nunique(),
            total_hits=lambda t: t.hits.sum(),
        )
    )

    result = (
        model.group_by("category")
        .aggregate("hit_count", "unique_hits", "total_hits")
        .execute()
        .set_index("category")
    )

    assert set(result.index) == {"filled", "empty", "null"}
    assert result.loc["filled", "hit_count"] == 3
    assert result.loc["filled", "unique_hits"] == 2
    assert result.loc["filled", "total_hits"] == 5
    for category in ("empty", "null"):
        assert result.loc[category, "hit_count"] == 0
        assert result.loc[category, "unique_hits"] == 0
        assert pd.isna(result.loc[category, "total_hits"])


def test_empty_nested_groups_are_retained_when_regular_measures_are_selected():
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table(
        "nested_empty_groups_mixed",
        pd.DataFrame(
            {
                "category": ["filled", "empty"],
                "hits": [[1, 2], []],
            }
        ),
    )
    model = (
        to_semantic_table(table, name="sessions")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(
            session_count=lambda t: t.count(),
            hit_count=lambda t: t.hits.count(),
        )
    )

    result = (
        model.group_by("category")
        .aggregate("session_count", "hit_count")
        .execute()
        .set_index("category")
    )

    assert result.loc["filled", "session_count"] == 1
    assert result.loc["filled", "hit_count"] == 2
    assert result.loc["empty", "session_count"] == 1
    assert result.loc["empty", "hit_count"] == 0
