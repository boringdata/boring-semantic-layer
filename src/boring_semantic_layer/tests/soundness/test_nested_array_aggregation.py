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
        model.group_by("session_id").aggregate("product_count").execute().sort_values("session_id")
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
def test_invalid_nested_array_child_fails_closed(measure_fn, requested_path, message):
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


@pytest.fixture
def joined_nested_model():
    """A nested source plus a sibling many-side arm that would fan it out."""
    con = ibis.duckdb.connect(":memory:")
    users_table = con.create_table(
        "joined_nested_users",
        pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "segment": ["A", "A", "B", "B", "C"],
            }
        ),
    )
    sessions_table = con.create_table(
        "joined_nested_sessions",
        pd.DataFrame(
            {
                "session_id": [10, 11, 12, 13, 14],
                "user_id": [1, 1, 2, 3, 4],
                "kind": ["x", "y", None, "x", "x"],
                "hits": [[1, 2, 2], [3], [], [10, 10], None],
            }
        ),
    )
    purchases_table = con.create_table(
        "joined_nested_purchases",
        pd.DataFrame(
            {
                "purchase_id": list(range(20, 28)),
                "user_id": [1, 1, 1, 2, 2, 3, 4, 4],
                "session_id": [10, 10, 11, 12, 12, 13, 14, 14],
                "spend": [10.0, 20.0, 5.0, 7.0, 8.0, 11.0, 13.0, 17.0],
            }
        ),
    )

    users = to_semantic_table(users_table, "users").with_dimensions(segment=lambda t: t.segment)
    sessions = (
        to_semantic_table(sessions_table, "sessions")
        .with_dimensions(kind=lambda t: t.kind)
        .with_measures(
            hit_count=lambda t: t.hits.count(),
            hit_sum=lambda t: t.hits.sum(),
            hit_mean=lambda t: t.hits.mean(),
            unique_hits=lambda t: t.hits.nunique(),
        )
        .with_measures(
            double_hits=lambda t: t.hit_count * 2,
            hit_share=lambda t: t.hit_count / t.all(t.hit_count) * 100,
        )
    )
    purchases = to_semantic_table(purchases_table, "purchases").with_measures(
        spend=lambda t: t.spend.sum()
    )
    joined = users.join_many(sessions, on="user_id").join_many(purchases, on="user_id")
    return joined, sessions, purchases


def test_joined_nested_measures_compile_at_exact_cross_source_grain(
    joined_nested_model,
):
    joined, _sessions, _purchases = joined_nested_model

    result = (
        joined.group_by("users.segment")
        .aggregate(
            "sessions.hit_count",
            "sessions.hit_sum",
            "sessions.hit_mean",
            "sessions.unique_hits",
            "sessions.double_hits",
            "sessions.hit_share",
            "purchases.spend",
        )
        .execute()
        .set_index("users.segment")
    )

    assert result.loc["A", "sessions.hit_count"] == 4
    assert result.loc["A", "sessions.hit_sum"] == 8
    assert result.loc["A", "sessions.hit_mean"] == pytest.approx(2.0)
    assert result.loc["A", "sessions.unique_hits"] == 3
    assert result.loc["A", "sessions.double_hits"] == 8
    assert result.loc["A", "sessions.hit_share"] == pytest.approx(200 / 3)
    assert result.loc["A", "purchases.spend"] == pytest.approx(50.0)

    assert result.loc["B", "sessions.hit_count"] == 2
    assert result.loc["B", "sessions.hit_sum"] == 20
    assert result.loc["B", "sessions.hit_mean"] == pytest.approx(10.0)
    assert result.loc["B", "sessions.unique_hits"] == 1
    assert result.loc["B", "sessions.hit_share"] == pytest.approx(100 / 3)
    assert result.loc["B", "purchases.spend"] == pytest.approx(41.0)

    # User 5 has no session or purchase. The joined group domain is retained
    # with each nested reduction's real empty-set identity.
    assert result.loc["C", "sessions.hit_count"] == 0
    assert result.loc["C", "sessions.unique_hits"] == 0
    assert pd.isna(result.loc["C", "sessions.hit_sum"])
    assert pd.isna(result.loc["C", "sessions.hit_mean"])
    assert pd.isna(result.loc["C", "purchases.spend"])


def test_joined_nested_scalar_and_nonroot_local_group(joined_nested_model):
    joined, _sessions, _purchases = joined_nested_model

    scalar = (
        joined.aggregate(
            "sessions.hit_count",
            "sessions.hit_sum",
            "sessions.hit_mean",
            "sessions.unique_hits",
            "purchases.spend",
        )
        .execute()
        .iloc[0]
    )
    assert scalar["sessions.hit_count"] == 6
    assert scalar["sessions.hit_sum"] == 28
    assert scalar["sessions.hit_mean"] == pytest.approx(28 / 6)
    assert scalar["sessions.unique_hits"] == 4
    assert scalar["purchases.spend"] == pytest.approx(91.0)

    by_kind = (
        joined.group_by("sessions.kind")
        .aggregate(
            "sessions.hit_count",
            "sessions.hit_sum",
            "sessions.unique_hits",
        )
        .execute()
    )
    x_row = by_kind[by_kind["sessions.kind"] == "x"].iloc[0]
    assert x_row["sessions.hit_count"] == 5
    assert x_row["sessions.hit_sum"] == 25
    assert x_row["sessions.unique_hits"] == 3
    null_row = by_kind[by_kind["sessions.kind"].isna()].iloc[0]
    assert null_row["sessions.hit_count"] == 0
    assert null_row["sessions.unique_hits"] == 0
    assert pd.isna(null_row["sessions.hit_sum"])

    # A join key can participate in several source-local dimension values.
    # The exact bridge must match both the key and local group value or user
    # 1's x/y arrays leak into each other's groups.
    mixed = (
        joined.group_by("users.segment", "sessions.kind")
        .aggregate("sessions.hit_count", "sessions.unique_hits")
        .execute()
    )
    a_x = mixed[(mixed["users.segment"] == "A") & (mixed["sessions.kind"] == "x")].iloc[0]
    a_y = mixed[(mixed["users.segment"] == "A") & (mixed["sessions.kind"] == "y")].iloc[0]
    assert a_x["sessions.hit_count"] == 3
    assert a_x["sessions.unique_hits"] == 2
    assert a_y["sessions.hit_count"] == 1
    assert a_y["sessions.unique_hits"] == 1


def test_root_nested_measures_are_not_inflated_by_join_many(joined_nested_model):
    _joined, sessions, purchases = joined_nested_model
    model = sessions.join_many(purchases, on="session_id")

    result = (
        model.group_by("sessions.kind")
        .aggregate(
            "sessions.hit_count",
            "sessions.hit_mean",
            "sessions.unique_hits",
            "purchases.spend",
        )
        .execute()
    )

    x_row = result[result["sessions.kind"] == "x"].iloc[0]
    assert x_row["sessions.hit_count"] == 5
    assert x_row["sessions.hit_mean"] == pytest.approx(5.0)
    assert x_row["sessions.unique_hits"] == 3
    assert x_row["purchases.spend"] == pytest.approx(71.0)

    null_row = result[result["sessions.kind"].isna()].iloc[0]
    assert null_row["sessions.hit_count"] == 0
    assert null_row["sessions.unique_hits"] == 0
    assert pd.isna(null_row["sessions.hit_mean"])
    assert null_row["purchases.spend"] == pytest.approx(15.0)


def test_joined_nested_measure_excludes_orphan_source_rows():
    con = ibis.duckdb.connect(":memory:")
    users_table = con.create_table(
        "nested_orphan_users", pd.DataFrame({"user_id": [1], "segment": ["kept"]})
    )
    sessions_table = con.create_table(
        "nested_orphan_sessions",
        pd.DataFrame(
            {
                "user_id": [1, 999, None],
                "hits": [[1, 2], [100, 200, 300], [400, 500]],
            }
        ),
    )
    users = to_semantic_table(users_table, "users").with_dimensions(segment=lambda t: t.segment)
    sessions = to_semantic_table(sessions_table, "sessions").with_measures(
        hit_count=lambda t: t.hits.count(),
        hit_sum=lambda t: t.hits.sum(),
    )

    result = (
        users.join_many(sessions, on="user_id")
        .aggregate("sessions.hit_count", "sessions.hit_sum")
        .execute()
        .iloc[0]
    )

    assert result["sessions.hit_count"] == 2
    assert result["sessions.hit_sum"] == 3
