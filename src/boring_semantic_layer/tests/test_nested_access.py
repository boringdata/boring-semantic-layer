"""Unit tests for automatic nested array access (Malloy-style)."""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


def test_single_level_nested_count():
    """Test automatic unnesting with single-level array (t.hits.count())."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2, 3],
            "user": ["alice", "bob", "alice"],
            "hits": [[1, 2, 3], [4, 5], [6]],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(user=lambda t: t.user)
        .with_measures(hits_count=lambda t: t.hits.count())
    )

    result = semantic.group_by("user").aggregate("hits_count").execute()

    # Alice: 3 + 1 = 4 hits, Bob: 2 hits
    assert result[result.user == "alice"]["hits_count"].iloc[0] == 4
    assert result[result.user == "bob"]["hits_count"].iloc[0] == 2


def test_double_level_nested_count():
    """Test automatic unnesting with double-nested arrays (t.hits.product.count())."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "hits": [
                [{"product": [1, 2]}, {"product": [3]}],
                [{"product": [4, 5, 6]}],
            ],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(product_count=lambda t: t.hits.product.count())
    )

    result = semantic.group_by("session_id").aggregate("product_count").execute()

    # Session 1: [1,2] + [3] = 3 products, Session 2: [4,5,6] = 3 products
    assert result[result.session_id == 1]["product_count"].iloc[0] == 3
    assert result[result.session_id == 2]["product_count"].iloc[0] == 3


def test_nested_sum():
    """Test sum aggregation on nested arrays (t.hits.sum())."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "hits": [[10, 20, 30], [40, 50]],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(total_hits=lambda t: t.hits.sum())
    )

    result = semantic.group_by("session_id").aggregate("total_hits").execute()

    # Session 1: 10 + 20 + 30 = 60, Session 2: 40 + 50 = 90
    assert result[result.session_id == 1]["total_hits"].iloc[0] == 60
    assert result[result.session_id == 2]["total_hits"].iloc[0] == 90


def test_nested_field_access():
    """Test accessing struct fields within nested arrays (t.hits.page.pageTitle.nunique())."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "hits": [
                [
                    {"page": {"pageTitle": "Home"}},
                    {"page": {"pageTitle": "About"}},
                ],
                [
                    {"page": {"pageTitle": "Home"}},
                    {"page": {"pageTitle": "Contact"}},
                ],
            ],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(unique_pages=lambda t: t.hits.page.pageTitle.nunique())
    )

    result = semantic.group_by("session_id").aggregate("unique_pages").execute()

    # Session 1: Home, About = 2, Session 2: Home, Contact = 2
    assert result[result.session_id == 1]["unique_pages"].iloc[0] == 2
    assert result[result.session_id == 2]["unique_pages"].iloc[0] == 2


def test_mixed_session_and_hit_level():
    """Test mixing session-level and hit-level measures in same query."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2, 3],
            "user": ["alice", "bob", "alice"],
            "hits": [[1, 2], [3], [4, 5, 6]],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(user=lambda t: t.user)
        .with_measures(
            session_count=lambda t: t.count(),
            user_count=lambda t: t.user.nunique(),
            hits_count=lambda t: t.hits.count(),
        )
    )

    result = (
        semantic.group_by("user").aggregate("session_count", "user_count", "hits_count").execute()
    )

    # Alice: 2 sessions, 1 user, 5 hits (2 + 3)
    alice_result = result[result.user == "alice"]
    assert alice_result["session_count"].iloc[0] == 2
    assert alice_result["user_count"].iloc[0] == 1
    assert alice_result["hits_count"].iloc[0] == 5

    # Bob: 1 session, 1 user, 1 hit
    bob_result = result[result.user == "bob"]
    assert bob_result["session_count"].iloc[0] == 1
    assert bob_result["user_count"].iloc[0] == 1
    assert bob_result["hits_count"].iloc[0] == 1


def test_grouped_nested_aggregation():
    """Test grouping with nested array aggregations."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "category": ["A", "B", "A"],
            "hits": [[1, 2], [3, 4, 5], [6]],
        }
    )
    tbl = con.create_table("data", data)

    semantic = (
        to_semantic_table(tbl, name="data")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(hits_count=lambda t: t.hits.count())
    )

    result = (
        semantic.group_by("category")
        .aggregate("hits_count")
        .order_by(ibis.desc("category"))
        .execute()
    )

    # Category B: 3 hits
    assert result[result.category == "B"]["hits_count"].iloc[0] == 3
    # Category A: 2 + 1 = 3 hits
    assert result[result.category == "A"]["hits_count"].iloc[0] == 3


def test_nested_mean():
    """Test mean aggregation on nested arrays."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "scores": [[10.0, 20.0, 30.0], [40.0, 50.0]],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(avg_score=lambda t: t.scores.mean())
    )

    result = semantic.group_by("session_id").aggregate("avg_score").execute()

    # Session 1: (10 + 20 + 30) / 3 = 20, Session 2: (40 + 50) / 2 = 45
    assert result[result.session_id == 1]["avg_score"].iloc[0] == 20.0
    assert result[result.session_id == 2]["avg_score"].iloc[0] == 45.0


def test_nested_min_max():
    """Test min/max aggregations on nested arrays."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "values": [[5, 10, 15], [20, 25]],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(
            min_val=lambda t: t.values.min(),
            max_val=lambda t: t.values.max(),
        )
    )

    result = semantic.group_by("session_id").aggregate("min_val", "max_val").execute()

    # Session 1: min=5, max=15, Session 2: min=20, max=25
    assert result[result.session_id == 1]["min_val"].iloc[0] == 5
    assert result[result.session_id == 1]["max_val"].iloc[0] == 15
    assert result[result.session_id == 2]["min_val"].iloc[0] == 20
    assert result[result.session_id == 2]["max_val"].iloc[0] == 25


def test_multiple_nested_levels_same_query():
    """Test mixing measures from different nesting levels."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "hits": [
                [{"product": [1, 2]}, {"product": [3]}],
                [{"product": [4]}],
            ],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(
            session_count=lambda t: t.count(),
            hit_count=lambda t: t.hits.count(),
            product_count=lambda t: t.hits.product.count(),
        )
    )

    result = (
        semantic.group_by("session_id")
        .aggregate("session_count", "hit_count", "product_count")
        .execute()
    )

    # Session 1: 1 session, 2 hits, 3 products (2 + 1)
    s1 = result[result.session_id == 1]
    assert s1["session_count"].iloc[0] == 1
    assert s1["hit_count"].iloc[0] == 2
    assert s1["product_count"].iloc[0] == 3

    # Session 2: 1 session, 1 hit, 1 product
    s2 = result[result.session_id == 2]
    assert s2["session_count"].iloc[0] == 1
    assert s2["hit_count"].iloc[0] == 1
    assert s2["product_count"].iloc[0] == 1


def test_nested_with_filter():
    """Test that filters work correctly with nested measures."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "category": ["A", "B", "A"],
            "hits": [[1, 2], [3, 4], [5]],
        }
    )
    tbl = con.create_table("data", data)

    semantic = (
        to_semantic_table(tbl, name="data")
        .with_dimensions(category=lambda t: t.category)
        .with_measures(hits_count=lambda t: t.hits.count())
    )

    result = (
        semantic.filter(lambda t: t.category == "A")
        .group_by("category")
        .aggregate("hits_count")
        .execute()
    )

    # Only category A: 2 + 1 = 3 hits
    assert result["hits_count"].iloc[0] == 3


def test_nested_nunique():
    """Test nunique (count distinct) on nested arrays."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "tags": [["a", "b", "a"], ["b", "c"]],
        }
    )
    tbl = con.create_table("sessions", data)

    semantic = (
        to_semantic_table(tbl, name="sessions")
        .with_dimensions(session_id=lambda t: t.session_id)
        .with_measures(unique_tags=lambda t: t.tags.nunique())
    )

    result = semantic.group_by("session_id").aggregate("unique_tags").execute()

    # Session 1: a, b (unique) = 2, Session 2: b, c = 2
    assert result[result.session_id == 1]["unique_tags"].iloc[0] == 2
    assert result[result.session_id == 2]["unique_tags"].iloc[0] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
