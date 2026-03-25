"""Tests for deferred join_one — dimension lookups joined after aggregation.

When a join_one dimension table's PK matches the join key and no measures
from that table are used, BSL defers the join until after aggregation for
better performance. Results must match the non-deferred path exactly.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import Dimension, to_semantic_table


@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def lookup_tables(con):
    """Users fact table + cost_centers dimension table."""
    users_df = pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 3],
            "cost_center_id": [101, 101, 102, 102, 101],
            "login_date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03", "2024-01-01"]
            ),
        }
    )
    cost_centers_df = pd.DataFrame(
        {
            "cc_id": [101, 102],
            "cc_name": ["Engineering", "Marketing"],
            "location": ["NYC", "SF"],
        }
    )
    return {
        "users": con.create_table("users", users_df),
        "cost_centers": con.create_table("cost_centers", cost_centers_df),
    }


def _make_users_model(tbl):
    return (
        to_semantic_table(tbl, name="users")
        .with_dimensions(
            user_id=Dimension(expr=lambda t: t.user_id, is_entity=True),
            cost_center_id=lambda t: t.cost_center_id,
        )
        .with_measures(login_count=lambda t: t.count())
    )


def _make_cost_centers_model(tbl):
    return (
        to_semantic_table(tbl, name="cost_centers")
        .with_dimensions(
            cc_id=Dimension(expr=lambda t: t.cc_id, is_entity=True),
            cc_name=lambda t: t.cc_name,
            location=lambda t: t.location,
        )
    )


class TestDeferredJoinDetection:
    """Test that deferrable joins are correctly identified."""

    def test_dimension_table_with_pk_is_deferrable(self, lookup_tables):
        """join_one to a dimension table with is_entity PK matching join key is deferrable."""
        users = _make_users_model(lookup_tables["users"])
        cc = _make_cost_centers_model(lookup_tables["cost_centers"])

        joined = users.join_one(
            cc, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        # Query only user measures — cost_centers should be deferred
        result = (
            joined.group_by("users.user_id")
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )

        # user 1: 2 logins, user 2: 2 logins, user 3: 1 login
        assert list(result["users.user_id"]) == [1, 2, 3]
        assert list(result["users.login_count"]) == [2, 2, 1]

    def test_no_deferral_without_entity(self, lookup_tables):
        """No deferral when dimension table has no is_entity dims."""
        users = _make_users_model(lookup_tables["users"])
        cc_no_pk = (
            to_semantic_table(lookup_tables["cost_centers"], name="cost_centers")
            .with_dimensions(
                cc_id=lambda t: t.cc_id,  # NOT is_entity
                cc_name=lambda t: t.cc_name,
            )
        )

        joined = users.join_one(
            cc_no_pk, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        # Should still work via standard path
        result = (
            joined.group_by("users.user_id")
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )
        assert list(result["users.login_count"]) == [2, 2, 1]


class TestDeferredJoinWithDimensions:
    """Test that deferred dimensions are correctly added post-aggregation."""

    def test_deferred_dim_added_post_agg(self, lookup_tables):
        """Deferred dimension columns appear in the result via post-agg join."""
        users = _make_users_model(lookup_tables["users"])
        cc = _make_cost_centers_model(lookup_tables["cost_centers"])

        joined = users.join_one(
            cc, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        # Include cost_center dims in group-by — should be deferred
        result = (
            joined.group_by("users.user_id", "cost_centers.cc_name")
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )

        assert list(result["users.user_id"]) == [1, 2, 3]
        assert list(result["cost_centers.cc_name"]) == [
            "Engineering",
            "Marketing",
            "Engineering",
        ]
        assert list(result["users.login_count"]) == [2, 2, 1]

    def test_multiple_deferred_dims(self, lookup_tables):
        """Multiple dimensions from the deferred table are all added."""
        users = _make_users_model(lookup_tables["users"])
        cc = _make_cost_centers_model(lookup_tables["cost_centers"])

        joined = users.join_one(
            cc, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        result = (
            joined.group_by(
                "users.user_id", "cost_centers.cc_name", "cost_centers.location"
            )
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )

        assert list(result["cost_centers.cc_name"]) == [
            "Engineering",
            "Marketing",
            "Engineering",
        ]
        assert list(result["cost_centers.location"]) == ["NYC", "SF", "NYC"]


class TestDeferredJoinCorrectness:
    """Verify deferred join results match standard (non-deferred) path."""

    def test_results_match_standard_path(self, lookup_tables):
        """Deferred join produces identical results to the standard join path."""
        users = _make_users_model(lookup_tables["users"])

        # Deferred path: cost_centers WITH is_entity PK
        cc_with_pk = _make_cost_centers_model(lookup_tables["cost_centers"])
        joined_deferred = users.join_one(
            cc_with_pk, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        # Standard path: cost_centers WITHOUT is_entity → no deferral
        cc_no_pk = (
            to_semantic_table(lookup_tables["cost_centers"], name="cost_centers")
            .with_dimensions(
                cc_id=lambda t: t.cc_id,
                cc_name=lambda t: t.cc_name,
            )
        )
        joined_standard = users.join_one(
            cc_no_pk, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        # Both should produce the same result
        result_deferred = (
            joined_deferred.group_by("users.user_id", "cost_centers.cc_name")
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )

        result_standard = (
            joined_standard.group_by("users.user_id", "cost_centers.cc_name")
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(result_deferred, result_standard)

    def test_scalar_aggregate_with_deferred(self, lookup_tables):
        """Scalar aggregate (no group-by) works with deferred joins."""
        users = _make_users_model(lookup_tables["users"])
        cc = _make_cost_centers_model(lookup_tables["cost_centers"])

        joined = users.join_one(
            cc, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        result = joined.aggregate("users.login_count").execute()
        assert result["users.login_count"].iloc[0] == 5

    def test_no_deferral_when_filter_on_deferred_table(self, lookup_tables):
        """Joins must NOT be deferred if a filter references the dimension table."""
        users = _make_users_model(lookup_tables["users"])
        cc = _make_cost_centers_model(lookup_tables["cost_centers"])

        joined = users.join_one(
            cc, on=lambda u, c: u.cost_center_id == c.cc_id
        )

        # Filter on the dimension table's column — must not defer
        result = (
            joined.filter(lambda t: t.cc_name == "Engineering")
            .group_by("users.user_id", "cost_centers.cc_name")
            .aggregate("users.login_count")
            .execute()
            .sort_values("users.user_id")
            .reset_index(drop=True)
        )

        # Only Engineering users: user_id 1 (2 logins) and user_id 3 (1 login)
        assert list(result["users.user_id"]) == [1, 3]
        assert list(result["users.login_count"]) == [2, 1]
        assert list(result["cost_centers.cc_name"]) == ["Engineering", "Engineering"]
