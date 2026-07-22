"""Regression coverage for source, grain, and namespace preservation in joins."""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import Dimension, to_semantic_table


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


def test_join_one_right_measures_stay_bound_to_right_rows(con):
    left = con.create_table(
        "source_left", pd.DataFrame({"id": [1, 2], "value": [10, 20]})
    )
    right = con.create_table(
        "source_right", pd.DataFrame({"id": [1, 3], "value": [100, 900]})
    )
    left_model = to_semantic_table(left, "left").with_measures(
        total=lambda t: t.value.sum()
    )
    right_model = to_semantic_table(right, "right").with_measures(
        total=lambda t: t.value.sum(),
        row_count=lambda t: t.count(),
        id_count=lambda t: t.id.count(),
    )

    result = left_model.join_one(right_model, on="id").aggregate(
        "left.total", "right.total", "right.row_count", "right.id_count"
    ).execute()

    assert result.iloc[0].to_dict() == {
        "left.total": 30,
        "right.total": 100,
        "right.row_count": 1,
        "right.id_count": 1,
    }


def test_grouped_right_measures_use_the_right_executable_join_key_alias(con):
    left = con.create_table(
        "different_key_left",
        pd.DataFrame(
            {
                "lkey": [1, 2],
                # Deliberately collides with the right join key, but is not
                # part of this relationship.
                "rkey": [999, 999],
                "group": ["a", "b"],
            }
        ),
    )
    right = con.create_table(
        "different_key_right",
        pd.DataFrame(
            {
                "rkey": [1, 1, 2],
                "value": [10, 11, 20],
            }
        ),
    )
    left_model = to_semantic_table(left, "left").with_dimensions(
        group=lambda t: t.group
    )
    right_model = to_semantic_table(right, "right").with_measures(
        total=lambda t: t.value.sum(),
        median=lambda t: t.value.median(),
        distinct_values=lambda t: t.value.nunique(),
    )

    joined = left_model.join_many(
        right_model,
        on=lambda left_row, right_row: left_row.lkey == right_row.rkey,
    )
    assert "rkey_right" in joined.columns

    result = (
        joined.group_by("left.group")
        .aggregate("right.total", "right.median", "right.distinct_values")
        .order_by("left.group")
        .execute()
    )

    assert result["left.group"].tolist() == ["a", "b"]
    assert result["right.total"].tolist() == [21, 20]
    assert result["right.median"].tolist() == [10.5, 20.0]
    assert result["right.distinct_values"].tolist() == [2, 1]

    filtered = (
        joined.filter(lambda t: t["left.group"] == "a")
        .group_by("left.group")
        .aggregate("right.total")
        .execute()
    )
    assert filtered.to_dict("records") == [
        {"left.group": "a", "right.total": 21}
    ]


def test_join_one_unmatched_right_counts_are_zero_but_sum_is_null(con):
    left = con.create_table(
        "count_identity_left", pd.DataFrame({"id": [1, 2]})
    )
    right = con.create_table(
        "count_identity_right", pd.DataFrame({"id": [1], "value": [100]})
    )
    left_model = to_semantic_table(left, "left").with_dimensions(
        id=lambda t: t.id
    )
    right_model = to_semantic_table(right, "right").with_measures(
        row_count=lambda t: t.count(),
        id_count=lambda t: t.id.count(),
        total=lambda t: t.value.sum(),
        doubled_count=lambda t: t.row_count * 2,
    )

    result = (
        left_model.join_one(right_model, on="id")
        .group_by("left.id")
        .aggregate(
            "right.row_count",
            "right.id_count",
            "right.total",
            "right.doubled_count",
        )
        .execute()
        .set_index("left.id")
    )

    assert result.loc[1, "right.row_count"] == 1
    assert result.loc[1, "right.id_count"] == 1
    assert result.loc[1, "right.total"] == 100
    assert result.loc[1, "right.doubled_count"] == 2
    assert result.loc[2, "right.row_count"] == 0
    assert result.loc[2, "right.id_count"] == 0
    assert pd.isna(result.loc[2, "right.total"])
    assert result.loc[2, "right.doubled_count"] == 0


@pytest.mark.parametrize("join_method", ["join_one", "join_many"])
def test_exact_measures_use_their_real_empty_source_result(con, join_method):
    left = con.create_table(
        f"exact_empty_left_{join_method}",
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "group": ["hit", "miss", "all_null"],
            }
        ),
    )
    right = con.create_table(
        f"exact_empty_right_{join_method}",
        pd.DataFrame(
            {
                "id": [1, 3],
                "kind": ["A", "B"],
                "token": ["x", None],
                "value": [10.0, None],
            }
        ),
    )
    left_model = to_semantic_table(left, "left").with_dimensions(
        group=lambda t: t.group
    )
    right_model = (
        to_semantic_table(right, "right")
        .with_dimensions(kind=lambda t: t.kind)
        .with_measures(
            row_count_plus_one=lambda t: t.count() + 1,
            zero_sum=lambda t: t.value.sum().fill_null(0),
            ratio=lambda t: t.value.sum() / t.token.count(),
            median=lambda t: t.value.median(),
        )
    )
    joined = getattr(left_model, join_method)(right_model, on="id")
    measures = (
        "right.row_count_plus_one",
        "right.zero_sum",
        "right.ratio",
        "right.median",
    )

    by_left = (
        joined.group_by("left.group")
        .aggregate(*measures)
        .execute()
        .set_index("left.group")
    )
    assert by_left.loc["hit", "right.row_count_plus_one"] == 2
    assert by_left.loc["hit", "right.zero_sum"] == 10
    assert by_left.loc["hit", "right.ratio"] == 10
    assert by_left.loc["hit", "right.median"] == 10
    assert by_left.loc["miss", "right.row_count_plus_one"] == 1
    assert by_left.loc["miss", "right.zero_sum"] == 0
    assert pd.isna(by_left.loc["miss", "right.ratio"])
    assert pd.isna(by_left.loc["miss", "right.median"])
    # A matched row whose inputs are all NULL is not an absent aggregate row.
    assert by_left.loc["all_null", "right.row_count_plus_one"] == 2
    assert by_left.loc["all_null", "right.zero_sum"] == 0
    assert pd.isna(by_left.loc["all_null", "right.ratio"])
    assert pd.isna(by_left.loc["all_null", "right.median"])

    by_right = (
        joined.group_by("right.kind")
        .aggregate(*measures)
        .execute()
    )
    unmatched = by_right[by_right["right.kind"].isna()].iloc[0]
    assert unmatched["right.row_count_plus_one"] == 1
    assert unmatched["right.zero_sum"] == 0
    assert pd.isna(unmatched["right.ratio"])
    assert pd.isna(unmatched["right.median"])


def test_join_one_count_distinct_uses_right_collision_alias_and_zero_identity(con):
    left = con.create_table(
        "distinct_identity_left",
        pd.DataFrame(
            {
                "join_id": [1, 2],
                # This unrelated left field collides with the right join key,
                # so the executable right key is ``id_right``.
                "id": [999, 888],
                "group": ["matched", "unmatched"],
            }
        ),
    )
    right = con.create_table(
        "distinct_identity_right", pd.DataFrame({"id": [1], "value": [10]})
    )
    left_model = to_semantic_table(left, "left").with_dimensions(
        group=lambda t: t.group
    )
    right_model = to_semantic_table(right, "right").with_measures(
        distinct_ids=lambda t: t.id.nunique(),
        doubled_distinct=lambda t: t.distinct_ids * 2,
    )

    result = (
        left_model.join_one(
            right_model, on=lambda left_row, right_row: left_row.join_id == right_row.id
        )
        .group_by("left.group")
        .aggregate("right.distinct_ids", "right.doubled_distinct")
        .execute()
        .set_index("left.group")
    )

    assert result.loc["matched", "right.distinct_ids"] == 1
    assert result.loc["unmatched", "right.distinct_ids"] == 0
    assert result.loc["matched", "right.doubled_distinct"] == 2
    assert result.loc["unmatched", "right.doubled_distinct"] == 0


def test_colliding_derived_dimension_keeps_complete_expression(con):
    left = con.create_table(
        "derived_left",
        pd.DataFrame({"id": [1, 2, 3], "name": ["L1", "L2", "L3"]}),
    )
    right = con.create_table(
        "derived_right",
        pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["alice", "bob"],
                "suffix": ["x", "y"],
            }
        ),
    )
    left_model = to_semantic_table(left, "left").with_measures(
        row_count=lambda t: t.count()
    )
    right_model = to_semantic_table(right, "right").with_dimensions(
        label=lambda t: t.name.upper() + "-" + t.suffix.upper(),
        right_id=lambda t: t.id,
    )

    joined = left_model.join_one(right_model, on="id")
    labels = joined.group_by("right.label").aggregate("left.row_count").execute()
    ids = joined.group_by("right.right_id").aggregate("left.row_count").execute()

    assert set(labels["right.label"].dropna()) == {"ALICE-X", "BOB-Y"}
    assert labels["right.label"].isna().sum() == 1
    assert ids["right.right_id"].isna().sum() == 1


def test_join_many_derived_dimension_is_measure_selection_invariant(con):
    orders = con.create_table(
        "selection_orders",
        pd.DataFrame({"id": [1, 2], "name": ["L1", "L2"], "amount": [10, 20]}),
    )
    customers = con.create_table(
        "selection_customers",
        pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]}),
    )
    order_model = to_semantic_table(orders, "orders").with_measures(
        total=lambda t: t.amount.sum()
    )
    customer_model = (
        to_semantic_table(customers, "customers")
        .with_dimensions(label=lambda t: t.name.upper())
        .with_measures(row_count=lambda t: t.count())
    )
    joined = order_model.join_many(customer_model, on="id")

    left_only = joined.group_by("customers.label").aggregate("orders.total").execute()
    right_only = joined.group_by("customers.label").aggregate("customers.row_count").execute()
    together = joined.group_by("customers.label").aggregate(
        "orders.total", "customers.row_count"
    ).execute()

    assert set(left_only["customers.label"]) == {"ALICE", "BOB"}
    assert set(right_only["customers.label"]) == {"ALICE", "BOB"}
    assert set(together["customers.label"]) == {"ALICE", "BOB"}
    assert together.set_index("customers.label").loc["ALICE"].to_dict() == {
        "orders.total": 10,
        "customers.row_count": 1,
    }


def test_non_equi_join_rejected_when_preaggregation_depends_on_it(con):
    left = con.create_table(
        "predicate_left", pd.DataFrame({"customer": [1, 1], "id": [1, 2]})
    )
    right = con.create_table(
        "predicate_right",
        pd.DataFrame({"customer": [1, 1], "id": [1, 2], "value": [100, 200]}),
    )
    left_model = to_semantic_table(left, "left")
    right_model = to_semantic_table(right, "right").with_measures(
        total=lambda t: t.value.sum()
    )
    joined = left_model.join_many(
        right_model,
        on=lambda left_row, right_row: (left_row.customer == right_row.customer)
        & (left_row.id != right_row.id),
    )

    with pytest.raises(ValueError, match="direct field equijoins"):
        joined.aggregate("right.total").execute()


def test_duplicate_model_names_are_rejected(con):
    left = con.create_table("duplicate_left", pd.DataFrame({"id": [1]}))
    right = con.create_table("duplicate_right", pd.DataFrame({"id": [1]}))

    with pytest.raises(ValueError, match="unique names"):
        to_semantic_table(left, "duplicate").join_many(
            to_semantic_table(right, "duplicate"), on="id"
        )


def test_join_one_orphans_stay_excluded_in_a_larger_join_tree(con):
    root = con.create_table("orphan_root", pd.DataFrame({"id": [1, 2]}))
    lookup = con.create_table(
        "orphan_lookup", pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 900]})
    )
    children = con.create_table(
        "orphan_children", pd.DataFrame({"id": [1, 1, 2]})
    )
    root_model = to_semantic_table(root, "root")
    lookup_model = to_semantic_table(lookup, "lookup").with_measures(
        total=lambda t: t.value.sum()
    )
    child_model = to_semantic_table(children, "children").with_measures(
        row_count=lambda t: t.count()
    )

    result = (
        root_model.join_one(lookup_model, on="id")
        .join_many(child_model, on="id")
        .aggregate("lookup.total", "children.row_count")
        .execute()
    )

    assert result["lookup.total"].iloc[0] == 30
    assert result["children.row_count"].iloc[0] == 3


def test_post_join_field_reduction_is_routed_to_its_unique_source(con):
    orders = con.create_table(
        "wrapper_orders", pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
    )
    items = con.create_table(
        "wrapper_items", pd.DataFrame({"id": [1, 1, 2], "quantity": [1, 2, 3]})
    )
    joined = to_semantic_table(orders, "orders").join_many(
        to_semantic_table(items, "items"), on="id"
    )

    result = joined.with_measures(
        source_total=lambda t: t.amount.sum(),
        joined_row_count=lambda t: t.count(),
    ).aggregate("source_total", "joined_row_count").execute()

    assert result["source_total"].iloc[0] == 300
    assert result["joined_row_count"].iloc[0] == 3


def test_count_distinct_matches_null_group_when_combined(con):
    orders = con.create_table(
        "null_distinct_orders",
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "group": pd.Series([None, None, "a"], dtype="string"),
                "value": [1, 1, 2],
                "amount": [1, 3, 5],
            }
        ),
    )
    items = con.create_table(
        "null_distinct_items", pd.DataFrame({"id": [1, 1, 2, 3]})
    )
    order_model = (
        to_semantic_table(orders, "orders")
        .with_dimensions(group=lambda t: t.group)
        .with_measures(
            total=lambda t: t.amount.sum(),
            distinct_values=lambda t: t.value.nunique(),
        )
    )
    joined = order_model.join_many(to_semantic_table(items, "items"), on="id")

    result = joined.group_by("orders.group").aggregate(
        "orders.total", "orders.distinct_values"
    ).execute()
    null_row = result[result["orders.group"].isna()].iloc[0]

    assert null_row["orders.total"] == 4
    assert null_row["orders.distinct_values"] == 1


def test_right_count_distinct_respects_right_dimension_with_reused_join_key(con):
    orders_df = pd.DataFrame(
        {
            "order_id": [10, 11, 12],
            # Collision-prone user columns must not affect the bridge.
            "order_ref": [901, 902, 903],
            "sku_right": [801, 802, 803],
            "__bsl_jk_order_ref": [701, 702, 703],
            "__exact_gb_0": [601, 602, 603],
        }
    )
    items_df = pd.DataFrame(
        {
            "order_ref": [10, 10, 10, 11, 99],
            "kind": ["P", "P", "Q", "P", "X"],
            "sku": ["a", "a", "d", "b", "c"],
            "sku_right": [1, 2, 3, 4, 5],
            "__bsl_jk_order_ref": [11, 12, 13, 14, 15],
            "__exact_gb_0": [21, 22, 23, 24, 25],
        }
    )
    orders = con.create_table("dimension_distinct_orders", orders_df)
    items = con.create_table("dimension_distinct_items", items_df)
    item_model = (
        to_semantic_table(items, "items")
        .with_dimensions(kind=lambda t: t.kind)
        .with_measures(distinct_skus=lambda t: t.sku.nunique())
    )
    joined = to_semantic_table(orders, "orders").join_many(
        item_model,
        on=lambda order, item: order.order_id == item.order_ref,
    )

    actual = (
        joined.group_by("items.kind")
        .aggregate("items.distinct_skus")
        .execute()
        .set_index("items.kind")["items.distinct_skus"]
    )
    expected = (
        orders_df.merge(
            items_df,
            how="left",
            left_on="order_id",
            right_on="order_ref",
            suffixes=("_order", "_item"),
        )
        .groupby("kind", dropna=False)["sku"]
        .nunique()
    )

    actual_by_kind = {
        "<NULL>" if pd.isna(kind) else kind: value
        for kind, value in actual.items()
    }
    expected_by_kind = {
        "<NULL>" if pd.isna(kind) else kind: value
        for kind, value in expected.items()
    }
    assert actual_by_kind == expected_by_kind == {"<NULL>": 0, "P": 2, "Q": 1}


def test_right_median_respects_local_dimension_at_mixed_group_grain(con):
    orders_df = pd.DataFrame(
        {
            "order_id": [10, 11, 12],
            "cohort": ["A", "A", "B"],
            "order_ref": [901, 902, 903],
            "value_right": [801, 802, 803],
            "__bsl_jk_order_ref": [701, 702, 703],
            "__exact_gb_0": [601, 602, 603],
        }
    )
    items_df = pd.DataFrame(
        {
            "order_ref": [10, 10, 10, 11, 99],
            "kind": ["P", "P", "Q", "P", "X"],
            "value": [5.0, 6.0, 4.0, 7.0, 9.0],
            "value_right": [1, 2, 3, 4, 5],
            "__bsl_jk_order_ref": [11, 12, 13, 14, 15],
            "__exact_gb_0": [21, 22, 23, 24, 25],
        }
    )
    orders = con.create_table("dimension_median_orders", orders_df)
    items = con.create_table("dimension_median_items", items_df)
    order_model = to_semantic_table(orders, "orders").with_dimensions(
        cohort=lambda t: t.cohort
    )
    item_model = (
        to_semantic_table(items, "items")
        .with_dimensions(kind=lambda t: t.kind)
        .with_measures(median_value=lambda t: t.value.median())
    )
    joined = order_model.join_many(
        item_model,
        on=lambda order, item: order.order_id == item.order_ref,
    )

    actual = (
        joined.group_by("orders.cohort", "items.kind")
        .aggregate("items.median_value")
        .execute()
    )
    expected = (
        orders_df.merge(
            items_df,
            how="left",
            left_on="order_id",
            right_on="order_ref",
            suffixes=("_order", "_item"),
        )
        .groupby(["cohort", "kind"], dropna=False)["value"]
        .median()
        .reset_index(name="items.median_value")
        .rename(columns={"cohort": "orders.cohort", "kind": "items.kind"})
    )
    actual["items.kind"] = actual["items.kind"].fillna("<NULL>")
    expected["items.kind"] = expected["items.kind"].fillna("<NULL>")
    actual = actual.sort_values(["orders.cohort", "items.kind"]).reset_index(drop=True)
    expected = expected.sort_values(["orders.cohort", "items.kind"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)


def test_join_wrapper_dimension_keeps_flattened_source_lineage(con):
    left_df = pd.DataFrame(
        {
            "id": [1, 2],
            "status": ["LEFT_A", "LEFT_B"],
        }
    )
    right_df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            # The same physical name is deliberately bound to different values
            # on the right.  A wrapper dimension is authored against the
            # flattened join, where the unsuffixed field belongs to the left.
            "status": ["R_X", "R_Y", "R_Z"],
            "value": [10, 20, 30],
        }
    )
    left = con.create_table("wrapper_dimension_left", left_df)
    right = con.create_table("wrapper_dimension_right", right_df)
    left_model = to_semantic_table(left, "left").with_dimensions(
        status=lambda t: t.status
    )
    right_model = to_semantic_table(right, "right").with_measures(
        total=lambda t: t.value.sum(),
        median=lambda t: t.value.median(),
        distinct_statuses=lambda t: t.status.nunique(),
    )
    joined = left_model.join_many(right_model, on="id").with_dimensions(
        bucket=lambda t: t.status
    )

    actual = (
        joined.group_by("bucket")
        .aggregate(
            "right.total",
            "right.median",
            "right.distinct_statuses",
        )
        .execute()
        .sort_values("bucket")
        .reset_index(drop=True)
    )
    expected = pd.DataFrame(
        {
            "bucket": ["LEFT_A", "LEFT_B"],
            "right.total": [30, 30],
            "right.median": [15.0, 30.0],
            "right.distinct_statuses": [2, 1],
        }
    )

    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)


def test_cross_source_wrapper_dimension_fails_closed_for_source_aggregates(con):
    left = con.create_table(
        "cross_wrapper_dimension_left",
        pd.DataFrame({"id": [1, 2], "prefix": ["A", "B"]}),
    )
    right = con.create_table(
        "cross_wrapper_dimension_right",
        pd.DataFrame(
            {
                "id": [1, 1, 2],
                "status": ["X", "Y", "Z"],
                "value": [10, 20, 30],
            }
        ),
    )
    left_model = to_semantic_table(left, "left").with_dimensions(
        prefix=lambda t: t.prefix
    )
    right_model = (
        to_semantic_table(right, "right")
        .with_dimensions(status=lambda t: t.status)
        .with_measures(
            total=lambda t: t.value.sum(),
            median=lambda t: t.value.median(),
            distinct_statuses=lambda t: t.status.nunique(),
        )
    )
    joined = left_model.join_many(right_model, on="id").with_dimensions(
        bucket=lambda t: t.left.prefix + "_" + t.right.status
    )

    with pytest.raises(
        ValueError,
        match="join-wrapper dimension.*spans multiple semantic models",
    ):
        joined.group_by("bucket").aggregate(
            "right.total",
            "right.median",
            "right.distinct_statuses",
        ).execute()


def test_transformed_same_name_right_dimension_preserves_every_measure_grain(con):
    orders = con.create_table(
        "transformed_same_name_orders",
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "cohort": ["A", "A", "B"],
            }
        ),
    )
    items = con.create_table(
        "transformed_same_name_items",
        pd.DataFrame(
            {
                "id": [1, 1, 2],
                "kind": ["p", "q", "p"],
                "value": [10, 100, 30],
                # Force the private group-key allocator past its preferred
                # spelling; user-owned internal-prefix columns are valid.
                "__bsl_gb_items_kind": [901, 902, 903],
            }
        ),
    )
    order_model = to_semantic_table(orders, "orders").with_dimensions(
        cohort=lambda t: t.cohort
    )
    item_model = (
        to_semantic_table(items, "items")
        # upper() retains the input expression name "kind". It must not be
        # mistaken for the direct physical field.
        .with_dimensions(kind=lambda t: t.kind.upper())
        .with_measures(
            total=lambda t: t.value.sum(),
            row_count=lambda t: t.count(),
            distinct_values=lambda t: t.value.nunique(),
            median_value=lambda t: t.value.median(),
        )
    )
    joined = order_model.join_many(item_model, on="id")
    measures = (
        "items.total",
        "items.row_count",
        "items.distinct_values",
        "items.median_value",
    )

    local = (
        joined.group_by("items.kind")
        .aggregate(*measures)
        .execute()
        .set_index("items.kind")
    )
    assert local.loc["P"].to_dict() == {
        "items.total": 40,
        "items.row_count": 2,
        "items.distinct_values": 2,
        "items.median_value": 20.0,
    }
    assert local.loc["Q"].to_dict() == {
        "items.total": 100,
        "items.row_count": 1,
        "items.distinct_values": 1,
        "items.median_value": 100.0,
    }
    null_local = local[local.index.isna()].iloc[0]
    assert pd.isna(null_local["items.total"])
    assert null_local["items.row_count"] == 0
    assert null_local["items.distinct_values"] == 0
    assert pd.isna(null_local["items.median_value"])

    mixed = (
        joined.group_by("orders.cohort", "items.kind")
        .aggregate(*measures)
        .execute()
    )
    matched = mixed[mixed["items.kind"].notna()].set_index("items.kind")
    assert matched.loc["P", "items.total"] == 40
    assert matched.loc["P", "items.distinct_values"] == 2
    assert matched.loc["P", "items.median_value"] == 20.0
    assert matched.loc["Q", "items.total"] == 100
    assert matched.loc["Q", "items.distinct_values"] == 1
    assert matched.loc["Q", "items.median_value"] == 100.0
    unmatched = mixed[mixed["items.kind"].isna()].iloc[0]
    assert unmatched["orders.cohort"] == "B"
    assert unmatched["items.row_count"] == 0
    assert unmatched["items.distinct_values"] == 0


def test_calculated_measure_dependencies_keep_root_scope(con):
    left = con.create_table("calc_left", pd.DataFrame({"id": [1, 2]}))
    right = con.create_table("calc_right", pd.DataFrame({"id": [1, 2]}))
    left_model = to_semantic_table(left, "left").with_measures(
        row_count=lambda t: t.count(), doubled=lambda t: t.row_count * 2
    )
    right_model = to_semantic_table(right, "right").with_measures(
        row_count=lambda t: t.count(), doubled=lambda t: t.row_count * 2
    )

    result = left_model.join_one(right_model, on="id").aggregate(
        "left.doubled", "right.doubled"
    ).execute()

    assert result.iloc[0].to_dict() == {"left.doubled": 4, "right.doubled": 4}


def test_join_schema_matches_executable_collision_aliases(con):
    left = con.create_table(
        "schema_left", pd.DataFrame({"id": [1], "value": [1]})
    )
    right = con.create_table(
        "schema_right", pd.DataFrame({"id": [1], "value": ["x"]})
    )
    joined = to_semantic_table(left, "left").join_one(
        to_semantic_table(right, "right"), on="id"
    )

    assert tuple(joined.columns) == tuple(joined.table.columns)
    assert tuple(joined.columns) == ("id", "value", "id_right", "value_right")
    assert str(joined.schema["value"]) == "int64"
    assert str(joined.schema["value_right"]) == "string"


def test_preexisting_right_suffix_is_preserved_and_right_fields_stay_bound(con):
    left = con.create_table(
        "reserved_suffix_left",
        pd.DataFrame(
            {
                "id": [1, 2],
                "value": [1, 2],
                "value_right": [900, 901],
            }
        ),
    )
    right = con.create_table(
        "reserved_suffix_right",
        pd.DataFrame({"id": [1, 2], "value": [10, 20]}),
    )
    left_model = to_semantic_table(left, "left")
    right_model = (
        to_semantic_table(right, "right")
        .with_dimensions(doubled=lambda t: t.value * 2)
        .with_measures(total=lambda t: t.value.sum())
    )
    joined = left_model.join_one(right_model, on="id")

    assert tuple(joined.columns) == (
        "id",
        "value",
        "value_right",
        "id_right",
        "value_right2",
    )
    raw = joined.execute().sort_values("id").reset_index(drop=True)
    assert raw["value_right"].tolist() == [900, 901]
    assert raw["value_right2"].tolist() == [10, 20]
    assert str(joined.schema["value_right"]) == "int64"
    assert str(joined.schema["value_right2"]) == "int64"

    result = (
        joined.group_by("right.doubled")
        .aggregate("right.total")
        .order_by("right.doubled")
        .execute()
    )
    assert result["right.doubled"].tolist() == [20, 40]
    assert result["right.total"].tolist() == [10, 20]


def test_dynamic_aliases_are_measure_selection_invariant_across_three_legs(con):
    root = con.create_table(
        "dynamic_alias_root",
        pd.DataFrame(
            {"id": [1, 2], "value": [1, 2], "value_right": [900, 901]}
        ),
    )
    middle = con.create_table(
        "dynamic_alias_middle",
        pd.DataFrame({"id": [1, 2], "value": [100, 200]}),
    )
    later = con.create_table(
        "dynamic_alias_later",
        pd.DataFrame({"id": [1, 2], "value": [10, 20]}),
    )
    middle_model = to_semantic_table(middle, "middle").with_measures(
        total=lambda t: t.value.sum()
    )
    later_model = (
        to_semantic_table(later, "later")
        .with_dimensions(doubled=lambda t: t.value * 2)
        .with_measures(total=lambda t: t.value.sum())
    )
    joined = (
        to_semantic_table(root, "root")
        .join_one(middle_model, on="id")
        .join_one(later_model, on="id")
    )

    later_only = (
        joined.group_by("later.doubled")
        .aggregate("later.total")
        .order_by("later.doubled")
        .execute()
    )
    with_middle = (
        joined.group_by("later.doubled")
        .aggregate("later.total", "middle.total")
        .order_by("later.doubled")
        .execute()
    )

    assert later_only["later.doubled"].tolist() == [20, 40]
    assert later_only["later.total"].tolist() == [10, 20]
    assert with_middle["later.doubled"].tolist() == [20, 40]
    assert with_middle["later.total"].tolist() == [10, 20]
    assert with_middle["middle.total"].tolist() == [100, 200]


def test_user_column_with_internal_join_prefix_does_not_change_predicate(con):
    left = con.create_table(
        "temporary_alias_left",
        pd.DataFrame({"id": [1], "__bsl_jk_id": [99]}),
    )
    right = con.create_table(
        "temporary_alias_right",
        pd.DataFrame({"id": [1], "value": [2]}),
    )
    joined = to_semantic_table(left, "left").join_one(
        to_semantic_table(right, "right"), on="id"
    )

    result = joined.execute()
    assert result.iloc[0].to_dict() == {
        "id": 1,
        "__bsl_jk_id": 99,
        "id_right": 1,
        "value": 2,
    }


def test_cross_join_uses_the_same_collision_safe_aliases(con):
    left = con.create_table(
        "cross_alias_left",
        pd.DataFrame({"value": [1], "value_right": [9]}),
    )
    right = con.create_table(
        "cross_alias_right",
        pd.DataFrame({"value": [2]}),
    )
    joined = to_semantic_table(left, "left").join_cross(
        to_semantic_table(right, "right")
    )

    assert tuple(joined.columns) == ("value", "value_right", "value_right2")
    assert joined.execute().iloc[0].to_dict() == {
        "value": 1,
        "value_right": 9,
        "value_right2": 2,
    }


def test_same_entity_names_not_joined_on_entity_keys_upgrade_grain(con):
    orders = con.create_table(
        "entity_orders", pd.DataFrame({"id": [1, 2], "customer_id": [10, 10]})
    )
    customers = con.create_table(
        "entity_customers", pd.DataFrame({"id": [100], "customer_id": [10]})
    )
    orders_model = (
        to_semantic_table(orders, "orders")
        .with_dimensions(id=Dimension(expr=lambda t: t.id, is_entity=True))
        .with_measures(row_count=lambda t: t.count())
    )
    customers_model = (
        to_semantic_table(customers, "customers")
        .with_dimensions(id=Dimension(expr=lambda t: t.id, is_entity=True))
        .with_measures(row_count=lambda t: t.count())
    )

    with pytest.warns(UserWarning, match="Grain mismatch"):
        joined = orders_model.join_one(customers_model, on="customer_id")

    assert joined.op().cardinality == "many"


def test_json_definition_includes_calculated_measure_metadata(con):
    table = con.create_table("json_calc", pd.DataFrame({"value": [1, 2]}))
    model = to_semantic_table(table, "model").with_measures(
        total={"expr": lambda t: t.value.sum(), "description": "Base"},
        doubled={"expr": lambda t: t.total * 2, "description": "Calculated"},
    )

    definition = model.json_definition
    assert definition["measures"]["doubled"]["description"] == "Calculated"
    assert definition["calculated_measures"]["doubled"]["description"] == "Calculated"


def _cross_table_filter_models(con):
    left = con.create_table(
        "cross_filter_root",
        pd.DataFrame(
            {
                # Multiple source rows deliberately share the join key. A
                # join-key-only bridge cannot preserve a row-level predicate.
                "id": [1, 1],
                "status": ["A", "B"],
                "value": [10, 20],
            }
        ),
    )
    right = con.create_table(
        "cross_filter_many",
        pd.DataFrame({"id": [1], "flag": [False]}),
    )
    left_model = (
        to_semantic_table(left, "left")
        .with_dimensions(status=lambda t: t.status)
        .with_measures(total=lambda t: t.value.sum())
    )
    right_model = to_semantic_table(right, "right").with_dimensions(
        flag=lambda t: t.flag
    )
    return left_model.join_many(right_model, on="id")


def test_cross_table_or_with_root_only_measure_fails_closed(con):
    joined = _cross_table_filter_models(con)
    query = joined.filter(
        lambda t: (t["left.status"] == "A") | t["right.flag"]
    ).aggregate("left.total")

    with pytest.raises(ValueError, match="row-precisely"):
        query.execute()


def test_cross_table_and_with_root_only_measure_remains_row_precise(con):
    joined = _cross_table_filter_models(con)
    result = joined.filter(
        lambda t: (t["left.status"] == "A") & ~t["right.flag"]
    ).aggregate("left.total").execute()

    assert result["left.total"].iloc[0] == 10
