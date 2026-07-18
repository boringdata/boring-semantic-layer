"""Regression tests for the round-4 nest= execution defect.

``nest=`` lambdas used to receive a scope over raw columns only: the
canonical measure-name form (``t.group_by("sku").aggregate("total_qty")``)
raised a column-not-found error, and inner aggregations whose result
names collided with raw columns executed but were silently discarded in
favour of one raw struct per source row.

Nest lambdas now receive the aggregation's semantic source table, so
measure names, dimension names, and inline lambdas resolve exactly like
a top-level aggregate. The inner query compiles at (outer keys + inner
keys) grain and is attached as an array-of-structs column via a
null-safe left join on the outer keys. Unsupported nested shapes raise
``NotImplementedError`` — never silent raw-row structs.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table

ORDERS_DF = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "sku": ["a", "a", "b", "b", "c", "c"],
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "qty": [1, 2, 3, 4, 5, 6],
        "status": ["open", "closed", "open", "closed", "open", "open"],
        "customer_id": [100, 100, 200, 200, 300, 999],
    }
)


@pytest.fixture
def orders():
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("orders", ORDERS_DF)
    return (
        to_semantic_table(tbl, name="orders")
        .with_dimensions(status=lambda t: t.status, sku=lambda t: t.sku)
        .with_measures(
            total_qty=lambda t: t.qty.sum(),
            total=lambda t: t.amount.sum(),
        )
    )


def _nested_frames(result, nest_col):
    """{outer key value: DataFrame of the nested structs} for easy comparison."""
    out = {}
    for _, row in result.iterrows():
        items = row[nest_col]
        out[row[result.columns[0]]] = (
            None if items is None else pd.DataFrame(list(items))
        )
    return out


def _ground_truth(df, outer_keys, inner_keys, value_col="qty"):
    """Pandas ground truth: inner sums computed within each outer group."""
    grouped = df.groupby([*outer_keys, *inner_keys], dropna=False)[value_col].sum()
    return grouped.reset_index()


def _assert_nested_matches(nested, truth, outer_key, inner_keys, measure_name):
    for outer_val, frame in nested.items():
        expected = truth[truth[outer_key] == outer_val]
        assert frame is not None, f"missing nested rows for {outer_val!r}"
        got = frame.sort_values(inner_keys).reset_index(drop=True)
        exp = expected.drop(columns=[outer_key]).sort_values(inner_keys).reset_index(drop=True)
        assert list(got.columns) == [*inner_keys, measure_name]
        for ik in inner_keys:
            assert list(got[ik]) == list(exp[ik]), (outer_val, ik)
        assert [float(v) for v in got[measure_name]] == [
            float(v) for v in exp["qty"]
        ], outer_val


def test_nest_canonical_measure_name_form(orders):
    """The Malloy-canonical form computes the inner measure per outer group."""
    result = (
        orders.group_by("status")
        .aggregate(
            "total",
            nest={"by_sku": lambda t: t.group_by("sku").aggregate("total_qty")},
        )
        .execute()
    )

    assert list(result.columns) == ["status", "total", "by_sku"]
    totals = dict(zip(result["status"], result["total"], strict=True))
    assert totals == {"open": 150.0, "closed": 60.0}

    truth = _ground_truth(ORDERS_DF, ["status"], ["sku"])
    _assert_nested_matches(
        _nested_frames(result, "by_sku"), truth, "status", ["sku"], "total_qty"
    )


def test_nest_aliased_inline_lambda_form(orders):
    """Inline lambdas inside the nested aggregate resolve like top-level ones."""
    result = (
        orders.group_by("status")
        .aggregate(
            "total",
            nest={
                "by_sku": lambda t: t.group_by("sku").aggregate(
                    sumq=lambda x: x.qty.sum()
                )
            },
        )
        .execute()
    )

    truth = _ground_truth(ORDERS_DF, ["status"], ["sku"])
    _assert_nested_matches(
        _nested_frames(result, "by_sku"), truth, "status", ["sku"], "sumq"
    )


def test_nest_inline_name_colliding_with_raw_column(orders):
    """The silent-discard shape: result names matching raw columns must now
    return aggregated values, not one raw struct per source row."""
    result = (
        orders.group_by("status")
        .aggregate(
            "total",
            nest={
                "by_sku": lambda t: t.group_by("sku").aggregate(
                    qty=lambda x: x.qty.sum()
                )
            },
        )
        .execute()
    )

    open_rows = _nested_frames(result, "by_sku")["open"]
    # Raw per-row structs would have 4 entries for "open" (two c rows).
    assert len(open_rows) == 3
    truth = _ground_truth(ORDERS_DF, ["status"], ["sku"])
    _assert_nested_matches(
        _nested_frames(result, "by_sku"), truth, "status", ["sku"], "qty"
    )


def test_nest_multi_key_inner_group_by(orders):
    result = (
        orders.group_by("status")
        .aggregate(
            "total",
            nest={
                "by_both": lambda t: t.group_by("sku", "customer_id").aggregate(
                    "total_qty"
                )
            },
        )
        .execute()
    )

    truth = _ground_truth(ORDERS_DF, ["status"], ["sku", "customer_id"])
    _assert_nested_matches(
        _nested_frames(result, "by_both"),
        truth,
        "status",
        ["sku", "customer_id"],
        "total_qty",
    )


def test_nest_within_filtered_outer_query(orders):
    """The outer filter restricts the nested aggregation's rows too."""
    result = (
        orders.filter(lambda t: t.qty >= 2)
        .group_by("status")
        .aggregate(
            "total",
            nest={"by_sku": lambda t: t.group_by("sku").aggregate("total_qty")},
        )
        .execute()
    )

    totals = dict(zip(result["status"], result["total"], strict=True))
    assert totals == {"open": 140.0, "closed": 60.0}

    filtered = ORDERS_DF[ORDERS_DF["qty"] >= 2]
    truth = _ground_truth(filtered, ["status"], ["sku"])
    _assert_nested_matches(
        _nested_frames(result, "by_sku"), truth, "status", ["sku"], "total_qty"
    )


def test_nest_inner_filter_keeps_outer_groups(orders):
    """A filter inside the nest lambda restricts only the nested rows; outer
    groups with no surviving inner rows keep a NULL array."""
    result = (
        orders.group_by("status")
        .aggregate(
            "total",
            nest={
                "big_skus": lambda t: t.filter(lambda x: x.qty >= 5)
                .group_by("sku")
                .aggregate("total_qty")
            },
        )
        .execute()
    )

    totals = dict(zip(result["status"], result["total"], strict=True))
    assert totals == {"open": 150.0, "closed": 60.0}

    nested = _nested_frames(result, "big_skus")
    assert nested["closed"] is None
    open_rows = nested["open"]
    assert len(open_rows) == 1
    assert open_rows.iloc[0]["sku"] == "c"
    assert float(open_rows.iloc[0]["total_qty"]) == 11.0


def test_nest_without_outer_keys(orders):
    result = orders.aggregate(
        "total",
        nest={"by_sku": lambda t: t.group_by("sku").aggregate("total_qty")},
    ).execute()

    assert len(result) == 1
    assert float(result["total"].iloc[0]) == 210.0
    by_sku = pd.DataFrame(list(result["by_sku"].iloc[0])).sort_values("sku")
    assert list(by_sku["sku"]) == ["a", "b", "c"]
    assert [float(v) for v in by_sku["total_qty"]] == [3.0, 7.0, 11.0]


def test_nest_bare_group_by_keeps_per_row_structs(orders):
    """Pinned historical semantics: bare group_by collects one struct per
    source row, duplicates included."""
    result = (
        orders.group_by("status")
        .aggregate("total", nest={"rows": lambda t: t.group_by(["sku", "qty"])})
        .execute()
    )

    nested = _nested_frames(result, "rows")
    open_rows = nested["open"].sort_values(["sku", "qty"]).reset_index(drop=True)
    assert len(open_rows) == 4  # both "c" rows survive
    assert list(open_rows["sku"]) == ["a", "b", "c", "c"]
    assert list(open_rows["qty"]) == [1, 3, 5, 6]


def test_nest_inner_order_by_orders_each_group_array(orders):
    """order_by after the inner aggregate orders each group's struct array."""
    result = (
        orders.group_by("status")
        .aggregate(
            "total",
            nest={
                "x": lambda t: t.group_by("sku")
                .aggregate("total_qty")
                .order_by(lambda t: t.total_qty.desc())
            },
        )
        .execute()
    )
    nested = _nested_frames(result, "x")
    open_qty = [float(v) for v in nested["open"]["total_qty"]]
    assert open_qty == sorted(open_qty, reverse=True)


def test_nest_unsupported_shapes_raise(orders):
    """Transformed bare group_by raises loudly instead of silently
    collecting raw rows."""
    with pytest.raises(NotImplementedError, match="bare group_by"):
        orders.group_by("status").aggregate(
            "total",
            nest={"x": lambda t: t.filter(lambda x: x.qty > 1).group_by("sku")},
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
