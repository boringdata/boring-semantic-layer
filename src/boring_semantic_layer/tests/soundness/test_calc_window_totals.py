"""Soundness round 4: windows over inline base-column reductions.

``lift_inline_reductions`` used to rewrite *every* ``WindowFunction``
whose ``func`` was a lifted base reduction to the grand-totals shape,
silently discarding the user's ``group_by=``/``order_by=`` window spec.
A partitioned share came back as the share of the GRAND total.

The fix routes each windowed base reduction by shape:

* empty window → grand totals (the ``t.all(...)`` shape; unchanged),
* non-empty window, decomposable reduction, window keys all group keys
  → re-aggregate the lifted per-group value over the output rows with
  the window keys remapped to output columns,
* anything else → :class:`WindowedBaseReductionError` (never a silent
  wrong answer).
"""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("xorq", reason="xorq not installed")

import xorq.api as xo  # noqa: E402

from boring_semantic_layer import to_semantic_table  # noqa: E402
from boring_semantic_layer._xorq import ibis as xibis  # noqa: E402
from boring_semantic_layer.calc_compiler import (  # noqa: E402
    WindowedBaseReductionError,
)


@pytest.fixture(scope="module")
def orders_st():
    con = xo.duckdb.connect()
    df = pd.DataFrame(
        {
            "status": ["a", "a", "b", "b", "c", "c"],
            "region": ["e", "w", "e", "w", "e", "w"],
            "day": [1, 1, 2, 2, 3, 3],
            "amount": [10.0, 20.0, 30.0, 40.0, 100.0, 200.0],
        }
    )
    tbl = con.create_table("orders_r4", df)
    return (
        to_semantic_table(tbl, name="orders")
        .with_dimensions(
            status=lambda t: t.status,
            region=lambda t: t.region,
            day=lambda t: t.day,
        )
        .with_measures(total=lambda t: t.amount.sum())
    )


def test_partitioned_window_uses_partition_totals(orders_st):
    """``sum().over(window(group_by=region))`` divides by the REGION
    total (e=140, w=260), not the grand total (400)."""
    st = orders_st.with_measures(
        region_share=lambda t: t.amount.sum() / t.amount.sum().over(xibis.window(group_by=t.region))
    )
    df = (
        st.group_by("status", "region")
        .aggregate("region_share")
        .order_by("status", "region")
        .execute()
    )
    expected = [10 / 140, 20 / 260, 30 / 140, 40 / 260, 100 / 140, 200 / 260]
    assert df["region_share"].tolist() == pytest.approx(expected)


def test_ordered_window_running_total(orders_st):
    """A cumulative window ordered by a group key accumulates per-group
    sums in output order instead of collapsing to the grand total."""
    st = orders_st.with_measures(
        running=lambda t: t.amount.sum().over(
            xibis.window(order_by=t.day, preceding=None, following=0)
        )
    )
    df = st.group_by("day").aggregate("running").order_by("day").execute()
    assert df["running"].tolist() == pytest.approx([30.0, 100.0, 400.0])


def test_partitioned_min_max_count_reaggregate(orders_st):
    """min/max re-aggregate with min/max; count re-aggregates with sum."""
    st = orders_st.with_measures(
        region_max=lambda t: t.amount.max().over(xibis.window(group_by=t.region)),
        region_min=lambda t: t.amount.min().over(xibis.window(group_by=t.region)),
        region_rows=lambda t: t.amount.count().over(xibis.window(group_by=t.region)),
    )
    df = (
        st.group_by("status", "region")
        .aggregate("region_max", "region_min", "region_rows")
        .order_by("status", "region")
        .execute()
    )
    assert df["region_max"].tolist() == pytest.approx([100.0, 200.0] * 3)
    assert df["region_min"].tolist() == pytest.approx([10.0, 20.0] * 3)
    assert df["region_rows"].tolist() == [3] * 6


def test_nondecomposable_windowed_reduction_raises(orders_st):
    """mean() cannot be recomputed from per-group values — loud error,
    pointing at the measure-reference form."""
    st = orders_st.with_measures(
        region_avg=lambda t: t.amount.mean().over(xibis.window(group_by=t.region))
    )
    with pytest.raises(WindowedBaseReductionError, match="not decomposable"):
        st.group_by("status", "region").aggregate("region_avg").execute()


def test_non_group_key_partition_raises(orders_st):
    """Partitioning by a column that is not a group key of the query
    cannot be computed at the query's grain — loud error."""
    st = orders_st.with_measures(
        region_share=lambda t: t.amount.sum() / t.amount.sum().over(xibis.window(group_by=t.region))
    )
    with pytest.raises(WindowedBaseReductionError, match="not a group key"):
        st.group_by("status").aggregate("region_share").execute()


def test_empty_window_totals_unchanged(orders_st):
    """The ``t.all(...)`` grand-totals shape (empty window) keeps its
    base-totals semantics."""
    st = orders_st.with_measures(pct=lambda t: t.amount.sum() / t.all(t.amount.sum()))
    df = st.group_by("status").aggregate("pct").order_by("status").execute()
    assert df["pct"].tolist() == pytest.approx([30 / 400, 70 / 400, 300 / 400])


def test_mutate_empty_window_over_group_key_column_raises(orders_st):
    """``t.status.count().over(window())`` where ``status`` is a group
    key is ambiguous (base-row total vs. output-row count) — loud error
    instead of silently returning the base-row count."""
    agg = orders_st.group_by("status").aggregate("total")
    with pytest.raises(WindowedBaseReductionError, match="ambiguous"):
        agg.mutate(n=lambda t: t.status.count().over(xibis.window())).execute()


def test_mutate_measure_ref_windows_unaffected(orders_st):
    """Measure-reference windows (the documented alternative) keep
    working: partitioned share-of-parent and output-row count."""
    agg = orders_st.group_by("status", "region").aggregate("total")
    df = (
        agg.mutate(share=lambda t: t.total / t.total.sum().over(xibis.window(group_by=t.status)))
        .order_by("status", "region")
        .execute()
    )
    expected = [10 / 30, 20 / 30, 30 / 70, 40 / 70, 100 / 300, 200 / 300]
    assert df["share"].tolist() == pytest.approx(expected)

    agg2 = orders_st.group_by("status").aggregate("total")
    df2 = agg2.mutate(n=lambda t: t.total.count().over(xibis.window())).execute()
    assert df2["n"].tolist() == [3, 3, 3]
