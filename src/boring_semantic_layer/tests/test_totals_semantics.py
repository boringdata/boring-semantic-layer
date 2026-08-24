"""Regression tests for ``t.all()`` / percent-of-total semantics.

``t.all(x)`` means "x over the whole filtered dataset, ignoring the group
by". Getting that wrong is silent: the query still runs and the shares still
look like shares. The data below is chosen so the correct answer and the
sum-of-group-values answer never coincide:

    carrier A: distance [10, 20, 30]  -> sum 60,  mean 20
    carrier B: distance [100]         -> sum 100, mean 100
    overall:   sum 160, mean 40       (sum of the two group means is 120)
"""

from __future__ import annotations

import ibis
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.errors import QueryError


@pytest.fixture
def model():
    flights = ibis.memtable(
        {
            "carrier": ["A", "A", "A", "B"],
            "distance": [10, 20, 30, 100],
            "origin": ["x", "y", "x", "y"],
        }
    )
    return (
        to_semantic_table(flights, "flights")
        .with_dimensions(carrier=lambda t: t.carrier, origin=lambda t: t.origin)
        .with_measures(
            total=lambda t: t.distance.sum(),
            avg=lambda t: t.distance.mean(),
        )
    )


@pytest.fixture
def airports():
    table = ibis.memtable({"code": ["x", "y"], "region": ["west", "east"]})
    return (
        to_semantic_table(table, "airports")
        .with_dimensions(code=lambda t: t.code, region=lambda t: t.region)
        .with_measures(cnt=lambda t: t.count())
    )


MEAN_SHARE = {"A": 20 / 40, "B": 100 / 40}
SUM_SHARE = {"A": 60 / 160, "B": 100 / 160}
SUM_OF_GROUP_MEANS = {"A": 20 / 120, "B": 100 / 120}


def _shares(model, col="share", key="carrier"):
    df = model.group_by(key).aggregate(col).execute()
    return {k: pytest.approx(float(v)) for k, v in zip(df[key], df[col], strict=True)}


# ---------------------------------------------------------------------------
# t.all() over an expression, not just a bare measure reference
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "share_fn",
    [
        pytest.param(lambda t: t.avg / t.all(t.avg), id="bare-reference"),
        pytest.param(lambda t: t.avg / t.all(t.avg * 1), id="scaled-inside"),
        pytest.param(lambda t: t.avg / (t.all(t.avg) + 0), id="arithmetic-outside"),
        pytest.param(lambda t: t.avg / t.all(t.avg + 0), id="arithmetic-inside"),
    ],
)
def test_all_of_a_measure_expression_uses_the_real_total(model, share_fn):
    """``t.all(m)`` and ``t.all(m * 1)`` must mean the same thing.

    Anything that was not an exact Field reference fell through to
    ``x.sum().over(window())``, which for a mean measure sums the per-group
    means — here 120 instead of 40, a 3x error with no warning.
    """
    assert _shares(model.with_measures(share=share_fn)) == MEAN_SHARE
    assert _shares(model.with_measures(share=share_fn)) != SUM_OF_GROUP_MEANS


def test_all_of_an_additive_measure_expression(model):
    share = model.with_measures(share=lambda t: t.total / t.all(t.total * 1))
    assert _shares(share) == SUM_SHARE


# ---------------------------------------------------------------------------
# Integer division
# ---------------------------------------------------------------------------


def test_integer_measure_over_integer_total_is_a_ratio(model):
    """Two integer operands must not truncate to 0 (xorq's DataFusion does)."""
    assert _shares(model.with_measures(share=lambda t: t.total / t.all(t.total))) == SUM_SHARE


def test_percent_of_total_is_order_independent():
    """Declaration order must not change the answer."""
    data = ibis.memtable({"carrier": ["AA", "UA", "DL", "WN", "B6"] * 10})
    expected = dict.fromkeys(["AA", "UA", "DL", "WN", "B6"], pytest.approx(0.2))

    dims_after = (
        to_semantic_table(data, "f")
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(ratio=lambda t: t.flight_count / t.all(t.flight_count))
        .with_dimensions(carrier=lambda t: t.carrier)
    )
    dims_before = (
        to_semantic_table(data, "f")
        .with_dimensions(carrier=lambda t: t.carrier)
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(ratio=lambda t: t.flight_count / t.all(t.flight_count))
    )
    assert _shares(dims_after, "ratio") == expected
    assert _shares(dims_before, "ratio") == expected


# ---------------------------------------------------------------------------
# Post-aggregation chain .mutate()
# ---------------------------------------------------------------------------


def test_chain_mutate_after_order_by_is_refused(model):
    """``aggregate().order_by().mutate()`` sees only grouped rows.

    A window sum over them equals the true total only for SUM/COUNT
    measures, so the spelling is refused outright: post-aggregation row
    math belongs on ``.to_untagged()``, and totals belong on the model
    (calc measure) or directly on the aggregate, where they are computed
    from the underlying rows.
    """
    with pytest.raises(QueryError, match="to_untagged"):
        (
            model.group_by("carrier")
            .aggregate("total")
            .order_by("carrier")
            .mutate(share=lambda t: t.total / t.all(t.total))
        )
    # The surviving spellings agree with each other.
    assert _shares(model.with_measures(share=lambda t: t.total / t.all(t.total))) == SUM_SHARE
    # The direct-on-the-aggregate spelling, pinned on duckdb: the memtable →
    # canonical-backend path truncates this integer ratio to 0.0 (pre-existing
    # xorq/DataFusion flavor defect, independent of the mutate desugaring —
    # the compiled SQL carries the float cast and duckdb executes it).
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table(
        "flights_chain",
        {"carrier": ["A", "A", "A", "B"], "distance": [10, 20, 30, 100]},
    )
    duck_model = (
        to_semantic_table(tbl, "flights")
        .with_dimensions(carrier=lambda t: t.carrier)
        .with_measures(total=lambda t: t.distance.sum())
    )
    df = (
        duck_model.group_by("carrier")
        .aggregate("total")
        .mutate(share=lambda t: t.total / t.all(t.total))
        .execute()
    )
    got = {k: pytest.approx(float(v)) for k, v in zip(df["carrier"], df["share"], strict=True)}
    assert got == SUM_SHARE


def test_chain_mutate_total_of_a_mean_measure_is_refused(model):
    """Summing per-group means is not the overall mean — refuse, don't guess.

    The old chain spelling returned a window sum that disagreed with the
    identical calc-measure formula (0.167 vs 0.5). The chain spelling is now
    refused entirely; the measure path computes the true overall mean from
    the underlying rows, so both surviving spellings agree.
    """
    with pytest.raises(QueryError, match="to_untagged"):
        (
            model.group_by("carrier")
            .aggregate("avg")
            .order_by("carrier")
            .mutate(share=lambda t: t.avg / t.all(t.avg))
        )
    assert _shares(model.with_measures(share=lambda t: t.avg / t.all(t.avg))) == MEAN_SHARE
    df = (
        model.group_by("carrier")
        .aggregate("avg")
        .mutate(share=lambda t: t.avg / t.all(t.avg))
        .execute()
    )
    got = {k: pytest.approx(float(v)) for k, v in zip(df["carrier"], df["share"], strict=True)}
    assert got == MEAN_SHARE


# ---------------------------------------------------------------------------
# Totals under a fan-out join
# ---------------------------------------------------------------------------


def test_totals_stay_correct_under_join_many(model, airports):
    """The denominator must not inflate with the join's fan-out."""
    joined = model.with_measures(share=lambda t: t.total / t.all(t.total)).join_many(
        airports, on=lambda left, right: left.origin == right.code
    )
    assert _shares(joined, "flights.share", "flights.carrier") == SUM_SHARE


def test_inline_reduction_in_totals_under_join_many_is_refused(model, airports):
    """A calc measure that builds its own reduction has no fan-out-safe base.

    This used to reach the engine as an aggregate with no columns at all and
    surface as an unrelated arrow error ("Schema and number of arrays
    unequal"); the message now names the problem and the fix.
    """
    joined = model.with_measures(
        pot=lambda t: t.distance.sum() / t.all(t.distance.sum())
    ).join_many(airports, on=lambda left, right: left.origin == right.code)
    with pytest.raises(ValueError, match="fan-out-safe"):
        joined.group_by("flights.carrier").aggregate("flights.pot").execute()

    # ...and the suggested spelling works.
    fixed = model.with_measures(share=lambda t: t.total / t.all(t.total)).join_many(
        airports, on=lambda left, right: left.origin == right.code
    )
    assert _shares(fixed, "flights.share", "flights.carrier") == SUM_SHARE
