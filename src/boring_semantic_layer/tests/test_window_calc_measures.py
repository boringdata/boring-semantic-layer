"""Test whether window functions can be expressed as calculated measures.

This tests the hypothesis that the calc measure AST (MeasureRef, MethodCall, BinOp)
can capture and compile window expressions like moving averages, without needing
SemanticMutateOp.
"""

import pandas as pd
import pytest
import xorq.api as xo

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    return xo.connect()


@pytest.fixture(scope="module")
def monthly_flights(con):
    df = pd.DataFrame(
        {
            "dep_month": pd.to_datetime(
                ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01", "2023-05-01", "2023-06-01"]
            ),
            "carrier": ["AA"] * 6,
            "flights": [100, 120, 90, 130, 110, 140],
            "delay": [10.0, 15.0, 8.0, 12.0, 20.0, 5.0],
        }
    )
    return con.create_table("monthly_flights", df)


@pytest.fixture(scope="module")
def flights_st(monthly_flights):
    return (
        to_semantic_table(monthly_flights, name="flights")
        .with_dimensions(
            dep_month=lambda t: t.dep_month,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            flight_count=lambda t: t.flights.sum(),
            avg_delay=lambda t: t.delay.mean(),
        )
    )


class TestWindowAsCalcMeasure:
    """Test window functions expressed as calculated measures."""

    def test_moving_avg_via_mutate_baseline(self, flights_st):
        """Baseline: moving average via .mutate() (the current way)."""
        result = (
            flights_st.group_by("dep_month")
            .aggregate("flight_count")
            .mutate(
                moving_avg=lambda t: t.flight_count.mean().over(
                    xo.window(order_by="dep_month", preceding=2, following=0)
                )
            )
            .order_by("dep_month")
            .execute()
        )
        assert "moving_avg" in result.columns
        assert len(result) == 6
        # First row: mean of just [100] = 100
        assert result["moving_avg"].iloc[0] == pytest.approx(100.0)
        # Third row: mean of [100, 120, 90] = 103.33
        assert result["moving_avg"].iloc[2] == pytest.approx(103.333, rel=1e-2)

    def test_moving_avg_via_calc_measure(self, flights_st):
        """Can we define a moving average as a calculated measure?

        The calc measure AST should capture:
          t.flight_count.mean().over(window(...))
        as:
          MethodCall(MethodCall(MeasureRef("flight_count"), "mean"), "over", (window,))
        """
        window = xo.window(order_by="dep_month", preceding=2, following=0)

        flights_with_window = flights_st.with_measures(
            moving_avg=lambda t: t.flight_count.mean().over(window),
        )

        result = (
            flights_with_window.group_by("dep_month")
            .aggregate("flight_count", "moving_avg")
            .order_by("dep_month")
            .execute()
        )
        assert "moving_avg" in result.columns
        assert len(result) == 6
        assert result["moving_avg"].iloc[0] == pytest.approx(100.0)
        assert result["moving_avg"].iloc[2] == pytest.approx(103.333, rel=1e-2)

    def test_cumulative_sum_via_calc_measure(self, flights_st):
        """Cumulative sum as a calculated measure."""
        window = xo.window(order_by="dep_month", preceding=None, following=0)

        flights_with_cumsum = flights_st.with_measures(
            cumulative_flights=lambda t: t.flight_count.sum().over(window),
        )

        result = (
            flights_with_cumsum.group_by("dep_month")
            .aggregate("flight_count", "cumulative_flights")
            .order_by("dep_month")
            .execute()
        )
        assert "cumulative_flights" in result.columns
        # Cumulative: 100, 220, 310, 440, 550, 690
        assert result["cumulative_flights"].iloc[0] == 100
        assert result["cumulative_flights"].iloc[2] == 310
        assert result["cumulative_flights"].iloc[5] == 690

    def test_rank_via_calc_measure(self, flights_st):
        """Rank using xo.rank() — a standalone analytic window function.

        xo.rank() bypasses MeasureScope (it's a standalone ibis call).
        PostAggCallable detects that the resulting Deferred can't resolve
        against the raw table and defers it to post-aggregation compilation.
        """
        flights_with_rank = flights_st.with_measures(
            flight_rank=lambda t: xo.rank().over(
                xo.window(order_by=xo.desc("flight_count"))
            ),
        )

        result = (
            flights_with_rank.group_by("dep_month")
            .aggregate("flight_count", "flight_rank")
            .order_by("flight_rank")
            .execute()
        )
        assert "flight_rank" in result.columns
        # Highest flight count (140) should be rank 0
        assert result["flight_count"].iloc[0] == 140

    def test_window_calc_measure_with_ratio(self, flights_st):
        """Combine window function with arithmetic in calc measure."""
        window = xo.window(order_by="dep_month", preceding=2, following=0)

        flights_enhanced = flights_st.with_measures(
            moving_avg=lambda t: t.flight_count.mean().over(window),
            # ratio of current to moving average
            vs_moving_avg=lambda t: t.flight_count / t.flight_count.mean().over(window),
        )

        result = (
            flights_enhanced.group_by("dep_month")
            .aggregate("flight_count", "moving_avg", "vs_moving_avg")
            .order_by("dep_month")
            .execute()
        )
        assert "vs_moving_avg" in result.columns
        # First row: 100 / 100 = 1.0
        assert result["vs_moving_avg"].iloc[0] == pytest.approx(1.0)
