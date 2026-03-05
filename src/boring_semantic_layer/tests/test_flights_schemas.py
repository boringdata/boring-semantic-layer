"""Complex schema tests using the Malloy flights dataset (R2 bucket).

Exercises star schema, snowflake schema, and multi-arm joins against real
production-scale data (344k flights, 19k airports, 3.6k aircraft, 60k models,
21 carriers) to surface aggregation bugs that synthetic data might miss.

All ground truth computed from raw ibis/SQL — BSL results must match.
"""

import ibis
import pandas as pd
import pytest
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


# ---------------------------------------------------------------------------
# Shared session fixture — download once, reuse across all test classes
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def raw_tables(con):
    """Load raw ibis tables from R2 parquet files."""
    return {
        "flights": con.read_parquet(f"{BASE_URL}/flights.parquet"),
        "carriers": con.read_parquet(f"{BASE_URL}/carriers.parquet"),
        "airports": con.read_parquet(f"{BASE_URL}/airports.parquet"),
        "aircraft": con.read_parquet(f"{BASE_URL}/aircraft.parquet"),
        "aircraft_models": con.read_parquet(f"{BASE_URL}/aircraft_models.parquet"),
    }


@pytest.fixture(scope="module")
def semantic_tables(raw_tables):
    """Build semantic models mirroring advanced_modeling.py."""
    carriers = (
        to_semantic_table(raw_tables["carriers"], name="carriers")
        .with_dimensions(
            code=_.code,
            name=_.name,
            nickname=_.nickname,
        )
        .with_measures(carrier_count=_.count())
    )

    flights = (
        to_semantic_table(raw_tables["flights"], name="flights")
        .with_dimensions(
            origin=_.origin,
            destination=_.destination,
            carrier=_.carrier,
            tail_num=_.tail_num,
        )
        .with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
            avg_distance=_.distance.mean(),
            avg_dep_delay=_.dep_delay.mean(),
            total_dep_delay=_.dep_delay.sum(),
            min_dep_delay=_.dep_delay.min(),
            max_dep_delay=_.dep_delay.max(),
        )
    )

    airports = (
        to_semantic_table(raw_tables["airports"], name="airports")
        .with_dimensions(
            code=_.code,
            state=_.state,
            city=_.city,
            fac_type=_.fac_type,
        )
        .with_measures(
            airport_count=_.count(),
        )
    )

    aircraft = (
        to_semantic_table(raw_tables["aircraft"], name="aircraft")
        .with_dimensions(
            tail_num=_.tail_num,
            aircraft_model_code=_.aircraft_model_code,
        )
        .with_measures(aircraft_count=_.count())
    )

    aircraft_models = (
        to_semantic_table(raw_tables["aircraft_models"], name="models")
        .with_dimensions(
            aircraft_model_code=_.aircraft_model_code,
            manufacturer=_.manufacturer,
            model=_.model,
            seats=_.seats,
        )
        .with_measures(
            model_count=_.count(),
            avg_seats=_.seats.mean(),
        )
    )

    # --- Build separate origin/dest airport models for diamond join ---
    origin_airports = (
        to_semantic_table(raw_tables["airports"], name="origin_airports")
        .with_dimensions(
            code=_.code,
            state=_.state,
            city=_.city,
        )
        .with_measures(origin_airport_count=_.count())
    )
    dest_airports = (
        to_semantic_table(raw_tables["airports"], name="dest_airports")
        .with_dimensions(
            code=_.code,
            state=_.state,
            city=_.city,
        )
        .with_measures(dest_airport_count=_.count())
    )

    return {
        "carriers": carriers,
        "flights": flights,
        "airports": airports,
        "aircraft": aircraft,
        "aircraft_models": aircraft_models,
        "origin_airports": origin_airports,
        "dest_airports": dest_airports,
    }


# ===================================================================
# 1. Star Schema: airports -< flights -- carriers
# ===================================================================
class TestStarSchema:
    """Star schema: airports (dim, one-side) joined to flights (fact, many-side)
    which is itself joined to carriers (dim, one-side via join_one).

    airports -< flights -- carriers
    """

    @pytest.fixture()
    def star(self, semantic_tables):
        s = semantic_tables
        flights_with_carriers = s["flights"].join_one(
            s["carriers"], on=lambda f, c: f.carrier == c.code,
        )
        return s["airports"].join_many(
            flights_with_carriers,
            on=lambda a, f: a.code == f.origin,
        )

    def test_scalar_flight_count(self, star):
        """Total flight count across the star should match raw data."""
        df = star.aggregate("flights.flight_count").execute()
        assert df["flights.flight_count"].iloc[0] == 344827

    def test_scalar_total_distance(self, star):
        """Total distance should not be inflated by join fan-out."""
        df = star.aggregate("flights.total_distance").execute()
        assert df["flights.total_distance"].iloc[0] == 255337195

    def test_scalar_avg_distance(self, star):
        """Mean distance must use sum/count decomposition correctly."""
        df = star.aggregate("flights.avg_distance").execute()
        assert df["flights.avg_distance"].iloc[0] == pytest.approx(740.48, abs=0.1)

    def test_group_by_state_top_states(self, star):
        """Group by origin state — top states by flight count."""
        df = (
            star.group_by("airports.state")
            .aggregate("flights.flight_count", "flights.total_distance")
            .execute()
        )
        ca = df[df["airports.state"] == "CA"]
        tx = df[df["airports.state"] == "TX"]
        assert ca["flights.flight_count"].iloc[0] == 40670
        assert tx["flights.flight_count"].iloc[0] == 40085

    def test_group_by_state_unmatched_rows_preserved(self, star):
        """States with airports but no origin flights should appear with NULL measures.

        Delaware (DE) has 42 airports but zero flights originating there.
        """
        df = (
            star.group_by("airports.state")
            .aggregate("flights.flight_count")
            .execute()
        )
        de = df[df["airports.state"] == "DE"]
        assert len(de) == 1, "Delaware should appear in results"
        # flight_count for unmatched state should be NULL
        assert de["flights.flight_count"].isna().iloc[0]

    def test_one_side_measure_airport_count_by_state(self, star):
        """One-side measure (airport_count) grouped by one-side dimension."""
        df = (
            star.group_by("airports.state")
            .aggregate("airports.airport_count")
            .execute()
        )
        ca = df[df["airports.state"] == "CA"]
        tx = df[df["airports.state"] == "TX"]
        assert ca["airports.airport_count"].iloc[0] == 984
        assert tx["airports.airport_count"].iloc[0] == 1845

    def test_both_sides_measures(self, star):
        """One-side + many-side measures in the same query."""
        df = (
            star.group_by("airports.state")
            .aggregate("airports.airport_count", "flights.flight_count")
            .execute()
        )
        ca = df[df["airports.state"] == "CA"]
        assert ca["airports.airport_count"].iloc[0] == 984
        assert ca["flights.flight_count"].iloc[0] == 40670

    def test_mean_by_state_not_inflated(self, star):
        """avg_dep_delay grouped by state must not be inflated by fan-out."""
        df = (
            star.group_by("airports.state")
            .aggregate("flights.avg_dep_delay")
            .execute()
        )
        ca = df[df["airports.state"] == "CA"]
        # Raw truth: CA avg dep_delay ≈ 7.63
        assert ca["flights.avg_dep_delay"].iloc[0] == pytest.approx(7.63, abs=0.1)


# ===================================================================
# 2. Snowflake Schema: flights -- carriers, flights -- aircraft -- models
# ===================================================================
class TestSnowflakeSchema:
    """Snowflake: flights joined to two arms via join_one:
      - carriers (1 level: carrier → code)
      - aircraft → models (2 levels: tail_num, then aircraft_model_code)
    """

    @pytest.fixture()
    def snowflake(self, semantic_tables):
        s = semantic_tables
        aircraft_with_models = s["aircraft"].join_one(
            s["aircraft_models"], on="aircraft_model_code",
        )
        return (
            s["flights"]
            .join_one(s["carriers"], on=lambda f, c: f.carrier == c.code)
            .join_one(aircraft_with_models, on="tail_num")
        )

    def test_scalar_flight_count(self, snowflake):
        """All 344827 flights should survive join_one chains."""
        df = snowflake.aggregate("flights.flight_count").execute()
        assert df["flights.flight_count"].iloc[0] == 344827

    def test_scalar_total_distance(self, snowflake):
        df = snowflake.aggregate("flights.total_distance").execute()
        assert df["flights.total_distance"].iloc[0] == 255337195

    def test_scalar_avg_distance(self, snowflake):
        df = snowflake.aggregate("flights.avg_distance").execute()
        assert df["flights.avg_distance"].iloc[0] == pytest.approx(740.48, abs=0.1)

    def test_group_by_carrier_nickname(self, snowflake):
        """Group by carriers.nickname — Southwest should have 88751 flights."""
        df = (
            snowflake.group_by("carriers.nickname")
            .aggregate("flights.flight_count", "flights.avg_distance")
            .execute()
        )
        sw = df[df["carriers.nickname"] == "Southwest"]
        assert sw["flights.flight_count"].iloc[0] == 88751
        assert sw["flights.avg_distance"].iloc[0] == pytest.approx(615.42, abs=0.1)

    def test_group_by_manufacturer(self, snowflake):
        """Group by models.manufacturer — Boeing should have 183236 flights."""
        df = (
            snowflake.group_by("models.manufacturer")
            .aggregate("flights.flight_count")
            .execute()
        )
        boeing = df[df["models.manufacturer"] == "BOEING"]
        assert boeing["flights.flight_count"].iloc[0] == 183236

    def test_group_by_carrier_and_manufacturer(self, snowflake):
        """Cross-arm group by: carriers.nickname x models.manufacturer."""
        df = (
            snowflake.group_by("carriers.nickname", "models.manufacturer")
            .aggregate("flights.flight_count")
            .execute()
        )
        # Southwest flies exclusively Boeing
        sw_boeing = df[
            (df["carriers.nickname"] == "Southwest")
            & (df["models.manufacturer"] == "BOEING")
        ]
        assert sw_boeing["flights.flight_count"].iloc[0] == 88751

    def test_avg_distance_by_manufacturer(self, snowflake):
        """Mean distance by manufacturer — tests sum/count decomposition through 2-level arm."""
        df = (
            snowflake.group_by("models.manufacturer")
            .aggregate("flights.avg_distance")
            .execute()
        )
        boeing = df[df["models.manufacturer"] == "BOEING"]
        assert boeing["flights.avg_distance"].iloc[0] == pytest.approx(718.80, abs=0.1)

    def test_three_level_measure(self, snowflake):
        """Access model_count (3 levels deep) — should count distinct models per group."""
        df = (
            snowflake.group_by("carriers.nickname")
            .aggregate("models.model_count")
            .execute()
        )
        sw = df[df["carriers.nickname"] == "Southwest"]
        assert sw["models.model_count"].iloc[0] > 0


# ===================================================================
# 3. Diamond Join: flights -- origin_airports, flights -- dest_airports
# ===================================================================
class TestDiamondJoin:
    """Diamond: flights joined to the same physical table (airports) via two
    different foreign keys (origin and destination), modelled as separate
    semantic tables (origin_airports, dest_airports).

    flights -- origin_airports (via origin == code)
    flights -- dest_airports   (via destination == code)
    """

    @pytest.fixture()
    def diamond(self, semantic_tables):
        s = semantic_tables
        return (
            s["flights"]
            .join_one(
                s["origin_airports"],
                on=lambda f, a: f.origin == a.code,
            )
            .join_one(
                s["dest_airports"],
                on=lambda f, a: f.destination == a.code,
            )
        )

    def test_flight_count_preserved(self, diamond):
        """Flight count should not be inflated by two join_one arms."""
        df = diamond.aggregate("flights.flight_count").execute()
        # Some flights may have origin/dest codes not in airports, so inner join
        # may lose a few. The key is no INFLATION.
        assert df["flights.flight_count"].iloc[0] <= 344827
        assert df["flights.flight_count"].iloc[0] > 340000  # negligible loss

    def test_group_by_origin_state(self, diamond):
        """Group by origin_airports.state should match the star schema test."""
        df = (
            diamond.group_by("origin_airports.state")
            .aggregate("flights.flight_count")
            .execute()
        )
        ca = df[df["origin_airports.state"] == "CA"]
        assert ca["flights.flight_count"].iloc[0] == 40670

    def test_group_by_dest_state(self, diamond):
        """Group by dest_airports.state — independent of origin grouping."""
        df = (
            diamond.group_by("dest_airports.state")
            .aggregate("flights.flight_count")
            .execute()
        )
        # CA is a top destination too
        ca = df[df["dest_airports.state"] == "CA"]
        assert ca["flights.flight_count"].iloc[0] > 30000

    def test_cross_arm_group_by(self, diamond):
        """Group by both origin and dest state simultaneously."""
        df = (
            diamond.group_by("origin_airports.state", "dest_airports.state")
            .aggregate("flights.flight_count")
            .execute()
        )
        # Total flights across all (origin_state, dest_state) combos should
        # equal total flights (no double-counting)
        assert df["flights.flight_count"].sum() == diamond.aggregate(
            "flights.flight_count"
        ).execute()["flights.flight_count"].iloc[0]

    def test_mean_not_inflated_across_diamond(self, diamond):
        """avg_distance should not be inflated by two join arms."""
        df = diamond.aggregate("flights.avg_distance").execute()
        assert df["flights.avg_distance"].iloc[0] == pytest.approx(740.5, abs=1.0)


# ===================================================================
# 4. Full Star + Snowflake: airports -< flights -- carriers -- aircraft -- models
# ===================================================================
class TestFullStarSnowflake:
    """The full schema from advanced_modeling.py:
    airports (one-side, join_many) -< flights (fact)
    flights -- carriers (join_one)
    flights -- aircraft (join_one) -- models (join_one)

    Tests both one-side and many-side measures through 4-level deep joins.
    """

    @pytest.fixture()
    def full_schema(self, semantic_tables):
        s = semantic_tables
        aircraft_with_models = s["aircraft"].join_one(
            s["aircraft_models"], on="aircraft_model_code",
        )
        flights_full = (
            s["flights"]
            .join_one(s["carriers"], on=lambda f, c: f.carrier == c.code)
            .join_one(aircraft_with_models, on="tail_num")
        )
        return s["airports"].join_many(
            flights_full,
            on=lambda a, f: a.code == f.origin,
        )

    def test_scalar_flight_count(self, full_schema):
        """344k+ flights through 4-table schema."""
        df = full_schema.aggregate("flights.flight_count").execute()
        assert df["flights.flight_count"].iloc[0] == 344827

    def test_one_side_airport_count(self, full_schema):
        """airports.airport_count at global level."""
        df = full_schema.aggregate("airports.airport_count").execute()
        assert df["airports.airport_count"].iloc[0] == 19793

    def test_group_by_state_with_deep_measures(self, full_schema):
        """Group by state, aggregate measures from all four levels."""
        df = (
            full_schema.group_by("airports.state")
            .aggregate(
                "airports.airport_count",
                "flights.flight_count",
                "flights.avg_distance",
            )
            .execute()
        )
        ca = df[df["airports.state"] == "CA"]
        assert ca["airports.airport_count"].iloc[0] == 984
        assert ca["flights.flight_count"].iloc[0] == 40670
        assert ca["flights.avg_distance"].iloc[0] == pytest.approx(926.74, abs=1.0)

    def test_group_by_manufacturer_through_star(self, full_schema):
        """Group by models.manufacturer (4 levels deep) through a star join."""
        df = (
            full_schema.group_by("models.manufacturer")
            .aggregate("flights.flight_count")
            .execute()
        )
        boeing = df[df["models.manufacturer"] == "BOEING"]
        assert boeing["flights.flight_count"].iloc[0] == 183236

    def test_unmatched_states_appear(self, full_schema):
        """States with airports but no flights should appear."""
        df = (
            full_schema.group_by("airports.state")
            .aggregate("flights.flight_count")
            .execute()
        )
        # Delaware has 42 airports but 0 flights
        de = df[df["airports.state"] == "DE"]
        assert len(de) == 1
        assert de["flights.flight_count"].isna().iloc[0]

    def test_mean_not_inflated_through_snowflake_arms(self, full_schema):
        """avg_distance by state should not inflate through snowflake arms."""
        df = (
            full_schema.group_by("airports.state")
            .aggregate("flights.avg_dep_delay")
            .execute()
        )
        ca = df[df["airports.state"] == "CA"]
        # Raw truth: CA avg dep_delay ≈ 7.63 — should NOT be 10x or 100x
        assert ca["flights.avg_dep_delay"].iloc[0] == pytest.approx(7.63, abs=0.5)


# ===================================================================
# 5. Filter pushdown across star schema
# ===================================================================
class TestStarSchemaFilters:
    """Filter pushdown through the star schema: airports -< flights -- carriers."""

    @pytest.fixture()
    def star(self, semantic_tables):
        s = semantic_tables
        flights_with_carriers = s["flights"].join_one(
            s["carriers"], on=lambda f, c: f.carrier == c.code,
        )
        return s["airports"].join_many(
            flights_with_carriers,
            on=lambda a, f: a.code == f.origin,
        )

    def test_filter_on_many_side_column(self, star):
        """Filter on flights.distance > 1000 should restrict correctly."""
        df = (
            star.filter(lambda t: t.distance > 1000)
            .aggregate("flights.flight_count")
            .execute()
        )
        assert df["flights.flight_count"].iloc[0] == pytest.approx(90000, abs=50)

    def test_filter_on_one_side_dimension(self, star):
        """Filter on airports.state == 'CA' should restrict via dim bridge."""
        df = (
            star.filter(lambda t: t.state == "CA")
            .aggregate("flights.flight_count")
            .execute()
        )
        assert df["flights.flight_count"].iloc[0] == 40670

    def test_filter_with_group_by(self, star):
        """Filter + group_by across the star."""
        df = (
            star.filter(lambda t: t.distance > 1000)
            .group_by("airports.state")
            .aggregate("flights.flight_count")
            .execute()
        )
        # CA should have fewer long-distance flights than total
        ca = df[df["airports.state"] == "CA"]
        assert ca["flights.flight_count"].iloc[0] < 40670
        assert ca["flights.flight_count"].iloc[0] > 0

    def test_filter_on_carrier_through_join(self, star):
        """Filter on carriers.nickname through the join tree."""
        df = (
            star.filter(lambda t: t.nickname == "Southwest")
            .aggregate("flights.flight_count")
            .execute()
        )
        assert df["flights.flight_count"].iloc[0] == pytest.approx(88751, abs=50)


# ===================================================================
# 6. Min/Max through snowflake
# ===================================================================
class TestMinMaxSnowflake:
    """Min/Max re-aggregation through snowflake arms."""

    @pytest.fixture()
    def snowflake(self, semantic_tables):
        s = semantic_tables
        aircraft_with_models = s["aircraft"].join_one(
            s["aircraft_models"], on="aircraft_model_code",
        )
        return (
            s["flights"]
            .join_one(s["carriers"], on=lambda f, c: f.carrier == c.code)
            .join_one(aircraft_with_models, on="tail_num")
        )

    def test_min_dep_delay_scalar(self, snowflake):
        """Global min dep_delay through snowflake should be correct."""
        df = snowflake.aggregate("flights.min_dep_delay").execute()
        assert df["flights.min_dep_delay"].iloc[0] == -1133

    def test_max_dep_delay_scalar(self, snowflake):
        """Global max dep_delay through snowflake should be correct."""
        df = snowflake.aggregate("flights.max_dep_delay").execute()
        assert df["flights.max_dep_delay"].iloc[0] == 1433

    def test_min_max_by_carrier(self, snowflake):
        """Min/Max grouped by carrier nickname."""
        df = (
            snowflake.group_by("carriers.nickname")
            .aggregate("flights.min_dep_delay", "flights.max_dep_delay")
            .execute()
        )
        sw = df[df["carriers.nickname"] == "Southwest"]
        # Min/max must not be inflated by snowflake arms
        assert sw["flights.min_dep_delay"].iloc[0] < 0
        assert sw["flights.max_dep_delay"].iloc[0] > 100


# ===================================================================
# 7. Mixed agg types through star
# ===================================================================
class TestMixedAggStar:
    """Sum, count, min, max, mean all in one query through the star schema."""

    @pytest.fixture()
    def star(self, semantic_tables):
        s = semantic_tables
        flights_with_carriers = s["flights"].join_one(
            s["carriers"], on=lambda f, c: f.carrier == c.code,
        )
        return s["airports"].join_many(
            flights_with_carriers,
            on=lambda a, f: a.code == f.origin,
        )

    def test_all_agg_types_scalar(self, star):
        """All agg types in one scalar query should be correct."""
        df = star.aggregate(
            "flights.flight_count",
            "flights.total_distance",
            "flights.avg_distance",
            "flights.total_dep_delay",
            "flights.avg_dep_delay",
            "flights.min_dep_delay",
            "flights.max_dep_delay",
        ).execute()

        assert df["flights.flight_count"].iloc[0] == 344827
        assert df["flights.total_distance"].iloc[0] == 255337195
        assert df["flights.avg_distance"].iloc[0] == pytest.approx(740.48, abs=0.1)
        assert df["flights.min_dep_delay"].iloc[0] == -1133
        assert df["flights.max_dep_delay"].iloc[0] == 1433

    def test_all_agg_types_by_state(self, star):
        """All agg types grouped by origin state."""
        df = (
            star.group_by("airports.state")
            .aggregate(
                "flights.flight_count",
                "flights.total_distance",
                "flights.avg_distance",
                "flights.avg_dep_delay",
            )
            .execute()
        )
        il = df[df["airports.state"] == "IL"]
        assert il["flights.flight_count"].iloc[0] == 20850
        assert il["flights.avg_dep_delay"].iloc[0] == pytest.approx(11.02, abs=0.1)
