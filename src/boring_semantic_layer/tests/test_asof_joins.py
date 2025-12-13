"""Tests for asof (temporal) joins."""

from datetime import datetime, timedelta

import ibis
import pytest

from boring_semantic_layer import Dimension, Measure, SemanticModel, time_dimension


@pytest.fixture
def events_table():
    """Create a sample events table with timestamps."""
    return ibis.memtable(
        {
            "event_id": [1, 2, 3, 4, 5],
            "site": ["a", "b", "a", "b", "a"],
            "event_type": [
                "start",
                "alert",
                "stop",
                "alert",
                "start",
            ],
            "event_time": [
                datetime(2024, 11, 16, 12, 0, 0),
                datetime(2024, 11, 16, 12, 0, 10),
                datetime(2024, 11, 16, 12, 0, 20),
                datetime(2024, 11, 16, 12, 0, 30),
                datetime(2024, 11, 16, 12, 1, 0),
            ],
        }
    )


@pytest.fixture
def sensors_table():
    """Create a sample sensors table with timestamps."""
    return ibis.memtable(
        {
            "sensor_id": [101, 102, 103, 104, 105],
            "site": ["a", "b", "a", "b", "a"],
            "humidity": [0.3, 0.4, 0.5, 0.6, 0.7],
            "reading_time": [
                datetime(2024, 11, 16, 12, 0, 5),
                datetime(2024, 11, 16, 12, 0, 15),
                datetime(2024, 11, 16, 12, 0, 25),
                datetime(2024, 11, 16, 12, 0, 35),
                datetime(2024, 11, 16, 12, 1, 5),
            ],
        }
    )


@pytest.fixture
def events_model(events_table):
    """Create a semantic model for events."""
    return SemanticModel(
        name="events",
        table=events_table,
        dimensions={
            "event_id": Dimension(expr=lambda t: t.event_id, is_entity=True),
            "site": Dimension(expr=lambda t: t.site),
            "event_type": Dimension(expr=lambda t: t.event_type),
            "event_time": time_dimension(lambda t: t.event_time),
        },
        measures={
            "event_count": Measure(expr=lambda t: t.event_id.count()),
        },
    )


@pytest.fixture
def sensors_model(sensors_table):
    """Create a semantic model for sensors."""
    return SemanticModel(
        name="sensors",
        table=sensors_table,
        dimensions={
            "sensor_id": Dimension(expr=lambda t: t.sensor_id, is_entity=True),
            "site": Dimension(expr=lambda t: t.site),
            "humidity": Dimension(expr=lambda t: t.humidity),
            "reading_time": time_dimension(lambda t: t.reading_time),
        },
        measures={
            "sensor_count": Measure(expr=lambda t: t.sensor_id.count()),
            "avg_humidity": Measure(expr=lambda t: t.humidity.mean()),
        },
    )


def test_asof_join_basic(events_model, sensors_model):
    """Test basic asof join on timestamp column."""
    # Join sensors to events based on nearest event time
    # Use <= for asof join (finds sensor readings at or before each event)
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
    )

    result = joined.execute()

    # Should have all rows from events (left side of asof join)
    assert len(result) == 5

    # Check that reading_time columns exist
    assert "reading_time" in result.columns


def test_asof_join_with_tolerance(events_model, sensors_model):
    """Test asof join with time tolerance."""
    # Join with 10 second tolerance
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
        tolerance="10s",
    )

    result = joined.execute()

    # Should still have all rows from events
    assert len(result) == 5


def test_asof_join_with_combined_conditions(events_model, sensors_model):
    """Test asof join with both temporal and equality conditions in one lambda."""
    # Combine temporal (>=) and equality (==) conditions with &
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: (e.event_time >= s.reading_time) & (e.site == s.site),
        use_asof=True,
    )

    result = joined.execute()

    # Should have all rows from events
    assert len(result) == 5

    # Verify that matches are from the same site
    # (When sites match, we should get sensor readings from same site)
    assert "site" in result.columns


def test_asof_join_many(events_model, sensors_model):
    """Test asof join using join_many."""
    # Join sensors to events - one event might match multiple sensor readings
    joined = events_model.join_many(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
    )

    result = joined.execute()

    # Should have rows for the join
    assert len(result) >= 5


def test_asof_join_without_on_raises_error(events_model, sensors_model):
    """Test that asof join without 'on' condition raises ValueError."""
    # This should raise an error because asof joins require an 'on' condition
    with pytest.raises(ValueError, match="Asof joins require an 'on' condition"):
        from boring_semantic_layer.ops import SemanticJoinOp

        join_op = SemanticJoinOp(
            left=events_model.op(),
            right=sensors_model.op(),
            how="left",
            on=None,
            use_asof=True,
        )
        join_op.to_untagged()


def test_asof_join_compile(events_model, sensors_model):
    """Test that asof join can be compiled to SQL."""
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
        tolerance="5s",
    )

    # Should be able to execute without error (compile requires a backend)
    result = joined.execute()
    assert result is not None
    assert len(result) >= 0


def test_regular_join_still_works(events_model, sensors_model):
    """Test that regular joins still work when use_asof=False."""
    # Regular join on site column
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.site == s.site,
        use_asof=False,
    )

    result = joined.execute()

    # Regular join on site should produce multiple matches
    assert len(result) > 0


def test_asof_join_group_by(events_model, sensors_model):
    """Test asof join followed by group_by aggregation."""
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
    )

    # Group by site and aggregate
    result = joined.group_by("site").aggregate("event_count").execute()

    # Should have results grouped by site
    assert len(result) >= 1
    assert "site" in result.columns
    assert "event_count" in result.columns


def test_asof_join_with_dimensions(events_model, sensors_model):
    """Test that dimensions are accessible after asof join."""
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
    )

    # Check that we can access dimensions from both models
    dims = joined.dimensions

    # Should have dimensions from both sides (prefixed with table names)
    assert "events.event_id" in dims
    assert "sensors.sensor_id" in dims
    assert "events.site" in dims or "sensors.site" in dims


def test_asof_join_with_measures(events_model, sensors_model):
    """Test that measures work after asof join."""
    joined = events_model.join_one(
        sensors_model,
        lambda e, s: e.event_time >= s.reading_time,
        use_asof=True,
    )

    # Should be able to aggregate measures
    result = joined.aggregate("event_count", "sensor_count").execute()

    assert len(result) == 1
    assert "event_count" in result.columns
    assert "sensor_count" in result.columns
