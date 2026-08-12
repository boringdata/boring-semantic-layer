"""Regression tests for a third round of defects.

Three independent problems, all of which either rejected a valid query or
quietly returned a model that was missing part of itself:

1. JSON filters coerced any complete-ISO string to a date literal without
   looking at the column, so a string column holding ISO-looking text could
   not be filtered at all.
2. One unserializable field emptied its whole field set during tagging, so
   the reconstructed model silently lost every measure (or every dimension).
3. Positional measures were named from ``id()``, making the result column
   name — and the tag metadata — differ between runs of the same query.
"""

from __future__ import annotations

import datetime

import ibis
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.serialization import from_tagged, to_tagged


@pytest.fixture
def sales():
    return ibis.memtable(
        {
            "region": ["west", "west", "east", "east", "east"],
            "rep": ["a", "b", "c", "d", "e"],
            "amount": [10, 20, 30, 40, 50],
            # ISO-looking *text*, not a date column
            "batch": ["2024-01-01", "2024-01-01", "2024-06-01", "2024-06-01", "2025-01-01"],
            "ts": [
                datetime.datetime(2024, 1, 15),
                datetime.datetime(2024, 2, 15),
                datetime.datetime(2024, 6, 15),
                datetime.datetime(2024, 7, 15),
                datetime.datetime(2025, 1, 15),
            ],
        }
    )


@pytest.fixture
def model(sales):
    return (
        to_semantic_table(sales, "sales")
        .with_dimensions(
            region=lambda t: t.region,
            batch=lambda t: t.batch,
            ts=lambda t: t.ts,
        )
        .with_measures(total=lambda t: t.amount.sum())
    )


def _total(model, *filters):
    df = model.query(dimensions=[], measures=["total"], filters=list(filters)).execute()
    return float(df["total"][0])


# ---------------------------------------------------------------------------
# 1. Date-literal coercion must respect the column's type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec,expected",
    [
        ({"field": "batch", "operator": "=", "value": "2024-01-01"}, 30.0),
        ({"field": "batch", "operator": "!=", "value": "2024-01-01"}, 120.0),
        (
            {"field": "batch", "operator": "in", "values": ["2024-01-01", "2025-01-01"]},
            80.0,
        ),
    ],
)
def test_iso_looking_text_filters_a_string_column(model, spec, expected):
    """A string column holding "2024-01-01" is compared as text.

    Coercing to a timestamp made the backend reject the comparison outright
    (``batch:string and Literal(...):timestamp are not comparable``).
    """
    assert _total(model, spec) == expected


@pytest.mark.parametrize(
    "value,expected",
    [("2024-01-01", 30.0), ("2024-%", 100.0), ("%-06-01", 70.0)],
)
def test_like_patterns_are_never_coerced(model, value, expected):
    """like/ilike take a string pattern, whatever the column's type."""
    assert _total(model, {"field": "batch", "operator": "like", "value": value}) == expected


def test_temporal_columns_still_get_typed_literals(model):
    """The coercion exists for backends that need typed dates — keep it."""
    assert _total(model, {"field": "ts", "operator": ">=", "value": "2024-06-01"}) == 120.0


def test_json_filter_matches_the_equivalent_lambda(model):
    json_filtered = _total(model, {"field": "region", "operator": "=", "value": "east"})
    lambda_filtered = float(
        model.filter(lambda t: t.region == "east").aggregate("total").execute()["total"][0]
    )
    assert json_filtered == lambda_filtered == 120.0


# ---------------------------------------------------------------------------
# 2. A field that cannot be serialized must not take the others with it
# ---------------------------------------------------------------------------


def test_unserializable_measure_fails_loudly(sales):
    """``value_or({})`` dropped *every* measure when one could not be written.

    The model then reconstructed with no measures and failed at query time
    with "Column 'total' is not found", blaming the query.
    """
    model = (
        to_semantic_table(sales, "m")
        .with_dimensions(region=lambda t: t.region)
        .with_measures(
            total=lambda t: t.amount.sum(),
            # a set is unhashable, so the resolver tree cannot hold it
            bad=lambda t: t.rep.isin({"a", "b"}).sum(),
        )
    )
    with pytest.raises(ValueError, match="measures"):
        to_tagged(model)


def test_unserializable_dimension_fails_loudly(sales):
    model = (
        to_semantic_table(sales, "m")
        .with_dimensions(region=lambda t: t.region, bad=lambda t: t.rep.isin({"a", "b"}))
        .with_measures(total=lambda t: t.amount.sum())
    )
    with pytest.raises(ValueError, match="dimensions"):
        to_tagged(model)


def test_serializable_model_still_round_trips_every_field(model):
    """The guard must not cost a well-formed model anything."""
    restored = from_tagged(to_tagged(model))
    assert set(restored.op().get_measures()) == {"total"}
    assert set(restored.op().get_dimensions()) == {"region", "batch", "ts"}
    assert float(restored.aggregate("total").execute()["total"][0]) == 150.0


# ---------------------------------------------------------------------------
# 3. Positional measures need a deterministic name
# ---------------------------------------------------------------------------


def test_positional_measure_name_is_positional(model):
    """``_measure_{id(item)}`` made the result column a memory address."""
    df = model.group_by("region").aggregate(lambda t: t.amount.sum()).execute()
    assert [c for c in df.columns if c != "region"] == ["_measure_0"]


def test_several_positional_measures_get_distinct_stable_names(model):
    df = (
        model.group_by("region")
        .aggregate(lambda t: t.amount.sum(), lambda t: t.amount.max())
        .execute()
    )
    assert sorted(c for c in df.columns if c != "region") == ["_measure_0", "_measure_1"]


def test_positional_measure_name_does_not_collide_with_an_alias(model):
    """A user alias occupying the positional name must not be overwritten."""
    df = (
        model.group_by("region")
        .aggregate(lambda t: t.amount.sum(), _measure_0=lambda t: t.amount.max())
        .execute()
    )
    cols = sorted(c for c in df.columns if c != "region")
    assert cols == ["_measure_0", "_measure_1"]
    west = df[df["region"] == "west"].iloc[0]
    assert {float(west["_measure_0"]), float(west["_measure_1"])} == {20.0, 30.0}


def test_tag_metadata_is_identical_for_the_same_query(model):
    """The name reaches the xorq tag, so an address made metadata unstable."""
    first = dict(to_tagged(model.group_by("region").aggregate(lambda t: t.amount.sum())).op().metadata)
    second = dict(to_tagged(model.group_by("region").aggregate(lambda t: t.amount.sum())).op().metadata)
    assert repr(first) == repr(second)
    assert "_measure_0" in repr(first)
