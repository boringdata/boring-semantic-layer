"""Unit tests for the shared prefixed-field parsing / suffix resolution."""

from boring_semantic_layer.fieldref import resolve_suffix, split_prefixed, suffix_matches


def test_split_prefixed():
    assert split_prefixed("flights.carrier") == ("flights", "carrier")
    assert split_prefixed("carrier") == (None, "carrier")
    assert split_prefixed("a.b.c") == ("a", "b.c")


def test_suffix_matches():
    cols = ["flights.carrier", "carriers.carrier", "carrier_code"]
    assert suffix_matches("carrier", cols) == ["flights.carrier", "carriers.carrier"]
    assert suffix_matches("code", cols) == []
    # The bare name itself is not a suffix match.
    assert suffix_matches("carrier_code", cols) == []


def test_resolve_suffix_exact_wins():
    assert resolve_suffix("carrier", ["carrier", "flights.carrier"]) == "carrier"


def test_resolve_suffix_unique_match():
    assert resolve_suffix("carrier", ["flights.carrier", "flights.origin"]) == "flights.carrier"


def test_resolve_suffix_ambiguous_returns_none():
    assert resolve_suffix("carrier", ["flights.carrier", "carriers.carrier"]) is None


def test_resolve_suffix_absent_returns_none():
    assert resolve_suffix("nope", ["flights.carrier"]) is None


def test_resolve_suffix_multiple_sets():
    base = ["flights.count"]
    calc = ["flights.share"]
    assert resolve_suffix("count", base, calc) == "flights.count"
    assert resolve_suffix("share", base, calc) == "flights.share"
    # ambiguity across sets is still ambiguity
    assert resolve_suffix("x", ["a.x"], ["b.x"]) is None
