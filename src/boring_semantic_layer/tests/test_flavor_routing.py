"""Regression tests for ibis/xorq flavor routing and cross-flavor detection.

Two classes of defect are covered:

1. Cross-flavor comparisons (plain-ibis literal vs xorq-vendored column)
   evaluate to a Python bool via identity fallback, which used to compile
   into a constant-false predicate and silently return wrong results.
   ``ops._reject_bool_resolution`` now raises TypeError instead.

2. The ibis flavor used to build filter literals was chosen by "is xorq
   importable" rather than by the flavor of the table being filtered. On
   backends xorq can't wrap (the table stays plain ibis), date filters
   raised TypeError (ordering) or silently returned empty (equality).
   ``Filter.to_callable`` now picks the flavor from the resolved table.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import Dimension, SemanticModel
from boring_semantic_layer._xorq import HAS_XORQ


@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect()


@pytest.fixture(scope="module")
def flights_table(con):
    df = pd.DataFrame(
        {"carrier": ["AA", "UA", "AA", "DL"], "dep_delay": [5.0, 10.0, 15.0, 2.0]}
    )
    return con.create_table("flavor_flights", df)


@pytest.fixture(scope="module")
def events_table(con):
    df = pd.DataFrame(
        {
            "d": pd.to_datetime(["2024-01-01", "2024-06-01", "2025-03-01"]),
            "v": [1.0, 2.0, 4.0],
        }
    )
    return con.create_table("flavor_events", df)


def _flights_model(table):
    return SemanticModel(
        table=table,
        dimensions={"carrier": lambda t: t.carrier},
        measures={
            "avg_delay": lambda t: t.dep_delay.mean(),
            "cnt": lambda t: t.count(),
        },
        name="flights",
    )


def _events_model(table):
    return SemanticModel(
        table=table,
        dimensions={"d": Dimension(expr=lambda t: t.d, is_time_dimension=True)},
        measures={"total": lambda t: t.v.sum()},
        name="events",
    )


@pytest.fixture
def unsupported_backend(monkeypatch):
    """Simulate a backend xorq can't wrap: from_ibis raises, tables stay plain."""
    import boring_semantic_layer._xorq as bsl_xorq

    def _raise(table):
        raise RuntimeError("simulated backend unsupported by xorq")

    monkeypatch.setattr(bsl_xorq, "from_ibis", _raise)


class TestCrossFlavorBoolTrap:
    """A predicate/expression resolving to a Python bool must raise, not
    silently compile into a constant predicate."""

    def test_constant_bool_filter_raises(self, flights_table):
        sm = _flights_model(flights_table)
        with pytest.raises(TypeError, match="resolved to the Python bool"):
            sm.filter(lambda t: True).group_by("carrier").aggregate("cnt").execute()

    @pytest.mark.skipif(not HAS_XORQ, reason="cross-flavor mixing requires xorq")
    def test_cross_flavor_equality_filter_raises(self, flights_table):
        # Plain-ibis literal vs xorq-backed column: Python identity fallback
        # yields False. Previously executed and returned an empty frame.
        sm = _flights_model(flights_table)
        with pytest.raises(TypeError, match="resolved to the Python bool"):
            (
                sm.filter(lambda t: t.carrier == ibis.literal("AA"))
                .group_by("carrier")
                .aggregate("cnt")
                .execute()
            )

    @pytest.mark.skipif(not HAS_XORQ, reason="cross-flavor mixing requires xorq")
    def test_cross_flavor_dimension_raises(self, flights_table):
        sm = SemanticModel(
            table=flights_table,
            dimensions={"is_aa": lambda t: t.carrier == ibis.literal("AA")},
            measures={"cnt": lambda t: t.count()},
            name="flights",
        )
        with pytest.raises(TypeError, match="resolved to the Python bool"):
            sm.group_by("is_aa").aggregate("cnt").execute()

    def test_plain_value_comparison_still_works(self, flights_table):
        sm = _flights_model(flights_table)
        out = (
            sm.filter(lambda t: t.carrier == "AA")
            .group_by("carrier")
            .aggregate("cnt")
            .execute()
        )
        assert len(out) == 1
        assert out["cnt"].iloc[0] == 2


class TestFilterFlavorByTable:
    """Filter literals must be built with the flavor of the table they
    resolve against, not the flavor that happens to be importable."""

    def test_dict_date_filter_converted_table(self, events_table):
        sm = _events_model(events_table)
        out = sm.query(
            dimensions=["d"],
            measures=["total"],
            filters=[{"field": "d", "operator": ">=", "value": "2024-02-01"}],
        ).execute()
        assert len(out) == 2

    @pytest.mark.skipif(not HAS_XORQ, reason="fallback only differs with xorq installed")
    def test_dict_date_ordering_filter_fallback_backend(
        self, events_table, unsupported_backend
    ):
        # Previously: xorq-flavored timestamp literal vs plain column -> TypeError.
        sm = _events_model(events_table)
        assert "xorq" not in type(sm.op().table).__module__
        out = sm.query(
            dimensions=["d"],
            measures=["total"],
            filters=[{"field": "d", "operator": ">=", "value": "2024-02-01"}],
        ).execute()
        assert len(out) == 2

    @pytest.mark.skipif(not HAS_XORQ, reason="fallback only differs with xorq installed")
    def test_dict_date_equality_filter_fallback_backend(
        self, events_table, unsupported_backend
    ):
        # Previously: silent constant-false predicate -> empty result.
        sm = _events_model(events_table)
        out = sm.query(
            dimensions=["d"],
            measures=["total"],
            filters=[{"field": "d", "operator": "=", "value": "2024-01-01"}],
        ).execute()
        assert len(out) == 1
        assert out["total"].iloc[0] == 1.0

    @pytest.mark.skipif(not HAS_XORQ, reason="fallback only differs with xorq installed")
    def test_string_filter_with_literal_fallback_backend(
        self, events_table, unsupported_backend
    ):
        # ibis.literal inside a string filter must use the table's flavor.
        sm = _events_model(events_table)
        out = sm.query(
            dimensions=["d"],
            measures=["total"],
            filters=["_.d >= ibis.literal('2024-02-01', type='timestamp')"],
        ).execute()
        assert len(out) == 2

    def test_string_filter_with_literal_converted_table(self, events_table):
        sm = _events_model(events_table)
        out = sm.query(
            dimensions=["d"],
            measures=["total"],
            filters=["_.d >= ibis.literal('2024-02-01', type='timestamp')"],
        ).execute()
        assert len(out) == 2

    def test_invalid_string_filter_fails_at_build_time(self):
        from returns.primitives.exceptions import UnwrapFailedError

        from boring_semantic_layer.query import Filter

        with pytest.raises(UnwrapFailedError):
            Filter(filter="__import__('os').system('true')").to_callable()


class TestAgentContextFlavor:
    """Agent query contexts must expose the ibis module matching the models."""

    def test_models_ibis_module_matches_table_flavor(self, flights_table):
        from boring_semantic_layer.agents.tools import _models_ibis_module
        from boring_semantic_layer.nested_compile import get_ibis_module

        sm = _flights_model(flights_table)
        module = _models_ibis_module({"flights": sm})
        assert module is get_ibis_module(sm.table)

    def test_models_ibis_module_empty_falls_back_to_plain(self):
        from boring_semantic_layer.agents.tools import _models_ibis_module

        assert _models_ibis_module({}) is ibis

    def test_agent_query_with_module_literal(self, flights_table):
        # End-to-end shape of tools._query_model: literal comparison built
        # from the flavor-matched module returns correct (non-empty) results.
        from boring_semantic_layer.agents.tools import _models_ibis_module
        from boring_semantic_layer.utils import safe_eval

        sm = _flights_model(flights_table)
        models = {"flights": sm}
        module = _models_ibis_module(models)
        query = "flights.filter(_.carrier == ibis.literal('AA')).group_by('carrier').aggregate('cnt')"
        result = safe_eval(
            query, context={**models, "ibis": module, "_": module._}
        ).unwrap()
        out = result.execute()
        assert len(out) == 1
        assert out["cnt"].iloc[0] == 2


class TestIbisStringToExprFlavor:
    """ibis_string_to_expr lambdas re-bind ``ibis`` to the flavor of the
    table they are called with."""

    def test_literal_expression_against_converted_table(self, flights_table):
        from boring_semantic_layer.utils import ibis_string_to_expr

        sm = _flights_model(flights_table)
        fn = ibis_string_to_expr("_.dep_delay >= ibis.literal(8.0)").unwrap()
        resolved = fn(sm.table)
        # Must be a real boolean expression of the table's own flavor,
        # not a Python bool from identity comparison.
        assert not isinstance(resolved, bool)
        assert type(resolved).__module__.split(".")[0] == type(sm.table).__module__.split(".")[0]

    def test_literal_expression_against_plain_table(self, flights_table):
        from boring_semantic_layer.utils import ibis_string_to_expr

        fn = ibis_string_to_expr("_.dep_delay >= ibis.literal(8.0)").unwrap()
        resolved = fn(flights_table)
        assert not isinstance(resolved, bool)
        assert type(resolved).__module__.startswith("ibis.")
