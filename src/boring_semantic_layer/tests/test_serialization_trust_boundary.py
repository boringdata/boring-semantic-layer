"""Regression tests for the serialization trust boundary and lossy encodings.

A serialized model is data that travels: xorq's ``from_tag_node`` entry point
(pyproject.toml) routes any ``bsl``-tagged expression here automatically, and
git catalogs store the resolver trees as editable YAML. Everything in this
file is about a payload that is not necessarily written by whoever reads it,
or about an encoding that used to lose information on the way through.

Each test below corresponds to a defect that produced either arbitrary code
execution or a silently wrong number through the public
``to_tagged``/``from_tagged`` API.
"""

from __future__ import annotations

import datetime
import decimal
import os

import ibis
import pytest
from returns.result import Failure

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.serialization import from_tagged, to_tagged
from boring_semantic_layer.serialization.context import BSLSerializationContext
from boring_semantic_layer.serialization.reconstruct import reconstruct_bsl_operation
from boring_semantic_layer.utils import (
    UntrustedCallableError,
    serialize_resolver,
    structured_to_expr,
)

xorq = pytest.importorskip("xorq", reason="xorq not installed")


@pytest.fixture
def table():
    return ibis.memtable(
        {
            "a": [1, 2, 3],
            "b": ["abcd", "efgh", "zzzz"],
            "d": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 6, 1),
                datetime.date(2025, 1, 1),
            ],
        }
    )


def _refusal(struct):
    """Deserialize *struct* and return the error it was refused with."""
    result = structured_to_expr(struct)
    assert isinstance(result, Failure), f"payload was accepted: {result}"
    return result.failure()


# ---------------------------------------------------------------------------
# Arbitrary code execution
# ---------------------------------------------------------------------------


def test_call_gadget_is_refused(tmp_path):
    """``Call.resolve()`` invokes its func, so a Just(callable) is a call gadget."""
    marker = tmp_path / "pwned"
    payload = (
        "call",
        ("fn", "builtins", "eval"),
        (("just", f"__import__('pathlib').Path({str(marker)!r}).touch()"),),
        (),
    )
    assert isinstance(_refusal(payload), UntrustedCallableError)
    assert not marker.exists()


def test_import_side_effects_do_not_run():
    """Importing is itself the side effect, so it must not happen at all."""
    assert isinstance(_refusal(("fn", "antigravity", "x")), UntrustedCallableError)
    assert "antigravity" not in os.sys.modules


@pytest.mark.parametrize(
    "module,qualname",
    [
        ("ibis.util", "os.system"),  # ibis.util imports os
        ("ibis.expr.api", "builtins.eval"),  # ibis.expr.api imports builtins
    ],
)
def test_attribute_chain_cannot_escape_a_trusted_module(module, qualname):
    """A qualname is a getattr chain: a trusted root is not enough on its own."""
    err = _refusal(("fn", module, qualname))
    assert isinstance(err, UntrustedCallableError)
    assert "outside the trusted" in str(err)


def test_untrusted_callable_is_refused_at_write_time():
    """Authors find out when they serialize, not readers when they load."""
    from boring_semantic_layer._xorq import Just

    def user_fn(x):
        return x

    with pytest.raises(UntrustedCallableError, match="not trusted"):
        serialize_resolver(Just(user_fn))


def test_expression_functions_still_round_trip(table):
    """The allowlist must not cost real expressions anything."""
    model = (
        to_semantic_table(table, "m")
        .with_dimensions(bucket=lambda t: (t.a > 1).ifelse("hi", "lo"))
        .with_measures(n=lambda t: t.count())
    )
    df = from_tagged(to_tagged(model)).group_by("bucket").aggregate("n").execute()
    assert dict(zip(df["bucket"], df["n"], strict=True)) == {"lo": 1, "hi": 2}


# ---------------------------------------------------------------------------
# Lossy tag encodings
# ---------------------------------------------------------------------------


def test_multi_argument_calls_survive_the_tag_round_trip(table):
    """``thaw`` read ``(("just", 0), ("just", 2))`` as a dict and kept the last.

    ``substr(0, 2)`` came back as ``substr(2)`` — still valid SQL, different
    answer, no error.
    """
    model = (
        to_semantic_table(table, "m")
        .with_dimensions(pre=lambda t: t.b.substr(0, 2))
        .with_measures(n=lambda t: t.count())
    )
    df = from_tagged(to_tagged(model)).group_by("pre").aggregate("n").execute()
    assert sorted(df["pre"]) == ["ab", "ef", "zz"]


def test_multi_element_isin_survives_the_tag_round_trip(table):
    model = to_semantic_table(table, "m").with_measures(n=lambda t: t.count())
    filtered = model.filter(lambda t: t.b.isin(["abcd", "efgh"]))
    df = from_tagged(to_tagged(filtered)).aggregate("n").execute()
    assert int(df["n"][0]) == 2


@pytest.mark.parametrize(
    "value,expected",
    [
        (datetime.date(2024, 3, 1), 2),
        (datetime.datetime(2024, 3, 1), 2),
    ],
)
def test_non_scalar_literals_keep_their_type(table, value, expected):
    """``freeze`` used to ``str()`` these, so the predicate compared to a string."""
    model = to_semantic_table(table, "m").with_measures(n=lambda t: t.count())
    filtered = model.filter(lambda t: t.d > value)
    df = from_tagged(to_tagged(filtered)).aggregate("n").execute()
    assert int(df["n"][0]) == expected


def test_decimal_literal_round_trips():
    from boring_semantic_layer._xorq import Just

    struct = serialize_resolver(Just(decimal.Decimal("1.5")))
    from boring_semantic_layer.utils import deserialize_resolver

    assert deserialize_resolver(struct).value == decimal.Decimal("1.5")


def test_unrepresentable_constant_fails_at_write_time():
    """Better a loud failure than a value silently replaced by its repr."""
    from boring_semantic_layer._xorq import Just

    with pytest.raises(ValueError, match="Cannot serialize constant"):
        serialize_resolver(Just(object()))


# ---------------------------------------------------------------------------
# Aggregate replay
# ---------------------------------------------------------------------------


def test_query_local_agg_beats_a_same_named_model_measure(table):
    """``aggregate(n=...)`` must not be replaced by the model's ``n``."""
    model = to_semantic_table(table, "m").with_measures(n=lambda t: t.a.sum())
    query = model.aggregate(n=lambda t: t.a.max())
    assert int(from_tagged(to_tagged(query)).execute()["n"][0]) == 3


def test_bare_measure_name_still_replays_by_name(table):
    """Bare names must keep routing through measure resolution (fan-out safety)."""
    model = to_semantic_table(table, "m").with_measures(n=lambda t: t.a.sum())
    assert int(from_tagged(to_tagged(model.aggregate("n"))).execute()["n"][0]) == 6


# ---------------------------------------------------------------------------
# Payload versioning
# ---------------------------------------------------------------------------


def test_unsupported_payload_version_is_refused():
    metadata = {
        "bsl_op_type": "SemanticTableOp",
        "bsl_version": "1.0",
        "dimensions": {"amount": {"expr_pickle": "gAWV"}},
        "measures": {},
    }
    with pytest.raises(ValueError, match="1.0"):
        reconstruct_bsl_operation(metadata, None, BSLSerializationContext())


def test_dimension_without_an_expression_is_refused():
    metadata = {
        "bsl_op_type": "SemanticTableOp",
        "bsl_version": "2.0",
        "dimensions": {"amount": {"description": "no expression here"}},
        "measures": {},
    }
    with pytest.raises(ValueError, match="no readable expression"):
        reconstruct_bsl_operation(metadata, None, BSLSerializationContext())


# ---------------------------------------------------------------------------
# Backend rebinding
# ---------------------------------------------------------------------------


def test_rebinding_does_not_repoint_a_table_at_another_database(tmp_path):
    """Rebinding every DatabaseTable made a join read both sides from one db."""
    prod = ibis.duckdb.connect(str(tmp_path / "prod.ddb"))
    prod.create_table("t", ibis.memtable({"k": [1, 2], "v": [10, 20]}).execute())
    staging = ibis.duckdb.connect(str(tmp_path / "staging.ddb"))
    staging.create_table("t", ibis.memtable({"k": [1, 2], "v": [999, 888]}).execute())

    left = (
        to_semantic_table(prod.table("t"), "p")
        .with_dimensions(k=lambda t: t.k)
        .with_measures(total=lambda t: t.v.sum())
    )
    right = (
        to_semantic_table(staging.table("t"), "s")
        .with_dimensions(k=lambda t: t.k)
        .with_measures(other=lambda t: t.v.sum())
    )

    assert int(left.aggregate("total").execute()["total"][0]) == 30

    joined = left.join_one(right, on=lambda x, y: x.k == y.k)
    try:
        got = int(joined.aggregate("p.total").execute()["p.total"][0])
    except Exception:
        return  # a loud cross-database failure is the acceptable outcome
    assert got == 30, "prod's measure was computed from staging's rows"


def test_duplicate_wrappers_of_one_connection_still_unify(tmp_path):
    """The case rebinding exists for: from_ibis() mints a Backend per call."""
    con = ibis.duckdb.connect(str(tmp_path / "one.ddb"))
    con.create_table("t", ibis.memtable({"k": [1, 2], "v": [10, 20]}).execute())

    left = (
        to_semantic_table(con.table("t"), "a")
        .with_dimensions(k=lambda t: t.k)
        .with_measures(total=lambda t: t.v.sum())
    )
    right = (
        to_semantic_table(con.table("t"), "b")
        .with_dimensions(k=lambda t: t.k)
        .with_measures(n=lambda t: t.count())
    )
    joined = left.join_one(right, on=lambda x, y: x.k == y.k)
    assert int(joined.aggregate("a.total").execute()["a.total"][0]) == 30
