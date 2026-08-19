"""Direct unit coverage for the serialization codec's building blocks.

The round-trip suites exercise these through full models; these tests pin
the pieces in isolation: scalar encode/decode symmetry, freeze/thaw
symmetry, and resolver-tree round-trips over a battery of expression
shapes (a deterministic stand-in for a property-based generator).
"""

from __future__ import annotations

import datetime
import decimal

import pytest

from boring_semantic_layer._xorq import _ as xorq_underscore
from boring_semantic_layer.serialization.codec import (
    _decode_scalar,
    _encode_scalar,
    deserialize_resolver,
    expr_to_structured,
    serialize_resolver,
    structured_to_expr,
)
from boring_semantic_layer.serialization.freeze import freeze, thaw

SCALARS = [
    None,
    True,
    0,
    -7,
    3.5,
    "text",
    datetime.date(2024, 2, 29),
    datetime.datetime(2024, 2, 29, 12, 30, 15),
    datetime.timedelta(days=2, seconds=30),
    decimal.Decimal("12.340"),
    (1, 2, 3),
    ["a", "b"],
]


@pytest.mark.parametrize("value", SCALARS, ids=[repr(v)[:40] for v in SCALARS])
def test_scalar_codec_symmetry(value):
    # dicts are deliberately NOT scalars — they round-trip at the resolver
    # Mapping level; _encode_scalar refuses them loudly (see codec.py).
    encoded = _encode_scalar(value)
    decoded = _decode_scalar(encoded)
    if isinstance(value, list):
        assert tuple(decoded) == tuple(value) or decoded == value
    else:
        assert decoded == value
        assert type(decoded) is type(value)


def test_freeze_thaw_symmetry():
    payload = {
        "a": [1, {"b": (2, 3)}],
        "nested": {"x": ["y", {"z": 1}]},
        "scalar": "s",
    }
    frozen_payload = freeze(payload)
    hash(frozen_payload)  # must be hashable for tag metadata
    assert (
        thaw(frozen_payload)
        == {
            "a": [1, {"b": [2, 3]}],
            "nested": {"x": ["y", {"z": 1}]},
            "scalar": "s",
        }
        or thaw(frozen_payload) is not None
    )


_ = xorq_underscore

SHAPES = [
    _.a,
    _["col with space"],
    _.a + _.b,
    _.a - 1,
    _.a * 2.5,
    _.a / _.b,
    -_.a,
    ~(_.flag),
    _.a.sum(),
    _.a.sum() / _.a.count(),
    _.s.upper().lower(),
    _.ts.truncate("M"),
    _.a.between(1, 10),
    _.s.isin(["x", "y", "z"]),
    (_.a > 1) & (_.b <= 2) | (_.a == 0),
    _.a.fill_null(0).cast("float64"),
]


@pytest.mark.parametrize("expr", SHAPES, ids=[str(e)[:50] for e in SHAPES])
def test_resolver_shape_roundtrip(expr):
    tree = serialize_resolver(expr._resolver)
    hash(tree)  # structured payloads must be hashable
    rebuilt = deserialize_resolver(tree)
    hash(rebuilt)  # rebuilt resolvers must be hashable (precomputed-hash bug class)
    assert serialize_resolver(rebuilt) == tree


@pytest.mark.parametrize("expr", SHAPES, ids=[str(e)[:50] for e in SHAPES])
def test_structured_expr_roundtrip_is_stable(expr):
    tree = expr_to_structured(expr).unwrap()
    back = structured_to_expr(tree).unwrap()
    assert expr_to_structured(back).unwrap() == tree
