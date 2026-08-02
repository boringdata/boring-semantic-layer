"""Recursive freeze/thaw utilities for xorq FrozenOrderedDict round-tripping.

xorq tag metadata stores dicts as tuples-of-pairs and lists as tuples
(FrozenOrderedDict). These utilities convert between mutable Python types
and the frozen representation.

Two invariants keep the round-trip lossless:

1. ``freeze`` never coerces a value it cannot represent. Anything that is
   not a scalar/list/dict raises, because the previous ``str(obj)``
   fallback silently turned dates, ``Decimal``s and ``bytes`` into strings
   that came back as the wrong type (or failed a type check much later, in
   the query compiler). Non-scalar constants inside expressions are
   encoded by ``utils._encode_scalar`` *before* they reach ``freeze``.

2. ``thaw`` never recurses into a serialized resolver tree. Resolver nodes
   such as ``("just", 0)`` are indistinguishable from the tuple-of-pairs
   encoding of a dict, so thawing them collapsed multi-argument calls into
   their last argument (``substr(0, 2)`` became ``substr(2)``). Values
   stored under a ``*_struct`` key are therefore passed through verbatim.
"""

from __future__ import annotations

from typing import Any

#: Metadata keys whose values are serialized resolver trees. ``thaw`` must
#: hand these back exactly as written — see invariant 2 above.
OPAQUE_STRUCT_KEYS = frozenset(
    {
        "expr_struct",
        "predicate_struct",
        "aggs_struct",
        "on_struct",
        "value_struct",
        "post_struct",
    }
)


class FreezeError(TypeError):
    """A value in tag metadata cannot be frozen without losing information."""


def freeze(obj: Any, *, path: str = "metadata") -> Any:
    """Recursively convert dicts to tuples-of-pairs and lists to tuples.

    Scalar types (str, int, float, bool, None) pass through unchanged.

    Raises:
        FreezeError: If *obj* contains a value with no lossless frozen
            representation.
    """
    if isinstance(obj, str | bool | int | float | type(None)):
        return obj
    if isinstance(obj, dict):
        return tuple((k, freeze(v, path=f"{path}.{k}")) for k, v in obj.items())
    if isinstance(obj, list | tuple):
        return tuple(freeze(item, path=f"{path}[{i}]") for i, item in enumerate(obj))
    raise FreezeError(
        f"Cannot serialize {path}: {type(obj).__name__} has no lossless "
        f"representation in xorq tag metadata (value: {obj!r}). Expression "
        "constants of this type must be encoded by "
        "boring_semantic_layer.utils._encode_scalar before reaching freeze()."
    )


def thaw(obj: Any, *, key: str | None = None) -> Any:
    """Recursively convert frozen tuples back to mutable dicts/lists.

    A tuple is treated as a dict if every element is a 2-tuple with a str key.
    Otherwise it is treated as a list.

    Values reached under one of :data:`OPAQUE_STRUCT_KEYS` are returned
    verbatim: they are resolver trees, whose ``("just", x)`` nodes would
    otherwise be misread as dict entries.
    """
    if key in OPAQUE_STRUCT_KEYS:
        return obj
    if isinstance(obj, tuple):
        if len(obj) == 0:
            return {}
        if all(
            isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
            for item in obj
        ):
            return {k: thaw(v, key=k) for k, v in obj}
        return [thaw(item) for item in obj]
    return obj


def thaw_shallow(obj: Any) -> dict:
    """One-level thaw: convert a FrozenOrderedDict-encoded tuple to a dict.

    Unlike ``thaw``, this does NOT recurse into values — resolver tuples
    stored as values are returned untouched.
    """
    if isinstance(obj, dict):
        return obj
    if (
        isinstance(obj, tuple)
        and obj
        and all(
            isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
            for item in obj
        )
    ):
        return dict(obj)
    return {}


def list_to_tuple(obj: Any) -> Any:
    """Recursively convert lists back to tuples.

    Reverses ``thaw`` for structured expression data that needs to stay
    as tuples for the resolver deserialization layer. Still needed for
    payloads written before ``OPAQUE_STRUCT_KEYS`` existed, and for
    struct data that arrives through other paths (e.g. YAML).
    """
    if isinstance(obj, list):
        return tuple(list_to_tuple(item) for item in obj)
    if isinstance(obj, dict):
        return tuple((k, list_to_tuple(v)) for k, v in obj.items())
    return obj
