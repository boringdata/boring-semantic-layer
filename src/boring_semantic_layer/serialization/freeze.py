"""Recursive freeze/thaw utilities for xorq FrozenOrderedDict round-tripping.

xorq tag metadata stores dicts as tuples-of-pairs and lists as tuples
(FrozenOrderedDict). These utilities convert between mutable Python types
and the frozen representation.
"""

from __future__ import annotations

from typing import Any


def freeze(obj: Any) -> Any:
    """Recursively convert dicts to tuples-of-pairs and lists to tuples.

    Scalar types (str, int, float, bool, None) pass through unchanged.
    Anything else is converted to ``str(obj)``.
    """
    if isinstance(obj, str | int | float | bool | type(None)):
        return obj
    if isinstance(obj, dict):
        return tuple((k, freeze(v)) for k, v in obj.items())
    if isinstance(obj, list | tuple):
        return tuple(freeze(item) for item in obj)
    return str(obj)


def thaw(obj: Any) -> Any:
    """Recursively convert frozen tuples back to mutable dicts/lists.

    A tuple is treated as a dict if every element is a 2-tuple with a str key.
    Otherwise it is treated as a list.
    """
    if isinstance(obj, tuple):
        if len(obj) == 0:
            return {}
        if all(
            isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
            for item in obj
        ):
            return {k: thaw(v) for k, v in obj}
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
        return {k: v for k, v in obj}
    return {}


def list_to_tuple(obj: Any) -> Any:
    """Recursively convert lists back to tuples.

    Reverses ``thaw`` for structured expression data that needs to stay
    as tuples for the resolver deserialization layer.
    """
    if isinstance(obj, list):
        return tuple(list_to_tuple(item) for item in obj)
    if isinstance(obj, dict):
        return tuple((k, list_to_tuple(v)) for k, v in obj.items())
    return obj
