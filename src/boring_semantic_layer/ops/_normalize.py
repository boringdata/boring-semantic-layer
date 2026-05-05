"""Argument normalizers for join predicates and dimension/measure names.

Used by the user-facing ``SemanticTable`` API to accept strings, simple
``Deferred`` expressions (``_.col``), and callable predicates uniformly.
"""

from __future__ import annotations

from ibis.common.deferred import Deferred

from ._values import _is_deferred


def _normalize_to_name(arg: str | Deferred) -> str:
    """Convert a string or simple ``_.name`` Deferred to a plain string name.

    Accepts a plain string (returned as-is) or a Deferred whose resolver is a
    simple attribute access on the top-level ``_`` variable (e.g. ``_.origin``).

    Complex expressions like ``_.distance.sum()`` or ``_.a.b`` are rejected
    with a ``TypeError``.
    """
    if isinstance(arg, str):
        return arg

    # Duck-type: works for both ibis and xorq Deferred objects
    resolver = getattr(arg, "_resolver", None)
    if resolver is None:
        raise TypeError(
            f"Expected a string name or Deferred expression (_.name), got {type(arg).__name__}"
        )

    obj = getattr(resolver, "obj", None)

    # Try attribute access first (_.name -> Attr resolver with .name)
    name_wrapper = getattr(resolver, "name", None)

    # Fall back to getitem access (_["name"] -> Item resolver with .indexer)
    if name_wrapper is None:
        name_wrapper = getattr(resolver, "indexer", None)

    if name_wrapper is None or obj is None:
        raise TypeError(
            f"Only simple Deferred expressions like _.name or _['name'] are supported "
            f"as positional arguments, got: {arg!r}"
        )

    # Reject chained access like _.a.b (obj would itself have an .obj attr)
    if getattr(obj, "obj", None) is not None:
        raise TypeError(
            f"Only simple Deferred expressions like _.name or _['name'] are supported "
            f"as positional arguments, got: {arg!r}"
        )

    # Attr.name / Item.indexer is a Just wrapper; unwrap via .value
    raw_name = getattr(name_wrapper, "value", name_wrapper)
    if not isinstance(raw_name, str):
        raise TypeError(f"Could not extract string name from Deferred expression: {arg!r}")

    return raw_name


def _normalize_join_predicate(on):
    """Normalize a join predicate to a two-argument callable.

    Accepts:
    - ``str`` – equi-join on a column present in both sides
    - ``Deferred`` (``_.col``) – same, after extracting the name
    - ``list[str | Deferred]`` – compound equi-join on multiple columns
    - ``callable`` (non-Deferred) – returned as-is (existing lambda API)
    - ``None`` – returned as-is (for cross joins)
    """
    if on is None:
        return on

    if isinstance(on, str):
        name = on
        return lambda left, right: getattr(left, name) == getattr(right, name)

    if _is_deferred(on):
        name = _normalize_to_name(on)
        return lambda left, right: getattr(left, name) == getattr(right, name)

    if isinstance(on, (list, tuple)):
        names = [_normalize_to_name(item) for item in on]
        if len(names) == 1:
            name = names[0]
            return lambda left, right: getattr(left, name) == getattr(right, name)

        def _compound_predicate(left, right):
            from functools import reduce
            from operator import and_

            preds = [getattr(left, n) == getattr(right, n) for n in names]
            return reduce(and_, preds)

        return _compound_predicate

    if callable(on):
        return on

    raise TypeError(
        f"join `on` must be a string, Deferred (_.col), list of strings/Deferred, "
        f"or a callable, got {type(on).__name__}"
    )
