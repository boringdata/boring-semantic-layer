"""Normalization of user join/group-by inputs and join-name allocation.

Turns the accepted predicate spellings (string, Deferred, list, lambda)
into canonical callables, and allocates collision-free temporary names
for join keys and right-side suffixes.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from ibis.common.deferred import Deferred

from ._values import _is_deferred

_JOIN_REMOVED_MESSAGE = (
    "The join() method has been removed. Use join_one(), join_many(), or join_cross() instead.\n\n"
    "For one-to-one relationships:\n"
    "  table.join_one(other, lambda l, r: l.id == r.id)\n\n"
    "For one-to-many relationships:\n"
    "  table.join_many(other, lambda l, r: l.id == r.id)\n\n"
    "For Cartesian product:\n"
    "  table.join_cross(other)"
)

_BSL_JOIN_KEY_TMP_PREFIX = "__bsl_jk_"


def _allocate_temporary_join_names(
    conflicting: Iterable[str],
    left_columns: Sequence[str],
    right_columns: Sequence[str],
) -> dict[str, str]:
    """Allocate collision-free internal names for left predicate columns."""
    occupied = set(left_columns) | set(right_columns)
    result: dict[str, str] = {}
    for name in left_columns:
        if name not in conflicting:
            continue
        preferred = f"{_BSL_JOIN_KEY_TMP_PREFIX}{name}"
        candidate = preferred
        suffix = 2
        while candidate in occupied:
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        result[name] = candidate
        occupied.add(candidate)
    return result


def _allocate_right_collision_names(
    conflicting: Iterable[str],
    left_columns: Sequence[str],
    right_columns: Sequence[str],
    depth: int,
    *,
    reserved: Iterable[str] = (),
) -> dict[str, str]:
    """Allocate the executable names of colliding right-side columns.

    The usual names are ``x_right``, ``x_right2``, ... according to join
    depth.  A source table is allowed to contain those spellings itself, so
    advance the numeric suffix until the name is unique across every column
    that will remain in the result.
    """
    conflicting = frozenset(conflicting)
    occupied = set(left_columns) | (set(right_columns) - conflicting) | set(reserved)
    result: dict[str, str] = {}
    for name in right_columns:
        if name not in conflicting:
            continue
        candidate = f"{name}_right" if depth <= 1 else f"{name}_right{depth}"
        suffix = max(2, depth + 1)
        while candidate in occupied:
            candidate = f"{name}_right{suffix}"
            suffix += 1
        result[name] = candidate
        occupied.add(candidate)
    return result


class _RenamedResolver:
    """Resolver that maps original column names to temporary names.

    Used during join predicate resolution to avoid ibis "Ambiguous field
    reference" errors when left and right tables share column names.

    This works around two upstream ibis behaviors that have no public
    issue tracker entry yet but are pinned by
    ``test_upstream_ibis_pins.py``:

    1. ``DerefMap`` raises ``IbisInputError: Ambiguous field reference``
       when a predicate column-name appears in more than one relation
       reachable from the join's LHS.
    2. The default ``rname='{name}_right'`` collides on the third
       table when 3+ joined relations share a column name, raising
       ``IntegrityError: Name collisions``.

    When the pinning tests start failing — i.e. ibis no longer raises
    these errors — the rename dance in ``SemanticJoinOp.to_untagged``
    can be removed.
    """

    __slots__ = ("_table", "_name_map")

    def __init__(self, table, name_map):
        object.__setattr__(self, "_table", table)
        object.__setattr__(self, "_name_map", name_map)

    def __getattr__(self, name):
        mapped = self._name_map.get(name, name)
        return getattr(self._table, mapped)

    def __getitem__(self, name):
        mapped = self._name_map.get(name, name)
        return self._table[mapped]


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
