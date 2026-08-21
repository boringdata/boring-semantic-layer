"""Prefixed-field parsing and short-name resolution.

Joined models qualify field names with their model prefix
(``flights.carrier``); users may still write the short name when it is
unambiguous. This module is the single implementation of that convention —
parsing ``model.field`` strings and resolving short names by unique suffix
match. Bottom layer: imports nothing from the package.
"""

from __future__ import annotations

from collections.abc import Iterable

SEPARATOR = "."


def split_prefixed(name: str) -> tuple[str | None, str]:
    """Split ``"model.field"`` into ``("model", "field")``.

    An unprefixed name returns ``(None, name)``. Only the first separator
    splits; ``"a.b.c"`` yields ``("a", "b.c")``.
    """
    if SEPARATOR in name:
        prefix, field = name.split(SEPARATOR, 1)
        return (prefix, field)
    return (None, name)


def suffix_matches(name: str, candidates: Iterable[str]) -> list[str]:
    """Return the candidates whose unqualified suffix is *name*.

    Matches ``"flights.carrier"`` for ``name="carrier"``; never matches the
    bare name itself (use an exact check for that).
    """
    suffix = f"{SEPARATOR}{name}"
    return [c for c in candidates if c.endswith(suffix)]


def resolve_suffix(name: str, *candidate_sets: Iterable[str]) -> str | None:
    """Resolve *name* against candidate names, allowing unique suffix lookup.

    An exact match in any set wins. Otherwise, if exactly one candidate
    across all sets ends in ``".{name}"``, that qualified name is returned.
    Ambiguous or absent names return ``None`` — the caller decides how loud
    to be.
    """
    materialized = [list(c) for c in candidate_sets]
    for candidates in materialized:
        if name in candidates:
            return name
    matches: list[str] = []
    for candidates in materialized:
        matches.extend(suffix_matches(name, candidates))
    if len(matches) == 1:
        return matches[0]
    return None
