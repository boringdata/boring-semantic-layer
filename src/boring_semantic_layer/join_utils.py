"""Small helpers shared by semantic join compilation paths."""

from __future__ import annotations


def null_safe_equal(left, right):
    """Return equality that also matches two NULL values.

    xorq/DataFusion currently misplans multiple ``identical_to`` join
    predicates by folding an integer key into a boolean ``AND``. Expressing
    the same semantics with ordinary equality and explicit NULL checks keeps
    multi-key joins portable across the plain-ibis and xorq backends.
    """
    return (left == right) | (left.isnull() & right.isnull())
