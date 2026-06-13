"""Pin upstream ibis behaviors that BSL works around.

Each test reproduces a specific ibis (or xorq-vendored ibis) behavior that
forced a workaround somewhere in BSL. The tests pass while ibis still
exhibits the behavior and fail when ibis is fixed — at which point the
corresponding BSL workaround can be deleted.

When a test in this file starts failing, do not "fix" it by adding the
workaround in: instead, locate the BSL workaround it pins (each test
docstring names the call site) and delete that workaround.

Each pin is parametrized over plain ``ibis`` and ``xorq.vendor.ibis``
because BSL's internal tables are xorq-vendored after construction-time
conversion, so the workaround actually fires against the xorq flavor —
but plain ibis is the upstream, and divergence between them is itself a
signal worth pinning.
"""

from __future__ import annotations

import pandas as pd
import pytest

import ibis as plain_ibis

pytest.importorskip("xorq", reason="xorq not installed")
from xorq.common.utils.ibis_utils import from_ibis  # noqa: E402


def _make_table(flavor: str, name: str, df: pd.DataFrame):
    """Create a table in the requested ibis flavor.

    For ``"ibis"`` we create directly via the duckdb backend.
    For ``"xorq"`` we create via plain ibis, then wrap with
    ``from_ibis`` so the resulting table is in ``xorq.vendor.ibis``.
    """
    con = plain_ibis.duckdb.connect()
    tbl = con.create_table(name, df)
    if flavor == "xorq":
        return from_ibis(tbl)
    return tbl


@pytest.mark.parametrize("flavor", ["ibis", "xorq"])
def test_pin_three_way_join_default_rname_collides(flavor):
    """3+ joined relations sharing a column name collide on ``_right``.

    Pinned BSL workaround: depth-based ``rname`` in
    ``SemanticJoinOp._rname_for_depth`` (``_right``, ``_right2``, …)
    plus the ``conflicting`` rename block in ``SemanticJoinOp.to_untagged``.

    When ibis auto-disambiguates collisions across nested joins, this
    test will succeed without raising — at which point the depth
    hack and rename block can be removed.
    """
    flights = _make_table(
        flavor,
        f"fl_pin_{flavor}",
        pd.DataFrame(
            {"carrier": ["AA"], "origin": ["JFK"], "destination": ["LAX"]}
        ),
    )
    carriers = _make_table(
        flavor,
        f"ca_pin_{flavor}",
        pd.DataFrame({"code": ["AA"], "name": ["American"]}),
    )
    airports1 = _make_table(
        flavor,
        f"ap1_pin_{flavor}",
        pd.DataFrame({"code": ["JFK"], "city": ["NY"]}),
    )
    airports2 = _make_table(
        flavor,
        f"ap2_pin_{flavor}",
        pd.DataFrame({"code": ["LAX"], "city": ["LA"]}),
    )

    fc = flights.join(carriers, flights.carrier == carriers.code)
    fca = fc.join(airports1, fc.origin == airports1.code)
    # Both ``fca`` and ``airports2`` carry a column called ``code``; ibis
    # tries to suffix the right one as ``code_right``, which already exists
    # from the previous join → IntegrityError. Plain ibis raises on schema
    # materialization (``.columns``); xorq's vendored ibis raises at
    # execute time. Force materialization to cover both.
    with pytest.raises(Exception, match=r"(?i)collision|integrity"):
        fca.join(airports2, fca.destination == airports2.code).execute()


@pytest.mark.parametrize("flavor", ["ibis", "xorq"])
def test_pin_self_reference_join_is_ambiguous_without_view(flavor):
    """Joining the same table twice without ``.view()`` is ambiguous.

    Pinned BSL workaround: BSL relies on the user calling ``.view()`` (or
    ``to_semantic_table`` on a viewed table) when joining the same source
    twice. The reconstruction path in ``serialization.reconstruct``
    explicitly preserves ``SelfReference`` nodes for the same reason.

    When ibis disambiguates self-references automatically, the
    ``SelfReference`` preservation in reconstruct can be relaxed.
    """
    ports = _make_table(
        flavor,
        f"ports_pin_{flavor}",
        pd.DataFrame({"code": ["JFK", "LAX"], "city": ["NY", "LA"]}),
    )
    flights = _make_table(
        flavor,
        f"fl_self_pin_{flavor}",
        pd.DataFrame({"origin": ["JFK"], "dest": ["LAX"]}),
    )

    j1 = flights.join(ports, flights.origin == ports.code)
    # Reusing ``ports`` (same identity) without ``.view()`` makes the
    # second predicate's ``ports.code`` ambiguous. Error fires at
    # predicate-resolution time during the second ``join`` call.
    with pytest.raises(Exception, match=r"(?i)ambiguous"):
        j1.join(ports, j1.dest == ports.code)
