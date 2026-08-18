"""The expression codec must work on plain ibis, without xorq installed.

Every other serialization suite starts with ``importorskip("xorq")``, which
is exactly how the plain-ibis ``Item`` slot divergence (``name`` vs
``indexer``) stayed invisible. This test runs the round-trip in a
subprocess with xorq import-blocked, so the ``HAS_XORQ=False`` branch of
the codec is exercised even on machines that have xorq.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_SCRIPT = """
import sys

class _BlockXorq:
    def find_spec(self, name, path=None, target=None):
        if name == "xorq" or name.startswith("xorq."):
            raise ImportError("xorq blocked for no-xorq codec test")

sys.meta_path.insert(0, _BlockXorq())
import ibis
from ibis import _
from boring_semantic_layer._xorq import HAS_XORQ
assert not HAS_XORQ, "xorq leaked through the blocker"
from boring_semantic_layer.serialization.codec import expr_to_structured, structured_to_expr

tbl = ibis.table({"a": "int64", "x": "int64", "b": "int64", "s": "string"}, name="t")
cases = [
    _.a + 1,
    _["x"],                      # Item: the slot renamed between flavors
    _.a.sum() / _.b.count(),
    -_.a,
    _.s.upper(),
    _["x"] * _.a,
    (_.a > 3) & (_.b < 9),
    _.s.isin(["u", "v"]),
]
for expr in cases:
    t = expr_to_structured(expr).unwrap()
    back = structured_to_expr(t).unwrap()
    r1 = expr.resolve(tbl)
    r2 = back.resolve(tbl) if hasattr(back, "resolve") else back(tbl)
    assert r1.equals(r2), (str(expr), str(r1), str(r2))
print("OK")
"""


def test_plain_ibis_roundtrip_without_xorq():
    src_dir = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        capture_output=True,
        text=True,
        cwd=src_dir,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout
