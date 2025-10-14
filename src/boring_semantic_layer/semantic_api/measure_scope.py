from __future__ import annotations
from typing import Iterable
from .measure_nodes import MeasureRef, AllOf

class MeasureScope:
    """
    Scope for measure lambdas:
    - Unknown attrs fall through to ibis columns (so you can write t.col.sum()).
    - Known measure names return MeasureRef (so you can write t.case_count).
    - t.all(t.<measure>) yields AllOf(MeasureRef(...)).
    """
    def __init__(self, ibis_table, known_measures: Iterable[str]):
        self._tbl = ibis_table
        self._known = set(known_measures)

    def __getattr__(self, name: str):
        if name in self._known:
            return MeasureRef(name)
        return getattr(self._tbl, name)

    def all(self, ref):
        if not isinstance(ref, MeasureRef):
            raise TypeError("t.all(...) expects a measure reference, e.g. t.all(t.flight_count)")
        return AllOf(ref)

class ColumnScope:
    """
    Scope for re-evaluating *base* measure lambdas against a fresh base table.
    Only exposes columns; no measure references; no t.all().
    """
    def __init__(self, ibis_table):
        self._tbl = ibis_table
    def __getattr__(self, name: str):
        return getattr(self._tbl, name)