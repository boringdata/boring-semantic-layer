from __future__ import annotations
from typing import Iterable
from .measure_nodes import MeasureRef, AllOf

class MeasureScope:
    """
    Scope for measure lambdas:
    - Unknown attrs fall through to ibis columns (so you can write t.col.sum()).
    - Known measure names return MeasureRef (so you can write t.case_count).
    - t.all(t.<measure>) yields AllOf(MeasureRef(...)).
    - t["column"] provides bracket-notation access to ibis columns.
    """
    def __init__(self, ibis_table, known_measures: Iterable[str]):
        self._tbl = ibis_table
        self._known = set(known_measures)

    def __getattr__(self, name: str):
        if name in self._known:
            return MeasureRef(name)
        return getattr(self._tbl, name)

    def __getitem__(self, name: str):
        """Bracket notation access to columns (returns ibis expression)."""
        return self._tbl[name]

    def all(self, ref):
        """
        Compute grand total of a measure.
        Accepts either a MeasureRef or an ibis expression (for post-aggregation use).
        """
        import ibis as ibis_mod
        if isinstance(ref, MeasureRef):
            return AllOf(ref)
        # If it's an ibis expression (post-aggregation column), compute sum over all rows
        elif hasattr(ref, '__class__') and 'ibis' in str(type(ref).__module__):
            # This is an ibis expression - return window function for grand total
            return ref.sum().over(ibis_mod.window())
        else:
            raise TypeError(
                "t.all(...) expects either a measure reference (e.g., t.flight_count) "
                "or an ibis column expression (e.g., t['aggregated_column'])"
            )

class ColumnScope:
    """
    Scope for re-evaluating *base* measure lambdas against a fresh base table.
    Only exposes columns; no measure references; no t.all().
    """
    def __init__(self, ibis_table):
        self._tbl = ibis_table
    def __getattr__(self, name: str):
        return getattr(self._tbl, name)