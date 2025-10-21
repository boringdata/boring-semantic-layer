from __future__ import annotations
from typing import Iterable
from .measure_nodes import MeasureRef, AllOf

class MeasureScope:
    """
    Scope for measure lambdas:
    - Unknown attrs fall through to ibis columns (so you can write t.col.sum()).
    - Known measure names return MeasureRef (so you can write t.case_count).
    - t.all(t.<measure>) yields AllOf(MeasureRef(...)).
    - t["column"] provides bracket-notation access to measures/columns.

    Args:
        post_aggregation: If True, bracket notation returns ibis columns instead of MeasureRef.
                         Used in .mutate() after .aggregate() where measures are materialized.
    """
    def __init__(self, ibis_table, known_measures: Iterable[str], post_aggregation: bool = False):
        self._tbl = ibis_table
        self._known = set(known_measures)
        self._post_agg = post_aggregation

    def __getattr__(self, name: str):
        # In post-aggregation context, always return ibis columns (measures are materialized)
        if self._post_agg:
            return getattr(self._tbl, name)
        # In pre-aggregation context, return MeasureRef for known measures
        # 1. Try exact match first
        if name in self._known:
            return MeasureRef(name)
        # 2. Try to find prefixed version (table__name format)
        for known_name in self._known:
            if known_name.endswith(f"__{name}"):
                return MeasureRef(known_name)
        # 3. Fall back to ibis column
        return getattr(self._tbl, name)

    def __getitem__(self, name: str):
        """Bracket notation access to measures and columns.

        In pre-aggregation context (post_aggregation=False):
            Returns MeasureRef for known measures, otherwise ibis columns.
            This makes t["measure_name"] and t.measure_name behave consistently.
            Supports both full prefixed names (table__measure) and short names (measure).

        In post-aggregation context (post_aggregation=True):
            Always returns ibis columns directly, since measures are materialized.
            Used in .mutate() after .aggregate().
        """
        if self._post_agg:
            # Post-aggregation: always return ibis column
            return self._tbl[name]
        # Pre-aggregation: return MeasureRef for known measures
        # 1. Try exact match first
        if name in self._known:
            return MeasureRef(name)
        # 2. Try to find prefixed version (table__name format)
        for known_name in self._known:
            if known_name.endswith(f"__{name}"):
                return MeasureRef(known_name)
        # 3. Fall back to ibis column
        return self._tbl[name]

    def all(self, ref):
        """
        Compute grand total of a measure.
        Accepts:
        - MeasureRef object (e.g., t.flight_count)
        - String measure name (e.g., "flight_count")
        - Ibis expression (for post-aggregation use)
        """
        import ibis as ibis_mod

        # Handle string measure names by converting to MeasureRef
        if isinstance(ref, str):
            # Use the same resolution logic as __getattr__
            if self._post_agg:
                # Post-aggregation: treat as column name and return window function
                return self._tbl[ref].sum().over(ibis_mod.window())
            # Pre-aggregation: convert string to MeasureRef
            # 1. Try exact match first
            if ref in self._known:
                return AllOf(MeasureRef(ref))
            # 2. Try to find prefixed version (table__name format)
            for known_name in self._known:
                if known_name.endswith(f"__{ref}"):
                    return AllOf(MeasureRef(known_name))
            # 3. Fall back to treating as column name (post-agg context)
            return self._tbl[ref].sum().over(ibis_mod.window())

        if isinstance(ref, MeasureRef):
            return AllOf(ref)
        # If it's an ibis expression (post-aggregation column), compute sum over all rows
        elif hasattr(ref, '__class__') and 'ibis' in str(type(ref).__module__):
            # This is an ibis expression - return window function for grand total
            return ref.sum().over(ibis_mod.window())
        else:
            raise TypeError(
                "t.all(...) expects either a measure reference (e.g., t.flight_count), "
                "a string measure name (e.g., 'flight_count'), "
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
    def __getitem__(self, name: str):
        """Bracket notation access to columns."""
        return self._tbl[name]