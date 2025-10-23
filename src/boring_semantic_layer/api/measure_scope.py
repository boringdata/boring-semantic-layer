from __future__ import annotations
from typing import Iterable
from .measure_nodes import MeasureRef, AllOf


def _resolve_measure_name(name: str, known: list[str], known_set: set[str]) -> str | None:
    return (name if name in known_set
            else next((k for k in known if k.endswith(f".{name}")), None))


class MeasureScope:
    def __init__(self, ibis_table, known_measures: Iterable[str], post_aggregation: bool = False):
        self._tbl = ibis_table
        self._known = list(known_measures) if not isinstance(known_measures, list) else known_measures
        self._known_set = set(self._known)
        self._post_agg = post_aggregation

    def __getattr__(self, name: str):
        if self._post_agg:
            return getattr(self._tbl, name)
        resolved = _resolve_measure_name(name, self._known, self._known_set)
        return MeasureRef(resolved) if resolved else getattr(self._tbl, name)

    def __getitem__(self, name: str):
        if self._post_agg:
            return self._tbl[name]
        resolved = _resolve_measure_name(name, self._known, self._known_set)
        return MeasureRef(resolved) if resolved else self._tbl[name]

    def all(self, ref):
        import ibis as ibis_mod

        if isinstance(ref, str):
            if self._post_agg:
                return self._tbl[ref].sum().over(ibis_mod.window())
            resolved = _resolve_measure_name(ref, self._known, self._known_set)
            return (AllOf(MeasureRef(resolved)) if resolved
                    else self._tbl[ref].sum().over(ibis_mod.window()))

        return (AllOf(ref) if isinstance(ref, MeasureRef)
                else ref.sum().over(ibis_mod.window()) if hasattr(ref, '__class__') and 'ibis' in str(type(ref).__module__)
                else (_ for _ in ()).throw(TypeError(
                    "t.all(...) expects either a measure reference (e.g., t.flight_count), "
                    "a string measure name (e.g., 'flight_count'), "
                    "or an ibis column expression (e.g., t['aggregated_column'])"
                )))


class ColumnScope:
    def __init__(self, ibis_table):
        self._tbl = ibis_table

    def __getattr__(self, name: str):
        return getattr(self._tbl, name)

    def __getitem__(self, name: str):
        return self._tbl[name]