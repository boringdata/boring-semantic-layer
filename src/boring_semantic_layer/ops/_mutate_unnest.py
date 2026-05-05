"""``SemanticMutateOp`` and ``SemanticUnnestOp``.

Mutate adds derived columns to an aggregated/projected table; Unnest
explodes an array column into one row per element. Both pass through
dimension/measure metadata to their source.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ibis.expr import datatypes as dt
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema

from .._xorq import FrozenDict, FrozenOrderedDict
from ..measure_scope import MeasureScope
from ._callable import _ensure_wrapped
from ._format import _semantic_repr
from ._values import Dimension, Measure


class SemanticMutateOp(Relation):
    source: Relation
    post: dict[
        str,
        Callable,
    ]  # Transformed to FrozenDict[str, _CallableWrapper] in __init__
    nested_columns: tuple[
        str,
        ...,
    ] = ()  # Inherited from source if it has nested columns

    def __init__(
        self,
        source: Relation,
        post: dict[str, Callable] | None,
        nested_columns: tuple[str, ...] = (),
    ) -> None:
        frozen_post = FrozenDict(
            {name: _ensure_wrapped(fn) for name, fn in (post or {}).items()},
        )
        source_nested = nested_columns if nested_columns else getattr(source, "nested_columns", ())

        super().__init__(
            source=Relation.__coerce__(source),
            post=frozen_post,
            nested_columns=source_nested,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        from ._core import _resolve_expr, _to_untagged, _unwrap

        agg_tbl = _to_untagged(self.source)

        # Process mutations incrementally so each can reference previous ones
        # This allows: .mutate(rank=..., is_other=lambda t: t["rank"] > 5)
        current_tbl = agg_tbl
        for name, fn_wrapped in self.post.items():
            proxy = MeasureScope(_tbl=current_tbl, _known=[], _post_agg=True)
            resolved = _resolve_expr(_unwrap(fn_wrapped), proxy)

            new_col = resolved.name(name)
            current_tbl = current_tbl.mutate([new_col])

        return current_tbl

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


class SemanticUnnestOp(Relation):
    """Unnest an array column, expanding rows (like Malloy's nested data pattern)."""

    source: Relation
    column: str

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def schema(self) -> Schema:
        # After unnesting, the schema changes - the array column is replaced by its element schema
        # For now, delegate to source schema (ideally we'd update it)
        return self.source.schema

    @property
    def values(self) -> FrozenDict:
        return FrozenDict({})

    def to_untagged(self):
        """Convert to Ibis expression with functional struct unpacking.

        Uses pure helper functions to extract struct fields when unnesting
        produces struct columns that need to be expanded.
        """
        from ._core import _to_untagged

        def build_struct_fields(col_expr, col_type):
            """Pure function: build dict of struct field selections."""
            return {name: col_expr[name] for name in col_type.names}

        def unpack_struct_if_needed(unnested_tbl, column_name):
            """Conditionally unpack struct fields into top-level columns."""
            if column_name not in unnested_tbl.columns:
                return unnested_tbl

            col_expr = unnested_tbl[column_name]
            col_type = col_expr.type()

            # Only Struct types have fields to unpack
            if isinstance(col_type, dt.Struct) and col_type.fields:
                struct_fields = build_struct_fields(col_expr, col_type)
                return unnested_tbl.select(unnested_tbl, **struct_fields)

            return unnested_tbl

        tbl = _to_untagged(self.source)

        if self.column not in tbl.columns:
            raise ValueError(f"Column '{self.column}' not found in table")

        try:
            unnested = tbl.unnest(self.column)
        except Exception as e:
            raise ValueError(f"Failed to unnest column '{self.column}': {e}") from e

        return unpack_struct_if_needed(unnested, self.column)

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()
