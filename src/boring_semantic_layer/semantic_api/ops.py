from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional

from attrs import frozen
from ibis.common.collections import FrozenDict, FrozenOrderedDict
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[Any], Any]
    description: Optional[str] = None
    is_time_dimension: bool = False
    smallest_time_grain: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        return self.expr(table)

    def to_json(self) -> Dict[str, Any]:
        """Convert dimension to JSON representation."""
        if self.is_time_dimension:
            return {
                "description": self.description,
                "smallest_time_grain": self.smallest_time_grain,
            }
        else:
            return {"description": self.description}


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[Any], Any]
    description: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        return self.expr(table)

    def to_json(self) -> Dict[str, Any]:
        """Convert measure to JSON representation."""
        return {"description": self.description}


# Notes on design:
# - .values must map column name -> Value ops that reference *parent* relations.
# - .schema must come from those values' dtypes, so Field(dtype) can resolve from rel.schema.


class SemanticTable(Relation):
    """Wrap a base Ibis table with semantic definitions (dimensions + measures)."""

    table: Any  # Relation | ir.Table is fine; Relation.__coerce__ will handle Expr
    dimensions: Any  # FrozenDict[str, Dimension]
    measures: Any  # FrozenDict[str, Measure]
    name: Optional[str]  # Name of the semantic table

    def __init__(
        self,
        table: Any,
        dimensions: dict[str, Dimension | Callable | dict] | None = None,
        measures: dict[str, Measure | Callable] | None = None,
        name: Optional[str] = None,
    ) -> None:
        # Convert dimensions to Dimension objects, supporting dict format for time dimensions
        dims = FrozenDict(
            {
                dim_name: self._create_dimension(dim)
                for dim_name, dim in (dimensions or {}).items()
            }
        )

        meas = FrozenDict(
            {
                meas_name: measure
                if isinstance(measure, Measure)
                else Measure(expr=measure, description=None)
                for meas_name, measure in (measures or {}).items()
            }
        )
        # Derive table name if not provided
        if name is None:
            try:
                table_expr = table.to_expr() if hasattr(table, "to_expr") else table
                derived_name = (
                    table_expr.get_name() if hasattr(table_expr, "get_name") else None
                )
            except Exception:
                derived_name = None
        else:
            derived_name = name

        base_rel = Relation.__coerce__(table.op() if hasattr(table, "op") else table)
        super().__init__(
            table=base_rel,
            dimensions=dims,
            measures=meas,
            name=derived_name,
        )

    def _create_dimension(self, expr) -> Dimension:
        """Create a Dimension object from various input formats."""
        if isinstance(expr, Dimension):
            return expr
        elif isinstance(expr, dict):
            # Handle time dimension specification: {"expr": lambda t: t.col, "smallest_time_grain": "day", "description": "..."}
            return Dimension(
                expr=expr["expr"],
                description=expr.get("description"),
                is_time_dimension=expr.get("is_time_dimension", False),
                smallest_time_grain=expr.get("smallest_time_grain"),
            )
        else:
            return Dimension(expr=expr, description=None)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        """Expose semantic fields as expressions referencing the base relation."""
        base_tbl = self.table.to_expr()
        out: dict[str, Any] = {}

        # Include all base table columns first
        for col_name in base_tbl.columns:
            out[col_name] = base_tbl[col_name].op()

        # Then add/override with semantic dimensions
        for name, fn in self.dimensions.items():
            expr = fn(base_tbl)
            out[name] = expr.op()

        # Then add measures
        for name, fn in self.measures.items():
            expr = fn(base_tbl)
            out[name] = expr.op()

        return FrozenOrderedDict(out)

    @property
    def schema(self) -> Schema:
        return Schema({name: v.dtype for name, v in self.values.items()})


    @property
    def json_definition(self) -> Dict[str, Any]:
        """
        Return a JSON-serializable definition of the semantic table.

        Returns:
            Dict[str, Any]: The semantic table metadata.
        """
        # Compute time dimensions on demand
        time_dims = {
            name: spec.to_json() 
            for name, spec in self.dimensions.items() 
            if spec.is_time_dimension
        }

        definition = {
            "dimensions": {
                name: spec.to_json() for name, spec in self.dimensions.items()
            },
            "measures": {name: spec.to_json() for name, spec in self.measures.items()},
            "time_dimensions": time_dims,
            "name": self.name,
        }

        return definition


class SemanticFilter(Relation):
    source: Any
    predicate: Callable

    def __init__(self, source: Any, predicate: Callable) -> None:
        super().__init__(source=Relation.__coerce__(source), predicate=predicate)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema


class SemanticProject(Relation):
    source: Any
    fields: tuple[str, ...]

    def __init__(self, source: Any, fields: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), fields=tuple(fields))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        src_vals = self.source.values
        return FrozenOrderedDict(
            {k: v for k, v in src_vals.items() if k in self.fields}
        )

    @property
    def schema(self) -> Schema:
        return Schema({k: v.dtype for k, v in self.values.items()})


class SemanticGroupBy(Relation):
    source: Any
    keys: tuple[str, ...]

    def __init__(self, source: Any, keys: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), keys=tuple(keys))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema


class SemanticAggregate(Relation):
    source: Any
    keys: tuple[str, ...]
    aggs: Any  # FrozenDict[str, Callable]

    def __init__(
        self,
        source: Any,
        keys: Iterable[str],
        aggs: dict[str, Callable] | None,
    ) -> None:
        frozen_aggs = FrozenDict(aggs or {})
        super().__init__(
            source=Relation.__coerce__(source), keys=tuple(keys), aggs=frozen_aggs
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        # Find all root models to handle joined tables properly
        all_roots = _find_all_root_models(self.source)

        # Use centralized prefixing logic
        merged_dimensions = _merge_fields_with_prefixing(
            all_roots, lambda root: root.dimensions
        )

        # Use the actual source table (which could be a join) as base_tbl
        base_tbl = self.source.to_expr()

        vals: dict[str, Any] = {}
        for k in self.keys:
            if k in merged_dimensions:
                vals[k] = merged_dimensions[k](base_tbl).op()
            else:
                vals[k] = base_tbl[k].op()
        for name, fn in self.aggs.items():
            vals[name] = fn(base_tbl).op()
        return FrozenOrderedDict(vals)

    @property
    def schema(self) -> Schema:
        return Schema({n: v.dtype for n, v in self.values.items()})


class SemanticMutate(Relation):
    source: Any
    post: Any  # FrozenDict[str, Callable]

    def __init__(self, source: Any, post: dict[str, Callable] | None) -> None:
        frozen_post = FrozenDict(post or {})
        super().__init__(source=Relation.__coerce__(source), post=frozen_post)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema


class SemanticJoin(Relation):
    left: Any
    right: Any
    how: str
    on: Callable[[Any, Any], Any] | None

    def __init__(
        self,
        left: Any,
        right: Any,
        how: str = "inner",
        on: Callable[[Any, Any], Any] | None = None,
    ) -> None:
        super().__init__(
            left=Relation.__coerce__(left),
            right=Relation.__coerce__(right),
            how=how,
            on=on,
        )

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        vals: dict[str, Any] = {}
        vals.update(self.left.values)
        vals.update(self.right.values)
        return FrozenOrderedDict(vals)

    @property
    def schema(self) -> Schema:
        return Schema({name: v.dtype for name, v in self.values.items()})

    @property
    def dimensions(self) -> FrozenDict[str, Dimension]:
        """Merge all dimensions from both sides of the join with prefixing."""
        all_roots = _find_all_root_models(self)
        merged_dims = _merge_fields_with_prefixing(
            all_roots, lambda root: root.dimensions
        )
        return FrozenDict(merged_dims)

    @property
    def measures(self) -> FrozenDict[str, Measure]:
        """Merge measures from both sides of the join with prefixing."""
        all_roots = _find_all_root_models(self)
        merged_measures = _merge_fields_with_prefixing(
            all_roots, lambda root: root.measures
        )
        return FrozenDict(merged_measures)


    @property
    def json_definition(self) -> Dict[str, Any]:
        """Return a JSON-serializable definition of the joined semantic table."""
        return {
            "dimensions": {
                name: dim.to_json() for name, dim in self.dimensions.items()
            },
            "measures": {
                name: measure.to_json() for name, measure in self.measures.items()
            },
            "time_dimensions": {
                name: dim.to_json() 
                for name, dim in self.dimensions.items() 
                if dim.is_time_dimension
            },
            "name": None,  # Joined tables don't have a single name
        }


class SemanticOrderBy(Relation):
    source: Any
    keys: tuple[Any, ...]  # Can be strings or ibis expressions with direction

    def __init__(self, source: Any, keys: Iterable[Any]) -> None:
        super().__init__(source=Relation.__coerce__(source), keys=tuple(keys))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema


class SemanticLimit(Relation):
    source: Any
    n: int
    offset: int

    def __init__(self, source: Any, n: int, offset: int = 0) -> None:
        super().__init__(source=Relation.__coerce__(source), n=n, offset=offset)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema


def _find_root_model(node: Any) -> SemanticTable | None:
    cur = node
    while cur is not None:
        if isinstance(cur, SemanticTable):
            return cur
        parent = getattr(cur, "source", None)
        cur = parent
    return None


def _find_all_root_models(node: Any) -> list[SemanticTable]:
    """Find all root SemanticTables in the operation tree (handles joins with multiple roots)."""
    if isinstance(node, SemanticTable):
        return [node]

    roots = []

    # Handle joins with left/right sides
    if hasattr(node, "left") and hasattr(node, "right"):
        roots.extend(_find_all_root_models(node.left))
        roots.extend(_find_all_root_models(node.right))
    # Handle single-source operations
    elif hasattr(node, "source") and node.source is not None:
        roots.extend(_find_all_root_models(node.source))

    return roots


def _merge_fields_with_prefixing(
    all_roots: list[SemanticTable], field_accessor: callable
) -> dict[str, Any]:
    """
    Generic function to merge any type of fields (dimensions, measures) with prefixing.

    Args:
        all_roots: List of SemanticTable root models
        field_accessor: Function that takes a root and returns the fields dict (e.g. lambda r: r.dimensions)

    Returns:
        Dictionary mapping field names (always prefixed with table name) to field values
    """
    if not all_roots:
        return {}

    merged_fields = {}

    # Always prefix fields with table name for consistency
    for root in all_roots:
        root_name = root.name
        fields_dict = field_accessor(root)

        for field_name, field_value in fields_dict.items():
            if root_name:
                # Always use prefixed name with __ separator
                prefixed_name = f"{root_name}__{field_name}"
                merged_fields[prefixed_name] = field_value
            else:
                # Fallback to original name if no root name
                merged_fields[field_name] = field_value

    return merged_fields
