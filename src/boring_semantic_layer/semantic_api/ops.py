from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional

from attrs import frozen
from ibis.common.collections import FrozenDict, FrozenOrderedDict
from ibis.common.deferred import Deferred
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema


@frozen
class _CallableWrapper:
    """Hashable wrapper for callables and deferred expressions."""
    _fn: Any

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def __hash__(self):
        # Use id for hashing since the actual callable/deferred may not be hashable
        return hash(id(self._fn))

    @property
    def unwrap(self):
        return self._fn


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[Any], Any] | Deferred
    description: Optional[str] = None
    is_time_dimension: bool = False
    smallest_time_grain: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        if isinstance(self.expr, Deferred):
            return self.expr.resolve(table)
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

    def __hash__(self) -> int:
        # Hash only metadata (expr/Deferred is unhashable)
        return hash((self.description, self.is_time_dimension, self.smallest_time_grain))


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[Any], Any] | Deferred
    description: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        if isinstance(self.expr, Deferred):
            return self.expr.resolve(table)
        return self.expr(table)

    def to_json(self) -> Dict[str, Any]:
        """Convert measure to JSON representation."""
        return {"description": self.description}

    def __hash__(self) -> int:
        # Hash only description (expr/Deferred is unhashable)
        return hash(self.description)


# Notes on design:
# - .values must map column name -> Value ops that reference *parent* relations.
# - .schema must come from those values' dtypes, so Field(dtype) can resolve from rel.schema.


class SemanticTable(Relation):
    """Wrap a base Ibis table with semantic definitions (dimensions + measures)."""

    table: Any  # Relation | ir.Table is fine; Relation.__coerce__ will handle Expr
    dimensions: Any  # FrozenDict[str, Dimension]
    measures: Any  # FrozenDict[str, Measure]
    name: Optional[str] = None  # Name of the semantic table

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

    def with_dimensions(self, **dims) -> "SemanticTable":
        """Add dimensions to the semantic table (fluent API). Returns new SemanticTable."""
        new_dims = dict(self.dimensions)
        new_dims.update({
            name: self._create_dimension(d) for name, d in dims.items()
        })
        return SemanticTable(
            table=self.table.to_expr(),
            dimensions=new_dims,
            measures=dict(self.measures),
            name=self.name
        )

    def with_measures(self, **meas) -> "SemanticTable":
        """Add measures to the semantic table (fluent API). Returns new SemanticTable."""
        new_meas = dict(self.measures)
        new_meas.update({
            name: m if isinstance(m, Measure) else Measure(expr=m)
            for name, m in meas.items()
        })
        return SemanticTable(
            table=self.table.to_expr(),
            dimensions=dict(self.dimensions),
            measures=new_meas,
            name=self.name
        )

    def filter(self, predicate: Callable) -> "SemanticFilter":
        """Filter rows (fluent API). Returns SemanticFilter operation."""
        return SemanticFilter(source=self, predicate=predicate)

    def group_by(self, *keys: str) -> "SemanticGroupBy":
        """Group by dimensions (fluent API). Returns SemanticGroupBy operation."""
        return SemanticGroupBy(source=self, keys=keys)

    def join(self, other: "SemanticTable", on: Callable[[Any, Any], Any] | None = None, how: str = "inner") -> "SemanticJoin":
        """Join with another semantic table (fluent API). Returns SemanticJoin operation."""
        return SemanticJoin(left=self, right=other, on=on, how=how)

    def join_one(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticJoin":
        """Inner join one-to-one or many-to-one on primary/foreign keys."""
        def predicate(left, right):
            return getattr(left, left_on) == getattr(right, right_on)
        return SemanticJoin(left=self, right=other, on=predicate, how="inner")

    def join_many(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticJoin":
        """Left join one-to-many on primary/foreign keys."""
        def predicate(left, right):
            return getattr(left, left_on) == getattr(right, right_on)
        return SemanticJoin(left=self, right=other, on=predicate, how="left")

    def join_cross(self, other: "SemanticTable") -> "SemanticJoin":
        """Cross join two semantic tables."""
        return SemanticJoin(left=self, right=other, on=None, how="cross")

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        return self.table.to_expr()

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


class SemanticFilter(Relation):
    source: Any
    predicate: Any  # Can be Callable, Deferred, or _CallableWrapper

    def __init__(self, source: Any, predicate: Callable) -> None:
        wrapped_pred = _CallableWrapper(predicate) if not isinstance(predicate, _CallableWrapper) else predicate
        super().__init__(source=Relation.__coerce__(source), predicate=wrapped_pred)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def filter(self, predicate: Callable) -> "SemanticFilter":
        """Chain another filter (fluent API)."""
        return SemanticFilter(source=self, predicate=predicate)

    def group_by(self, *keys: str) -> "SemanticGroupBy":
        """Group by dimensions (fluent API)."""
        return SemanticGroupBy(source=self, keys=keys)

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        from .lower import _Resolver

        all_roots = _find_all_root_models(self.source)
        base_tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()

        # Check if filtering after aggregation
        if isinstance(self.source, SemanticAggregate):
            dim_map = {}
        else:
            if len(all_roots) > 1:
                dim_map = _merge_fields_with_prefixing(all_roots, lambda r: r.dimensions)
            else:
                dim_map = all_roots[0].dimensions if all_roots else {}

        # Unwrap the predicate if it's wrapped
        pred_fn = self.predicate.unwrap if isinstance(self.predicate, _CallableWrapper) else self.predicate
        # Handle both Deferred and Callable predicates
        if isinstance(pred_fn, Deferred):
            pred = pred_fn.resolve(_Resolver(base_tbl, dim_map))
        else:
            pred = pred_fn(_Resolver(base_tbl, dim_map))
        return base_tbl.filter(pred)

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


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

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        all_roots = _find_all_root_models(self.source)
        if not all_roots:
            tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()
            cols = [getattr(tbl, f) for f in self.fields]
            return tbl.select(cols)

        tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()

        # Get merged fields
        if len(all_roots) > 1:
            merged_dimensions = _merge_fields_with_prefixing(all_roots, lambda r: r.dimensions)
            merged_measures = _merge_fields_with_prefixing(all_roots, lambda r: r.measures)
        else:
            merged_dimensions = all_roots[0].dimensions if all_roots else {}
            merged_measures = all_roots[0].measures if all_roots else {}

        dims = [f for f in self.fields if f in merged_dimensions]
        meas = [f for f in self.fields if f in merged_measures]
        raw_fields = [f for f in self.fields if f not in merged_dimensions and f not in merged_measures]

        dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]
        meas_exprs = [merged_measures[name](tbl).name(name) for name in meas]
        raw_exprs = [getattr(tbl, name) for name in raw_fields if hasattr(tbl, name)]

        if meas_exprs:
            if dim_exprs:
                return tbl.group_by(dim_exprs).aggregate(meas_exprs)
            else:
                return tbl.aggregate(meas_exprs)
        else:
            all_exprs = dim_exprs + raw_exprs
            return tbl.select(all_exprs) if all_exprs else tbl

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


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

    def aggregate(self, *measure_names: str, **aliased) -> "SemanticAggregate":
        """Aggregate measures (fluent API)."""
        aggs = {name: lambda t, n=name: getattr(t, n) for name in measure_names}
        aggs.update(aliased)
        return SemanticAggregate(source=self, keys=self.keys, aggs=aggs)

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        # SemanticGroupBy is just a holder - return the source as-is
        return self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


class SemanticAggregate(Relation):
    source: Any
    keys: tuple[str, ...]
    aggs: Any  # FrozenDict[str, _CallableWrapper]

    def __init__(
        self,
        source: Any,
        keys: Iterable[str],
        aggs: dict[str, Callable] | None,
    ) -> None:
        # Wrap all callables/deferred in _CallableWrapper to make them hashable
        wrapped_aggs = {
            name: _CallableWrapper(fn) if not isinstance(fn, _CallableWrapper) else fn
            for name, fn in (aggs or {}).items()
        }
        frozen_aggs = FrozenDict(wrapped_aggs)
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

    def mutate(self, **post) -> "SemanticMutate":
        """Add computed columns (fluent API)."""
        return SemanticMutate(source=self, post=post)

    def order_by(self, *keys: Any) -> "SemanticOrderBy":
        """Order results (fluent API)."""
        return SemanticOrderBy(source=self, keys=keys)

    def limit(self, n: int, offset: int = 0) -> "SemanticLimit":
        """Limit results (fluent API)."""
        return SemanticLimit(source=self, n=n, offset=offset)

    def filter(self, predicate: Callable) -> "SemanticFilter":
        """Filter aggregated results (fluent API)."""
        return SemanticFilter(source=self, predicate=predicate)

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        from attrs import frozen
        from .lower import _Resolver

        all_roots = _find_all_root_models(self.source)
        tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()

        # Get merged fields
        if len(all_roots) > 1:
            merged_dimensions = _merge_fields_with_prefixing(all_roots, lambda r: r.dimensions)
            merged_measures = _merge_fields_with_prefixing(all_roots, lambda r: r.measures)
        else:
            merged_dimensions = all_roots[0].dimensions if all_roots else {}
            merged_measures = all_roots[0].measures if all_roots else {}

        # Build group by expressions
        group_exprs = []
        for k in self.keys:
            if k in merged_dimensions:
                group_exprs.append(merged_dimensions[k](tbl).name(k))
            else:
                group_exprs.append(getattr(tbl, k).name(k))

        # Build aggregate expressions
        @frozen
        class _AggResolver:
            _t: Any
            _dims: dict[str, Callable]
            _meas: dict[str, Callable]

            def __getattr__(self, key: str):
                if key in self._dims:
                    return self._dims[key](self._t)
                if key in self._meas:
                    return self._meas[key](self._t)

                for dim_name, dim_func in self._dims.items():
                    if dim_name.endswith(f"__{key}"):
                        return dim_func(self._t)
                for meas_name, meas_func in self._meas.items():
                    if meas_name.endswith(f"__{key}"):
                        return meas_func(self._t)

                return getattr(self._t, key)

            def __getitem__(self, key: str):
                return getattr(self._t, key)

        proxy = _AggResolver(tbl, merged_dimensions, merged_measures)
        meas_exprs = []
        for name, fn_wrapped in self.aggs.items():
            # Unwrap the callable/deferred
            fn = fn_wrapped.unwrap if isinstance(fn_wrapped, _CallableWrapper) else fn_wrapped
            # Handle both Deferred and Callable
            if isinstance(fn, Deferred):
                expr = fn.resolve(proxy)
            else:
                expr = fn(proxy)
            meas_exprs.append(expr.name(name))

        # Build metrics mapping
        metrics = FrozenOrderedDict({expr.get_name(): expr for expr in meas_exprs})

        if group_exprs:
            return tbl.group_by(group_exprs).aggregate(metrics)
        else:
            return tbl.aggregate(metrics)

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


class SemanticMutate(Relation):
    source: Any
    post: Any  # FrozenDict[str, _CallableWrapper]

    def __init__(self, source: Any, post: dict[str, Callable] | None) -> None:
        # Wrap all callables/deferred in _CallableWrapper to make them hashable
        wrapped_post = {
            name: _CallableWrapper(fn) if not isinstance(fn, _CallableWrapper) else fn
            for name, fn in (post or {}).items()
        }
        frozen_post = FrozenDict(wrapped_post)
        super().__init__(source=Relation.__coerce__(source), post=frozen_post)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        from attrs import frozen

        agg_tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()

        @frozen
        class _AggProxy:
            _t: Any

            def __getattr__(self, key: str):
                return self._t[key]

            def __getitem__(self, key: str):
                return self._t[key]

        proxy = _AggProxy(agg_tbl)
        new_cols = []
        for name, fn_wrapped in self.post.items():
            # Unwrap the callable/deferred
            fn = fn_wrapped.unwrap if isinstance(fn_wrapped, _CallableWrapper) else fn_wrapped
            # Handle both Deferred and Callable
            if isinstance(fn, Deferred):
                expr = fn.resolve(proxy)
            else:
                expr = fn(proxy)
            new_cols.append(expr.name(name))
        return agg_tbl.mutate(new_cols) if new_cols else agg_tbl

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


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

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        from .lower import _Resolver

        left_tbl = self.left.to_ibis() if hasattr(self.left, 'to_ibis') else self.left.to_expr()
        right_tbl = self.right.to_ibis() if hasattr(self.right, 'to_ibis') else self.right.to_expr()

        if self.on is not None:
            pred = self.on(_Resolver(left_tbl), _Resolver(right_tbl))
            return left_tbl.join(right_tbl, pred, how=self.how)
        else:
            return left_tbl.join(right_tbl, how=self.how)

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


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

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        from .lower import _Resolver

        tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()
        order_keys = []

        for key in self.keys:
            if isinstance(key, str):
                if hasattr(tbl, key) or key in tbl.columns:
                    order_keys.append(tbl[key] if key in tbl.columns else getattr(tbl, key))
                else:
                    order_keys.append(key)
            elif isinstance(key, tuple) and len(key) == 2 and key[0] == "__deferred__":
                deferred_fn = key[1]
                order_keys.append(deferred_fn(tbl))
            else:
                order_keys.append(key)

        return tbl.order_by(order_keys)

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


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

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        tbl = self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()
        if self.offset == 0:
            return tbl.limit(self.n)
        else:
            return tbl.limit(self.n, offset=self.offset)

    def execute(self):
        """Execute the query and return results as a pandas DataFrame."""
        return self.to_ibis().execute()


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
