from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional

from attrs import frozen
from ibis.common.collections import FrozenDict, FrozenOrderedDict
from ibis.common.deferred import Deferred
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema


def _to_ibis(source: Any) -> Any:
    return source.to_ibis() if hasattr(source, 'to_ibis') else source.to_expr()


def _unwrap(wrapped: Any) -> Any:
    return wrapped.unwrap if isinstance(wrapped, _CallableWrapper) else wrapped


def _resolve_expr(expr: Any, scope: Any) -> Any:
    return (expr.resolve(scope) if isinstance(expr, Deferred)
            else expr(scope) if callable(expr) else expr)


def _get_field_dict(root: Any, field_type: str) -> dict:
    method_map = {
        'dims': ('_dims_dict', 'dimensions'),
        'measures': ('_measures_dict', 'measures'),
        'calc_measures': ('_calc_measures_dict', 'calc_measures')
    }
    methods = method_map.get(field_type, ('', ''))
    return (getattr(root, methods[0])() if hasattr(root, methods[0])
            else getattr(root, methods[1], {}))


def _get_merged_fields(all_roots: list, field_type: str) -> dict:
    return (_merge_fields_with_prefixing(all_roots, lambda r: _get_field_dict(r, field_type))
            if len(all_roots) > 1
            else _get_field_dict(all_roots[0], field_type) if all_roots
            else {})


def _collect_measure_refs(expr, refs_out: set):
    from .measure_nodes import MeasureRef, AllOf, BinOp
    if isinstance(expr, MeasureRef):
        refs_out.add(expr.name)
    elif isinstance(expr, AllOf):
        refs_out.add(expr.ref.name)
    elif isinstance(expr, BinOp):
        _collect_measure_refs(expr.left, refs_out)
        _collect_measure_refs(expr.right, refs_out)


@frozen
class _CallableWrapper:
    _fn: Any

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def __hash__(self):
        return hash(id(self._fn))

    @property
    def unwrap(self):
        return self._fn


def _ensure_wrapped(fn: Any) -> _CallableWrapper:
    return fn if isinstance(fn, _CallableWrapper) else _CallableWrapper(fn)


def _classify_measure(fn_or_expr: Any, scope: Any):
    from .measure_nodes import MeasureRef, AllOf, BinOp
    from .measure_scope import ColumnScope

    val = _resolve_expr(fn_or_expr, scope)
    is_calc = isinstance(val, (MeasureRef, AllOf, BinOp, int, float))

    if is_calc:
        return ('calc', val)
    elif isinstance(fn_or_expr, Measure):
        return ('base', fn_or_expr)
    else:
        return ('base', Measure(
            expr=lambda t, fn=fn_or_expr: (
                fn.resolve(ColumnScope(t)) if isinstance(fn, Deferred)
                else fn(ColumnScope(t))
            ),
            description=None
        ))


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[Any], Any] | Deferred
    description: Optional[str] = None
    is_time_dimension: bool = False
    smallest_time_grain: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        return self.expr.resolve(table) if isinstance(self.expr, Deferred) else self.expr(table)

    def to_json(self) -> Dict[str, Any]:
        base = {"description": self.description}
        return {**base, "smallest_time_grain": self.smallest_time_grain} if self.is_time_dimension else base

    def __hash__(self) -> int:
        return hash((self.description, self.is_time_dimension, self.smallest_time_grain))


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[Any], Any] | Deferred
    description: Optional[str] = None

    def __call__(self, table: Any) -> Any:
        return self.expr.resolve(table) if isinstance(self.expr, Deferred) else self.expr(table)

    def to_json(self) -> Dict[str, Any]:
        return {"description": self.description}

    def __hash__(self) -> int:
        return hash(self.description)


class SemanticTable(Relation):
    table: Any
    dimensions: Any
    measures: Any
    calc_measures: Any
    name: Optional[str] = None

    def __init__(
        self,
        table: Any,
        dimensions: dict[str, Dimension | Callable | dict] | None = None,
        measures: dict[str, Measure | Callable] | None = None,
        calc_measures: dict[str, Any] | None = None,
        name: Optional[str] = None,
    ) -> None:
        dims = FrozenDict({
            dim_name: self._create_dimension(dim)
            for dim_name, dim in (dimensions or {}).items()
        })

        meas = FrozenDict({
            meas_name: measure if isinstance(measure, Measure) else Measure(expr=measure, description=None)
            for meas_name, measure in (measures or {}).items()
        })

        calc_meas = FrozenDict(calc_measures or {})

        derived_name = name or self._derive_name(table)
        base_rel = Relation.__coerce__(table.op() if hasattr(table, "op") else table)

        super().__init__(
            table=base_rel,
            dimensions=dims,
            measures=meas,
            calc_measures=calc_meas,
            name=derived_name,
        )

    def _derive_name(self, table: Any) -> Optional[str]:
        try:
            table_expr = table.to_expr() if hasattr(table, "to_expr") else table
            return table_expr.get_name() if hasattr(table_expr, "get_name") else None
        except Exception:
            return None

    def _create_dimension(self, expr) -> Dimension:
        if isinstance(expr, Dimension):
            return expr
        if isinstance(expr, dict):
            return Dimension(
                expr=expr["expr"],
                description=expr.get("description"),
                is_time_dimension=expr.get("is_time_dimension", False),
                smallest_time_grain=expr.get("smallest_time_grain"),
            )
        return Dimension(expr=expr, description=None)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        base_tbl = self.table.to_expr()
        out = {col_name: base_tbl[col_name].op() for col_name in base_tbl.columns}

        for name, fn in self._dims_dict().items():
            out[name] = fn(base_tbl).op()

        for name, fn in self._measures_dict().items():
            out[name] = fn(base_tbl).op()

        return FrozenOrderedDict(out)

    @property
    def schema(self) -> Schema:
        return Schema({name: v.dtype for name, v in self.values.items()})

    @property
    def json_definition(self) -> Dict[str, Any]:
        dims_dict = self._dims_dict()
        meas_dict = self._measures_dict()

        return {
            "dimensions": {name: spec.to_json() for name, spec in dims_dict.items()},
            "measures": {name: spec.to_json() for name, spec in meas_dict.items()},
            "time_dimensions": {
                name: spec.to_json()
                for name, spec in dims_dict.items()
                if spec.is_time_dimension
            },
            "name": self.name,
        }

    @property
    def dims(self) -> list[str]:
        return list(self._dims_dict().keys())

    @property
    def _dims(self) -> dict[str, Dimension]:
        return dict(self._dims_dict())

    @property
    def _base_measures(self) -> dict[str, Measure]:
        return dict(object.__getattribute__(self, "measures"))

    @property
    def _calc_measures(self) -> dict[str, Any]:
        return dict(object.__getattribute__(self, "calc_measures"))

    def _measures_dict(self) -> FrozenDict:
        return object.__getattribute__(self, "measures")

    def _dims_dict(self) -> FrozenDict:
        return object.__getattribute__(self, "dimensions")

    def _calc_measures_dict(self) -> FrozenDict:
        return object.__getattribute__(self, "calc_measures")

    def __getattribute__(self, name: str):
        if name == "measures":
            base_meas = object.__getattribute__(self, "measures")
            calc_meas = object.__getattribute__(self, "calc_measures")
            return list(base_meas.keys()) + list(calc_meas.keys())
        return object.__getattribute__(self, name)

    def with_dimensions(self, **dims) -> "SemanticTable":
        return SemanticTable(
            table=self.table.to_expr(),
            dimensions={**self._dims_dict(), **{n: self._create_dimension(d) for n, d in dims.items()}},
            measures=dict(self._measures_dict()),
            calc_measures=dict(self._calc_measures_dict()),
            name=self.name
        )

    def with_measures(self, **meas) -> "SemanticTable":
        from .measure_scope import MeasureScope

        new_base_meas = dict(self._measures_dict())
        new_calc_meas = dict(self._calc_measures_dict())

        all_measure_names = list(new_base_meas.keys()) + list(new_calc_meas.keys()) + list(meas.keys())
        base_tbl = self.table.to_expr()
        scope = MeasureScope(base_tbl, known_measures=all_measure_names)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == 'calc' else new_base_meas)[name] = value

        return SemanticTable(
            table=self.table.to_expr(),
            dimensions=dict(self._dims_dict()),
            measures=new_base_meas,
            calc_measures=new_calc_meas,
            name=self.name
        )

    def filter(self, predicate: Callable) -> "SemanticFilter":
        return SemanticFilter(source=self, predicate=predicate)

    def group_by(self, *keys: str) -> "SemanticGroupBy":
        return SemanticGroupBy(source=self, keys=keys)

    def join(self, other: "SemanticTable", on: Callable[[Any, Any], Any] | None = None, how: str = "inner") -> "SemanticJoin":
        return SemanticJoin(left=self, right=other, on=on, how=how)

    def join_one(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticJoin":
        return SemanticJoin(left=self, right=other,
                          on=lambda l, r: getattr(l, left_on) == getattr(r, right_on), how="inner")

    def join_many(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticJoin":
        return SemanticJoin(left=self, right=other,
                          on=lambda l, r: getattr(l, left_on) == getattr(r, right_on), how="left")

    def join_cross(self, other: "SemanticTable") -> "SemanticJoin":
        return SemanticJoin(left=self, right=other, on=None, how="cross")

    def to_ibis(self):
        return self.table.to_expr()

    def execute(self):
        return self.to_ibis().execute()


class SemanticFilter(Relation):
    source: Any
    predicate: Any  # Can be Callable, Deferred, or _CallableWrapper

    def __init__(self, source: Any, predicate: Callable) -> None:
        super().__init__(source=Relation.__coerce__(source), predicate=_ensure_wrapped(predicate))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def filter(self, predicate: Callable) -> "SemanticFilter":
        return SemanticFilter(source=self, predicate=predicate)

    def group_by(self, *keys: str) -> "SemanticGroupBy":
        return SemanticGroupBy(source=self, keys=keys)

    def to_ibis(self):
        from .lower import _Resolver

        all_roots = _find_all_root_models(self.source)
        base_tbl = _to_ibis(self.source)
        dim_map = {} if isinstance(self.source, SemanticAggregate) else _get_merged_fields(all_roots, 'dims')

        pred_fn = _unwrap(self.predicate)
        resolver = _Resolver(base_tbl, dim_map)
        pred = _resolve_expr(pred_fn, resolver)
        return base_tbl.filter(pred)

    def execute(self):
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
        all_roots = _find_all_root_models(self.source)
        tbl = _to_ibis(self.source)

        if not all_roots:
            return tbl.select([getattr(tbl, f) for f in self.fields])

        merged_dimensions = _get_merged_fields(all_roots, 'dims')
        merged_measures = _get_merged_fields(all_roots, 'measures')

        dims = [f for f in self.fields if f in merged_dimensions]
        meas = [f for f in self.fields if f in merged_measures]
        raw_fields = [f for f in self.fields if f not in merged_dimensions and f not in merged_measures]

        dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]
        meas_exprs = [merged_measures[name](tbl).name(name) for name in meas]
        raw_exprs = [getattr(tbl, name) for name in raw_fields if hasattr(tbl, name)]

        return (tbl.group_by(dim_exprs).aggregate(meas_exprs) if meas_exprs and dim_exprs
                else tbl.aggregate(meas_exprs) if meas_exprs
                else tbl.select(dim_exprs + raw_exprs) if dim_exprs or raw_exprs
                else tbl)

    def execute(self):
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

    def aggregate(self, *measure_names, **aliased) -> "SemanticAggregate":
        """Aggregate measures (fluent API).

        Args:
            *measure_names: Measure names (strings) or callables
            **aliased: Named measure expressions
        """
        aggs = {}
        for item in measure_names:
            if isinstance(item, str):
                # String measure name - wrap in lambda to look it up
                aggs[item] = lambda t, n=item: getattr(t, n)
            elif callable(item):
                # Callable - use a generated name
                # Use id() to generate a unique name for the measure
                name = f"_measure_{id(item)}"
                aggs[name] = item
            else:
                raise TypeError(f"measure_names must be strings or callables, got {type(item)}")
        aggs.update(aliased)
        return SemanticAggregate(source=self, keys=self.keys, aggs=aggs)

    def to_ibis(self):
        """Convert to regular Ibis expression."""
        # SemanticGroupBy is just a holder - return the source as-is
        return self.source.to_ibis() if hasattr(self.source, 'to_ibis') else self.source.to_expr()

    def execute(self):
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
        frozen_aggs = FrozenDict({name: _ensure_wrapped(fn) for name, fn in (aggs or {}).items()})
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

    @property
    def measures(self) -> list[str]:
        """After aggregation, measures are materialized as columns, so return empty list."""
        return []

    def mutate(self, **post) -> "SemanticMutate":
        return SemanticMutate(source=self, post=post)

    def order_by(self, *keys: Any) -> "SemanticOrderBy":
        return SemanticOrderBy(source=self, keys=keys)

    def limit(self, n: int, offset: int = 0) -> "SemanticLimit":
        return SemanticLimit(source=self, n=n, offset=offset)

    def filter(self, predicate: Callable) -> "SemanticFilter":
        return SemanticFilter(source=self, predicate=predicate)

    def join(self, other: "SemanticTable", on: Callable[[Any, Any], Any] | None = None, how: str = "inner") -> "SemanticJoin":
        return SemanticJoin(left=self, right=other, on=on, how=how)

    def join_one(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticJoin":
        return SemanticJoin(left=self, right=other,
                          on=lambda l, r: l[left_on] == r[right_on], how="inner")

    def join_many(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticJoin":
        return SemanticJoin(left=self, right=other,
                          on=lambda l, r: l[left_on] == r[right_on], how="left")

    def to_ibis(self):
        from .measure_scope import MeasureScope, ColumnScope
        from .measure_nodes import MeasureRef, AllOf, BinOp
        from .compile_all import compile_grouped_with_all

        all_roots = _find_all_root_models(self.source)
        tbl = _to_ibis(self.source)

        merged_dimensions = _get_merged_fields(all_roots, 'dims')
        merged_base_measures = _get_merged_fields(all_roots, 'measures')
        merged_calc_measures = _get_merged_fields(all_roots, 'calc_measures')

        dim_mutations = {k: merged_dimensions[k](tbl) for k in self.keys if k in merged_dimensions}
        tbl = tbl.mutate(**dim_mutations) if dim_mutations else tbl

        all_measure_names = list(merged_base_measures.keys()) + list(merged_calc_measures.keys())
        scope = MeasureScope(tbl, known_measures=all_measure_names)

        agg_specs = {}
        calc_specs = {}

        for name, fn_wrapped in self.aggs.items():
            fn = _unwrap(fn_wrapped)
            val = _resolve_expr(fn, scope)

            if isinstance(val, MeasureRef):
                ref_name = val.name
                if ref_name in merged_calc_measures:
                    calc_specs[name] = merged_calc_measures[ref_name]
                elif ref_name in merged_base_measures:
                    measure_obj = merged_base_measures[ref_name]
                    agg_specs[name] = lambda t, m=measure_obj: m(t)
                else:
                    calc_specs[name] = val
            elif isinstance(val, (AllOf, BinOp, int, float)):
                calc_specs[name] = val
            else:
                agg_specs[name] = lambda t, f=fn: (f.resolve(ColumnScope(t)) if isinstance(f, Deferred)
                                                   else f(ColumnScope(t)))

        referenced_measures = set()
        for calc_expr in calc_specs.values():
            _collect_measure_refs(calc_expr, referenced_measures)

        for ref_name in referenced_measures:
            if ref_name not in agg_specs and ref_name in merged_base_measures:
                measure_obj = merged_base_measures[ref_name]
                agg_specs[ref_name] = lambda t, m=measure_obj: m(t)

        requested_measure_names = list(self.aggs.keys())
        by_cols = list(self.keys)

        return (compile_grouped_with_all(tbl, by_cols, agg_specs, calc_specs, requested_measures=requested_measure_names)
                if calc_specs or by_cols
                else tbl.aggregate({name: agg_fn(tbl) for name, agg_fn in agg_specs.items()}))

    def execute(self):
        return self.to_ibis().execute()


class SemanticMutate(Relation):
    source: Any
    post: Any  # FrozenDict[str, _CallableWrapper]

    def __init__(self, source: Any, post: dict[str, Callable] | None) -> None:
        frozen_post = FrozenDict({name: _ensure_wrapped(fn) for name, fn in (post or {}).items()})
        super().__init__(source=Relation.__coerce__(source), post=frozen_post)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def mutate(self, **post) -> "SemanticMutate":
        new_post = {**self.post, **{name: _ensure_wrapped(fn) for name, fn in post.items()}}
        return SemanticMutate(source=self.source, post=new_post)

    def order_by(self, *keys: Any) -> "SemanticOrderBy":
        """Order results (fluent API)."""
        return SemanticOrderBy(source=self, keys=keys)

    def limit(self, n: int, offset: int = 0) -> "SemanticLimit":
        """Limit results (fluent API)."""
        return SemanticLimit(source=self, n=n, offset=offset)

    def filter(self, predicate: Callable) -> "SemanticFilter":
        """Filter after mutate (fluent API)."""
        return SemanticFilter(source=self, predicate=predicate)

    def to_ibis(self):
        from .measure_scope import MeasureScope

        agg_tbl = _to_ibis(self.source)
        proxy = MeasureScope(agg_tbl, known_measures=[], post_aggregation=True)

        new_cols = [_resolve_expr(_unwrap(fn_wrapped), proxy).name(name)
                    for name, fn_wrapped in self.post.items()]

        return agg_tbl.mutate(new_cols) if new_cols else agg_tbl

    def execute(self):
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

    def _dims_dict(self) -> FrozenDict[str, Dimension]:
        """Internal: Get merged dimensions dict from both sides of the join."""
        all_roots = _find_all_root_models(self)
        merged_dims = _merge_fields_with_prefixing(
            all_roots, lambda root: root._dims_dict() if hasattr(root, '_dims_dict') else root.dimensions
        )
        return FrozenDict(merged_dims)

    def _measures_dict(self) -> FrozenDict[str, Measure]:
        """Internal: Get merged base measures dict from both sides of the join."""
        all_roots = _find_all_root_models(self)
        merged_measures = _merge_fields_with_prefixing(
            all_roots, lambda root: root._measures_dict() if hasattr(root, '_measures_dict') else root.measures
        )
        return FrozenDict(merged_measures)

    def _calc_measures_dict(self) -> FrozenDict:
        """Internal: Get merged calculated measures dict from both sides of the join."""
        all_roots = _find_all_root_models(self)
        merged_calc_measures = _merge_fields_with_prefixing(
            all_roots, lambda root: root._calc_measures_dict() if hasattr(root, '_calc_measures_dict') else root.calc_measures
        )
        return FrozenDict(merged_calc_measures)

    @property
    def dimensions(self) -> FrozenDict[str, Dimension]:
        """Merge all dimensions from both sides of the join with prefixing."""
        return self._dims_dict()

    @property
    def dims(self) -> list[str]:
        """Return list of dimension names (for introspection)."""
        return list(self._dims_dict().keys())

    @property
    def _dims(self) -> dict[str, Dimension]:
        """Return raw dimensions dict (for tests)."""
        return dict(self._dims_dict())

    @property
    def _base_measures(self) -> dict[str, Measure]:
        """Return raw base measures dict (for tests)."""
        return dict(self._measures_dict())

    @property
    def _calc_measures(self) -> dict[str, Any]:
        """Return raw calculated measures dict (for tests)."""
        return dict(self._calc_measures_dict())

    @property
    def measures(self) -> list[str]:
        """Return list of all measure names (for introspection)."""
        base_measures = self._measures_dict()
        calc_measures = self._calc_measures_dict()
        return list(base_measures.keys()) + list(calc_measures.keys())

    @property
    def json_definition(self) -> Dict[str, Any]:
        """Return a JSON-serializable definition of the joined semantic table."""
        dims_dict = self._dims_dict()
        meas_dict = self._measures_dict()
        return {
            "dimensions": {
                name: dim.to_json() for name, dim in dims_dict.items()
            },
            "measures": {
                name: measure.to_json() for name, measure in meas_dict.items()
            },
            "time_dimensions": {
                name: dim.to_json()
                for name, dim in dims_dict.items()
                if dim.is_time_dimension
            },
            "name": None,  # Joined tables don't have a single name
        }

    def with_dimensions(self, **dims) -> "SemanticTable":
        """Add dimensions after join (fluent API). Returns new SemanticTable wrapping the join."""
        # Get all existing dimensions and measures from both sides (already prefixed)
        existing_dimensions = self._dims_dict()
        existing_base_measures = self._measures_dict()
        existing_calc_measures = self._calc_measures_dict()

        # Materialize the join to create the base table
        joined_tbl = self.to_ibis()

        # Merge existing and new dimensions
        merged_dimensions = dict(existing_dimensions)
        merged_dimensions.update(dims)

        return SemanticTable(
            table=joined_tbl,
            dimensions=merged_dimensions,  # Include both existing and new dimensions
            measures=dict(existing_base_measures),  # Preserve existing base measures
            calc_measures=dict(existing_calc_measures),  # Preserve existing calc measures
            name=None
        )

    def with_measures(self, **meas) -> "SemanticTable":
        """Add measures after join (fluent API). Returns new SemanticTable wrapping the join."""
        from .measure_scope import MeasureScope

        existing_dimensions = self._dims_dict()
        existing_base_measures = self._measures_dict()
        existing_calc_measures = self._calc_measures_dict()

        all_known_measures = list(existing_base_measures.keys()) + list(existing_calc_measures.keys()) + list(meas.keys())
        joined_tbl = self.to_ibis()
        scope = MeasureScope(joined_tbl, known_measures=all_known_measures)

        new_base_meas = dict(existing_base_measures)
        new_calc_meas = dict(existing_calc_measures)

        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope)
            (new_calc_meas if kind == 'calc' else new_base_meas)[name] = value

        return SemanticTable(
            table=joined_tbl,
            dimensions=dict(existing_dimensions),
            measures=new_base_meas,
            calc_measures=new_calc_meas,
            name=None
        )

    def group_by(self, *keys: str) -> "SemanticGroupBy":
        """Group by dimensions (fluent API)."""
        return SemanticGroupBy(source=self, keys=keys)

    def filter(self, predicate: Callable) -> "SemanticFilter":
        """Filter after join (fluent API)."""
        return SemanticFilter(source=self, predicate=predicate)

    def join(self, other: "SemanticTable", on: Callable[[Any, Any], Any] | None = None, how: str = "inner") -> "SemanticJoin":
        """Chain another join (fluent API)."""
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

    def to_ibis(self):
        from .lower import _Resolver

        left_tbl = _to_ibis(self.left)
        right_tbl = _to_ibis(self.right)

        return (left_tbl.join(right_tbl, self.on(_Resolver(left_tbl), _Resolver(right_tbl)), how=self.how)
                if self.on is not None
                else left_tbl.join(right_tbl, how=self.how))

    def execute(self):
        return self.to_ibis().execute()


class SemanticOrderBy(Relation):
    source: Any
    keys: tuple[Any, ...]  # Can be strings or _CallableWrapper (wrapping Deferred/callable)

    def __init__(self, source: Any, keys: Iterable[Any]) -> None:
        def wrap_key(k):
            return k if isinstance(k, (str, _CallableWrapper)) else _CallableWrapper(k)
        super().__init__(source=Relation.__coerce__(source), keys=tuple(wrap_key(k) for k in keys))

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def limit(self, n: int, offset: int = 0) -> "SemanticLimit":
        """Limit results after ordering (fluent API)."""
        return SemanticLimit(source=self, n=n, offset=offset)

    def to_ibis(self):
        tbl = _to_ibis(self.source)

        def resolve_order_key(key):
            if isinstance(key, str):
                return (tbl[key] if key in tbl.columns
                        else getattr(tbl, key) if hasattr(tbl, key)
                        else key)
            elif isinstance(key, _CallableWrapper):
                unwrapped = _unwrap(key)
                return _resolve_expr(unwrapped, tbl)
            return key

        return tbl.order_by([resolve_order_key(key) for key in self.keys])

    def execute(self):
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
        tbl = _to_ibis(self.source)
        return tbl.limit(self.n) if self.offset == 0 else tbl.limit(self.n, offset=self.offset)

    def execute(self):
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


def _update_measure_refs_in_calc(expr, prefix_map: dict[str, str]):
    """
    Recursively update MeasureRef names in a calculated measure expression.

    Args:
        expr: A MeasureExpr (MeasureRef, AllOf, BinOp, or literal)
        prefix_map: Mapping from old name to new prefixed name

    Returns:
        Updated expression with prefixed MeasureRef names
    """
    from .measure_nodes import MeasureRef, AllOf, BinOp

    if isinstance(expr, MeasureRef):
        # Update the measure reference name if it's in the map
        new_name = prefix_map.get(expr.name, expr.name)
        return MeasureRef(new_name)
    elif isinstance(expr, AllOf):
        # Update the inner MeasureRef
        updated_ref = _update_measure_refs_in_calc(expr.ref, prefix_map)
        return AllOf(updated_ref)
    elif isinstance(expr, BinOp):
        # Recursively update left and right
        updated_left = _update_measure_refs_in_calc(expr.left, prefix_map)
        updated_right = _update_measure_refs_in_calc(expr.right, prefix_map)
        return BinOp(op=expr.op, left=updated_left, right=updated_right)
    else:
        # Literal number or other - return as-is
        return expr


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

    # Special handling for calculated measures - need to update internal MeasureRefs
    # Determine if we're processing calc measures by checking the field type
    is_calc_measures = False
    if all_roots:
        sample_fields = field_accessor(all_roots[0])
        if sample_fields:
            from .measure_nodes import MeasureRef, AllOf, BinOp
            first_val = next(iter(sample_fields.values()), None)
            is_calc_measures = isinstance(first_val, (MeasureRef, AllOf, BinOp, int, float))

    # Always prefix fields with table name for consistency
    for root in all_roots:
        root_name = root.name
        fields_dict = field_accessor(root)

        if is_calc_measures and root_name:
            base_map = {k: f"{root_name}__{k}" for k in root._measures_dict().keys()} if hasattr(root, '_measures_dict') else {}
            calc_map = {k: f"{root_name}__{k}" for k in root._calc_measures_dict().keys()} if hasattr(root, '_calc_measures_dict') else {}
            prefix_map = {**base_map, **calc_map}

        for field_name, field_value in fields_dict.items():
            if root_name:
                # Always use prefixed name with __ separator
                prefixed_name = f"{root_name}__{field_name}"

                # If it's a calculated measure, update internal MeasureRefs
                if is_calc_measures:
                    field_value = _update_measure_refs_in_calc(field_value, prefix_map)

                merged_fields[prefixed_name] = field_value
            else:
                # Fallback to original name if no root name
                merged_fields[field_name] = field_value

    return merged_fields
