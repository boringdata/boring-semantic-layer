"""Lightweight semantic layer for Malloy-style data models using Ibis."""

from attrs import frozen, field, evolve
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Literal,
    ClassVar,
    Mapping,
)
import pandas as pd

try:
    import xorq.vendor.ibis as ibis_mod

    IS_XORQ_USED = True
except ImportError:
    import ibis as ibis_mod

    IS_XORQ_USED = False

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._

# Join strategies
How = Literal["inner", "left", "cross"]
Cardinality = Literal["one", "many", "cross"]

Dimension = Callable[[Expr], Expr]
Measure = Callable[[Expr], Expr]

TimeGrain = Literal[
    "TIME_GRAIN_YEAR",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_SECOND",
]

# Time grain transformation functions
TIME_GRAIN_TRANSFORMATIONS = {
    "TIME_GRAIN_YEAR": lambda t: t.year(),
    "TIME_GRAIN_QUARTER": lambda t: t.quarter(),
    "TIME_GRAIN_MONTH": lambda t: t.month(),
    "TIME_GRAIN_WEEK": lambda t: t.week(),
    "TIME_GRAIN_DAY": lambda t: t.date(),
    "TIME_GRAIN_HOUR": lambda t: t.hour(),
    "TIME_GRAIN_MINUTE": lambda t: t.minute(),
    "TIME_GRAIN_SECOND": lambda t: t.second(),
}

# Time grain ordering for validation
TIME_GRAIN_ORDER = [
    "TIME_GRAIN_SECOND",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_YEAR",
]


@frozen(kw_only=True, slots=True)
class Join:
    """Definition of a join relationship in the semantic model."""

    alias: str
    model: "SemanticModel"
    on: Callable[[Expr, Expr], Expr]
    how: How = "inner"
    kind: Cardinality = "one"


@frozen(kw_only=True, slots=True)
class Filter:
    """
    Unified filter class that handles all filter types and returns an unbound ibis expression.

    Supports:
    1. JSON filter objects (simple or compound)
    2. String expressions (eval as unbound ibis expressions)
    3. Callable functions that take a table and return a boolean expression

    Examples:
        # JSON simple filter
        Filter(filter={"field": "country", "operator": "=", "value": "US"})

        # JSON compound filter with table reference
        Filter(filter={
            "operator": "AND",
            "conditions": [
                {"field": "orders.country", "operator": "=", "value": "US"},
                {"field": "customers.tier", "operator": "in", "values": ["gold", "platinum"]}
            ]
        })

        # String expression
        Filter(filter="_.dep_time.year() == 2024")

        # Callable function
        Filter(filter=lambda t: t.amount > 1000)
    """

    filter: Union[Dict, str, Callable[[Expr], Expr]]

    # Class level constants
    OPERATOR_MAPPING = {
        "=": lambda x, y: x == y,
        "!=": lambda x, y: x != y,
        ">": lambda x, y: x > y,
        ">=": lambda x, y: x >= y,
        "<": lambda x, y: x < y,
        "<=": lambda x, y: x <= y,
        "in": lambda x, y: x.isin(y),
        "not in": lambda x, y: ~x.isin(y),
        "like": lambda x, y: x.like(y),
        "not like": lambda x, y: ~x.like(y),
        "is null": lambda x, _: x.isnull(),
        "is not null": lambda x, _: x.notnull(),
        "AND": lambda x, y: x & y,
        "OR": lambda x, y: x | y,
    }
    OPERATORS: ClassVar[set] = set(OPERATOR_MAPPING.keys())
    COMPOUND_OPERATORS: ClassVar[set] = {"AND", "OR"}

    def __attrs_post_init__(self):
        """Validate filter after initialization."""
        if not isinstance(self.filter, (dict, str)) and not callable(self.filter):
            raise ValueError("Filter must be a dict, string, or callable")

    def _get_field_expr(
        self, field: str, table: Optional[Expr], model: Optional["SemanticModel"] = None
    ) -> Expr:
        """Get field expression with proper error handling and join support."""
        if "." in field:
            table_name, field_name = field.split(".", 1)
            if model is not None and table is not None:
                if table_name not in model.joins:
                    raise KeyError(f"Unknown join alias: {table_name}")
                join = model.joins[table_name]
                if field_name not in join.model.dimensions:
                    raise KeyError(
                        f"Unknown dimension '{field_name}' in joined model '{table_name}'"
                    )
                return join.model.dimensions[field_name](table)
            else:
                # Unbound expression for table.field reference
                return getattr(getattr(_, table_name), field_name)
        else:
            if model is not None and table is not None:
                if field not in model.dimensions:
                    raise KeyError(f"Unknown dimension: {field}")
                return model.dimensions[field](table)
            else:
                # Unbound expression for field reference
                return getattr(_, field)

    def _parse_json_filter(
        self,
        filter_obj: Dict,
        table: Optional[Expr] = None,
        model: Optional["SemanticModel"] = None,
    ) -> Expr:
        """Convert a JSON filter to an Ibis expression."""
        # Handle compound filters (AND/OR)
        if (
            "operator" in filter_obj
            and filter_obj["operator"] in self.COMPOUND_OPERATORS
        ):
            if "conditions" not in filter_obj or not filter_obj["conditions"]:
                raise ValueError("Compound filter must have non-empty conditions list")

            # Process first condition
            if not filter_obj["conditions"]:
                raise ValueError("Compound filter must have at least one condition")

            result = self._parse_json_filter(filter_obj["conditions"][0], table, model)

            # Then combine with remaining conditions
            for condition in filter_obj["conditions"][1:]:
                next_expr = self._parse_json_filter(condition, table, model)
                result = self.OPERATOR_MAPPING[filter_obj["operator"]](
                    result, next_expr
                )

            return result

        # Handle simple filters
        required_keys = {"field", "operator"}
        missing_keys = required_keys - set(filter_obj.keys())
        if missing_keys:
            raise KeyError(f"Missing required keys in filter: {missing_keys}")

        # Get field expression
        field = filter_obj["field"]
        field_expr = self._get_field_expr(field, table, model)

        # Apply operator
        operator = filter_obj["operator"]
        if operator not in self.OPERATORS:
            raise ValueError(f"Unsupported operator: {operator}")

        # For 'in' and 'not in' operators, use values list
        if operator in ["in", "not in"]:
            if "values" not in filter_obj:
                raise ValueError(f"Operator '{operator}' requires 'values' field")
            return self.OPERATOR_MAPPING[operator](field_expr, filter_obj["values"])

        # For null checks, value is not needed
        elif operator in ["is null", "is not null"]:
            if any(k in filter_obj for k in ["value", "values"]):
                raise ValueError(
                    f"Operator '{operator}' should not have 'value' or 'values' fields"
                )
            return self.OPERATOR_MAPPING[operator](field_expr, None)

        # For all other operators, use the value field
        else:
            if "value" not in filter_obj:
                raise ValueError(f"Operator '{operator}' requires 'value' field")
            return self.OPERATOR_MAPPING[operator](field_expr, filter_obj["value"])

    def to_ibis(self, table: Expr, model: Optional["SemanticModel"] = None) -> Expr:
        """
        Convert the filter to an Ibis expression.

        Args:
            table: The Ibis table expression to filter
            model: Optional SemanticModel for validating field references
        """
        if isinstance(self.filter, dict):
            return self._parse_json_filter(self.filter, table, model)
        elif isinstance(self.filter, str):
            return eval(self.filter)
        elif callable(self.filter):
            return self.filter(table)
        else:
            raise ValueError("Filter must be a dict, string, or callable")


def _compile_query(qe) -> Expr:
    model = qe.model
    # Validate time grain
    model._validate_time_grain(qe.time_grain)
    # Start with the base table
    t = model.table
    # Apply joins
    for alias, join in model.joins.items():
        right = join.model.table
        if join.how == "cross":
            t = t.cross_join(right)
        else:
            cond = join.on(t, right)
            t = t.join(right, cond, how=join.how)
    # Transform time dimension if needed
    t, dimensions = model._transform_time_dimension(t, qe.time_grain)
    # Apply time range filter if provided
    if qe.time_range and model.timeDimension:
        start, end = qe.time_range
        time_filter = {
            "operator": "AND",
            "conditions": [
                {"field": model.timeDimension, "operator": ">=", "value": start},
                {"field": model.timeDimension, "operator": "<=", "value": end},
            ],
        }
        t = t.filter(Filter(filter=time_filter).to_ibis(t, model))
    # Apply other filters
    for flt in qe.filters:
        t = t.filter(flt.to_ibis(t, model))
    # Prepare dimensions and measures lists
    dims = list(qe.dims)
    if qe.time_grain and model.timeDimension and model.timeDimension not in dims:
        dims.append(model.timeDimension)
    measures = list(qe.measures)
    # Validate dimensions
    for d in dims:
        if "." in d:
            alias, field = d.split(".", 1)
            join = model.joins.get(alias)
            if not join or field not in join.model.dimensions:
                raise KeyError(f"Unknown dimension: {d}")
        elif d not in dimensions:
            raise KeyError(f"Unknown dimension: {d}")
    # Validate measures
    for m in measures:
        if "." in m:
            alias, field = m.split(".", 1)
            join = model.joins.get(alias)
            if not join or field not in join.model.measures:
                raise KeyError(f"Unknown measure: {m}")
        elif m not in model.measures:
            raise KeyError(f"Unknown measure: {m}")
    # Build aggregate expressions
    agg_kwargs: Dict[str, Expr] = {}
    for m in measures:
        if "." in m:
            alias, field = m.split(".", 1)
            join = model.joins[alias]
            expr = join.model.measures[field](t)
            name = f"{alias}_{field}"
            agg_kwargs[name] = expr.name(name)
        else:
            expr = model.measures[m](t)
            agg_kwargs[m] = expr.name(m)
    # Group and aggregate
    if dims:
        dim_exprs = []
        for d in dims:
            if "." in d:
                alias, field = d.split(".", 1)
                name = f"{alias}_{field}"
                expr = model.joins[alias].model.dimensions[field](t).name(name)
            else:
                expr = dimensions[d](t).name(d)
            dim_exprs.append(expr)
        result = t.aggregate(by=dim_exprs, **agg_kwargs)
    else:
        result = t.aggregate(**agg_kwargs)
    # Ordering
    if qe.order_by:
        order_exprs = []
        for field, direction in qe.order_by:
            col_name = field.replace(".", "_")
            col = result[col_name]
            order_exprs.append(
                col.desc() if direction.lower().startswith("desc") else col.asc()
            )
        result = result.order_by(order_exprs)
    # Limit
    if qe.limit is not None:
        result = result.limit(qe.limit)
    return result


@frozen(kw_only=True, slots=True)
class QueryExpr:
    model: "SemanticModel"
    dims: Tuple[str, ...] = field(factory=tuple)
    measures: Tuple[str, ...] = field(factory=tuple)
    filters: Tuple[Filter, ...] = field(factory=tuple)
    order_by: Tuple[Tuple[str, str], ...] = field(factory=tuple)
    limit: Optional[int] = None
    time_range: Optional[Tuple[str, str]] = None
    time_grain: Optional[TimeGrain] = None

    def with_dims(self, *dims: str) -> "QueryExpr":
        return self.clone(dims=self.dims + dims)

    def with_measures(self, *measures: str) -> "QueryExpr":
        return self.clone(measures=self.measures + measures)

    def with_filters(
        self, *f: Union[Filter, Dict[str, Any], str, Callable[[Expr], Expr]]
    ) -> "QueryExpr":
        wrapped = tuple(fi if isinstance(fi, Filter) else Filter(filter=fi) for fi in f)
        return self.clone(filters=self.filters + wrapped)

    def sorted(self, *order: Tuple[str, str]) -> "QueryExpr":
        return self.clone(order_by=self.order_by + order)

    def top(self, n: int) -> "QueryExpr":
        return self.clone(limit=n)

    def grain(self, g: TimeGrain) -> "QueryExpr":
        return self.clone(time_grain=g)

    def clone(self, **changes) -> "QueryExpr":
        return evolve(self, **changes)

    def to_ibis(self) -> Expr:
        return _compile_query(self)

    def execute(self, *args, **kwargs):
        return self.to_ibis().execute(*args, **kwargs)

    def sql(self) -> str:
        """Render the SQL for debugging or logging."""
        return ibis_mod.to_sql(self.to_ibis())


@frozen(kw_only=True, slots=True)
class SemanticModel:
    """
    Define a semantic model over an Ibis table expression with reusable dimensions and measures.

    Attributes:
        table: Base Ibis table expression.
        dimensions: Mapping of dimension names to callables producing column expressions.
        measures: Mapping of measure names to callables producing aggregate expressions.
        timeDimension: Optional name of the time dimension column.
        smallestTimeGrain: Optional smallest time grain for the time dimension.

    Example:
        con = xo.duckdb.connect()
        flights_tbl = con.table('flights')
        flights = SemanticModel(
            table=flights_tbl,
            dimensions={
                'origin': lambda t: t.origin,
                'destination': lambda t: t.destination,
                'carrier': lambda t: t.carrier,
            },
            measures={
                'flight_count': lambda t: t.count(),
                'avg_distance': lambda t: t.distance.mean(),
            },
            timeDimension='date',
            smallestTimeGrain='TIME_GRAIN_DAY'
        )
    """

    # Immutable fields
    table: Expr = field()
    dimensions: Mapping[str, Dimension] = field(
        converter=lambda d: MappingProxyType(dict(d))
    )
    measures: Mapping[str, Measure] = field(
        converter=lambda m: MappingProxyType(dict(m))
    )
    joins: Mapping[str, Join] = field(
        converter=lambda j: MappingProxyType(dict(j or {})),
        default=MappingProxyType({}),
    )
    primary_key: Optional[str] = field(default=None)
    name: Optional[str] = field(default=None)
    timeDimension: Optional[str] = field(default=None)
    smallestTimeGrain: Optional[TimeGrain] = field(default=None)

    def __attrs_post_init__(self):
        # Derive model name if not provided
        if self.name is None:
            try:
                nm = self.table.get_name()
            except Exception:
                nm = None
            object.__setattr__(self, "name", nm)
        # Validate smallestTimeGrain
        if (
            self.smallestTimeGrain is not None
            and self.smallestTimeGrain not in TIME_GRAIN_TRANSFORMATIONS
        ):
            raise ValueError(
                f"Invalid smallestTimeGrain. Must be one of: {', '.join(TIME_GRAIN_TRANSFORMATIONS.keys())}"
            )

    def build_query(self) -> "QueryExpr":
        """Return a deferred, composable QueryExpr."""
        return QueryExpr(model=self)

    def _validate_time_grain(self, time_grain: Optional[TimeGrain]) -> None:
        """Validate that the requested time grain is not finer than the smallest allowed grain."""
        if time_grain is None or self.smallestTimeGrain is None:
            return

        requested_idx = TIME_GRAIN_ORDER.index(time_grain)
        smallest_idx = TIME_GRAIN_ORDER.index(self.smallestTimeGrain)

        if requested_idx < smallest_idx:
            raise ValueError(
                f"Requested time grain '{time_grain}' is finer than the smallest allowed grain '{self.smallestTimeGrain}'"
            )

    def _transform_time_dimension(
        self, table: Expr, time_grain: Optional[TimeGrain]
    ) -> Tuple[Expr, Dict[str, Dimension]]:
        """Transform the time dimension based on the specified grain."""
        if not self.timeDimension or not time_grain:
            return table, self.dimensions.copy()

        # Create a copy of dimensions
        dimensions = self.dimensions.copy()

        # Get or create the time dimension function
        if self.timeDimension in dimensions:
            time_dim_func = dimensions[self.timeDimension]
        else:
            # Create a default time dimension function that accesses the column directly
            def time_dim_func(t: Expr) -> Expr:
                return getattr(t, self.timeDimension)

            dimensions[self.timeDimension] = time_dim_func

        # Create the transformed dimension function
        transform_func = TIME_GRAIN_TRANSFORMATIONS[time_grain]
        dimensions[self.timeDimension] = lambda t: transform_func(time_dim_func(t))

        return table, dimensions

    def query(
        self,
        dims: Optional[List[str]] = None,
        measures: Optional[List[str]] = None,
        filters: Optional[
            List[Union[Dict[str, Any], str, Callable[[Expr], Expr]]]
        ] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        time_range: Optional[Dict[str, str]] = None,
        time_grain: Optional[TimeGrain] = None,
    ) -> "QueryExpr":
        """Return a deferred, composable QueryExpr."""
        # Validate time grain
        self._validate_time_grain(time_grain)
        # Prepare components
        dims_list = list(dims) if dims else []
        measures_list = list(measures) if measures else []
        # Validate dimensions
        for d in dims_list:
            if isinstance(d, str) and "." in d:
                alias, field = d.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.dimensions:
                    raise KeyError(f"Unknown dimension: {d}")
            else:
                if d not in self.dimensions:
                    raise KeyError(f"Unknown dimension: {d}")
        # Validate measures
        for m in measures_list:
            if isinstance(m, str) and "." in m:
                alias, field = m.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.measures:
                    raise KeyError(f"Unknown measure: {m}")
            else:
                if m not in self.measures:
                    raise KeyError(f"Unknown measure: {m}")
        # Normalize filters to list
        if filters is None:
            filters_list = []
        else:
            filters_list = filters if isinstance(filters, list) else [filters]
        # Validate time_range format
        if time_range is not None:
            if (
                not isinstance(time_range, dict)
                or "start" not in time_range
                or "end" not in time_range
            ):
                raise ValueError(
                    "time_range must be a dictionary with 'start' and 'end' keys"
                )
        # Normalize order_by to list
        order_list = list(order_by) if order_by else []
        # Normalize time_range to tuple
        time_range_tuple = None
        if time_range:
            time_range_tuple = (time_range.get("start"), time_range.get("end"))
        # Early JSON filter validation to catch invalid specs
        # - Simple filters require 'field' and 'operator'; compound filters deferred
        for f in filters_list:
            if not isinstance(f, dict):
                continue
            # Skip compound filters here
            if f.get("operator") in Filter.COMPOUND_OPERATORS and "conditions" in f:
                continue
            # Validate required keys for simple filters
            required = {"field", "operator"}
            missing = required - set(f.keys())
            if missing:
                raise KeyError(f"Missing required keys in filter: {missing}")
            # Validate via Ibis parse to catch invalid operators or field refs
            Filter(filter=f).to_ibis(self.table, self)
        return QueryExpr(
            model=self,
            dims=tuple(dims_list),
            measures=tuple(measures_list),
            filters=tuple(
                f if isinstance(f, Filter) else Filter(filter=f) for f in filters_list
            ),
            order_by=tuple(tuple(o) for o in order_list),
            limit=limit,
            time_range=time_range_tuple,
            time_grain=time_grain,
        )

    def get_time_range(self) -> Dict[str, Any]:
        """Get the available time range for the model's time dimension.

        Returns:
            A dictionary with 'start' and 'end' dates in ISO format, or an error if no time dimension
        """
        if not self.timeDimension:
            return {"error": "Model does not have a time dimension"}

        # Get the original time dimension function
        time_dim_func = self.dimensions[self.timeDimension]

        # Query the min and max dates
        time_range = self.table.aggregate(
            start=time_dim_func(self.table).min(), end=time_dim_func(self.table).max()
        ).execute()

        # Convert to ISO format if not None
        # Access the first (and only) row's values directly
        start_date = (
            pd.Timestamp(time_range["start"].iloc[0]).isoformat()
            if pd.notna(time_range["start"].iloc[0])
            else None
        )
        end_date = (
            pd.Timestamp(time_range["end"].iloc[0]).isoformat()
            if pd.notna(time_range["end"].iloc[0])
            else None
        )

        return {"start": start_date, "end": end_date}

    @property
    def available_dimensions(self) -> List[str]:
        """List available dimension keys, including joined model dimensions."""
        keys = list(self.dimensions.keys())
        # Include time dimension if it exists and is not already in dimensions
        if self.timeDimension and self.timeDimension not in keys:
            keys.append(self.timeDimension)
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{d}" for d in join.model.dimensions.keys()])
        return keys

    @property
    def available_measures(self) -> List[str]:
        """List available measure keys, including joined model measures."""
        keys = list(self.measures.keys())
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{m}" for m in join.model.measures.keys()])
        return keys

    @property
    def json_definition(self) -> Dict[str, Any]:
        """Return model metadata including name, dimensions, measures, time dimension, and time grain."""
        definition = {
            "name": self.name,
            "dimensions": self.available_dimensions,
            "measures": self.available_measures,
        }

        # Add time dimension info if present
        if self.timeDimension:
            definition["timeDimension"] = self.timeDimension

        # Add smallest time grain if present
        if self.smallestTimeGrain:
            definition["smallestTimeGrain"] = self.smallestTimeGrain

        return definition

    @staticmethod
    def _is_additive(expr: Expr) -> bool:
        op = expr.op()
        name = type(op).__name__
        if name not in ("Sum", "Count", "Min", "Max"):
            return False
        if getattr(op, "distinct", False):
            return False
        return True

    def materialize(
        self,
        *,
        time_grain: TimeGrain = "TIME_GRAIN_DAY",
        cutoff: Union[str, pd.Timestamp, None] = None,
        dims: Optional[List[str]] = None,
        storage: Any = None,
    ) -> "SemanticModel":
        if not IS_XORQ_USED:
            raise RuntimeError("materialize() requires xorq vendor ibis backend")
        mod = self.table.__class__.__module__
        if not mod.startswith("xorq.vendor.ibis"):
            raise RuntimeError(
                f"materialize() requires xorq.vendor.ibis expressions, got module {mod}"
            )
        flat = self.table
        for alias, join in self.joins.items():
            right = join.model.table
            cond = join.on(flat, right)
            flat = flat.join(right, cond, how=join.how)

        if cutoff is not None and self.timeDimension:
            cutoff_ts = pd.to_datetime(cutoff)
            flat = flat.filter(getattr(flat, self.timeDimension) <= cutoff_ts)

        keys = dims if dims is not None else list(self.dimensions.keys())

        group_exprs: List[Expr] = []
        for key in keys:
            if key == self.timeDimension:
                col = flat[self.timeDimension]
                transform = TIME_GRAIN_TRANSFORMATIONS[time_grain]
                grouped_col = transform(col).name(key)
            else:
                grouped_col = self.dimensions[key](flat).name(key)
            group_exprs.append(grouped_col)

        agg_kwargs: Dict[str, Expr] = {}
        for name, fn in self.measures.items():
            expr = fn(flat)
            if self._is_additive(expr):
                agg_kwargs[name] = expr.name(name)

        if agg_kwargs:
            cube_expr = flat.group_by(*group_exprs).aggregate(**agg_kwargs)
        else:
            cube_expr = flat
        cube_table = cube_expr.cache(storage=storage)

        new_dims = {key: (lambda t, c=key: t[c]) for key in keys}
        new_measures: Dict[str, Measure] = {}
        for name in agg_kwargs:
            new_measures[name] = lambda t, c=name: t[c]
        for name, fn in self.measures.items():
            if name not in agg_kwargs:
                new_measures[name] = fn

        return SemanticModel(
            table=cube_table,
            dimensions=new_dims,
            measures=new_measures,
            joins={},
            name=f"{self.name}_cube_{time_grain.lower()}",
            timeDimension=self.timeDimension,
            smallestTimeGrain=time_grain,
        )


# functions for Malloy-style joins
def join_one(
    alias: str,
    model: SemanticModel,
    with_: Optional[Callable[[Expr], Expr]] = None,
) -> Join:
    if with_ is None:
        raise ValueError("join_one requires a 'with_' callable for foreign key mapping")
    if not callable(with_):
        raise TypeError(
            "'with_' must be a callable mapping the left table to a column expression"
        )
    if not model.primary_key:
        raise ValueError(f"Model does not have 'primary_key' defined for join: {alias}")

    def on_expr(left, right):
        return with_(left) == getattr(right, model.primary_key)

    # use inner join for one-to-one relationships
    return Join(alias=alias, model=model, on=on_expr, how="inner", kind="one")


def join_many(
    alias: str,
    model: SemanticModel,
    with_: Optional[Callable[[Expr], Expr]] = None,
) -> Join:
    if with_ is None:
        raise ValueError(
            "join_many requires a 'with_' callable for foreign key mapping"
        )
    if not callable(with_):
        raise TypeError(
            "'with_' must be a callable mapping the left table to a column expression"
        )
    if not model.primary_key:
        raise ValueError(f"Model does not have 'primary_key' defined for join: {alias}")

    def on_expr(left, right):
        return with_(left) == getattr(right, model.primary_key)

    return Join(alias=alias, model=model, on=on_expr, how="left", kind="many")


def join_cross(alias: str, model: SemanticModel) -> Join:
    return Join(
        alias=alias, model=model, on=lambda left, right: None, how="cross", kind="cross"
    )
