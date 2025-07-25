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
    TYPE_CHECKING,
)
import datetime

if TYPE_CHECKING:
    import altair

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
    "TIME_GRAIN_YEAR": lambda t: t.truncate("Y"),
    "TIME_GRAIN_QUARTER": lambda t: t.truncate("Q"),
    "TIME_GRAIN_MONTH": lambda t: t.truncate("M"),
    "TIME_GRAIN_WEEK": lambda t: t.truncate("W"),
    "TIME_GRAIN_DAY": lambda t: t.truncate("D"),
    "TIME_GRAIN_HOUR": lambda t: t.truncate("h"),
    "TIME_GRAIN_MINUTE": lambda t: t.truncate("m"),
    "TIME_GRAIN_SECOND": lambda t: t.truncate("s"),
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

OPERATOR_MAPPING = {
    "=": lambda x, y: x == y,
    "eq": lambda x, y: x == y,
    "equals": lambda x, y: x == y,
    "!=": lambda x, y: x != y,
    ">": lambda x, y: x > y,
    ">=": lambda x, y: x >= y,
    "<": lambda x, y: x < y,
    "<=": lambda x, y: x <= y,
    "in": lambda x, y: x.isin(y),
    "not in": lambda x, y: ~x.isin(y),
    "like": lambda x, y: x.like(y),
    "not like": lambda x, y: ~x.like(y),
    "ilike": lambda x, y: x.ilike(y),
    "not ilike": lambda x, y: ~x.ilike(y),
    "is null": lambda x, _: x.isnull(),
    "is not null": lambda x, _: x.notnull(),
    "AND": lambda x, y: x & y,
    "OR": lambda x, y: x | y,
}


@frozen(kw_only=True, slots=True)
class Join:
    """Definition of a join relationship in the semantic model."""

    alias: str
    model: "SemanticModel"
    on: Callable[[Expr, Expr], Expr]
    how: How = "inner"
    kind: Cardinality = "one"

    @classmethod
    def one(
        cls,
        alias: str,
        model: "SemanticModel",
        with_: Optional[Callable[[Expr], Expr]] = None,
    ) -> "Join":
        """
        Create a one-to-one join relationship for a semantic model.

        Args:
            alias: Alias for the join.
            model: The joined SemanticModel.
            with_: Callable mapping the left table to a column expression (foreign key).
        Returns:
            Join: The Join object representing the relationship.
        Raises:
            ValueError: If 'with_' is not provided or model has no primary key.
            TypeError: If 'with_' is not callable.
        """
        if with_ is None:
            raise ValueError(
                "Join.one requires a 'with_' callable for foreign key mapping"
            )
        if not callable(with_):
            raise TypeError(
                "'with_' must be a callable mapping the left table to a column expression"
            )
        if not model.primary_key:
            raise ValueError(
                f"Model does not have 'primary_key' defined for join: {alias}"
            )

        def on_expr(left, right):
            return with_(left) == getattr(right, model.primary_key)

        return cls(alias=alias, model=model, on=on_expr, how="inner", kind="one")

    @classmethod
    def many(
        cls,
        alias: str,
        model: "SemanticModel",
        with_: Optional[Callable[[Expr], Expr]] = None,
    ) -> "Join":
        """
        Create a one-to-many join relationship for a semantic model.

        Args:
            alias: Alias for the join.
            model: The joined SemanticModel.
            with_: Callable mapping the left table to a column expression (foreign key).
        Returns:
            Join: The Join object representing the relationship.
        Raises:
            ValueError: If 'with_' is not provided or model has no primary key.
            TypeError: If 'with_' is not callable.
        """
        if with_ is None:
            raise ValueError(
                "Join.many requires a 'with_' callable for foreign key mapping"
            )
        if not callable(with_):
            raise TypeError(
                "'with_' must be a callable mapping the left table to a column expression"
            )
        if not model.primary_key:
            raise ValueError(
                f"Model does not have 'primary_key' defined for join: {alias}"
            )

        def on_expr(left, right):
            return with_(left) == getattr(right, model.primary_key)

        return cls(alias=alias, model=model, on=on_expr, how="left", kind="many")

    @classmethod
    def cross(
        cls,
        alias: str,
        model: "SemanticModel",
    ) -> "Join":
        """
        Create a cross join relationship for a semantic model.

        Args:
            alias: Alias for the join.
            model: The joined SemanticModel.
        Returns:
            Join: The Join object representing the cross join relationship.
        """
        return cls(
            alias=alias,
            model=model,
            on=lambda left, right: None,
            how="cross",
            kind="cross",
        )


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
                result = OPERATOR_MAPPING[filter_obj["operator"]](result, next_expr)

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
            return OPERATOR_MAPPING[operator](field_expr, filter_obj["values"])

        # For null checks, value is not needed
        elif operator in ["is null", "is not null"]:
            if any(k in filter_obj for k in ["value", "values"]):
                raise ValueError(
                    f"Operator '{operator}' should not have 'value' or 'values' fields"
                )
            return OPERATOR_MAPPING[operator](field_expr, None)

        else:
            if "value" not in filter_obj:
                raise ValueError(f"Operator '{operator}' requires 'value' field")
            return OPERATOR_MAPPING[operator](field_expr, filter_obj["value"])

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
    """Compile a QueryExpr into an Ibis expression."""
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
    t, dim_map = model._transform_time_dimension(t, qe.time_grain)

    # Apply time range filter if provided
    if qe.time_range and model.time_dimension:
        start, end = qe.time_range
        time_filter = {
            "operator": "AND",
            "conditions": [
                {"field": model.time_dimension, "operator": ">=", "value": start},
                {"field": model.time_dimension, "operator": "<=", "value": end},
            ],
        }
        t = t.filter(Filter(filter=time_filter).to_ibis(t, model))

    # Apply other filters
    for flt in qe.filters:
        t = t.filter(flt.to_ibis(t, model))

    # Prepare dimensions and measures lists
    dimensions = list(qe.dimensions)
    if (
        qe.time_grain
        and model.time_dimension
        and model.time_dimension not in dimensions
    ):
        dimensions.append(model.time_dimension)
    measures = list(qe.measures)

    # Validate dimensions
    for d in dimensions:
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
    if dimensions:
        dim_exprs = []
        for d in dimensions:
            if "." in d:
                alias, field = d.split(".", 1)
                name = f"{alias}_{field}"
                expr = model.joins[alias].model.dimensions[field](t).name(name)
            else:
                # Use possibly transformed dimension function
                expr = dim_map[d](t).name(d)
            dim_exprs.append(expr)
        result = t.aggregate(by=dim_exprs, **agg_kwargs)
    else:
        result = t.aggregate(**agg_kwargs)

    # Apply ordering
    if qe.order_by:
        order_exprs = []
        for field, direction in qe.order_by:
            col_name = field.replace(".", "_")
            col = result[col_name]
            order_exprs.append(
                col.desc() if direction.lower().startswith("desc") else col.asc()
            )
        result = result.order_by(order_exprs)

    # Apply limit
    if qe.limit is not None:
        result = result.limit(qe.limit)

    return result


def _detect_chart_spec(
    dimensions: List[str],
    measures: List[str],
    time_dimension: Optional[str] = None,
    time_grain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Detect an appropriate chart type and return a Vega-Lite specification.

    Args:
        dimensions: List of dimension names
        measures: List of measure names
        time_dimension: Optional name of the time dimension
        time_grain: Optional time grain for temporal formatting

    Returns:
        A Vega-Lite specification dict with appropriate chart type
    """
    num_dims = len(dimensions)
    num_measures = len(measures)

    # Single value - text display
    if num_dims == 0 and num_measures == 1:
        return {
            "mark": {"type": "text", "size": 40},
            "encoding": {"text": {"field": measures[0], "type": "quantitative"}},
        }

    # Check if we have a time dimension
    has_time = time_dimension and time_dimension in dimensions
    time_dim_index = dimensions.index(time_dimension) if has_time else -1

    # Determine appropriate date format and axis config based on time grain
    if has_time and time_grain:
        if "YEAR" in time_grain:
            date_format = "%Y"
            axis_config = {"format": date_format, "labelAngle": 0}
        elif "QUARTER" in time_grain:
            date_format = "%Y Q%q"
            axis_config = {"format": date_format, "labelAngle": -45}
        elif "MONTH" in time_grain:
            date_format = "%Y-%m"
            axis_config = {"format": date_format, "labelAngle": -45}
        elif "WEEK" in time_grain:
            date_format = "%Y W%W"
            axis_config = {"format": date_format, "labelAngle": -45, "tickCount": 10}
        elif "DAY" in time_grain:
            date_format = "%Y-%m-%d"
            axis_config = {"format": date_format, "labelAngle": -45}
        elif "HOUR" in time_grain:
            date_format = "%m-%d %H:00"
            axis_config = {"format": date_format, "labelAngle": -45, "tickCount": 12}
        else:
            date_format = "%Y-%m-%d"
            axis_config = {"format": date_format, "labelAngle": -45}
    else:
        date_format = "%Y-%m-%d"
        axis_config = {"format": date_format, "labelAngle": -45}

    # Single dimension, single measure
    if num_dims == 1 and num_measures == 1:
        if has_time:
            # Time series - line chart
            return {
                "mark": "line",
                "encoding": {
                    "x": {
                        "field": dimensions[0],
                        "type": "temporal",
                        "axis": axis_config,
                    },
                    "y": {"field": measures[0], "type": "quantitative"},
                    "tooltip": [
                        {
                            "field": dimensions[0],
                            "type": "temporal",
                            "format": date_format,
                        },
                        {"field": measures[0], "type": "quantitative"},
                    ],
                },
            }
        else:
            # Categorical - bar chart
            return {
                "mark": "bar",
                "encoding": {
                    "x": {"field": dimensions[0], "type": "nominal"},
                    "y": {"field": measures[0], "type": "quantitative"},
                    "tooltip": [
                        {"field": dimensions[0], "type": "nominal"},
                        {"field": measures[0], "type": "quantitative"},
                    ],
                },
            }

    # Single dimension, multiple measures - grouped bar chart
    if num_dims == 1 and num_measures >= 2:
        # Need to reshape data for multiple measures
        # This requires a transform to fold the measures into a single column
        return {
            "transform": [{"fold": measures, "as": ["measure", "value"]}],
            "mark": "bar",
            "encoding": {
                "x": {"field": dimensions[0], "type": "nominal"},
                "y": {"field": "value", "type": "quantitative"},
                "color": {"field": "measure", "type": "nominal"},
                "xOffset": {"field": "measure"},  # Groups bars side by side
                "tooltip": [
                    {"field": dimensions[0], "type": "nominal"},
                    {"field": "measure", "type": "nominal"},
                    {"field": "value", "type": "quantitative"},
                ],
            },
        }

    # Time series with additional dimension(s) - multi-line chart
    if has_time and num_dims >= 2 and num_measures == 1:
        # Get non-time dimensions for color encoding
        non_time_dims = [d for i, d in enumerate(dimensions) if i != time_dim_index]

        # Build tooltip fields
        tooltip_fields = [
            {"field": time_dimension, "type": "temporal", "format": date_format},
            {"field": non_time_dims[0], "type": "nominal"},
            {"field": measures[0], "type": "quantitative"},
        ]

        return {
            "mark": "line",
            "encoding": {
                "x": {"field": time_dimension, "type": "temporal", "axis": axis_config},
                "y": {"field": measures[0], "type": "quantitative"},
                "color": {
                    "field": non_time_dims[0],
                    "type": "nominal",
                },  # Color by first non-time dimension
                "tooltip": tooltip_fields,
            },
        }

    # Time series with multiple measures
    if has_time and num_dims == 1 and num_measures >= 2:
        # Multi-line chart with one line per measure
        return {
            "transform": [{"fold": measures, "as": ["measure", "value"]}],
            "mark": "line",
            "encoding": {
                "x": {"field": dimensions[0], "type": "temporal", "axis": axis_config},
                "y": {"field": "value", "type": "quantitative"},
                "color": {"field": "measure", "type": "nominal"},
                "tooltip": [
                    {"field": dimensions[0], "type": "temporal", "format": date_format},
                    {"field": "measure", "type": "nominal"},
                    {"field": "value", "type": "quantitative"},
                ],
            },
        }

    # Two dimensions, one measure - heatmap
    if num_dims == 2 and num_measures == 1:
        return {
            "mark": "rect",
            "encoding": {
                "x": {"field": dimensions[0], "type": "nominal"},
                "y": {"field": dimensions[1], "type": "nominal"},
                "color": {"field": measures[0], "type": "quantitative"},
                "tooltip": [
                    {"field": dimensions[0], "type": "nominal"},
                    {"field": dimensions[1], "type": "nominal"},
                    {"field": measures[0], "type": "quantitative"},
                ],
            },
        }

    # Default for complex queries
    return {
        "mark": "text",
        "encoding": {
            "text": {"value": "Complex query - consider custom visualization"}
        },
    }


@frozen(kw_only=True, slots=True)
class QueryExpr:
    model: "SemanticModel"
    dimensions: Tuple[str, ...] = field(factory=tuple)
    measures: Tuple[str, ...] = field(factory=tuple)
    filters: Tuple[Filter, ...] = field(factory=tuple)
    order_by: Tuple[Tuple[str, str], ...] = field(factory=tuple)
    limit: Optional[int] = None
    time_range: Optional[Tuple[str, str]] = None
    time_grain: Optional[TimeGrain] = None

    def with_dimensions(self, *dimensions: str) -> "QueryExpr":
        """
        Return a new QueryExpr with additional dimensions added.

        Args:
            *dimensions: Dimension names to add.
        Returns:
            QueryExpr: A new QueryExpr with the specified dimensions.
        """
        return self.clone(dimensions=self.dimensions + dimensions)

    def with_measures(self, *measures: str) -> "QueryExpr":
        """
        Return a new QueryExpr with additional measures added.

        Args:
            *measures: Measure names to add.
        Returns:
            QueryExpr: A new QueryExpr with the specified measures.
        """
        return self.clone(measures=self.measures + measures)

    def with_filters(
        self, *f: Union[Filter, Dict[str, Any], str, Callable[[Expr], Expr]]
    ) -> "QueryExpr":
        """
        Return a new QueryExpr with additional filters added.

        Args:
            *f: Filters to add (Filter, dict, str, or callable).
        Returns:
            QueryExpr: A new QueryExpr with the specified filters.
        """
        wrapped = tuple(fi if isinstance(fi, Filter) else Filter(filter=fi) for fi in f)
        return self.clone(filters=self.filters + wrapped)

    def sorted(self, *order: Tuple[str, str]) -> "QueryExpr":
        """
        Return a new QueryExpr with additional order by clauses.

        Args:
            *order: Tuples of (field, direction) to order by.
        Returns:
            QueryExpr: A new QueryExpr with the specified ordering.
        """
        return self.clone(order_by=self.order_by + order)

    def top(self, n: int) -> "QueryExpr":
        """
        Return a new QueryExpr with a row limit applied.

        Args:
            n: The maximum number of rows to return.
        Returns:
            QueryExpr: A new QueryExpr with the specified row limit.
        """
        return self.clone(limit=n)

    def grain(self, g: TimeGrain) -> "QueryExpr":
        """
        Return a new QueryExpr with a specified time grain.

        Args:
            g: The time grain to use.
        Returns:
            QueryExpr: A new QueryExpr with the specified time grain.
        """
        return self.clone(time_grain=g)

    def clone(self, **changes) -> "QueryExpr":
        """
        Return a copy of this QueryExpr with the specified changes applied.

        Args:
            **changes: Fields to override in the new QueryExpr.
        Returns:
            QueryExpr: A new QueryExpr with the changes applied.
        """
        return evolve(self, **changes)

    def to_expr(self) -> Expr:
        """
        Compile this QueryExpr into an Ibis expression.

        Returns:
            Expr: The compiled Ibis expression representing the query.
        """
        return _compile_query(self)

    to_ibis = to_expr

    def execute(self, *args, **kwargs):
        """
        Execute the compiled Ibis expression and return the result.

        Args:
            *args: Positional arguments passed to Ibis execute().
            **kwargs: Keyword arguments passed to Ibis execute().
        Returns:
            The result of executing the query.
        """
        return self.to_expr().execute(*args, **kwargs)

    def sql(self) -> str:
        """
        Return the SQL string for the compiled query.

        Returns:
            str: The SQL representation of the query.
        """
        return ibis_mod.to_sql(self.to_expr())

    def maybe_to_expr(self) -> Optional[Expr]:
        """
        Try to compile this QueryExpr to an Ibis expression, returning None if it fails.

        Returns:
            Optional[Expr]: The compiled Ibis expression, or None if compilation fails.
        """
        try:
            return self.to_expr()
        except Exception:
            return None

    def chart(
        self,
        spec: Optional[Dict[str, Any]] = None,
        format: str = "altair",
    ) -> Union["altair.Chart", Dict[str, Any], bytes, str]:
        """
        Create a chart from the query using native Ibis-Altair integration.

        Args:
            spec: Optional Vega-Lite specification for the chart.
                  If not provided, will auto-detect chart type based on query.
                  If partial spec is provided (e.g., only encoding or only mark),
                  missing parts will be auto-detected and merged.
            format: The output format of the chart:
                - "altair" (default): Returns Altair Chart object
                - "interactive": Returns interactive Altair Chart with tooltip
                - "json": Returns Vega-Lite JSON specification
                - "png": Returns PNG image bytes
                - "svg": Returns SVG string

        Returns:
            Chart in the requested format:
                - altair/interactive: Altair Chart object
                - json: Dict containing Vega-Lite specification
                - png: bytes of PNG image
                - svg: str containing SVG markup

        Raises:
            ImportError: If Altair is not installed
            ValueError: If an unsupported format is specified
        """
        try:
            import altair as alt
        except ImportError:
            raise ImportError(
                "Altair is required for chart creation. "
                "Install it with: pip install 'boring-semantic-layer[visualization]'"
            )

        # Always start with auto-detected spec as base
        base_spec = _detect_chart_spec(
            dimensions=list(self.dimensions),
            measures=list(self.measures),
            time_dimension=self.model.time_dimension,
            time_grain=self.time_grain,
        )

        if spec is None:
            spec = base_spec
        else:
            if "mark" not in spec.keys():
                spec["mark"] = base_spec.get("mark", "point")

            if "encoding" not in spec.keys():
                spec["encoding"] = base_spec.get("encoding", {})

            if "transform" not in spec.keys():
                spec["transform"] = base_spec.get("transform", [])

        chart = alt.Chart(self.to_expr(), **spec)

        # Handle different output formats
        if format == "altair":
            return chart
        elif format == "interactive":
            return chart.interactive()
        elif format == "json":
            return chart.to_dict()
        elif format in ["png", "svg"]:
            try:
                import io

                buffer = io.BytesIO()
                chart.save(buffer, format=format)
                return buffer.getvalue()
            except Exception as e:
                raise ImportError(
                    f"{format} export requires additional dependencies: {e}. "
                    "Install with: pip install 'altair[all]' or pip install vl-convert-python"
                )
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'altair', 'interactive', 'json', 'png', 'svg'"
            )


@frozen(kw_only=True, slots=True)
class SemanticModel:
    """
    Define a semantic model over an Ibis table expression with reusable dimensions and measures.

    Attributes:
        table: Base Ibis table expression.
        dimensions: Mapping of dimension names to callables producing column expressions.
        measures: Mapping of measure names to callables producing aggregate expressions.
        time_dimension: Optional name of the time dimension column.
        smallest_time_grain: Optional smallest time grain for the time dimension.

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
            time_dimension='date',
            smallest_time_grain='TIME_GRAIN_DAY'
        )
    """

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
    time_dimension: Optional[str] = field(default=None)
    smallest_time_grain: Optional[TimeGrain] = field(default=None)

    def __attrs_post_init__(self):
        # Derive model name if not provided
        if self.name is None:
            try:
                nm = self.table.get_name()
            except Exception:
                nm = None
            object.__setattr__(self, "name", nm)
        # Validate smallest_time_grain
        if (
            self.smallest_time_grain is not None
            and self.smallest_time_grain not in TIME_GRAIN_TRANSFORMATIONS
        ):
            # Error message indicates invalid smallest_time_grain
            valid_grains = ", ".join(TIME_GRAIN_TRANSFORMATIONS.keys())
            raise ValueError(
                f"Invalid smallest_time_grain. Must be one of: {valid_grains}"
            )

    def build_query(self) -> "QueryExpr":
        """
        Create a new QueryExpr for this SemanticModel.

        Returns:
            QueryExpr: A new QueryExpr instance for building queries.
        """
        return QueryExpr(model=self)

    def _validate_time_grain(self, time_grain: Optional[TimeGrain]) -> None:
        """Validate that the requested time grain is not finer than the smallest allowed grain."""
        if time_grain is None or self.smallest_time_grain is None:
            return

        requested_idx = TIME_GRAIN_ORDER.index(time_grain)
        smallest_idx = TIME_GRAIN_ORDER.index(self.smallest_time_grain)

        if requested_idx < smallest_idx:
            raise ValueError(
                f"Requested time grain '{time_grain}' is finer than the smallest allowed grain '{self.smallest_time_grain}'"
            )

    def _transform_time_dimension(
        self, table: Expr, time_grain: Optional[TimeGrain]
    ) -> Tuple[Expr, Dict[str, Dimension]]:
        """Transform the time dimension based on the specified grain."""
        if not self.time_dimension or not time_grain:
            return table, self.dimensions.copy()

        # Create a copy of dimensions
        dimensions = self.dimensions.copy()

        # Get or create the time dimension function
        if self.time_dimension in dimensions:
            time_dim_func = dimensions[self.time_dimension]
        else:
            # Create a default time dimension function that accesses the column directly
            def time_dim_func(t: Expr) -> Expr:
                return getattr(t, self.time_dimension)

            dimensions[self.time_dimension] = time_dim_func

        # Create the transformed dimension function
        transform_func = TIME_GRAIN_TRANSFORMATIONS[time_grain]
        dimensions[self.time_dimension] = lambda t: transform_func(time_dim_func(t))

        return table, dimensions

    def query(
        self,
        dimensions: Optional[List[str]] = None,
        measures: Optional[List[str]] = None,
        filters: Optional[
            List[Union[Dict[str, Any], str, Callable[[Expr], Expr]]]
        ] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        time_range: Optional[Dict[str, str]] = None,
        time_grain: Optional[TimeGrain] = None,
    ) -> "QueryExpr":
        """
        Build a QueryExpr for this model with the specified query parameters.

        Args:
            dimensions: List of dimension names to include.
            measures: List of measure names to include.
            filters: List of filters (dict, str, callable, or Filter).
            order_by: List of (field, direction) tuples for ordering.
            limit: Maximum number of rows to return.
            time_range: Dict with 'start' and 'end' keys for time filtering.
            time_grain: The time grain to use for the time dimension.
        Returns:
            QueryExpr: The constructed QueryExpr.
        """
        # Validate time grain
        self._validate_time_grain(time_grain)
        # Prepare components, alias 'dimensions' to dimension names
        dimensions_list = list(dimensions) if dimensions else []
        measures_list = list(measures) if measures else []
        # Validate dimensions
        for d in dimensions_list:
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
            dimensions=tuple(dimensions_list),
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
        if not self.time_dimension:
            return {"error": "Model does not have a time dimension"}

        # Get the original time dimension function
        time_dim_func = self.dimensions[self.time_dimension]

        # Query the min and max dates
        time_range = self.table.aggregate(
            start=time_dim_func(self.table).min(), end=time_dim_func(self.table).max()
        ).execute()

        # Convert to ISO format if not None
        # Access the first (and only) row's values directly
        start_val = time_range["start"].iloc[0]
        end_val = time_range["end"].iloc[0]
        start_date = start_val.isoformat() if start_val is not None else None
        end_date = end_val.isoformat() if end_val is not None else None

        return {"start": start_date, "end": end_date}

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str,
        tables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, "SemanticModel"]:
        """
        Load all semantic models from YAML file using Ibis deferred expressions.

        Args:
            yaml_path: Path to YAML file
            tables: Dictionary containing tables and models referenced in YAML

        Returns:
            Dict[str, SemanticModel]: Dictionary mapping model names to SemanticModel instances

        Raises:
            FileNotFoundError: If YAML file doesn't exist
            KeyError: If referenced table or model not found
            ValueError: If YAML structure is invalid

        Examples:
            # Load all models from file
            models = SemanticModel.from_yaml("models.yml", tables={"flights_tbl": table})
            flights_model = models["flights"]
            carriers_model = models["carriers"]
        """
        import yaml

        if tables is None:
            tables = {}

        with open(yaml_path, "r") as f:
            yaml_configs = yaml.safe_load(f)

        models = {}

        # First pass: Create all models without joins
        for name, config in yaml_configs.items():
            if not isinstance(config, dict):
                continue

            # Get table from tables
            table_name = config.get("table")
            if not table_name:
                raise ValueError(f"Model '{name}' must specify 'table' field")

            if table_name not in tables:
                available = ", ".join(
                    sorted(k for k in tables.keys() if hasattr(tables[k], "execute"))
                )
                raise KeyError(
                    f"Table '{table_name}' not found in tables.\n"
                    f"Available tables: {available}"
                )
            table = tables[table_name]

            # Parse dimensions and measures
            dimensions = cls._parse_expressions(config.get("dimensions", {}))
            measures = cls._parse_expressions(config.get("measures", {}))

            # Create model without joins first
            models[name] = cls(
                name=name,
                table=table,
                dimensions=dimensions,
                measures=measures,
                joins={},
                primary_key=config.get("primary_key"),
                time_dimension=config.get("time_dimension"),
                smallest_time_grain=config.get("smallest_time_grain"),
            )

        # Second pass: Add joins now that all models exist
        for name, config in yaml_configs.items():
            if not isinstance(config, dict):
                continue

            if "joins" in config and config["joins"]:
                # Create combined tables with loaded models
                extended_tables = {**tables, **models}
                joins = cls._parse_joins(
                    config["joins"], extended_tables, yaml_configs, name
                )

                # Update the model with joins
                models[name] = evolve(models[name], joins=joins)

        return models

    @classmethod
    def _parse_expressions(cls, expressions: Dict[str, str]) -> Dict[str, Callable]:
        """Parse dimension or measure expressions."""
        result = {}
        for name, expr_str in expressions.items():
            deferred = eval(expr_str, {"_": ibis_mod._, "__builtins__": {}})
            result[name] = lambda t, d=deferred: d.resolve(t)
        return result

    @classmethod
    def _parse_joins(
        cls,
        joins_config: Dict[str, Dict[str, Any]],
        tables: Dict[str, Any],
        yaml_configs: Dict[str, Any],
        current_model_name: str,
    ) -> Dict[str, Join]:
        """Parse join configurations."""
        joins = {}

        for alias, join_config in joins_config.items():
            join_model_name = join_config.get("model")
            if not join_model_name:
                raise ValueError(f"Join '{alias}' must specify 'model' field")

            # Look for model in tables (includes both external models and loaded models)
            if join_model_name in tables:
                model = tables[join_model_name]
                if not isinstance(model, SemanticModel):
                    raise TypeError(
                        f"Join '{alias}' references '{join_model_name}' which is not a SemanticModel"
                    )
            else:
                # Check if it's a model defined in the same YAML file
                available_models = list(yaml_configs.keys()) + [
                    k for k in tables.keys() if isinstance(tables[k], SemanticModel)
                ]
                if join_model_name in yaml_configs:
                    raise ValueError(
                        f"Model '{join_model_name}' referenced in join '{alias}' is defined in the same YAML file "
                        f"but not yet loaded. Use SemanticModel.from_yaml() without model_name to load all models together."
                    )
                else:
                    raise KeyError(
                        f"Model '{join_model_name}' referenced in join '{alias}' not found.\n"
                        f"Available models: {', '.join(sorted(available_models))}"
                    )

            join_type = join_config.get("type", "one")

            if join_type in ["one", "many"]:
                with_expr_str = join_config.get("with")
                if not with_expr_str:
                    raise ValueError(
                        f"Join '{alias}' of type '{join_type}' must specify 'with' field"
                    )
                with_expr = eval(with_expr_str, {"_": ibis_mod._, "__builtins__": {}})

                def with_func(t, e=with_expr):
                    return e.resolve(t)

                if join_type == "one":
                    joins[alias] = Join.one(alias, model, with_func)
                else:
                    joins[alias] = Join.many(alias, model, with_func)
            elif join_type == "cross":
                joins[alias] = Join.cross(alias, model)
            else:
                raise ValueError(
                    f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'"
                )

        return joins

    @property
    def available_dimensions(self) -> List[str]:
        """
        List all available dimension keys, including joined model dimensions.

        Returns:
            List[str]: The available dimension names.
        """
        keys = list(self.dimensions.keys())
        # Include time dimension if it exists and is not already in dimensions
        if self.time_dimension and self.time_dimension not in keys:
            keys.append(self.time_dimension)
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{d}" for d in join.model.dimensions.keys()])
        return keys

    @property
    def available_measures(self) -> List[str]:
        """
        List all available measure keys, including joined model measures.

        Returns:
            List[str]: The available measure names.
        """
        keys = list(self.measures.keys())
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{m}" for m in join.model.measures.keys()])
        return keys

    @property
    def json_definition(self) -> Dict[str, Any]:
        """
        Return a JSON-serializable definition of the model, including name, dimensions, measures, time dimension, and time grain.

        Returns:
            Dict[str, Any]: The model metadata.
        """
        definition = {
            "name": self.name,
            "dimensions": self.available_dimensions,
            "measures": self.available_measures,
        }

        # Add time dimension info if present
        if self.time_dimension:
            definition["time_dimension"] = self.time_dimension

        # Add smallest time grain if present
        if self.smallest_time_grain:
            definition["smallest_time_grain"] = self.smallest_time_grain

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
        cutoff: Union[str, datetime.datetime, datetime.date, None] = None,
        dimensions: Optional[List[str]] = None,
        storage: Any = None,
    ) -> "SemanticModel":
        """
        Materialize the model at a specified time grain, optionally filtering by cutoff and restricting dimensions.

        Args:
            time_grain: The time grain to use for materialization.
            cutoff: Optional cutoff date/time for filtering.
            dimensions: Optional list of dimensions to include.
            storage: Optional storage backend for caching.
        Returns:
            SemanticModel: A new materialized SemanticModel.
        Raises:
            RuntimeError: If not using the xorq vendor ibis backend.
        """
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

        if cutoff is not None and self.time_dimension:
            if isinstance(cutoff, str):
                try:
                    cutoff_ts = datetime.datetime.fromisoformat(cutoff)
                except ValueError:
                    cutoff_ts = datetime.datetime.strptime(cutoff, "%Y-%m-%d")
            else:
                cutoff_ts = cutoff
            flat = flat.filter(getattr(flat, self.time_dimension) <= cutoff_ts)

        keys = dimensions if dimensions is not None else list(self.dimensions.keys())

        group_exprs: List[Expr] = []
        for key in keys:
            if key == self.time_dimension:
                col = flat[self.time_dimension]
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

        new_dimensions = {key: (lambda t, c=key: t[c]) for key in keys}
        new_measures: Dict[str, Measure] = {}
        for name in agg_kwargs:
            new_measures[name] = lambda t, c=name: t[c]
        for name, fn in self.measures.items():
            if name not in agg_kwargs:
                new_measures[name] = fn

        return SemanticModel(
            table=cube_table,
            dimensions=new_dimensions,
            measures=new_measures,
            joins={},
            name=f"{self.name}_cube_{time_grain.lower()}",
            time_dimension=self.time_dimension,
            smallest_time_grain=time_grain,
        )


# MCP functionality - only available if mcp package is installed
try:
    from mcp.server.fastmcp import FastMCP
    from typing import Annotated

    class MCPSemanticModel(FastMCP):
        """
        MCP server specialized for semantic models.

        Provides pre-defined tools for interacting with semantic models:
        - list_models: List all available semantic model names
        - get_model: Get model metadata and schema information
        - get_time_range: Get available time range for time-series data
        - query_model: Execute queries and optionally return visualizations

        Example:
            >>> from boring_semantic_layer import SemanticModel, MCPSemanticModel
            >>>
            >>> # Create semantic models
            >>> flights_sm = SemanticModel(
            ...     name="flights",
            ...     table=flights_table,
            ...     dimensions={...},
            ...     measures={...}
            ... )
            >>>
            >>> # Create MCP server
            >>> mcp_server = MCPSemanticModel(
            ...     models={"flights": flights_sm},
            ...     name="Flight Data Server"
            ... )
            >>>
            >>> # Run server
            >>> mcp_server.run()
        """

        def __init__(
            self,
            models: Dict[str, SemanticModel],
            name: str = "Semantic Layer MCP Server",
            *args,
            **kwargs,
        ):
            """
            Initialize MCP server with semantic models.

            Args:
                models: Dictionary mapping model names to SemanticModel instances
                name: Name of the MCP server
                *args, **kwargs: Additional arguments passed to FastMCP
            """
            super().__init__(name, *args, **kwargs)
            self.models = models
            self._register_tools()

        def _register_tools(self):
            """Register the standard semantic layer tools."""

            @self.tool()
            def list_models() -> List[str]:
                """List all available semantic model names."""
                return list(self.models.keys())

            @self.tool()
            def get_model(model_name: str) -> Dict[str, Any]:
                """Get details about a specific semantic model including available dimensions and measures."""
                if model_name not in self.models:
                    raise ValueError(f"Model {model_name} not found")
                return self.models[model_name].json_definition

            @self.tool()
            def get_time_range(model_name: str) -> Dict[str, Any]:
                """Get the available time range for a model's time dimension.

                Returns:
                    A dictionary with 'start' and 'end' dates in ISO format, or an error if the model has no time dimension
                """
                if model_name not in self.models:
                    raise ValueError(f"Model {model_name} not found")
                return self.models[model_name].get_time_range()

            @self.tool()
            def query_model(
                model_name: str,
                dimensions: Optional[List[str]] = [],
                measures: Optional[List[str]] = [],
                filters: Annotated[
                    Optional[Union[Dict, List[Dict]]],
                    """
                    List of JSON filter objects with the following structure:
                       
                    Simple Filter:
                    {
                        "field": "dimension_name",  # Must be an existing dimension (check model schema first!). Can include join references like "customer.country" or time dimensions like "order_date"
                        "operator": "=",            # See operator list below
                        "value": "single_value"     # For single-value operators (=, !=, >, >=, <, <=, ilike, not ilike, like, not like)
                        # OR for 'in'/'not in' operators only:
                        "values": ["val1", "val2"]  # REQUIRED for 'in' and 'not in' operators
                    }
                    
                    IMPORTANT OPERATOR GUIDELINES:
                    - Equality: Use "=" (preferred), "eq", or "equals" - all work identically
                    - Text matching: Use "ilike" (case-insensitive) instead of "like" for better results
                    - List membership: "in" requires "values" field (array), NOT "value"
                    - Negated list: "not in" requires "values" field (array), NOT "value"
                    - Pattern matching: "ilike" and "not ilike" support wildcards (%, _)
                    - Null checks: "is null" and "is not null" need no value/values field
                    
                    Available operators:
                    - "=" / "eq" / "equals": exact match (use "value")
                    - "!=": not equal (use "value") 
                    - ">", ">=", "<", "<=": comparisons (use "value")
                    - "in": value is in list (use "values" array)
                    - "not in": value not in list (use "values" array)
                    - "ilike": case-insensitive pattern match (use "value" with % wildcards)
                    - "not ilike": negated case-insensitive pattern (use "value" with % wildcards)  
                    - "like": case-sensitive pattern match (use "value" with % wildcards)
                    - "not like": negated case-sensitive pattern (use "value" with % wildcards)
                    - "is null": field is null (no value/values needed)
                    - "is not null": field is not null (no value/values needed)
                    
                    COMMON MISTAKES TO AVOID:
                    1. Don't use "value" with "in"/"not in" - use "values" array instead
                    2. Don't filter on measures - only filter on dimensions
                    3. Don't use .month(), .year() etc. - use time_grain parameter instead
                    4. For case-insensitive text search, prefer "ilike" over "like"
                       
                    Compound Filter (AND/OR):
                    {
                        "operator": "AND",          # or "OR"
                        "conditions": [             # Non-empty list of other filter objects
                            {
                                "field": "country",
                                "operator": "equals",   # or "=" or "eq" - all equivalent
                                "value": "US"
                            },
                            {
                                "field": "tier",
                                "operator": "in",       # MUST use "values" array for "in"
                                "values": ["gold", "platinum"]  # NOT "value"
                            },
                            {
                                "field": "name",
                                "operator": "ilike",    # Case-insensitive pattern matching
                                "value": "%john%"       # Wildcards supported
                            }
                        ]
                    }
                       
                    Example filters:
                    [
                        {"field": "status", "operator": "in", "values": ["active", "pending"]},
                        {"field": "name", "operator": "ilike", "value": "%smith%"},
                        {"field": "created_date", "operator": ">=", "value": "2024-01-01"},
                        {"field": "email", "operator": "not ilike", "value": "%spam%"}
                    ]
                    Example of a complex nested filter with time ranges:
                    [{
                        "operator": "AND",
                        "conditions": [
                            {
                                "operator": "AND",
                                "conditions": [
                                    {"field": "flight_date", "operator": ">=", "value": "2024-01-01"},
                                    {"field": "flight_date", "operator": "<", "value": "2024-04-01"}
                                ]
                            },
                            {"field": "carrier.country", "operator": "eq", "value": "US"}
                        ]
                    }]
                    """,
                ] = [],
                order_by: Annotated[
                    List[Tuple[str, str]],
                    "The order by clause to apply to the query (list of tuples: [('field', 'asc|desc')]",
                ] = [],
                limit: Annotated[int, "The limit to apply to the query"] = None,
                time_range: Annotated[
                    Optional[Dict[str, str]],
                    """Optional time range filter with format:
                        {
                            "start": "2024-01-01T00:00:00Z",  # ISO 8601 format
                            "end": "2024-12-31T23:59:59Z"     # ISO 8601 format
                        }
                        
                        Using time_range is preferred over using filters for time-based filtering because:
                        1. It automatically applies to the model's primary time dimension
                        2. It ensures proper time zone handling with ISO 8601 format
                        3. It's more concise than creating complex filter conditions
                        4. It works seamlessly with time_grain parameter for time-based aggregations
                    """,
                ] = None,
                time_grain: Annotated[
                    Optional[
                        Literal[
                            "TIME_GRAIN_YEAR",
                            "TIME_GRAIN_QUARTER",
                            "TIME_GRAIN_MONTH",
                            "TIME_GRAIN_WEEK",
                            "TIME_GRAIN_DAY",
                            "TIME_GRAIN_HOUR",
                            "TIME_GRAIN_MINUTE",
                            "TIME_GRAIN_SECOND",
                        ]
                    ],
                    """Time grain for aggregating time-based dimensions.
                    
                    IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
                    use this time_grain parameter to aggregate by time periods. The system will 
                    automatically handle time dimension transformations.
                    
                    Examples:
                    - For monthly data: time_grain="TIME_GRAIN_MONTH" 
                    - For yearly data: time_grain="TIME_GRAIN_YEAR"
                    - For daily data: time_grain="TIME_GRAIN_DAY"
                    
                    Then filter using the time_range parameter or regular date filters like:
                    {"field": "date_column", "operator": ">=", "value": "2024-01-01"}
                    """,
                ] = None,
                chart_spec: Annotated[
                    Optional[Union[bool, Dict[str, Any]]],
                    """Controls chart generation:
                    - None/False: Returns {"records": [...]} (default)
                    - True: Returns {"records": [...], "chart": {...}} with auto-detected chart
                    - Dict: Returns {"records": [...], "chart": {...}} with custom Vega-Lite specification
                      Can be partial (e.g., just {"mark": "line"} or {"encoding": {"y": {"scale": {"zero": False}}}}).
                      BSL intelligently merges partial specs with auto-detected defaults.
                    
                    Common chart specifications:
                    - {"mark": "bar"} - Bar chart
                    - {"mark": "line"} - Line chart  
                    - {"mark": "point"} - Scatter plot
                    - {"mark": "rect"} - Heatmap
                    - {"title": "My Chart"} - Add title
                    - {"width": 600, "height": 400} - Set size
                    - {"encoding": {"color": {"field": "category"}}} - Color by field
                    - {"encoding": {"y": {"scale": {"zero": False}}}} - Don't start Y-axis at zero
                    
                    BSL auto-detection logic:
                    - Time series (time dimension + measure) → Line chart
                    - Categorical (1 dimension + 1 measure) → Bar chart
                    - Multiple measures → Multi-series chart
                    - Two dimensions → Heatmap
                    """,
                ] = None,
                chart_format: Annotated[
                    Optional[Literal["altair", "interactive", "json", "png", "svg"]],
                    """Chart output format when chart_spec is provided:
                    - "altair": Altair Chart object (default)
                    - "interactive": Interactive Altair Chart with tooltips
                    - "json": Vega-Lite JSON specification
                    - "png": Base64-encoded PNG image {"format": "png", "data": "base64..."} (requires altair[all])
                    - "svg": SVG string {"format": "svg", "data": "svg..."} (requires altair[all])
                    """,
                ] = "json",
            ) -> Dict[str, Any]:
                """Query a semantic model with JSON-based filtering and optional visualization.

                Args:
                    model_name: The name of the model to query.
                    dimensions: The dimensions to group by. Can include time dimensions like "flight_date", "flight_month", "flight_year".
                    measures: The measures to aggregate.
                    filters: List of JSON filter objects (see detailed description above).
                    order_by: The order by clause to apply to the query (list of tuples: [("field", "asc|desc")]).
                    limit: The limit to apply to the query (integer).
                    time_range: Optional time range filter for time dimensions. Preferred over using filters for time-based filtering.
                    time_grain: Optional time grain for time-based dimensions (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
                    chart_spec: Controls chart generation - None/False for data, True for auto-detected chart, or Dict for custom spec.
                    chart_format: Output format when chart_spec is provided.

                Example queries:
                ```python
                # 1. Get data as records (default)
                query_model(
                    model_name="flights",
                    dimensions=["flight_month", "carrier"],
                    measures=["total_delay", "avg_delay"],
                    time_range={"start": "2024-01-01", "end": "2024-03-31"},
                    time_grain="TIME_GRAIN_MONTH",
                    order_by=[("avg_delay", "desc")],
                    limit=10
                )  # Returns {"records": [...]}

                # 2. Get data with auto-detected chart
                query_model(
                    model_name="flights",
                    dimensions=["date"],
                    measures=["flight_count"],
                    time_grain="TIME_GRAIN_WEEK",
                    chart_spec=True  # Returns {"records": [...], "chart": {...}}
                )

                # 3. Get data with custom chart styling
                query_model(
                    model_name="flights",
                    dimensions=["date", "carrier"],
                    measures=["on_time_rate"],
                    filters=[{"field": "carrier", "operator": "in", "values": ["AA", "UA", "DL"]}],
                    time_grain="TIME_GRAIN_MONTH",
                    chart_spec={
                        "encoding": {"y": {"scale": {"zero": False}}},
                        "title": "Carrier Performance Comparison"
                    },
                    chart_format="interactive"  # Chart format in the response
                )

                # 4. Get data with PNG chart image
                query_model(
                    model_name="flights",
                    dimensions=["carrier"],
                    measures=["flight_count"],
                    chart_spec={"mark": "bar", "title": "Flight Count by Carrier"},
                    chart_format="png"  # Chart will be {"format": "png", "data": "base64..."}
                )
                ```

                Raises:
                    ValueError: If any filter object doesn't match the required structure or model not found
                """
                if not isinstance(order_by, list):
                    raise ValueError("order_by must be a list of tuples")
                for item in order_by:
                    if not (isinstance(item, (list, tuple)) and len(item) == 2):
                        raise ValueError(
                            "Each order_by item must be a tuple of (field, direction)"
                        )
                    field, direction = item
                    if not isinstance(field, str) or direction not in ("asc", "desc"):
                        raise ValueError(
                            "Each order_by tuple must be (field: str, direction: 'asc' or 'desc')"
                        )

                if model_name not in self.models:
                    raise ValueError(f"Model {model_name} not found")

                model = self.models[model_name]

                # Validate time grain if provided
                if time_grain and model.smallest_time_grain:
                    grain_order = [
                        "TIME_GRAIN_SECOND",
                        "TIME_GRAIN_MINUTE",
                        "TIME_GRAIN_HOUR",
                        "TIME_GRAIN_DAY",
                        "TIME_GRAIN_WEEK",
                        "TIME_GRAIN_MONTH",
                        "TIME_GRAIN_QUARTER",
                        "TIME_GRAIN_YEAR",
                    ]
                    if grain_order.index(time_grain) < grain_order.index(
                        model.smallest_time_grain
                    ):
                        raise ValueError(
                            f"Time grain {time_grain} is smaller than model's smallest allowed grain {model.smallest_time_grain}"
                        )

                query = model.query(
                    dimensions=dimensions,
                    measures=measures,
                    filters=filters,
                    order_by=order_by,
                    limit=limit,
                    time_range=time_range,
                    time_grain=time_grain,
                )

                output = {
                    "records": query.execute().to_dict(orient="records"),
                }
                # Check if chart is requested
                if chart_spec is not None:
                    # Handle boolean True for auto-detection
                    spec = None if chart_spec is True else chart_spec

                    # Generate the chart
                    chart = query.chart(spec=spec, format=chart_format)

                    output["chart"] = chart

                return output


except ImportError:
    # MCP not available, this is fine
    pass
