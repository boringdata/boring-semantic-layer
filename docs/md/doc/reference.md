# API Reference

Complete API documentation for the Boring Semantic Layer.

## Table Creation & Configuration

Methods for creating and configuring semantic tables.

### to_semantic_table()

```python
to_semantic_table(
    table: ibis.Table,
    name: str,
    description: str = None
) -> SemanticTable
```

Create a semantic table from an Ibis table. This is the primary entry point for building semantic models.

| Parameter | Type | Description |
|-----------|------|-------------|
| `table` | `ibis.Table` | Ibis table to build the model from |
| `name` | `str` | Unique identifier for the semantic table |
| `description` | `str` | Optional description of the semantic table |

**Example:**
```python
import ibis
from boring_semantic_layer import to_semantic_table

flights = ibis.read_parquet("flights.parquet")
flights_st = to_semantic_table(flights, "flights")
```

### with_dimensions()

```python
with_dimensions(
    **dimensions: Callable | Dimension
) -> SemanticTable
```

Define dimensions for grouping and analysis. Dimensions are attributes that categorize data.

**Example:**
```python
flights_st = flights_st.with_dimensions(
    origin=lambda t: t.origin,
    dest=lambda t: t.dest,
    carrier=lambda t: t.carrier
)
```

### with_measures()

```python
with_measures(
    **measures: Callable | Measure
) -> SemanticTable
```

Define aggregations and calculations. Measures are numeric values that can be aggregated.

**Example:**
```python
flights_st = flights_st.with_measures(
    flight_count=lambda t: t.count(),
    avg_delay=lambda t: t.arr_delay.mean(),
    total_distance=lambda t: t.distance.sum()
)
```

### from_yaml()

```python
from_yaml(
    yaml_path: str,
    connection: ibis.Connection = None
) -> dict[str, SemanticTable]
```

Load semantic models from a YAML configuration file. Returns a dictionary of semantic tables.

| Parameter | Type | Description |
|-----------|------|-------------|
| `yaml_path` | `str` | Path to YAML configuration file |
| `connection` | `ibis.Connection` | Optional Ibis connection for database tables |

**Example:**
```python
from boring_semantic_layer.yaml import from_yaml

models = from_yaml("models.yaml")
flights_st = models["flights"]
```

### Dimension Class

```python
Dimension(
    expr: Callable,
    description: str = None
)
```

Self-documenting dimension with description. Use for better API documentation.

**Example:**
```python
from boring_semantic_layer import Dimension

flights_st = flights_st.with_dimensions(
    origin=Dimension(
        expr=lambda t: t.origin,
        description="Airport code where the flight departed from"
    )
)
```

### Measure Class

```python
Measure(
    expr: Callable,
    description: str = None
)
```

Self-documenting measure with description. Use for better API documentation.

**Example:**
```python
from boring_semantic_layer import Measure

flights_st = flights_st.with_measures(
    avg_delay=Measure(
        expr=lambda t: t.arr_delay.mean(),
        description="Average arrival delay in minutes"
    )
)
```

### all()

```python
st.all()
```

Reference the entire dataset within measure definitions. Primarily used for percentage-of-total calculations.

**Example:**
```python
flights_st = to_semantic_table(data, "flights").with_measures(
    flight_count=lambda t: t.count(),
    pct_of_total=lambda t: (
        t.count() / t.all().count() * 100
    )
)
```

## Join Methods

Methods for composing semantic tables through joins.

### join_many()

```python
join_many(
    other: SemanticTable,
    on: Callable,
    name: str = None
) -> SemanticTable
```

One-to-many relationship join (LEFT JOIN). Use when the left table can match multiple rows in the right table.

| Parameter | Type | Description |
|-----------|------|-------------|
| `other` | `SemanticTable` | The semantic table to join with |
| `on` | `Callable` | Lambda function defining the join condition |
| `name` | `str` | Optional name for the joined table reference |

**Example:**
```python
flights_st = flights_st.join_many(
    carriers_st,
    on=lambda l, r: l.carrier == r.code,
    name="carrier_info"
)
```

### join_one()

```python
join_one(
    other: SemanticTable,
    on: Callable,
    name: str = None
) -> SemanticTable
```

One-to-one relationship join (INNER JOIN). Use when each row in the left table matches exactly one row in the right table.

**Example:**
```python
flights_st = flights_st.join_one(
    airports_st,
    on=lambda l, r: l.origin == r.code
)
```

### join_cross()

```python
join_cross(
    other: SemanticTable,
    name: str = None
) -> SemanticTable
```

Cross join (CARTESIAN PRODUCT). Creates all possible combinations of rows from both tables.

### join()

```python
join(
    other: SemanticTable,
    on: Callable,
    how: str = "inner",
    name: str = None
) -> SemanticTable
```

Custom join with flexible join type. Supports 'inner', 'left', 'right', 'outer', and 'cross'.

| Parameter | Type | Description |
|-----------|------|-------------|
| `other` | `SemanticTable` | The semantic table to join with |
| `on` | `Callable` | Lambda function defining the join condition |
| `how` | `str` | Join type: 'inner', 'left', 'right', 'outer', or 'cross' |
| `name` | `str` | Optional name for the joined table reference |

## Query Methods

Methods for querying and transforming semantic tables.

### group_by()

```python
group_by(
    *dimensions: str
) -> QueryBuilder
```

Group data by one or more dimension names. Returns a query builder for chaining with aggregate().

**Example:**
```python
result = flights_st.group_by("origin", "carrier").aggregate("flight_count")
```

### aggregate()

```python
aggregate(
    *measures: str,
    **kwargs
) -> ibis.Table
```

Calculate one or more measures. Can be used standalone or after group_by().

**Examples:**
```python
# Without grouping
total = flights_st.aggregate("flight_count")

# With grouping
by_origin = flights_st.group_by("origin").aggregate("flight_count", "avg_delay")
```

### filter()

```python
filter(
    condition: Callable
) -> SemanticTable
```

Apply conditions to filter data. Use lambda functions with Ibis expressions.

**Example:**
```python
delayed_flights = flights_st.filter(lambda t: t.arr_delay > 0)
```

### order_by()

```python
order_by(
    *columns: str | ibis.Expression
) -> ibis.Table
```

Sort query results. Use `ibis.desc()` for descending order.

**Example:**
```python
result = flights_st.group_by("origin").aggregate("flight_count")
result = result.order_by(ibis.desc("flight_count"))
```

### limit()

```python
limit(
    n: int
) -> ibis.Table
```

Restrict the number of rows returned.

**Example:**
```python
top_10 = result.order_by(ibis.desc("flight_count")).limit(10)
```

### mutate()

```python
mutate(
    **expressions: Callable | ibis.Expression
) -> ibis.Table
```

Add or transform columns in aggregated results. Useful for calculations after aggregation.

**Example:**
```python
result = flights_st.group_by("month").aggregate("revenue")
result = result.mutate(
    growth_rate=lambda t: (t.revenue - t.revenue.lag()) / t.revenue.lag() * 100
)
```

### select()

```python
select(
    *columns: str | ibis.Expression
) -> ibis.Table
```

Select specific columns from the result. Often used in nesting operations.

**Example:**
```python
result.select("origin", "flight_count")
```

## Nesting

Create nested data structures within aggregations.

### nest Parameter

```python
aggregate(
    *measures,
    nest={
        "nested_column": lambda t: t.group_by([...]) | t.select(...)
    }
)
```

Create nested arrays of structs within aggregation results. Useful for hierarchical data or subtotals.

**Example:**
```python
result = flights_st.group_by("carrier").aggregate(
    "total_flights",
    nest={
        "by_month": lambda t: t.group_by("month").aggregate("monthly_flights")
    }
)
```

## Charting

Generate visualizations from query results.

### chart()

```python
chart(
    result: ibis.Table,
    backend: str = "altair",
    spec: dict = None,
    format: str = "interactive"
) -> Chart
```

Create visualizations from query results. Supports Altair (default) and Plotly backends.

| Parameter | Type | Description |
|-----------|------|-------------|
| `result` | `ibis.Table` | Query result table to visualize |
| `backend` | `str` | "altair" or "plotly" |
| `spec` | `dict` | Custom Vega-Lite specification (for Altair) |
| `format` | `str` | "interactive", "json", "png", "svg" |

**Auto-detection:**
BSL automatically selects appropriate chart types:
- Single dimension + measure → Bar chart
- Time dimension + measure → Line chart
- Two dimensions + measure → Heatmap

**Example:**
```python
from boring_semantic_layer.chart import chart

result = flights_st.group_by("month").aggregate("flight_count")
chart(result, backend="altair")
```

## Dimensional Indexing

Create searchable catalogs of dimension values.

### index()

```python
index(
    dimensions: Callable | None = None,
    by: str = None,
    sample: int = None
) -> ibis.Table
```

Create a searchable catalog of unique dimension values with optional weighting and sampling.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dimensions` | `Callable` | None (all dimensions) or lambda returning list of fields |
| `by` | `str` | Measure name for weighting results |
| `sample` | `int` | Number of rows to sample (for large datasets) |

**Examples:**
```python
# Index all dimensions
flights_st.index()

# Index specific dimensions
flights_st.index(lambda t: [t.origin, t.dest])

# Weight by measure
flights_st.index(by="flight_count")

# Sample large dataset
flights_st.index(sample=10000)
```

## Other

### MCP Integration

#### MCPSemanticModel()

```python
MCPSemanticModel(
    models: dict[str, SemanticTable] | str,
    description: str = None
)
```

Create an MCP server to expose semantic models to LLMs like Claude. Accepts either a dictionary of models or a path to a YAML configuration file.

| Parameter | Type | Description |
|-----------|------|-------------|
| `models` | `dict` or `str` | Dictionary of SemanticTable objects or path to YAML config |
| `description` | `str` | Optional description of the semantic model |

**Available MCP Tools:**

| Tool | Description |
|------|-------------|
| `list_models()` | List all available semantic model names |
| `get_model()` | Get detailed model information (dimensions, measures, joins) |
| `get_time_range()` | Get available time range for time-series data |
| `query_model()` | Execute queries against semantic models |

**Example:**
```python
from boring_semantic_layer import MCPSemanticModel

# From dictionary
server = MCPSemanticModel(
    models={"flights": flights_st, "airports": airports_st},
    description="Flight data analysis"
)

# From YAML
server = MCPSemanticModel("config.yaml")
```

### YAML Configuration

#### YAML Structure

```yaml
model_name:
  table: table_reference
  description: "Optional description"

  dimensions:
    dimension_name: expression
    # or with description
    dimension_name:
      expr: expression
      description: "Dimension description"

  measures:
    measure_name: expression
    # or with description
    measure_name:
      expr: expression
      description: "Measure description"

  joins:
    join_name:
      model: model_reference
      on: join_condition
      how: join_type  # left, inner, right, outer, cross
```

#### Expression Syntax

| Expression | Description |
|------------|-------------|
| `_` | Reference to the table |
| `_.column` | Reference a column |
| `_.count()` | Count aggregation |
| `_.column.sum()` | Sum aggregation |
| `_.column.mean()` | Average aggregation |
| `_.column.min()` | Minimum value |
| `_.column.max()` | Maximum value |

**Example:**
```yaml
flights:
  table: flights_data
  description: "Flight operations data"

  dimensions:
    origin: _.origin
    dest: _.dest
    carrier:
      expr: _.carrier
      description: "Airline carrier code"

  measures:
    flight_count: _.count()
    avg_delay:
      expr: _.arr_delay.mean()
      description: "Average arrival delay in minutes"
```

## Next Steps

- Learn about [Semantic Tables](/building/semantic-tables)
- Explore [Query Methods](/querying/methods)
- See [Advanced Patterns](/advanced/percentage-total)
