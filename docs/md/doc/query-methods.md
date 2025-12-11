# Query Methods

## Overview

BSL provides a simple and consistent query API for retrieving data from your semantic tables. Queries are built by chaining methods, then executed or inspected using output methods.

Start with a semantic table and chain methods together. Here's the typical query flow:

```setup_table
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table

# Create Ibis table
flights_tbl = ibis.memtable({
    "origin": ["NYC", "LAX", "NYC", "SFO", "LAX", "NYC", "SFO", "LAX", "NYC"],
    "carrier": ["AA", "UA", "AA", "UA", "AA", "UA", "AA", "UA", "AA"],
    "distance": [2789, 2789, 2902, 2902, 347, 2789, 347, 347, 2789],
    "duration": [330, 330, 360, 360, 65, 330, 65, 65, 330],
})

# Create semantic table
flights_st = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        origin=lambda t: t.origin,
        carrier=lambda t: t.carrier,
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_duration=lambda t: t.duration.mean(),
    )
)
```
<collapsedcodeblock code-block="setup_table" title="Setup: Create Ibis Table and Semantic Table"></collapsedcodeblock>


```python
result = (
    flights_st                                   # Start with semantic table
    .filter(_.distance > 1000)                   # 1. Filter (optional)
    .group_by("origin")                          # 2. Group by dimensions
    .aggregate("flight_count", "total_distance") # 3. Aggregate measures
    .mutate(avg=lambda t: t.total_distance / t.flight_count)  # 4. Transform (optional)
    .order_by(ibis.desc("flight_count"))         # 5. Sort (optional)
    .limit(10)                                   # 6. Limit rows (optional)
)
```

Once you've built a query, you can inspect it or execute it:

```simple_demo

# Build a query
result = flights_st.group_by("origin").aggregate("flight_count")

# Option 1: Execute and get data as pandas DataFrame
df = result.execute()

# Option 2: View the generated SQL
print(result.sql())

# Option 3: Generate a visualization (when applicable)
chart = result.chart()

# Option 4: See the semantic query plan
print(result)

result
```
<bslquery code-block="simple_demo"></bslquery>

The output above includes a **Query Plan** tab showing how BSL translates this query into semantic operations. 

You print the query object directly to see the plan:
```python
print(result)
```

This displays operations like `SemanticTableOp`, `SemanticGroupByOp`, and `SemanticAggregateOp`, useful for debugging and understanding query execution.

Let's get now into the details of each query method.

## group_by()

The `group_by()` method groups data by one or more dimensions.

<note type="info">
`group_by()` only accepts string dimension names that were previously defined in `with_dimensions()`. It does not support lambda functions or unbound `_` syntax.
</note>

### Single Dimension

Group by a single dimension:

```query_single_dimension
# Group by one dimension
result = flights_st.group_by("origin").aggregate("flight_count")
```

<bslquery code-block="query_single_dimension"></bslquery>

### Multiple Dimensions

Group by multiple dimensions to create detailed breakdowns:

```query_multiple_dimensions
# Group by multiple dimensions
result = flights_st.group_by("origin", "carrier").aggregate("flight_count")
```

<bslquery code-block="query_multiple_dimensions"></bslquery>

### No Grouping

Calculate overall statistics across all rows using `group_by()` with no arguments:

```query_no_grouping
# Aggregate entire dataset without grouping
result = flights_st.group_by().aggregate("flight_count", "total_distance", "avg_duration")
```

<bslquery code-block="query_no_grouping"></bslquery>

## aggregate()

The `aggregate()` method calculates measures after grouping. You can reference pre-defined measures or compute new ones on-the-fly.

<note type="warning">
**CRITICAL**: `aggregate()` takes **measure names as strings**, not expressions or lambdas directly. Use measure names from `get_model()` output.
```python
# ✅ CORRECT - measure names as strings
model.group_by("category").aggregate("flight_count", "total_revenue")

# ❌ WRONG - no standalone lambdas in aggregate
model.aggregate(total=lambda t: t.sum())  # ERROR!
```
</note>

### Pre-defined Measures

Reference measures by their string names:

```query_predefined_measures
# Use measures defined in with_measures()
result = flights_st.group_by("origin").aggregate("flight_count", "avg_duration")
```

<bslquery code-block="query_predefined_measures"></bslquery>

### On-the-Fly Transformations

Add computed measures directly in `aggregate()` without modifying the semantic table:

```query_onthefly_measures
# Mix predefined and computed measures
result = (
    flights_st
    .group_by("origin")
    .aggregate(
        "flight_count",              # Pre-defined measure
        "avg_duration",               # Pre-defined measure
        total_miles=lambda t: t.distance.sum(),  # Computed on-the-fly
        max_distance=lambda t: t.flight_count + 2  # You can reference other measures as well
    )
)
```

<bslquery code-block="query_onthefly_measures"></bslquery>

<note type="info">
On-the-fly measures let you add context-specific calculations without modifying your semantic table definition. This keeps your base model clean while enabling flexible queries.
</note>

### Referencing Table Columns

You can reference **any column from the underlying table** in `aggregate()`, not just pre-defined measures. This is useful when you need one-off calculations without cluttering your semantic table definition.

```query_table_columns
# Reference table columns directly in aggregate()
result = (
    flights_st
    .group_by("origin")
    .aggregate(
        "flight_count",                           # Pre-defined measure
        total_distance=lambda t: t.distance.sum(),  # Table column 'distance'
        avg_duration=lambda t: t.duration.mean(),   # Table column 'duration'
        distance_in_km=lambda t: (t.distance * 1.60934).sum()  # Transform then aggregate
    )
)
```

<bslquery code-block="query_table_columns"></bslquery>

**Key points:**
- Table columns **must be aggregated** (e.g., `.sum()`, `.mean()`, `.max()`, `.count()`)
- You can transform columns before aggregating (e.g., `(t.distance * 1.60934).sum()`)
- This works for any column in the underlying table, even if not defined as a dimension or measure
- Use this for ad-hoc calculations without modifying your semantic table

<note type="warning">
Table columns cannot be used without an aggregation function. For example, `lambda t: t.distance` will fail. You must use `lambda t: t.distance.sum()` or another aggregation.
</note>

## filter() / order_by() / limit() 

Combine `filter()`, `order_by()`, and `limit()` to refine your query results.

```query_filter_order_limit
from ibis import _

# Filter data, sort, and limit results
result = (
    flights_st
    .filter(lambda t: t.origin.isin(["NYC", "LAX"]))  # Filter origins
    .filter(_.distance > 500)                          # Filter distance using _ syntax
    .group_by("origin")
    .aggregate("flight_count", "avg_duration")        # Aggregate both measures
    .order_by(ibis.desc("flight_count"))              # Sort by flight_count descending
    .limit(5)                                          # Top 5 results
)
```

<bslquery code-block="query_filter_order_limit"></bslquery>

**Key points:**
- **`filter()`**: Use lambda or `_` syntax to apply conditions before aggregation
- **`order_by()`**: Use `ibis.desc()` for descending order, or column name for ascending
- **`limit()`**: Restrict the number of rows returned

### Critical Filter Patterns

**Multiple conditions** - use `ibis.and_()` or `ibis.or_()`:

```python
# Multiple conditions with AND
model.filter(lambda t: ibis.and_(t.amount > 1000, t.year >= 2023))

# Multiple conditions with OR
model.filter(lambda t: ibis.or_(t.status == "active", t.status == "pending"))
```

**IN operator** - MUST use `.isin()` method:

```python
# ✅ CORRECT - use .isin() method
model.filter(lambda t: t.region.isin(["US", "EU", "APAC"]))

# ❌ WRONG - Python's 'in' does NOT work!
model.filter(lambda t: t.region in ["US", "EU"])  # ERROR: truth value of Ibis expression is not defined
```

**Lambda column names** - use column names directly, never prefix with model name:

```python
# ✅ CORRECT - use column name directly
model.filter(lambda t: t.carrier == "AA")

# ❌ WRONG - do NOT prefix with model name
model.filter(lambda t: t.model.carrier == "AA")  # ERROR!
```

**Joined columns** - use exact prefixed name from `get_model()`:

```python
# If get_model() shows "customers.country", use it exactly:
model.filter(lambda t: t.customers.country == "US")

# ❌ WRONG - don't call methods on ID columns
model.filter(lambda t: t.customer_id.country())  # ERROR: no such method!
```

## nest()

The `nest` parameter in `aggregate()` creates nested data structures (arrays of structs) in your query results. This is useful for API responses, hierarchical visualizations, and preserving relationships in aggregated data.

Use `nest` to collect rows as structured arrays within each group:

```query_basic_nest
from ibis import _

# Nest flight details within each origin
result = (
    flights_st
    .group_by("origin")
    .aggregate(
        "flight_count",
        "total_distance",
        # Create nested array of flight details
        nest={"flights": lambda t: t.group_by(["carrier", "distance"])}
    )
)
```

<bslquery code-block="query_basic_nest"></bslquery>

**How it works:**
- The `nest` parameter accepts a dictionary: `{"column_name": lambda t: ...}`
- The lambda specifies which columns to collect using `.group_by()` or `.select()`
- Results in an array of structs column named `flights`

You can also use `.select()` to specify which columns to nest:

```query_nest_select
# Nest specific columns
result = (
    flights_st
    .group_by("carrier")
    .aggregate(
        "flight_count",
        nest={"routes": lambda t: t.select("origin", "distance", "duration")}
    )
)
```

<bslquery code-block="query_nest_select"></bslquery>

After nesting, you can re-group which automatically unnests, then access the nested fields.

**Step 1: Create nested data**

First, create the nested structure. Notice the `flights` column contains arrays of structs:

```query_nest_step1
from ibis import _

# Create nested data structure
result = (
    flights_st
    .group_by("origin")
    .aggregate(
        "flight_count",
        nest={"flights": lambda t: t.group_by(["carrier", "distance"])}
    )
)
```

<bslquery code-block="query_nest_step1"></bslquery>

**Step 2: Re-group to unnest and access fields**

Now re-group on the same dimension, which automatically unnests the array, allowing you to access the nested fields:

```query_nest_step2
from ibis import _

# Re-grouping automatically unnests the 'flights' array
result = (
    result
    .group_by("origin")
    .aggregate(
        total_flights=lambda t: t.flight_count.sum(),
        # Access unnested fields from the flights array
        unique_carriers=lambda t: t.flights.carrier.nunique(),
        avg_distance=lambda t: t.flights.distance.mean()
    )
)
```

<bslquery code-block="query_nest_step2"></bslquery>

**Use cases for nesting:**
- **API responses**: Create JSON-compatible hierarchical structures
- **Hierarchical data**: Preserve parent-child relationships in results
- **Data export**: Generate nested documents for external systems
- **Drill-down analysis**: Keep detailed records available in aggregated views

<note type="info">
For more complex nesting patterns and multi-level hierarchies, see [Nested Subtotals](/advanced/nested-subtotals).
</note>

## mutate()

The `mutate()` method transforms aggregated results by adding new computed columns. This is different from on-the-fly measures in `aggregate()` — `mutate()` works on already-aggregated data.

<note type="warning">
**Key difference:** `.aggregate()` computes from raw data, while `.mutate()` transforms already-aggregated results.
</note>

```query_mutate
from ibis import _

# Add post-aggregation calculations
result = (
    flights_st
    .group_by("origin")
    .aggregate("flight_count", "total_distance")
    .mutate(
        avg_distance_per_flight=lambda t: t.total_distance / t.flight_count,
        flight_category=lambda t: ibis.cases(
            (t.flight_count >= 3, "high"),
            (t.flight_count >= 2, "medium"),
            else_="low"
        )
    )
)
```

<bslquery code-block="query_mutate"></bslquery>

**Use cases for `mutate()`:**
- Calculate ratios from aggregated measures (e.g., `total / count`)
- Create categories based on aggregated values
- Add labels or formatting to results
- Transform aggregated columns using the full power of Ibis

For more transformations, see [Ibis Table API reference](https://ibis-project.org/reference/expression-tables.html#ibis.expr.types.relations.Table.mutate).

## Window Functions with .over()

Window functions perform calculations across ordered rows, enabling operations like running totals, moving averages, and ranking. Unlike regular aggregations that reduce many rows to one, window functions preserve row count while adding computed values.

<note type="warning">
**Important:** Window functions can only be applied **after aggregation**, typically within a `.mutate()` call. They cannot be defined directly in measures.
</note>

**Common window functions:**
- **`lag()` / `lead()`**: Access previous/next row values for period-over-period comparisons
- **`cumsum()`**: Calculate running totals
- **`.over(window)`**: Apply functions over sliding windows (e.g., moving averages)
- **`rank()` / `row_number()`**: Assign ranks or sequential numbers to rows

Here's a simple example:

```query_window_example
from ibis import _

# First aggregate to daily level
daily_flights = (
    flights_st
    .group_by("origin")
    .aggregate("flight_count", "total_distance")
    .order_by("origin")
)

# Then apply window function for cumulative distance
window_spec = ibis.window(order_by="origin")

result = daily_flights.mutate(
    cumulative_distance=_.total_distance.cumsum(),
    flight_rank=lambda t: ibis.rank().over(ibis.window(order_by=_.flight_count.desc()))
).limit(10)
```

<bslquery code-block="query_window_example"></bslquery>

**Key points:**
- Window functions are applied **after** `.aggregate()` using `.mutate()`
- Use `.order_by()` to establish row order for window operations
- Combine with `ibis.window()` for advanced sliding window calculations

For comprehensive examples including lag/lead, moving averages, and ranking, see [Window Functions](/advanced/windowing).

## as_table()

After filtering or aggregating data, you may want to perform additional semantic operations. However, intermediate results don't always preserve the semantic table's dimensions and measures.

The Problem: Lost Semantic Information

When you aggregate data, the result loses semantic metadata. The aggregated result is a `SemanticAggregate` expression, which doesn't have `.dimensions` or `.measures` attributes:

```query_as_table_problem
from ibis import _

# Aggregate the data - this returns a SemanticAggregate
agg_result = flights_st.group_by("origin").aggregate("flight_count", "total_distance")

# Show the type/class of the result
result_type = type(agg_result).__name__

# Try to access .dimensions - this will raise an AttributeError
try:
    dimensions = agg_result.dimensions
    result = f"Type: {result_type}\nDimensions: {dimensions}"
except AttributeError as e:
    result = f"Type: {result_type}\nError: {str(e)}"

result
```

<regularoutput code-block="query_as_table_problem"></regularoutput>


After aggregation, you can no longer access the original semantic table's dimensions and measures metadata.

The Solution: Use as_table()

The `as_table()` method converts results back into a `SemanticModel`. However, note that for aggregations, the metadata is intentionally cleared (since columns are now materialized):

```query_as_table_after_aggregate
from ibis import _

# Aggregate the data
agg_result = flights_st.group_by("origin").aggregate("flight_count", "total_distance")

# Convert to SemanticModel using as_table()
agg_table = agg_result.as_table()

# Now .dimensions and .measures attributes exist, but they're empty (metadata was cleared)
result = f"Type: {type(agg_table).__name__}\nDimensions: {agg_table.dimensions}\nMeasures: {agg_table.measures}"
```

<regularoutput code-block="query_as_table_after_aggregate"></regularoutput>

When are metadata preserved ?

For operations like `filter()`, `order_by()`, and `limit()`, `as_table()` **preserves** the original semantic metadata:

```query_as_table_filter_preserved
from ibis import _

# Filter the data
filtered = flights_st.filter(_.distance > 2000)

# Convert back to SemanticModel - metadata is preserved!
filtered_table = filtered.as_table()

# Dimensions and measures are still available (preserved from original semantic table)
result = f"Type: {type(filtered_table).__name__}\nDimensions: {filtered_table.dimensions}\nMeasures: {filtered_table.measures}"
```

<regularoutput code-block="query_as_table_filter_preserved"></regularoutput>

Notice how the dimensions and measures are preserved, unlike the aggregation case above where they were empty.

**Key points:**
- **Operations that preserve metadata**: `filter()`, `order_by()`, `limit()`, `unnest()` — calling `as_table()` restores full semantic capabilities with original dimensions/measures
- **Operations that clear metadata**: `aggregate()`, `mutate()` — calling `as_table()` returns a `SemanticModel` with empty dimensions/measures (columns are materialized)
- Use `as_table()` when you need to continue semantic operations on intermediate results

## Next Steps

- Learn about [Building Semantic Tables](/building/semantic-tables) to define dimensions and measures
- Explore [Composing Models](/building/compose) for multi-table queries
- Try [Advanced Patterns](/advanced/percentage-total) for complex analytics
