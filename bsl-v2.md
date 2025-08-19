Thank you for your feedback and testing of BSL.

Here are the main issues we received:
- Complex computations: percentage of total, window functions, etc (#13)
- Remove cube name from queries (#21) - queries should work on the whole semantic model
- Filter on measures (#31)
- Multiple joins to the same table (#32)
- Joins should work with any field, not just primary keys


Hussain and I analyzed these issues and reached the same conclusion: we need better integration with Ibis to use advanced features like window functions, especially for complex computations and measure filtering.

We also studied Malloy, which has already solved these problems. We've decided to use it as a reference for BSL v2.

We want to refactor as follows:

Instead of a standalone Semantic Model class, we'll create a new Semantic Table class that directly emits an [Ibis table expression](https://ibis-project.org/reference/expression-tables).

This class will inherit all Ibis expression features: [group_by](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.group_by), [aggregate](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.aggregate), [mutate](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.mutate), [filter](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.filter), etc.

and add the metadata needed to perform queries:
- what columns are dimensions
- what columns are measures
- what joins are defined

That way we can get a clean computation graph and fully leverage Ibis features.

Current Implementation Semantic Model Definition:
```
from boring_semantic_layer import SemanticModel
flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={"origin": lambda t: t.origin},
    measures={"flight_count": lambda t: t.count()}
)
```

New Implementation Semantic Table Definition:
```
from boring_semantic_layer import to_semantic_table
flights_st = to_semantic_table(flights_tbl)
    .with_dimensions(
        origin=lambda t: t.origin,
    )
    .with_measures(
        flight_count=lambda t: t.count()
    )
```

Previous way to query:
```
flight_sm.query(
    dimensions=["origin"],
    measures=["flight_count"],
)
```
New way to query:
```
flight_st.group_by('origin').aggregate(flight_count).select(['origin', 'flight_count'])
```

Here is a short demo of the new [interface](https://asciinema.org/a/734396):
https://asciinema.org/a/734396


This offers these advantages:
- **Composability**: Semantic tables work like regular Ibis table expressions and compose naturally
- **Advanced query capabilities**: The system detects measure dependencies and automatically splits calculations between pre-aggregation and post-aggregation steps for maximum flexibility

The system detects when measures reference other measures:

```python
flights_st = to_semantic_table(flights_tbl)
    .with_dimensions(
        origin=lambda t: t.origin,
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_dep_delay=lambda t: t.dep_delay.mean(),
        delay_efficiency=lambda t: t.flight_count / (t.avg_dep_delay + 1)
    )

efficiency_analysis = (
    flights_st
    .group_by('origin')
    .aggregate(
        flight_count=flights_st.flight_count,      # Regular measure
        delay_efficiency=flights_st.delay_efficiency  # Post-agg measure
    )
)

# System rewrites to:
# flights.group_by('origin').aggregate(
#     flight_count=lambda t: t.count(),
#     avg_dep_delay=lambda t: t.dep_delay.mean()  # Auto-added dependency
# ).mutate(
#     delay_efficiency=lambda t: t.flight_count / (t.avg_dep_delay + 1)
# ).select(['origin', 'flight_count', 'delay_efficiency'])
```

## Advanced Query Examples

These examples show queries that are now possible. The logic can be used directly in queries or encapsulated within measure definitions.

### Window Functions & Percentages
```python
# Market share
carrier_market_share = (
    flights_st
    .group_by('carrier')
    .aggregate(flight_count=flights_st.flight_count)
    .mutate(
        market_share=lambda t: t.flight_count / t.flight_count.sum()
    )
)

# Rolling averages
rolling_window = ibis.window(order_by='month', rows=(2, 0))
monthly_trends = (
    flights_st
    .group_by(flights_st.arr_time.truncate('month').name('month'), 'carrier')
    .aggregate(monthly_flights=flights_st.flight_count)
    .mutate(
        rolling_avg=lambda t: t.monthly_flights.mean().over(rolling_window)
    )
)
```

## Removing Cube Names from Queries

We're reconsidering whether to remove cube names from queries. Since semantic tables can be linked through join relations, we could expose a single unified semantic table to users.

Does this approach make sense?

## Flexible Join Support

Joins will work with any field, not just primary keys. We'll support:
- `join_one()` - one-to-one relationships
- `join_many()` - one-to-many relationships  
- `join_cross()` - cross joins
- `join(condition=...)` - custom join conditions


## Time Dimensions

We're still determining the best approach for handling time dimensions.

Currently, users must define:
- `time_dimension`
- `time_grain`

Then specify a time grain at query time. This feels cumbersome.

For now, we want to stay close to native Ibis patterns:
```python
flight_st.group_by(
    flight_st.arr_time.truncate('month').name('month')
).aggregate(
    flight_count=flight_st.flight_count,
)
```

We'll move the time grain logic to the MCP interface only where queries need proper JSON serialization and keep the core of BSL as close as possible to Ibis.

We'd love your feedback on this approach.

Don't hesitate if you have questions or ideas.

We're really open to suggestions.

Let's build the best (boring) semantic layer.

