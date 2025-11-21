# Dimensional Indexing

Create a searchable catalog of all unique values across your dimensions for data exploration, autocomplete features, and understanding data distributions. Inspired by [Malloy's index pattern](https://docs.malloydata.dev/documentation/patterns/dim_index).

## Overview

Dimensional indexing allows you to:

- **Catalog all values**: Extract and count all unique values across dimensions
- **Search dimensions**: Build autocomplete and search features
- **Profile data**: Understand cardinality and distributions
- **Weight by measures**: Find values ranked by custom metrics (e.g., highest revenue cities)
- **Index across joins**: Search values from related tables

The `index()` method returns a standardized table with columns:
- `fieldName`: The dimension name
- `fieldValue`: The unique value
- `fieldType`: The data type (string, number, etc.)
- `weight`: Count or custom measure value for ranking

## Setup

Let's create an airports semantic table for our examples:

```setup_airports
import ibis
from boring_semantic_layer import to_semantic_table

# Create synthetic airports data
airports_data = ibis.memtable({
    "code": ["JFK", "LAX", "ORD", "ATL", "DFW", "DEN", "SFO", "LAS", "SEA", "PHX",
             "IAH", "MCO", "EWR", "BOS", "MIA", "SAN", "LGA", "PHL", "DTW", "MSP"],
    "city": ["NEW YORK", "LOS ANGELES", "CHICAGO", "ATLANTA", "DALLAS", "DENVER",
             "SAN FRANCISCO", "LAS VEGAS", "SEATTLE", "PHOENIX", "HOUSTON", "ORLANDO",
             "NEWARK", "BOSTON", "MIAMI", "SAN DIEGO", "NEW YORK", "PHILADELPHIA",
             "DETROIT", "MINNEAPOLIS"],
    "state": ["NY", "CA", "IL", "GA", "TX", "CO", "CA", "NV", "WA", "AZ",
              "TX", "FL", "NJ", "MA", "FL", "CA", "NY", "PA", "MI", "MN"],
    "fac_type": ["AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT",
                 "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT",
                 "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT",
                 "AIRPORT", "AIRPORT"],
    "elevation": [13, 128, 672, 1026, 607, 5433, 13, 2181, 433, 1135,
                  97, 96, 18, 19, 8, 17, 21, 36, 645, 841]
})

# Define semantic table
airports = (
    to_semantic_table(airports_data, name="airports")
    .with_dimensions(
        code=lambda t: t.code,
        city=lambda t: t.city,
        state=lambda t: t.state,
        fac_type=lambda t: t.fac_type,
        elevation=lambda t: t.elevation,
    )
    .with_measures(
        airport_count=lambda t: t.count(),
        avg_elevation=lambda t: t.elevation.mean(),
    )
)
```

<collapsedcodeblock code-block="setup_airports" title="Setup: Create Airports Table"></collapsedcodeblock>

## Basic Index: All Dimensions

Index all dimensions to see every unique value with its frequency:

```query_index_all
# Index all dimensions (None means all)
result = airports.index(None).limit(10)
```

<bslquery code-block="query_index_all"></bslquery>

The `weight` column shows the count for each value. Use this to understand which values are most common across your dataset.

## Index Specific Fields

Focus on specific dimensions by selecting them:

```query_index_specific
# Index only state and city
result = (
    airports.index(lambda t: [t.state, t.city])
    .order_by(lambda t: t.weight.desc())
    .limit(10)
)
```

<bslquery code-block="query_index_specific"></bslquery>

This is useful when you only care about certain dimensions, reducing noise and improving performance.

## Search Pattern: Autocomplete

Build autocomplete features by filtering the index with pattern matching:

```query_autocomplete
# Get city suggestions starting with "SAN"
result = (
    airports.index(lambda t: t.city)
    .filter(lambda t: t.fieldValue.like("SAN%"))
    .order_by(lambda t: t.weight.desc())
    .limit(10)
)
```

<bslquery code-block="query_autocomplete"></bslquery>

<note type="info">
Use pattern matching with `like()` to implement autocomplete, search suggestions, or fuzzy matching features in your application.
</note>

## Filter by Field Type

Analyze only string or numeric fields:

```query_by_type
# Get only string field values
result = (
    airports.index(None)
    .filter(lambda t: t.fieldType == "string")
    .order_by(lambda t: t.weight.desc())
    .limit(10)
)
```

<bslquery code-block="query_by_type"></bslquery>

This helps when you want to focus on categorical vs. numeric dimensions separately.

## Custom Weights: Rank by Measure

Instead of counting occurrences, weight values by a custom measure:

```query_custom_weight
# Find states with most airports
result = (
    airports.index(lambda t: t.state, by="airport_count")
    .order_by(lambda t: t.weight.desc())
    .limit(10)
)
```

<bslquery code-block="query_custom_weight"></bslquery>

<note type="info">
The `by` parameter lets you rank dimension values by any measure. This is powerful for finding "top cities by revenue", "states by average temperature", etc.
</note>

## Sampling for Large Datasets

For very large datasets, use sampling to get quick insights:

```query_sampled
# Sample 100 rows before indexing
result = (
    airports.index(None, sample=100)
    .filter(lambda t: t.fieldType == "string")
    .order_by(lambda t: t.weight.desc())
    .limit(10)
)
```

<bslquery code-block="query_sampled"></bslquery>

Sampling trades perfect accuracy for speed, which is often acceptable for exploratory analysis.

## Index Across Joins

Index dimensions from joined tables:

```query_index_joins
# Create synthetic flights data
flights_data = ibis.memtable({
    "flight_id": list(range(1, 31)),
    "carrier": ["AA", "UA", "DL", "WN", "B6", "AA", "UA", "DL", "WN", "B6"] * 3,
    "origin": ["JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "SEA", "DEN", "PHX", "BOS"] * 3,
})

flights = (
    to_semantic_table(flights_data, name="flights")
    .with_dimensions(
        carrier=lambda t: t.carrier,
        origin=lambda t: t.origin,
    )
    .with_measures(
        flight_count=lambda t: t.count(),
    )
)

# Join flights with airports
flights_with_origin = flights.join_one(airports, left_on="origin", right_on="code")

# Index across the join
result = (
    flights_with_origin.index(["flights.carrier", "airports.state"])
    .order_by(lambda t: t.weight.desc())
    .limit(10)
)
```

<bslquery code-block="query_index_joins"></bslquery>

<note type="warning">
When referencing dimensions from joined tables in the index, use dot notation with table name prefix: `"airports.state"` instead of just `"state"`.
</note>

## Use Cases

**Data Discovery**: Quickly explore what values exist in your dimensions without writing complex group-by queries. Perfect for understanding unfamiliar datasets.

**Autocomplete & Search**: Build type-ahead search features by indexing dimension values and filtering with pattern matching. The weight helps rank suggestions by relevance.

**Data Profiling**: Understand data quality by examining cardinality, common values, and distributions across dimensions. Identify outliers or data entry errors.

**Metric-Weighted Ranking**: Find dimension values that matter most for your metrics - e.g., "cities with highest revenue", "products with most returns", "states with longest delivery times".

**Cross-Table Search**: Index dimensions across joined tables to search related data simultaneously, enabling unified search experiences.

## Key Takeaways

- Use `index(None)` to catalog all dimension values
- Use `index(lambda t: [t.field1, t.field2])` for specific fields or `index(lambda t: t.field)` for a single field
- Filter by `fieldType` to focus on strings or numbers
- Use `by="measure_name"` to weight by custom measures instead of counts
- Add `sample=N` to analyze large datasets quickly
- The index works across joins - use `"table.field"` syntax for joined dimensions
- Perfect for building autocomplete, search, and data profiling features

## Next Steps

- Learn about [Nested Subtotals](/advanced/nested-subtotals) for hierarchical data structures
- Explore [Query Methods](/querying/methods) for more query patterns
