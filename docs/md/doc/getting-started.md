# Getting Started with BSL

BSL (Boring Semantic Layer) is a lightweight semantic layer built on top of Ibis. It allows you to define your data models once and query them anywhere.

## Installation

```bash
pip install boring-semantic-layer
```

## Quick Start

Let's create your first Semantic Table using synthetic data in Ibis.

```setup_flights
import ibis
from boring_semantic_layer import to_semantic_table

# Create sample flight data
flights_tbl = ibis.memtable({
    "origin": ["NYC", "LAX", "NYC", "SFO", "LAX", "NYC", "SFO", "LAX"],
    "destination": ["LAX", "NYC", "SFO", "NYC", "SFO", "LAX", "LAX", "SFO"],
    "distance": [2789, 2789, 2902, 2902, 347, 2789, 347, 347],
    "duration": [330, 330, 360, 360, 65, 330, 65, 65],
})
```

You can then convert these tables in Semantic Tables that contains dimensios and measures definitions:

```define_semantic_table
# Define semantic table with dimensions and measures
flights_st = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        origin=lambda t: t.origin,
        destination=lambda t: t.destination,
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_duration=lambda t: t.duration.mean(),
    )
)
```

## Query Your Data

Now let's query the semantic table by grouping flights by origin:

```query_by_origin
# Group flights by origin airport
result = flights_st.group_by("origin").aggregate(
    "flight_count",
    "total_distance",
    "avg_duration"
)
```

<bslquery code-block="query_by_origin"></bslquery>

You can also group by destination:

```query_by_destination
# Group flights by destination airport
result = flights_st.group_by("destination").aggregate(
    "flight_count",
    "total_distance"
)
```

<bslquery code-block="query_by_destination"></bslquery>

## Next Steps

- Learn how to [Build Semantic Tables](/examples/semantic-table) with dimensions, measures, and joins
- Explore [Query Methods](/examples/query-methods) for retrieving data
- Discover how to [Compose Models](/examples/compose) together
