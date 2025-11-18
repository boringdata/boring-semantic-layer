# Percentage of Total

Calculate percentages relative to total values across different dimensions. Use this pattern when you need to understand market share, contribution ratios, or what proportion each segment represents of the whole.

## Overview

The percentage of total pattern allows you to:

- Define percentage measures using the `.all()` method
- Calculate individual segment values as percentages of the grand total
- Maintain dimensional breakdowns while computing percentage contributions
- Support multiple aggregation functions (sum, count, average)

## Setup

Let's use the flights dataset with carrier information to demonstrate market share calculations:

```setup_data
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table

# Create synthetic flights data with carrier information
flights_data = ibis.memtable({
    "flight_id": list(range(1, 51)),
    "carrier": ["AA", "UA", "DL", "WN", "B6"] * 10,
    "nickname": ["American Airlines", "United Airlines", "Delta Air Lines",
                 "Southwest Airlines", "JetBlue Airways"] * 10,
    "origin": ["JFK", "LAX", "ORD", "ATL", "DFW"] * 10,
    "distance": [2475, 1745, 733, 946, 1383, 2475, 1745, 733, 946, 1383,
                 2475, 1745, 733, 946, 1383, 2475, 1745, 733, 946, 1383,
                 2475, 1745, 733, 946, 1383, 2475, 1745, 733, 946, 1383,
                 2475, 1745, 733, 946, 1383, 2475, 1745, 733, 946, 1383,
                 2475, 1745, 733, 946, 1383, 2475, 1745, 733, 946, 1383]
})

# Create semantic table with measures including percentage calculations
flights = (
    to_semantic_table(flights_data, name="flights")
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
    )
    .with_measures(
        market_share=lambda t: t.flight_count / t.all(t.flight_count) * 100,
        distance_share=lambda t: t.total_distance / t.all(t.total_distance) * 100,
    )
)
```

<collapsedcodeblock code-block="setup_data" title="Setup: Create Flights and Carriers Data"></collapsedcodeblock>

<note type="info">
The `.all()` method calculates the grand total across all groups, allowing you to define percentage measures directly in the semantic table. This is more elegant than using window functions in post-processing.
</note>

## Market Share by Carrier

Calculate each carrier's percentage of total flights:

```query_market_share
from ibis import _

result = (
    flights.group_by("nickname")
    .aggregate("flight_count", "market_share")
    .order_by(_.market_share.desc())
    .limit(10)
)
```

<bslquery code-block="query_market_share"></bslquery>

## Market Share by Origin and Carrier

Calculate market share broken down by both origin airport and carrier:

```query_market_share_by_origin
from ibis import _

result = (
    flights.group_by("origin", "nickname")
    .aggregate("flight_count", "market_share")
    .order_by(_.market_share.desc())
    .limit(15)
)
```

<bslquery code-block="query_market_share_by_origin"></bslquery>

## Use Cases

**Market Share Analysis**: Calculate each carrier's, product's, or region's share of total volume.

**Traffic Distribution**: Determine what percentage of total website visits or conversions come from each source.

**Resource Allocation**: Understand how resources (budget, time, capacity) are distributed as percentages of the total.

## Key Takeaways

- Define percentage measures using `.all()` to reference the grand total
- The `.all(measure)` method calculates the total across all groups
- Percentage measures work seamlessly across different dimensional breakdowns
- More elegant than post-processing with window functions

## Next Steps

- Learn about [Nested Subtotals](/advanced/nested-subtotals) for hierarchical aggregations
- Explore [Bucketing](/advanced/bucketing) to group continuous values
