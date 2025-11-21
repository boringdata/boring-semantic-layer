# BSL Query Example

This is a test of the BSL render CLI.

```setup_data
import ibis
from boring_semantic_layer import to_semantic_table

flights = ibis.memtable({
    "origin": ["JFK", "LAX", "ORD", "JFK", "LAX"],
    "dest": ["LAX", "JFK", "LAX", "ORD", "ORD"],
    "passengers": [100, 150, 200, 80, 120],
})

result = to_semantic_table(flights)
```

<bslquery code-block="setup_data"/>

## Flight Summary

Let's see the flights data:

```query_summary
q = result.group_by("origin").agg(
    total_passengers=lambda _: _.passengers.sum(),
    flight_count=lambda _: _.count()
)
result = q
```

<bslquery code-block="query_summary"/>
