# Flights Dashboard

A sample dashboard demonstrating BSL grid layout.

```bsl size=[16,1]
# Setup: Create semantic table (hidden in a full-width cell)
flights_tbl = ibis.memtable({
    "origin": ["NYC", "LAX", "NYC", "SFO", "LAX", "NYC", "SFO", "LAX", "NYC", "CHI", "NYC", "LAX"],
    "carrier": ["AA", "UA", "AA", "UA", "AA", "UA", "AA", "UA", "AA", "UA", "DL", "DL"],
    "distance": [2789, 2789, 2902, 2902, 347, 2789, 347, 347, 2789, 1200, 1500, 2400],
    "duration": [330, 330, 360, 360, 65, 330, 65, 65, 330, 180, 200, 300],
})
flights_st = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(origin=lambda t: t.origin, carrier=lambda t: t.carrier)
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_duration=lambda t: t.duration.mean(),
    )
)
result = flights_st
```

```bsl size=[4,2]
# Total flights KPI
flights_st.group_by().aggregate("flight_count")
```
```bsl size=[4,2]
# Total distance KPI
flights_st.group_by().aggregate("total_distance")
```
```bsl size=[4,2]
# Average duration KPI
flights_st.group_by().aggregate("avg_duration")
```
```bsl size=[4,2]
# Unique carriers KPI
result = flights_st.group_by().aggregate(
    unique_carriers=lambda t: t.carrier.nunique()
)
```

```bsl size=[8,6]
# Flights by carrier (bar chart)
flights_st.group_by("carrier").aggregate("flight_count").order_by(ibis.desc("flight_count"))
```
```bsl size=[8,6]
# Distance by origin (bar chart)
flights_st.group_by("origin").aggregate("total_distance").order_by(ibis.desc("total_distance"))
```

```bsl size=[16,5]
# Full breakdown table
flights_st.group_by("origin", "carrier").aggregate("flight_count", "total_distance", "avg_duration").order_by(ibis.desc("flight_count")).limit(10)
```
