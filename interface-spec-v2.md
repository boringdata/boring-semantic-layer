# Malloy-Inspired Semantic Layer Interface

## 1. Core Concepts

### Semantic Tables
- **Individual enhanced Ibis tables** from source tables
- Add business context: dimensions, measures, joins, time dimensions
- Still behave as standard Ibis tables
- `to_semantic_table(base_tbl).with_dimensions(...).with_measures(...)`

### Semantic Models  
- **Composition of semantic tables** into unified business models
- Global access to all dimensions and measures across tables
- Automatic join resolution when crossing table boundaries
- `SemanticModel(tables=[flights, carriers, weather])`

---

## 2. Building Semantic Tables

```python
import ibis
from boring_semantic_layer import to_semantic_table

# Individual semantic tables
carriers = (
    to_semantic_table(carriers_tbl)
    .with_primary_key('code')
    .with_dimensions(
        code=lambda t: t.code,
        name=lambda t: t.name,
        nickname=lambda t: t.nickname
    )
    .with_measures(
        carrier_count=lambda t: t.count()
    )
)

flights = (
    to_semantic_table(flights_tbl)
    .with_time_dimension('arr_time')
    .with_smallest_time_grain('TIME_GRAIN_SECOND')
    .with_dimensions(
        origin=lambda t: t.origin,
        destination=lambda t: t.destination,
        carrier=lambda t: t.carrier,
        tail_num=lambda t: t.tail_num,
        arr_time=lambda t: t.arr_time
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_dep_delay=lambda t: t.dep_delay.mean(),
        avg_distance=lambda t: t.distance.mean(),
        # Post-agg measures detected automatically by referencing other measures
        delay_efficiency=lambda t: t.flight_count / (t.avg_dep_delay + 1),
        on_time_rate=lambda t: t.on_time_flights / t.flight_count * 100
    )
    .with_joins(
        carriers=lambda: carriers.join_one(_.carrier)
    )
)
```

---

## 3. Composing Semantic Models

```python
from boring_semantic_layer import SemanticModel

# Compose semantic tables into unified model
flight_analytics_model = SemanticModel(
    name="flight_analytics",
    tables={
        "flights": flights,
        "carriers": carriers, 
        "weather": weather_data
    }
)

# Global access across all tables
model_query = flight_analytics_model.query(
    dimensions=['origin', 'name', 'conditions'],  # from flights, carriers, weather
    measures=['flight_count', 'avg_dep_delay', 'avg_temp'],
    filters={'origin': 'LAX'},
    time_grain='month'
)
```

---

## 4. Querying Patterns

### Simple Table Queries
```python
# Standard Ibis operations work directly
top_routes = (
    flights
    .filter(flights.carrier == 'AA')
    .group_by('route')
    .aggregate(total_flights=flights.flight_count)
    .order_by('total_flights', desc=True)
    .limit(10)
)
```

### Cross-Table Queries
```python
# Join semantic tables
flight_carrier_stats = (
    flights
    .join(carriers, flights.carrier == carriers.code)
    .group_by('origin', 'carriers.name')
    .aggregate(
        total_flights=flights.flight_count,
        avg_delay=flights.avg_dep_delay,
        delay_efficiency=flights.delay_efficiency  # Post-agg handled automatically
    )
)
```

### Time Dimension Queries
```python
# Nice time interface using predefined time dimension
daily_flights = (
    flights
    .time_grain('day')  # Uses defined time dimension
    .group_by('flight_date')  # Auto-generated dimension
    .aggregate(
        daily_count=flights.flight_count,
        avg_delay=flights.avg_dep_delay
    )
    .filter(lambda t: t.flight_date >= '2024-01-01')
)
```

### Window Functions & Percentages
```python
# Market share calculation
total_window = ibis.window()
carrier_market_share = (
    flights
    .group_by('carrier')
    .aggregate(flight_count=flights.flight_count)
    .mutate(
        market_share=lambda t: t.flight_count / t.flight_count.sum()
    )
)

# Rolling averages
rolling_window = ibis.window(order_by='month', rows=(2, 0))
monthly_trends = (
    flights
    .group_by(flights.arr_time.truncate('month').name('month'), 'carrier')
    .aggregate(monthly_flights=flights.flight_count)
    .mutate(
        rolling_avg=lambda t: t.monthly_flights.mean().over(rolling_window)
    )
)
```

---

## 5. Advanced Features

### Automatic Post-Aggregation Detection

The system automatically detects when measures reference other measures:

```python
# User writes simple code
efficiency_analysis = (
    flights
    .group_by('origin')
    .aggregate(
        flight_count=flights.flight_count,      # Regular measure
        delay_efficiency=flights.delay_efficiency  # Post-agg measure
    )
)

# System auto-rewrites to:
# flights.group_by('origin').aggregate(
#     flight_count=lambda t: t.count(),
#     avg_dep_delay=lambda t: t.dep_delay.mean()  # Auto-added dependency
# ).mutate(
#     delay_efficiency=lambda t: t.flight_count / (t.avg_dep_delay + 1)
# ).select(['origin', 'flight_count', 'delay_efficiency'])
```

### Complex Dependency Chains

```python
# Nested dependencies resolved automatically
# Level 1: avg_delay = lambda t: t.dep_delay.mean()
# Level 2: delay_efficiency = lambda t: t.flight_count / t.avg_delay  
# Level 3: performance_score = lambda t: t.delay_efficiency * 100

# User requests Level 3 measure
performance_query = flights.aggregate(
    performance_score=flights.performance_score
)

# System generates:
# aggregate(flight_count, avg_delay)
# .mutate(delay_efficiency)
# .mutate(performance_score)
# .select(['performance_score'])
```

---

## 6. Implementation Details

### Type System
```python
class SemanticTable(ibis.Table):
    def __init__(self, base_table):
        super().__init__(base_table)
        self._measures = {}
        self._dimensions = {}

    def __getattr__(self, name):
        if name in self._measures:
            return MeasureReference(name, self._measures[name])
        return super().__getattr__(name)  # Raw column access

class MeasureReference:
    def __init__(self, name, definition):
        self.name = name
        self.is_measure = True
```

### Dependency Resolution
1. **Build dependency graph**: Analyze measure references
2. **Topological sort**: Order by dependency levels  
3. **Query rewriting**: Generate aggregate + mutate chain
4. **Column selection**: Return only requested measures

---

## 7. JSON Serialization & MCP Integration

### Query Serialization
```python
query_json = {
    "model": "flight_analytics",
    "dimensions": ["origin", "carrier_name"],
    "measures": ["flight_count", "delay_efficiency"],
    "filters": [
        {"field": "origin", "operator": "=", "value": "LAX"},
        {"field": "arr_time", "operator": ">=", "value": "2024-01-01"}
    ],
    "time_grain": "month",
    "limit": 100
}
```


---

## Other considerations:
- from_yaml()
- chart()

### Error Handling
- Invalid dimension/measure references
- Circular dependencies in post-agg measures
- Join relationship validation

### Performance Optimization via xorq
- Caching strategies + materialization
