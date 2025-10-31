# Composing Models

Build complex data models by combining multiple semantic tables through joins. Model composition allows you to create rich, multi-dimensional views of your data.

## Composition via Joins

Model composition in BSL is achieved through **joins**. When you join semantic tables, the result is a new composed model that contains **all dimensions and measures** from both tables.

<note type="info">
Each join creates a new semantic model with the combined dimensions and measures from all joined tables. This allows you to build progressively richer models.
</note>

## Example: Two-Level Composition

Let's build a composed model step-by-step, showing available dimensions and measures at each level.

### Level 0: Base Models

First, let's set up our base tables:

```setup_ibis_tables
import ibis
from boring_semantic_layer import to_semantic_table

# Create sample data
con = ibis.duckdb.connect(":memory:")

# Flights table
flights_data = ibis.memtable({
    "flight_id": [1, 2, 3],
    "carrier_code": ["AA", "UA", "DL"],
    "aircraft_id": [101, 102, 103],
    "distance": [1000, 1500, 800],
    "passengers": [150, 180, 120]
})
flights_tbl = con.create_table("flights", flights_data)

# Carriers table
carriers_data = ibis.memtable({
    "code": ["AA", "UA", "DL"],
    "name": ["American Airlines", "United Airlines", "Delta Air Lines"],
    "country": ["USA", "USA", "USA"]
})
carriers_tbl = con.create_table("carriers", carriers_data)

# Aircraft table
aircraft_data = ibis.memtable({
    "id": [101, 102, 103],
    "model": ["Boeing 737", "Airbus A320", "Boeing 777"],
    "capacity": [180, 200, 350]
})
aircraft_tbl = con.create_table("aircraft", aircraft_data)
```

<collapsedcodeblock code-block="setup_ibis_tables" title="Define Ibis Tables"></collapsedcodeblock>

```setup_semantic_models
# Create semantic tables
flights_st = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        flight_id=lambda t: t.flight_id,
        carrier_code=lambda t: t.carrier_code,
        aircraft_id=lambda t: t.aircraft_id
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        total_passengers=lambda t: t.passengers.sum()
    )
)

carriers_st = (
    to_semantic_table(carriers_tbl, name="carriers")
    .with_dimensions(
        code=lambda t: t.code,
        name=lambda t: t.name,
        country=lambda t: t.country
    )
    .with_measures(
        carrier_count=lambda t: t.count()
    )
)

aircraft_st = (
    to_semantic_table(aircraft_tbl, name="aircraft")
    .with_dimensions(
        id=lambda t: t.id,
        model=lambda t: t.model
    )
    .with_measures(
        aircraft_count=lambda t: t.count(),
        total_capacity=lambda t: t.capacity.sum()
    )
)
```

<collapsedcodeblock code-block="setup_semantic_models" title="Define Semantic Models"></collapsedcodeblock>

```level0_dimensions
flights_st.dimensions, flights_st.measures
```

<regularoutput code-block="level0_dimensions"></regularoutput>

### Level 1: First Join (Flights + Carriers)

Join carriers to flights to add carrier information:

```level1_join
# Join carriers to flights
flights_with_carriers = flights_st.join_many(
    carriers_st,
    left_on="carrier_code",
    right_on="code"
)

# Inspect dimensions - now includes both flights and carriers
flights_with_carriers.dimensions, flights_with_carriers.measures
```
<regularoutput code-block="level1_join"></regularoutput>

### Level 2: Second Join (+ Aircraft)

Add aircraft information to create a fully composed model:

```level2_join
# Join aircraft to the composed model
full_model = flights_with_carriers.join_many(
    aircraft_st,
    left_on="aircraft_id",
    right_on="id"
)

# Inspect dimensions - now includes flights, carriers, AND aircraft
full_model.dimensions, full_model.measures
```
<regularoutput code-block="level2_join"></regularoutput>

## Query the Composed Model

Now you can query across all joined tables:

```composed_query
# Query using dimensions and measures from all three tables
result = (
    full_model
    .group_by( "aircraft.model")
    .aggregate("flight_count", "total_passengers", "total_capacity")
)
```

<bslquery code-block="composed_query"></bslquery>

## Key Takeaways

- **Composition via Joins**: Use `join_many()`, `join_one()`, or `join()` to compose models
- **Additive**: Each join adds dimensions and measures from the joined table
- **Table Prefixes**: Dimensions/measures are prefixed with table names (`flights.`, `carriers.`, `aircraft.`)
- **No Limit**: Compose as many models as needed for your analysis
- **Incremental**: Build from simple to complex, one join at a time

## Next Steps

- Learn about [YAML Configuration](/building/yaml) for declarative model composition
- Explore [Query Methods](/querying/methods) for querying composed models
