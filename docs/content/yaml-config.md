# YAML Configuration

Define your semantic models using YAML for better organization and maintainability.

## Why YAML?

YAML configuration provides several advantages:
- **Better organization**: Keep your model definitions separate from your code
- **Version control**: Track changes to your data model structure
- **Collaboration**: Non-developers can review and understand the model
- **Reusability**: Share model definitions across different projects

## Expression Syntax

Here's a complete example with dimensions, measures, and joins:

<yamlcontent path="yaml_example.yaml"></yamlcontent>

<note type="warning">
In YAML configuration, **only unbound syntax (`_`) is accepted** for expressions. Lambda expressions are not supported in YAML files.
</note>

## Loading YAML Models

Ibis table objects must be created separately in Python and passed to the YAML loader. Tables are resolved by the names specified in the YAML `table` field.

Create your ibis tables:

```yaml_setup
import ibis

flights_tbl = ibis.memtable({
    "origin": ["JFK", "LAX", "SFO"],
    "dest": ["LAX", "SFO", "JFK"],
    "carrier": ["AA", "UA", "DL"],
    "year": [2023, 2023, 2024],
    "distance": [2475, 337, 382]
})

carriers_tbl = ibis.memtable({
    "code": ["AA", "UA", "DL"],
    "name": ["American Airlines", "United Airlines", "Delta Air Lines"]
})
```

And pass them to the loaded YAML file defining your Semantic Tables:


```load_yaml_example
from boring_semantic_layer import from_yaml

# Load models from YAML file
models = from_yaml(
    "content/yaml_example.yaml",
    tables={
        "flights_tbl": flights_tbl,
        "carriers_tbl": carriers_tbl
    }
)

flights_sm = models["flights"]
carriers_sm = models["carriers"]

# Inspect the loaded models
flights_sm.dimensions, flights_sm.measures
```

<regularoutput code-block="load_yaml_example"></regularoutput>

## Querying YAML Models

YAML-defined models work exactly like Python-defined models. You can use the same `group_by()` and `aggregate()` methods to query your data.

```query_yaml_model
# Query the YAML-defined model
result = (
    flights_sm
    .group_by("origin")
    .aggregate("flight_count", "avg_distance")
)
```

<bslquery code-block="query_yaml_model"></bslquery>

## Next Steps

- See [Building Semantic Tables](/building/semantic-tables) for Python-based definitions
- Learn [Query Methods](/querying/methods) for querying YAML-defined models
- Explore [Composing Models](/building/compose) for joining YAML models
