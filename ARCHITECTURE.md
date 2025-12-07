# Architecture: FeatureView as SemanticModel

## Class Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                         ir.Table (ibis)                         │
│                    Base table expression                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │ extends
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                      SemanticTable                              │
│  • Base wrapper around ir.Table                                 │
│  • Adds semantic operations (filter, group_by, etc.)            │
└──────────────────────────────┬──────────────────────────────────┘
                               │ extends
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                      SemanticModel                              │
│  • Dimensions: 0+ entity, 0+ timestamp, any regular             │
│  • Measures: aggregatable metrics                               │
│  • Operations: join_one, join_many, join_cross, join_asof       │
│  • Method: is_feature_view() → bool                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │ extends
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                       FeatureView                               │
│  • Constraints: EXACTLY 1 entity + EXACTLY 1 timestamp          │
│  • Additional methods:                                          │
│    - get_entity() → str                                         │
│    - get_timestamp_field() → str                                │
│    - get_ttl() → timedelta | None                               │
│    - materialize(backend, table_name, ttl)                      │
│  • Inherits: ALL SemanticModel operations                       │
└─────────────────────────────────────────────────────────────────┘
```

## Concept Mapping

### SemanticModel → FeatureView

```
SemanticModel                          FeatureView
─────────────────────────────────────────────────────────────────
Dimensions (general)           →       Entity (exactly 1)
  ├─ entity_dimension()        →         is_entity=True
  ├─ time_dimension()          →         is_event_timestamp=True
  └─ regular dimension         →         regular dimensions

Measures                       →       Features
  ├─ lambda t: t.col.sum()     →         Features (measures)
  └─ windowed_measure()        →         Time-windowed features

Operations                     →       Operations
  ├─ filter()                  →         filter()
  ├─ group_by()                →         group_by()
  ├─ aggregate()               →         aggregate()
  ├─ join_one()                →         join_one()
  ├─ join_many()               →         join_many()
  └─ join_asof()               →         join_asof() (PIT joins)

                               →       Additional
                               →         get_entity()
                               →         get_timestamp_field()
                               →         materialize()
```

### FeatureView ↔ Feast

```
boring-semantic-layer              Feast (via interop)
─────────────────────────────────────────────────────────────────
FeatureView                  ←→   feast.FeatureView
  .get_entity()              ←→     .entities[0]
  .get_timestamp_field()     ←→     .batch_source.timestamp_field
  .get_measures()            ←→     .features (schema)
  .get_ttl()                 ←→     .ttl

entity_dimension()           ←→   Entity(join_keys=[...])
time_dimension()             ←→   timestamp_field in FileSource
Measure                      ←→   Field(name=..., dtype=...)

join_asof()                  ←→   Point-in-time correct joins
materialize()                ←→   Materialization + online serving
```

## Data Flow

### 1. Creating a FeatureView

```
┌─────────────────┐
│  Ibis Table     │
│  (raw data)     │
└────────┬────────┘
         │ to_semantic_table()
         ↓
┌─────────────────┐
│ SemanticModel   │
│ (no constraints)│
└────────┬────────┘
         │ .with_dimensions(
         │   entity_dimension(),
         │   time_dimension()
         │ )
         ↓
┌─────────────────┐
│ SemanticModel   │
│ (1 entity +     │
│  1 timestamp)   │
└────────┬────────┘
         │ FeatureView.from_semantic_model()
         │ (validates constraints)
         ↓
┌─────────────────┐
│  FeatureView    │
│  (validated)    │
└─────────────────┘
```

### 2. Using FeatureView as SemanticModel

```
┌─────────────────┐
│  FeatureView    │
└────────┬────────┘
         │ inherited operations
         ├─→ .filter(...)         → SemanticFilter
         ├─→ .group_by(...)       → SemanticGroupBy
         ├─→ .aggregate(...)      → SemanticAggregate
         ├─→ .join_one(...)       → SemanticJoin
         ├─→ .join_many(...)      → SemanticJoin
         └─→ .join_asof(...)      → SemanticJoin (PIT)
```

### 3. Feast Interoperability

```
SemanticModel
    ↓
FeatureView (boring-semantic-layer)
    ↓ convert
FeastFeatureView (attrs wrapper)
    ↓ .to_feast()
feast.FeatureView
    ↓ Feast operations
    │ - .apply()
    │ - .get_online_features()
    │ - .get_historical_features()
    ↓ .from_feast()
FeastFeatureView
    ↓ convert back
FeatureView (boring-semantic-layer)
```

## File Organization

```
src/boring_semantic_layer/
│
├── expr.py                      # Core classes
│   ├── SemanticTable
│   ├── SemanticModel
│   │   └── is_feature_view() → bool
│   ├── SemanticJoin
│   ├── SemanticFilter
│   ├── SemanticGroupBy
│   ├── SemanticAggregate
│   └── ...
│
├── feature_view.py              # FeatureView (NEW)
│   └── FeatureView
│       ├── __init__(validates constraints)
│       ├── from_semantic_model()
│       ├── get_entity()
│       ├── get_timestamp_field()
│       ├── get_ttl()
│       └── materialize()
│
├── ops.py                       # Operations
│   ├── Dimension
│   │   ├── is_entity
│   │   └── is_event_timestamp
│   ├── Measure
│   └── SemanticTableOp
│
├── api.py                       # Public API
│   ├── to_semantic_table()
│   ├── entity_dimension()
│   └── time_dimension()
│
├── graph_utils.py               # Validation
│   ├── find_entity_dimensions()
│   ├── find_event_timestamp_dimensions()
│   └── is_feature_view()
│
└── tests/
    ├── test_feature_view.py     # FeatureView tests (NEW)
    └── test_graph_utils.py      # Validation tests
```

## Example Usage Flow

### Step 1: Define Data

```python
transactions = con.create_table("transactions", {
    "user_id": [1, 2, 3],
    "amount": [100, 200, 300],
    "transaction_timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
})
```

### Step 2: Create SemanticModel

```python
model = (
    to_semantic_table(transactions, name="transaction_features")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id),
        transaction_timestamp=time_dimension(lambda t: t.transaction_timestamp),
    )
    .with_measures(
        total_spend=lambda t: t.amount.sum(),
    )
)
```

### Step 3: Validate & Convert to FeatureView

```python
assert model.is_feature_view()  # True
fv = FeatureView.from_semantic_model(model)
```

### Step 4: Use as SemanticModel

```python
result = (
    fv
    .filter(lambda t: t.user_id == 1)
    .aggregate("total_spend")
    .execute()
)
```

### Step 5: Use FeatureView-Specific Features

```python
entity = fv.get_entity()                # "user_id"
timestamp = fv.get_timestamp_field()    # "transaction_timestamp"

# Materialize for online serving
cached = fv.materialize(backend=con, table_name="cache.features")
```

### Step 6: Interop with Feast (Optional)

```python
# Convert to Feast
feast_fv = FeastFeatureView(
    name=fv.name,
    source=FeastFileSource(...),
    entities=[fv.get_entity()],
    schema=[...],
    ttl=fv.get_ttl(),
)

# Use with Feast
feast_fv.to_feast()
```

## Key Design Principles

### 1. Liskov Substitution Principle

```python
def process_semantic_model(model: SemanticModel):
    return model.filter(...).aggregate(...)

fv = FeatureView.from_semantic_model(...)
result = process_semantic_model(fv)  # ✓ Works!
```

FeatureView can be used anywhere SemanticModel is expected.

### 2. Constraint Validation

```python
# Valid FeatureView
model = SemanticModel(dimensions={
    "user_id": entity_dimension(...),           # 1 entity ✓
    "timestamp": time_dimension(...),           # 1 timestamp ✓
})
fv = FeatureView.from_semantic_model(model)    # ✓ Success

# Invalid FeatureView
model = SemanticModel(dimensions={
    "user_id": entity_dimension(...),           # 1 entity ✓
    "account_id": entity_dimension(...),        # 2 entities ✗
    "timestamp": time_dimension(...),           # 1 timestamp ✓
})
fv = FeatureView.from_semantic_model(model)    # ✗ ValueError
```

### 3. Type Safety

```python
isinstance(fv, FeatureView)      # True
isinstance(fv, SemanticModel)    # True
issubclass(FeatureView, SemanticModel)  # True
```

## Comparison Table

| Aspect | SemanticModel | FeatureView | Feast FeatureView |
|--------|---------------|-------------|-------------------|
| **Purpose** | General semantic modeling | ML feature engineering | ML feature store |
| **Entity Dimensions** | 0 or more | Exactly 1 | 1 or more |
| **Timestamp Dimensions** | 0 or more | Exactly 1 | Exactly 1 |
| **Measures/Features** | Any | Any | Defined schema |
| **Point-in-time Joins** | ✓ `join_asof()` | ✓ Inherited | ✓ Built-in |
| **Filtering** | ✓ | ✓ Inherited | ✗ (at query time) |
| **Grouping** | ✓ | ✓ Inherited | ✗ (at query time) |
| **Aggregation** | ✓ | ✓ Inherited | ✗ (at definition) |
| **Online Serving** | ✗ | ✓ `materialize()` | ✓ Built-in |
| **SQL Compilation** | ✓ | ✓ Inherited | ✗ |
| **Type System** | Python classes | Extends SemanticModel | Separate hierarchy |

## Summary

**FeatureView IS-A SemanticModel** proven by:

1. ✓ **Class inheritance** - Direct subclass relationship
2. ✓ **Constraint specialization** - Stricter requirements (1 entity + 1 timestamp)
3. ✓ **Method inheritance** - All SemanticModel operations work
4. ✓ **Type compatibility** - Can be used anywhere SemanticModel is expected
5. ✓ **Concept mapping** - Feast concepts map to semantic model concepts
6. ✓ **Interoperability** - Can convert to/from Feast when needed

This architecture unifies semantic modeling and feature engineering into a single coherent framework.
