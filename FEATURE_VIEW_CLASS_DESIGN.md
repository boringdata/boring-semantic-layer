# FeatureView Class Design

## Goal

Create **FeatureView** classes that follow Feast's API to make the claim concrete: **"A FeatureView IS-A SemanticModel"**.

## Architecture

```
SemanticModel (base semantic layer)
    ↓ extends
FeatureView (Feast-like API, specialized for feature engineering)
    - entity: single entity dimension
    - timestamp_field: single event timestamp
    - source: underlying data source
    - schema: list of feature fields
    - ttl: time-to-live for features
    - online/offline: serving modes
```

## Key Design Decisions

### 1. FeatureView extends SemanticModel

Instead of wrapping SemanticModel, FeatureView **IS** a SemanticModel with:
- Exactly 1 entity dimension (enforced)
- Exactly 1 event timestamp dimension (enforced)
- Additional Feast-like API methods
- Validation that ensures FeatureView constraints

### 2. Feast API Compatibility

Support Feast's key patterns:
```python
# Define a FeatureView
fv = FeatureView(
    name="transaction_features",
    entities=["user_id"],  # Single entity for simplicity
    source=FileSource(...),  # Data source
    schema=[
        Field(name="spend_7d", dtype=Float64),
        Field(name="spend_30d", dtype=Float64),
    ],
    ttl=timedelta(days=30),
)

# Use FeatureView like a SemanticModel
result = (
    fv
    .filter(lambda t: t.user_id == 123)
    .aggregate("spend_7d", "spend_30d")
    .execute()
)

# FeatureView-specific operations
fv.join_asof(other_fv)  # Point-in-time join
fv.materialize(backend, table_name)  # Caching with freshness
```

### 3. Unify with Existing API

The `to_semantic_table()` + `.with_dimensions()` pattern should produce a FeatureView when constraints are met:

```python
# Current API (should work as-is)
model = (
    to_semantic_table(tbl, name="features")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id),
        timestamp=time_dimension(lambda t: t.timestamp),
    )
    .with_measures(
        spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days"),
    )
)

# This should automatically be a FeatureView!
assert isinstance(model, FeatureView)
assert model.is_feature_view()  # True
```

## Implementation Plan

### Option A: FeatureView as Subclass (Recommended)

**Pros:**
- Clear IS-A relationship
- Type-safe: `isinstance(fv, SemanticModel)` returns True
- Inherits all SemanticModel methods automatically
- Can add FeatureView-specific methods

**Cons:**
- Need to handle automatic promotion when conditions are met
- Slightly more complex construction logic

```python
class FeatureView(SemanticModel):
    """A SemanticModel specialized for feature engineering with Feast-like API.

    A FeatureView enforces:
    - Exactly 1 entity dimension (join key)
    - Exactly 1 event timestamp dimension (for point-in-time correctness)

    It adds Feast-like API methods:
    - join_asof() - Point-in-time correct joins
    - materialize() - Freshness-based caching
    - get_entity() - Get the entity dimension name
    - get_timestamp_field() - Get the timestamp field name
    """

    def __init__(self, ...):
        # Validate constraints
        if not self._validate_feature_view():
            raise ValueError("FeatureView requires exactly 1 entity and 1 timestamp")
        super().__init__(...)

    @classmethod
    def from_semantic_model(cls, model: SemanticModel) -> FeatureView:
        """Promote a SemanticModel to FeatureView if it meets constraints."""
        if not model.is_feature_view():
            raise ValueError("Model does not meet FeatureView requirements")
        return cls(...)

    def get_entity(self) -> str:
        """Get the entity dimension name."""
        entities = find_entity_dimensions(self)
        return entities[0].name  # Guaranteed to be exactly 1

    def get_timestamp_field(self) -> str:
        """Get the event timestamp field name."""
        timestamps = find_event_timestamp_dimensions(self)
        return timestamps[0].name  # Guaranteed to be exactly 1
```

### Option B: Factory Function Pattern

Keep SemanticModel as-is, provide `FeatureView()` factory that returns a SemanticModel:

**Pros:**
- No inheritance complexity
- Existing code works unchanged

**Cons:**
- Less clear IS-A relationship
- Can't use `isinstance(fv, FeatureView)` for type checking
- FeatureView is just a construction helper, not a type

```python
def FeatureView(
    name: str,
    entities: list[str],
    source: DataSource,
    schema: list[Field],
    ttl: timedelta | None = None,
    **kwargs,
) -> SemanticModel:
    """Create a FeatureView (returns SemanticModel with FeatureView constraints).

    This is a factory function that provides Feast-like API for creating
    a SemanticModel with exactly 1 entity and 1 timestamp dimension.
    """
    # Convert Feast concepts to SemanticModel concepts
    # ...

    model = to_semantic_table(source.to_table(), name=name)
    # Add dimensions based on entities and schema
    # Validate it's a valid FeatureView

    return model
```

**Recommendation: Use Option A (Subclass)**

This makes the relationship explicit and type-safe.

## API Examples

### Creating FeatureView (Feast-style)

```python
from boring_semantic_layer import FeatureView, Field, FileSource
from feast.types import Float64, Int64, String
from datetime import timedelta

# Define data source
source = FileSource(
    path="transactions.parquet",
    timestamp_field="transaction_timestamp",
)

# Create FeatureView with Feast-like API
transactions = FeatureView(
    name="transaction_features",
    entities=["user_id"],
    source=source,
    schema=[
        Field(name="spend_7d", dtype=Float64),
        Field(name="spend_30d", dtype=Float64),
        Field(name="transaction_count", dtype=Int64),
    ],
    ttl=timedelta(days=30),
)

# Use as SemanticModel
result = (
    transactions
    .filter(lambda t: t.user_id == 123)
    .aggregate("spend_7d", "spend_30d")
    .execute()
)

# FeatureView-specific operations
entity = transactions.get_entity()  # "user_id"
timestamp = transactions.get_timestamp_field()  # "transaction_timestamp"
```

### Creating FeatureView (Existing boring-semantic-layer style)

```python
from boring_semantic_layer import (
    to_semantic_table,
    entity_dimension,
    time_dimension,
    windowed_measure,
)

# This automatically creates a FeatureView if constraints are met
transactions = (
    to_semantic_table(tbl, name="transaction_features")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id),
        transaction_timestamp=time_dimension(lambda t: t.transaction_timestamp),
    )
    .with_measures(
        spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days"),
        spend_30d=windowed_measure(lambda t: t.amount.sum(), window="30 days"),
    )
)

# Automatically promoted to FeatureView
assert isinstance(transactions, FeatureView)
assert transactions.is_feature_view()
```

### Point-in-Time Joins

```python
# Join two FeatureViews with point-in-time correctness
labels = FeatureView(
    name="fraud_labels",
    entities=["user_id"],
    source=FileSource("labels.parquet", timestamp_field="label_timestamp"),
    schema=[Field(name="is_fraud", dtype=Bool)],
)

# join_asof automatically uses entity + timestamp
training_data = labels.join_asof(transactions)

# This is already implemented and working!
# See: src/boring_semantic_layer/expr.py:395-431
```

### Materialization with Freshness

```python
# Materialize features with automatic TTL from windows
materialized = transactions.materialize(
    backend=con,
    table_name="cache.transaction_features",
    # TTL auto-detected from max window (30 days)
)

# Use in online serving
@app.get("/features/{user_id}")
def get_features(user_id: int):
    return (
        materialized
        .filter(lambda t: t.user_id == user_id)
        .aggregate("spend_7d", "spend_30d")
        .execute()
    )
```

## Implementation Tasks

1. **Create FeatureView class** (extends SemanticModel)
   - Add validation in `__init__`
   - Add `get_entity()` and `get_timestamp_field()` methods
   - Add `from_semantic_model()` classmethod

2. **Auto-promotion in SemanticModel construction**
   - Modify `with_dimensions()` to return FeatureView when constraints met
   - Ensure type is preserved through operations

3. **Add Feast-style factory functions**
   - `FeatureView()` constructor with Feast API
   - `Field()` helper (reuse existing from feast interop)
   - `FileSource()` wrapper

4. **Update documentation**
   - Show FeatureView IS-A SemanticModel relationship
   - Demonstrate both construction styles
   - Highlight feature engineering patterns

## Benefits

1. **Proves the claim**: FeatureView literally inherits from SemanticModel
2. **Type-safe**: `isinstance(fv, SemanticModel)` works
3. **Feast compatibility**: Familiar API for Feast users
4. **Unified mental model**: Everything is a semantic model, FeatureView adds constraints
5. **Leverages existing code**: join_asof, materialize, cached all work

## Summary

By making **FeatureView extend SemanticModel**, we:
- Prove concretely that a FeatureView IS a semantic model
- Provide Feast-compatible API for feature engineering
- Maintain all existing boring-semantic-layer functionality
- Enable type-safe validation and specialized methods
