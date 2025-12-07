"""Example demonstrating that FeatureView IS-A SemanticModel.

This example shows:
1. Creating a SemanticModel
2. Converting to FeatureView (when constraints are met)
3. Using FeatureView as a SemanticModel (inheritance)
4. Converting back to SemanticModel
5. FeatureView-specific operations
"""

from datetime import timedelta

import ibis
from boring_semantic_layer import (
    FeatureView,
    SemanticModel,
    entity_dimension,
    time_dimension,
    to_semantic_table,
)

# Create a sample dataset
con = ibis.connect("duckdb://")
transactions = con.create_table(
    "transactions",
    {
        "user_id": [1, 1, 2, 2, 3],
        "transaction_id": [101, 102, 103, 104, 105],
        "amount": [50.0, 75.0, 100.0, 25.0, 200.0],
        "transaction_timestamp": [
            "2024-01-01 10:00:00",
            "2024-01-02 14:00:00",
            "2024-01-01 12:00:00",
            "2024-01-03 09:00:00",
            "2024-01-02 16:00:00",
        ],
    },
)

print("=" * 80)
print("DEMONSTRATING: FeatureView IS-A SemanticModel")
print("=" * 80)

# Step 1: Create a SemanticModel with FeatureView constraints
print("\n1. Creating SemanticModel with entity + timestamp dimensions...")
semantic_model = (
    to_semantic_table(transactions, name="transaction_features")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id, "User identifier"),
        transaction_timestamp=time_dimension(
            lambda t: t.transaction_timestamp,
            "Transaction event timestamp",
        ),
    )
    .with_measures(
        total_spend=lambda t: t.amount.sum(),
        transaction_count=lambda t: t.transaction_id.count(),
    )
)

print(f"   Created: {type(semantic_model).__name__}")
print(f"   Name: {semantic_model.name}")
print(f"   Is FeatureView? {semantic_model.is_feature_view()}")

# Step 2: Convert SemanticModel to FeatureView
print("\n2. Converting SemanticModel to FeatureView...")
feature_view = FeatureView.from_semantic_model(semantic_model)

print(f"   Converted: {type(feature_view).__name__}")
print(f"   Name: {feature_view.name}")
print(f"   Representation: {feature_view}")

# Step 3: Prove FeatureView IS-A SemanticModel
print("\n3. Proving FeatureView IS-A SemanticModel (inheritance)...")
print(f"   isinstance(feature_view, FeatureView): {isinstance(feature_view, FeatureView)}")
print(f"   isinstance(feature_view, SemanticModel): {isinstance(feature_view, SemanticModel)}")
print(f"   ✓ FeatureView inherits from SemanticModel!")

# Step 4: Use FeatureView like a SemanticModel
print("\n4. Using FeatureView with SemanticModel methods...")

# Filter (SemanticModel method)
filtered = feature_view.filter(lambda t: t.user_id == 1)
print(f"   After filter: {type(filtered).__name__}")

# Aggregate (SemanticModel method)
aggregated = feature_view.aggregate("total_spend", "transaction_count")
print(f"   After aggregate: {type(aggregated).__name__}")

# Get dimensions (SemanticModel method)
dimensions = feature_view.get_dimensions()
print(f"   Dimensions: {list(dimensions.keys())}")

# Get measures (SemanticModel method)
measures = feature_view.get_measures()
print(f"   Measures: {list(measures.keys())}")

# Step 5: Use FeatureView-specific methods
print("\n5. Using FeatureView-specific methods...")

# Get entity
entity = feature_view.get_entity()
print(f"   Entity dimension: {entity}")

# Get timestamp field
timestamp = feature_view.get_timestamp_field()
print(f"   Timestamp field: {timestamp}")

# Get TTL (returns None because we don't have windowed measures)
ttl = feature_view.get_ttl()
print(f"   TTL: {ttl}")

# Step 6: Show that operations preserve FeatureView type through SemanticModel operations
print("\n6. Type preservation through operations...")
print(f"   feature_view type: {type(feature_view).__name__}")
print(f"   feature_view.filter(...) type: {type(filtered).__name__}")
print(f"   Note: Operations return SemanticModel (not FeatureView) to avoid constraint violations")

# Step 7: Try creating FeatureView with invalid constraints (should fail)
print("\n7. Validating FeatureView constraints...")
try:
    invalid_model = (
        to_semantic_table(transactions, name="invalid")
        .with_dimensions(
            user_id=entity_dimension(lambda t: t.user_id),
            # Missing timestamp dimension!
        )
    )
    invalid_fv = FeatureView.from_semantic_model(invalid_model)
    print("   ✗ Should have raised ValueError!")
except ValueError as e:
    print(f"   ✓ Correctly rejected: {str(e)[:60]}...")

# Step 8: Summary
print("\n" + "=" * 80)
print("SUMMARY: FeatureView proves the claim")
print("=" * 80)
print("✓ FeatureView extends SemanticModel (inheritance)")
print("✓ FeatureView adds strict constraints (1 entity + 1 timestamp)")
print("✓ FeatureView adds specialized methods (get_entity, get_timestamp_field, etc.)")
print("✓ FeatureView can be used anywhere SemanticModel is expected")
print("✓ SemanticModel can be converted to FeatureView when constraints are met")
print("\nConclusion: A FeatureView IS-A SemanticModel! 🎉")
