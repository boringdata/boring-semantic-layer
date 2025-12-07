# Materialize = Cached with Freshness Strategy

## Core Concept

**materialize() is syntactic sugar for .cached(strategy="freshness")**

Instead of managing materialized tables manually, use xorq's cache with time-based invalidation:
- Cache invalidates when `(current_time - cache_creation_time) > ttl`
- TTL auto-detected from max window duration
- No spine, no manual refresh - xorq handles everything

## API Design

### Basic Usage

```python
from boring_semantic_layer import entity_dimension, time_dimension, windowed_measure

# Define features with windows
transactions = to_semantic_table(txn_tbl, name="transactions").with_dimensions(
    user_id=entity_dimension(lambda t: t.user_id),
    transaction_timestamp=time_dimension(lambda t: t.transaction_timestamp),
).with_measures(
    spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days"),
    spend_30d=windowed_measure(lambda t: t.amount.sum(), window="30 days"),
)

# Option 1: Use materialize() (recommended)
materialized = transactions.materialize(
    backend=con,
    table_name="cache.transaction_features",
)

# Option 2: Explicit cached() call (same thing)
materialized = transactions.cached(
    backend=con,
    table_name="cache.transaction_features",
    strategy="freshness",  # Time-based invalidation
    ttl=None,  # Auto-detect from max window (30 days)
)
```

## How It Works

### 1. Auto-Detect TTL from Windows

```python
def _get_max_window_duration(self) -> timedelta:
    """Extract maximum window duration from all windowed measures."""
    measures = self.get_measures()
    max_window = timedelta(0)

    for measure in measures.values():
        if hasattr(measure, 'window') and measure.window:
            window_duration = self._parse_window_duration(measure.window)
            max_window = max(max_window, window_duration)

    return max_window

# Example:
# measures = {
#     "spend_7d": windowed_measure(..., window="7 days"),
#     "spend_30d": windowed_measure(..., window="30 days"),
# }
# → max_window = 30 days
```

### 2. Cache with Freshness Strategy

```python
def materialize(self, backend, table_name, ttl=None):
    """Materialize = cached with freshness strategy."""
    from xorq.caching import ParquetStorage, CurrentTimeFreshnessStrategy

    # Auto-detect TTL if not provided
    if ttl is None:
        ttl = self._get_max_window_duration()
        if ttl == timedelta(0):
            raise ValueError("No windowed measures found, must specify ttl")

    # Create freshness-based cache
    storage = ParquetStorage(source=backend, relative_path=table_name)
    strategy = CurrentTimeFreshnessStrategy(tolerance=ttl)

    # Cache the expression
    return self.to_untagged().cache(storage=storage, strategy=strategy)
```

### 3. CurrentTimeFreshnessStrategy

```python
@frozen
class CurrentTimeFreshnessStrategy(CacheStrategy):
    """Invalidate cache based on time since creation."""

    tolerance: timedelta  # TTL

    def is_valid(self, cache_metadata: dict) -> bool:
        """Check if cache is still fresh."""
        created_at = cache_metadata["created_at"]
        current_time = datetime.now()
        cache_age = current_time - created_at

        return cache_age < self.tolerance

    def get_key(self, expr: ir.Expr):
        """Standard tokenization (same as ModificationTimeStrategy)."""
        return expr.ls.tokenized
```

## Cache Invalidation Examples

### Example 1: 7-Day Window

```python
features = transactions.with_measures(
    spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days")
).materialize(backend=con, table_name="cache.features")

# Timeline:
# 2024-01-15 10:00 → First query → Cache miss → Compute → Store (created_at = 10:00)
# 2024-01-15 14:00 → Query → Cache age = 4 hours < 7 days → Cache hit
# 2024-01-20 10:00 → Query → Cache age = 5 days < 7 days → Cache hit
# 2024-01-22 11:00 → Query → Cache age = 7 days 1 hour > 7 days → Cache miss → Recompute
```

### Example 2: Multiple Windows

```python
features = transactions.with_measures(
    spend_1h=windowed_measure(lambda t: t.amount.sum(), window="1 hour"),
    spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days"),
).materialize(backend=con, table_name="cache.features")

# TTL = max(1 hour, 7 days) = 7 days
# Cache stays valid for 7 days (most conservative)
```

### Example 3: Explicit TTL Override

```python
# Short-lived cache even with long windows
features = transactions.with_measures(
    spend_30d=windowed_measure(lambda t: t.amount.sum(), window="30 days")
).materialize(
    backend=con,
    table_name="cache.features",
    ttl=timedelta(hours=1),  # Override: refresh hourly instead of monthly
)
```

## For Non-Windowed Features

Without windowed measures, you must provide explicit TTL:

```python
# Snapshot features (no time windows)
user_profiles = (
    to_semantic_table(users_tbl, name="users")
    .with_dimensions(user_id=entity_dimension(lambda t: t.user_id))
    .with_measures(
        account_age_days=lambda t: (ibis.now() - t.created_at).days,
        is_premium=lambda t: t.subscription_tier == "premium",
    )
)

# Must specify TTL explicitly
cached = user_profiles.materialize(
    backend=con,
    table_name="cache.user_profiles",
    ttl=timedelta(hours=24),  # Refresh daily
)

# Or use cached() directly
cached = user_profiles.cached(
    backend=con,
    table_name="cache.user_profiles",
    strategy="freshness",
    ttl=timedelta(hours=24),
)
```

## Comparison: Freshness vs Modification Strategy

### Freshness Strategy (materialize)
- Invalidates based on **time since cache creation**
- Independent of source data changes
- Predictable refresh schedule
- Good for: Online features, windowed aggregations

```python
cached = features.cached(strategy="freshness", ttl=timedelta(hours=1))
# Cache valid for 1 hour regardless of source changes
```

### Modification Strategy (default xorq)
- Invalidates based on **source table modification time**
- Cache updates when source data changes
- Unpredictable refresh timing
- Good for: Reports, dashboards, batch pipelines

```python
cached = features.cached(strategy="modification")
# Cache invalidates when source table changes
```

## Implementation Checklist

1. ✅ Add `window` field to Measure class
2. ✅ Implement `windowed_measure()` helper
3. ✅ Implement `_get_max_window_duration()`
4. ✅ Implement `_parse_window_duration()`
5. ⬜ Implement `CurrentTimeFreshnessStrategy` in xorq (or check if exists)
6. ⬜ Implement `.cached()` method with strategy parameter
7. ⬜ Implement `.materialize()` as wrapper for `.cached(strategy="freshness")`
8. ⬜ Add tests for TTL auto-detection
9. ⬜ Add tests for cache invalidation timing

## Usage Patterns

### Pattern 1: Online Feature Serving

```python
# Define features
features = (
    to_semantic_table(events, name="features")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id),
        event_timestamp=time_dimension(lambda t: t.event_timestamp),
    )
    .with_measures(
        events_1h=windowed_measure(lambda t: t.event_id.count(), window="1 hour"),
        events_24h=windowed_measure(lambda t: t.event_id.count(), window="24 hours"),
    )
)

# Materialize with auto-detected TTL (24 hours)
cached = features.materialize(backend=con, table_name="cache.features")

# Serve online
@app.get("/features/{user_id}")
def get_features(user_id: int):
    return cached.filter(lambda t: t.user_id == user_id).execute()
```

### Pattern 2: Training Data Generation

```python
# Historical features for training
training_features = (
    transactions
    .with_measures(
        spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days"),
        spend_30d=windowed_measure(lambda t: t.amount.sum(), window="30 days"),
    )
    .materialize(
        backend=con,
        table_name="cache.training_features",
        ttl=timedelta(days=1),  # Refresh daily for training
    )
)

# Join with labels
training_data = labels.join_asof(training_features)
```

### Pattern 3: Mixed Windows

```python
# Different refresh requirements
features = (
    transactions
    .with_measures(
        # Hourly features
        spend_1h=windowed_measure(lambda t: t.amount.sum(), window="1 hour"),
        # Daily features
        spend_24h=windowed_measure(lambda t: t.amount.sum(), window="24 hours"),
        # Weekly features
        spend_7d=windowed_measure(lambda t: t.amount.sum(), window="7 days"),
    )
    .materialize(
        backend=con,
        table_name="cache.features",
        # TTL = max(1h, 24h, 7d) = 7 days
    )
)
```

## Advantages Over Manual Materialization

1. **Automatic Refresh**: No manual refresh jobs needed
2. **Declarative TTL**: Auto-detected from feature semantics
3. **No Table Management**: Xorq handles storage and cleanup
4. **Transparent Caching**: Same query API, caching is invisible
5. **Flexible Strategies**: Switch between freshness/modification easily
6. **Composable**: Works with standard xorq infrastructure

## Key Insights

1. **materialize() = cached(strategy="freshness")**
   - Just syntactic sugar
   - No special semantics

2. **TTL = max(window_durations)**
   - Conservative approach
   - All features stay valid together

3. **No spine needed**
   - Freshness strategy handles invalidation
   - Much simpler than spine + time bucketing

4. **CurrentTimeFreshnessStrategy is the key**
   - Polls current time
   - Compares to cache creation time
   - Simple and predictable
