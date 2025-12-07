# Online Feature Serving with Freshness-Based Caching

## The Problem

For **online feature serving**, we need:
1. Get features for specific `(entity_id, timestamp)` pairs
2. Compute windowed aggregations: `[timestamp - window, timestamp]`
3. Cache results efficiently even though timestamp changes per request
4. Invalidate cache when it becomes stale relative to window size

## Key Insight: Freshness-Based Cache Invalidation

**Online features = Historical features pipeline with freshness-based caching**

Instead of spine-based bucketing, use **CurrentTimeFreshnessStrategy**:
- Cache is valid if `(current_time - cache_creation_time) < tolerance`
- Tolerance = max window duration (or explicit TTL)
- No spine needed - just cache the computation with time-based invalidation

**Example:**
- Feature with `window="7 days"`, tolerance = 7 days
- Cache created at `2024-01-15 10:00`
- Query at `2024-01-15 14:00` → Age = 4 hours < 7 days → Cache hit
- Query at `2024-01-22 11:00` → Age = 7 days 1 hour > 7 days → Cache miss, recompute

## API Design

### Step 1: Define FeatureView with Windows

```python
from boring_semantic_layer import entity_dimension, time_dimension, windowed_measure

transactions = to_semantic_table(txn_tbl, name="transactions").with_dimensions(
    user_id=entity_dimension(lambda t: t.user_id),
    transaction_timestamp=time_dimension(lambda t: t.transaction_timestamp),
).with_measures(
    spend_7d=windowed_measure(
        lambda t: t.amount.sum(),
        window="7 days",
    ),
    spend_30d=windowed_measure(
        lambda t: t.amount.sum(),
        window="30 days",
    ),
    num_transactions_1h=windowed_measure(
        lambda t: t.transaction_id.count(),
        window="1 hour",
    ),
)
```

### Step 2: Cache with Freshness Strategy

```python
# cached() with freshness strategy
cached_features = transactions.cached(
    backend=con,
    table_name="cache.transaction_features",
    strategy="freshness",  # Use time-based invalidation
    ttl=None,  # Auto-detect from max window (30 days in this case)
)

# Or use materialize() as shorthand (same thing)
materialized = transactions.materialize(
    backend=con,
    table_name="cache.transaction_features",
)
```

**What this does:**
1. Wraps the expression with xorq cache
2. Uses `CurrentTimeFreshnessStrategy(tolerance=30 days)` (max window)
3. Cache is valid for 30 days from creation time
4. After 30 days, cache invalidates and recomputes on next query

### Step 3: Query for Online Features

Just filter and execute - caching is transparent:

```python
from datetime import datetime

# Get features for a specific entity at current time
query_timestamp = datetime.now()

features = (
    cached_features
    .filter(lambda t: (
        (t.user_id == 123) &
        (t.transaction_timestamp <= query_timestamp)
    ))
    .aggregate(
        spend_7d=lambda t: t.spend_7d,
        spend_30d=lambda t: t.spend_30d,
        num_transactions_1h=lambda t: t.num_transactions_1h,
    )
    .execute()
)

# Result: {'spend_7d': 1500, 'spend_30d': 5000, 'num_transactions_1h': 3}
```

**How caching works:**
- First query → Cache miss → Compute → Store with timestamp
- Subsequent queries within TTL → Cache hit
- After TTL expires → Cache invalidates → Recompute on next query

## For Non-Windowed Features: Explicit TTL

For features without windows, specify TTL explicitly:

```python
# Snapshot features without time windows
user_profiles = (
    to_semantic_table(users_tbl, name="users")
    .with_dimensions(user_id=entity_dimension(lambda t: t.user_id))
    .with_measures(
        account_age_days=lambda t: (ibis.now() - t.created_at).days,
        is_premium=lambda t: t.subscription_tier == "premium",
    )
)

# Cache with explicit TTL
cached_profiles = user_profiles.cached(
    backend=con,
    table_name="cache.user_profiles",
    strategy="freshness",
    ttl=timedelta(hours=24),  # Refresh daily
)
```

## Implementation

### 1. Define windowed_measure()

```python
from attr import frozen
from datetime import timedelta

@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    window: str | None = None  # "7 days", "1 hour"

def windowed_measure(
    expr: Callable[[ir.Table], ir.Value],
    window: str,
    description: str | None = None,
) -> Measure:
    """Create a windowed measure.

    Args:
        expr: Aggregation expression
        window: Window duration (e.g., "7 days", "1 hour")
        description: Human-readable description
    """
    return Measure(
        expr=expr,
        window=window,
        description=description,
    )
```

### 2. Implement cached() with Freshness Strategy

```python
class SemanticModel:
    def cached(
        self,
        backend: BaseBackend,
        table_name: str,
        strategy: str = "freshness",
        ttl: timedelta | None = None,
    ):
        """Cache expression with invalidation strategy.

        Args:
            backend: Database backend
            table_name: Cache table/directory name
            strategy:
                - "freshness": Invalidate based on time since cache creation
                - "modification": Invalidate based on source table changes (default xorq)
            ttl: Cache TTL for freshness strategy. Auto-detected from windowed measures if None.

        Returns:
            Cached xorq expression

        Examples:
            # Auto-detect TTL from windowed measures
            >>> cached = features.cached(
            ...     backend=con,
            ...     table_name="cache.features",
            ...     strategy="freshness"
            ... )

            # Explicit TTL
            >>> cached = features.cached(
            ...     backend=con,
            ...     table_name="cache.profiles",
            ...     strategy="freshness",
            ...     ttl=timedelta(hours=24)
            ... )
        """
        from xorq.caching import ParquetStorage

        storage = ParquetStorage(source=backend, relative_path=table_name)

        if strategy == "freshness":
            # Auto-detect TTL from windowed measures
            if ttl is None:
                ttl = self._get_max_window_duration()
                if ttl == timedelta(0):
                    raise ValueError(
                        "Cannot determine TTL: no windowed measures found. "
                        "Pass ttl parameter explicitly."
                    )

            # Use CurrentTimeFreshnessStrategy
            from xorq.caching import CurrentTimeFreshnessStrategy
            cache_strategy = CurrentTimeFreshnessStrategy(tolerance=ttl)

            return self.to_untagged().cache(storage=storage, strategy=cache_strategy)
        else:
            # Use default xorq ModificationTimeStrategy
            return self.to_untagged().cache(storage=storage)

    def materialize(
        self,
        backend: BaseBackend,
        table_name: str,
        ttl: timedelta | None = None,
    ):
        """Materialize features with freshness-based caching.

        This is syntactic sugar for .cached(strategy="freshness").

        Args:
            backend: Database backend
            table_name: Cache table/directory name
            ttl: Cache TTL. Auto-detected from windowed measures if None.

        Returns:
            Cached xorq expression

        Examples:
            >>> materialized = features.materialize(
            ...     backend=con,
            ...     table_name="cache.features"
            ... )
        """
        return self.cached(
            backend=backend,
            table_name=table_name,
            strategy="freshness",
            ttl=ttl,
        )

    def _get_max_window_duration(self) -> timedelta:
        """Extract maximum window duration from all windowed measures."""
        measures = self.get_measures()
        max_window = timedelta(0)

        for measure in measures.values():
            if hasattr(measure, 'window') and measure.window:
                window_duration = self._parse_window_duration(measure.window)
                max_window = max(max_window, window_duration)

        return max_window

    def _parse_window_duration(self, window: str) -> timedelta:
        """Parse window string like '7 days', '1 hour' to timedelta."""
        import re
        match = re.match(r'(\d+)\s*(day|days|hour|hours|minute|minutes)', window.lower())
        if not match:
            raise ValueError(f"Invalid window format: {window}")

        value = int(match.group(1))
        unit = match.group(2)

        if 'day' in unit:
            return timedelta(days=value)
        elif 'hour' in unit:
            return timedelta(hours=value)
        elif 'minute' in unit:
            return timedelta(minutes=value)
        else:
            raise ValueError(f"Unsupported time unit: {unit}")
```

## How CurrentTimeFreshnessStrategy Works

Based on xorq's caching system, the freshness strategy would work like:

```python
@frozen
class CurrentTimeFreshnessStrategy(CacheStrategy):
    """Invalidate cache based on time since cache creation."""

    tolerance: timedelta  # How long cache stays valid

    def is_valid(self, cache_metadata: dict) -> bool:
        """Check if cache is still fresh.

        Args:
            cache_metadata: {"created_at": datetime, ...}

        Returns:
            True if cache age < tolerance
        """
        created_at = cache_metadata["created_at"]
        current_time = datetime.now()
        cache_age = current_time - created_at

        return cache_age < self.tolerance
```

## Example Usage

### Scenario 1: E-commerce Product Recommendations

```python
# Define user activity features
user_activity = (
    to_semantic_table(page_views, name="user_activity")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id),
        view_timestamp=time_dimension(lambda t: t.view_timestamp),
    )
    .with_measures(
        # Recent activity
        views_1h=windowed_measure(lambda t: t.page_id.count(), window="1 hour"),
        # Longer-term patterns
        views_7d=windowed_measure(lambda t: t.page_id.count(), window="7 days"),
        unique_categories_30d=windowed_measure(lambda t: t.category.nunique(), window="30 days"),
    )
)

# Cache with freshness (TTL = 30 days from max window)
cached = user_activity.materialize(
    backend=con,
    table_name="cache.user_activity"
)

# Online serving endpoint
@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: int):
    features = (
        cached
        .filter(lambda t: t.user_id == user_id)
        .aggregate(
            views_1h=lambda t: t.views_1h,
            views_7d=lambda t: t.views_7d,
            unique_categories_30d=lambda t: t.unique_categories_30d,
        )
        .execute()
    )

    return recommend_products(features)
```

### Scenario 2: Fraud Detection

```python
# Real-time fraud features
fraud_features = (
    to_semantic_table(transactions, name="fraud_features")
    .with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id),
        transaction_timestamp=time_dimension(lambda t: t.transaction_timestamp),
    )
    .with_measures(
        # Very recent activity
        transaction_count_10m=windowed_measure(
            lambda t: t.transaction_id.count(),
            window="10 minutes",
        ),
        total_amount_10m=windowed_measure(
            lambda t: t.amount.sum(),
            window="10 minutes",
        ),
        # Hourly patterns
        transaction_count_1h=windowed_measure(
            lambda t: t.transaction_id.count(),
            window="1 hour",
        ),
    )
)

# Cache with short TTL (1 hour from max window)
cached = fraud_features.materialize(
    backend=con,
    table_name="cache.fraud_features"
)

# Check transaction
@app.post("/check_transaction")
def check_transaction(user_id: int, amount: float):
    features = (
        cached
        .filter(lambda t: t.user_id == user_id)
        .aggregate(...)
        .execute()
    )

    return fraud_model.predict(features)
```

## Key Benefits

1. **Simple**: No spine, no time bucketing - just freshness-based invalidation
2. **Declarative**: TTL auto-detected from window durations
3. **Transparent**: Caching is invisible to query logic
4. **Flexible**: Override TTL for custom freshness requirements
5. **Composable**: Works with standard xorq caching infrastructure

## Summary

**materialize() = cached(strategy="freshness")**

- Windowed features automatically determine TTL from max window
- Non-windowed features require explicit TTL
- Cache invalidates based on time since creation
- No spine or time bucketing needed - xorq handles everything
