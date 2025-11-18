# Sessionized Data

Analyze time-series events grouped into sessions based on activity gaps. This pattern identifies and aggregates user or system behavior within discrete time-bounded sessions.

## Overview

The sessionization pattern allows you to:

- Define session boundaries based on inactivity timeouts
- Group sequential events into logical sessions
- Calculate session-level metrics (duration, event count, conversion)
- Handle session spanning across multiple time periods

## Setup

Let's create user activity data with timestamps:

```setup_raw_data
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table

# Create user activity events with minute offsets instead of timestamps
activity_data = ibis.memtable({
    "user_id": ["user1", "user1", "user1", "user1", "user2", "user2", "user2", "user3", "user3", "user3", "user3", "user3"],
    "minute_offset": [0, 5, 10, 45, 2, 40, 42, 1, 3, 7, 50, 52],  # Minutes from start
    "page_url": ["/home", "/products", "/cart", "/checkout", "/home", "/products", "/cart",
                 "/home", "/about", "/products", "/home", "/contact"],
    "action": ["view", "view", "view", "purchase", "view", "view", "view",
               "view", "view", "view", "view", "view"]
})
```

<collapsedcodeblock code-block="setup_raw_data" title="Setup: Create Raw Activity Data"></collapsedcodeblock>

Now create a semantic table with dimensions and measures:

```semantic_table_def
from boring_semantic_layer import to_semantic_table

activity_st = (
    to_semantic_table(activity_data, name="activity")
    .with_dimensions(
        user_id=lambda t: t.user_id,
        minute_offset=lambda t: t.minute_offset,
        page_url=lambda t: t.page_url,
        action=lambda t: t.action
    )
    .with_measures(
        event_count=lambda t: t.count(),
        unique_users=lambda t: t.user_id.nunique()
    )
)
```

## Identify Session Boundaries

Use window functions to identify session starts based on inactivity gaps:

```query_session_boundaries
from ibis import _

result = (
    activity_st
    .group_by("user_id", "minute_offset", "page_url", "action")
    .aggregate()
    .mutate(
        # Calculate time since previous event for same user
        prev_minute=lambda t: t.minute_offset.lag().over(
            group_by="user_id",
            order_by=t.minute_offset
        ),
        # Calculate minutes since last event
        minutes_since_last=lambda t: t.minute_offset - t.prev_minute,
        # Mark session start (>30 min gap or first event)
        is_session_start=lambda t: (t.minutes_since_last > 30) | t.prev_minute.isnull()
    )
    .order_by(_.user_id, _.minute_offset)
)
```

<bslquery code-block="query_session_boundaries" />

## Assign Session IDs

Create session identifiers by counting session starts:

```query_with_session_ids
from ibis import _

result = (
    activity_st
    .group_by("user_id", "minute_offset", "page_url", "action")
    .aggregate()
    .mutate(
        prev_minute=lambda t: t.minute_offset.lag().over(
            group_by="user_id",
            order_by=t.minute_offset
        ),
        minutes_since_last=lambda t: t.minute_offset - t.prev_minute,
        is_session_start=lambda t: (t.minutes_since_last > 30) | t.prev_minute.isnull(),
        # Cumulative sum of session starts gives session ID
        session_id=lambda t: t.is_session_start.cast("int32").sum().over(
            group_by="user_id",
            order_by=t.minute_offset,
            rows=(None, 0)  # Cumulative sum
        )
    )
    .order_by(_.user_id, _.minute_offset)
)
```

<bslquery code-block="query_with_session_ids" />

## Calculate Session Metrics

Aggregate events by session to get session-level metrics:

```query_session_metrics
from ibis import _

result = (
    activity_st
    .group_by("user_id", "minute_offset", "action")
    .aggregate()
    .mutate(
        prev_minute=lambda t: t.minute_offset.lag().over(
            group_by="user_id",
            order_by=t.minute_offset
        ),
        minutes_since_last=lambda t: t.minute_offset - t.prev_minute,
        is_session_start=lambda t: (t.minutes_since_last > 30) | t.prev_minute.isnull(),
        session_id=lambda t: t.is_session_start.cast("int32").sum().over(
            group_by="user_id",
            order_by=t.minute_offset,
            rows=(None, 0)
        )
    )
    .group_by("user_id", "session_id")
    .aggregate(
        events_in_session=lambda t: t.count(),
        session_start_min=lambda t: t.minute_offset.min(),
        session_end_min=lambda t: t.minute_offset.max(),
        has_purchase=lambda t: (t.action == "purchase").any()
    )
    .mutate(
        session_duration_min=lambda t: (t.session_end_min - t.session_start_min)
    )
    .order_by(_.user_id, _.session_id)
)
```

<bslquery code-block="query_session_metrics" />

## User-Level Session Summary

Summarize sessions per user:

```query_user_summary
from ibis import _

result = (
    activity_st
    .group_by("user_id", "minute_offset", "action")
    .aggregate()
    .mutate(
        prev_minute=lambda t: t.minute_offset.lag().over(
            group_by="user_id",
            order_by=t.minute_offset
        ),
        minutes_since_last=lambda t: t.minute_offset - t.prev_minute,
        is_session_start=lambda t: (t.minutes_since_last > 30) | t.prev_minute.isnull(),
        session_id=lambda t: t.is_session_start.cast("int32").sum().over(
            group_by="user_id",
            order_by=t.minute_offset,
            rows=(None, 0)
        )
    )
    .group_by("user_id", "session_id")
    .aggregate(
        events_in_session=lambda t: t.count(),
        has_purchase=lambda t: (t.action == "purchase").any()
    )
    .group_by("user_id")
    .aggregate(
        total_sessions=lambda t: t.count(),
        total_events=lambda t: t.events_in_session.sum(),
        sessions_with_purchase=lambda t: t.has_purchase.cast("int32").sum(),
        avg_events_per_session=lambda t: t.events_in_session.mean().round(2)
    )
    .mutate(
        conversion_rate=lambda t: (t.sessions_with_purchase / t.total_sessions * 100).round(2)
    )
    .order_by(_.total_events.desc())
)
```

<bslquery code-block="query_user_summary" />

## Use Cases

**Web Analytics**: Group user page views and interactions into sessions, with a session ending after 30 minutes of inactivity. Calculate metrics like session duration, pages per session, and conversion rate.

**IoT Device Monitoring**: Sessionize sensor readings to identify distinct usage periods and calculate metrics like average session length and activity intensity.

**Application Usage Tracking**: Analyze how users interact with applications by grouping activities into sessions, identifying drop-off points, and measuring engagement patterns.

## Key Takeaways

- Use `lag()` window function to find time since previous event
- Compare time gaps to session timeout threshold (e.g., 30 minutes)
- Use cumulative sum of session starts to assign session IDs
- Calculate session metrics like duration, event count, and conversions
- Aggregate sessions to user level for summary statistics

## Next Steps

- Learn about [Indexing](/advanced/indexing) for trend analysis
- Explore [Bucketing](/advanced/bucketing) to categorize session durations
