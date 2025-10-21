# BSL v2 API Design: Nested Views (Malloy-inspired)

## Overview

This document proposes the API design for nested views in BSL v2, inspired by Malloy's `nest:` feature.

## Core Principles

1. **Pythonic** - Use Python idioms (lambdas, kwargs, method chaining)
2. **Type-safe** - Leverage Python type hints
3. **Composable** - Support arbitrary nesting depth
4. **Clear** - Obvious what's happening at each level

## Proposed API

### Option A: Dict-based nest parameter (Simple, declarative)

```python
# Malloy:
# run: airports -> {
#   group_by: state
#   aggregate: airport_count
#   nest: by_facility is {
#     group_by: fac_type
#     aggregate: airport_count
#   }
# }

# BSL v2:
result = (
    airports_st
    .group_by("state")
    .aggregate(
        "airport_count",
        nest={
            "by_facility": {
                "group_by": ["fac_type"],
                "aggregate": ["airport_count"]
            }
        }
    )
)
```

**Pros:**
- Simple, declarative syntax
- Easy to serialize/deserialize
- No lambda complexity

**Cons:**
- Doesn't leverage SemanticTable's fluent API
- String-based specs lose type safety
- Harder to compose with existing queries

---

### Option B: Lambda-based nest parameter (Recommended)

```python
# Malloy:
# run: airports -> {
#   group_by: state
#   aggregate: airport_count
#   nest: by_facility is {
#     group_by: fac_type
#     aggregate: airport_count
#   }
# }

# BSL v2:
result = (
    airports_st
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            by_facility=lambda t: (
                t.group_by("fac_type")
                .aggregate("airport_count")
            )
        )
    )
)
```

**Two-level nesting:**
```python
# Malloy:
# run: airports -> {
#   group_by: state
#   aggregate: airport_count
#   nest: top_5_counties is {
#     limit: 5
#     group_by: county
#     aggregate: airport_count
#     nest: by_facility is {
#       group_by: fac_type
#       aggregate: airport_count
#     }
#   }
# }

# BSL v2:
result = (
    airports_st
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            top_5_counties=lambda t: (
                t.group_by("county")
                .aggregate("airport_count")
                .limit(5)
                .nest(
                    by_facility=lambda t: (
                        t.group_by("fac_type")
                        .aggregate("airport_count")
                    )
                )
            )
        )
    )
)
```

**With filters at nested levels:**
```python
# Malloy:
# nest: major_facilities is {
#   where: major = 'Y'
#   group_by: name
# }

# BSL v2:
result = (
    airports_st
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            major_facilities=lambda t: (
                t.filter(lambda x: x.major == 'Y')
                .group_by("name")
                .aggregate("airport_count")
            )
        )
    )
)
```

**Pros:**
- Leverages existing SemanticTable API
- Type-safe (IDE autocomplete works)
- Composable with all existing methods
- Pythonic (lambdas are idiomatic)

**Cons:**
- Slightly more verbose than Malloy
- Lambda inside lambda can be confusing
- Harder to serialize

---

### Option C: Separate .nest() method (Most explicit)

```python
# BSL v2:
result = (
    airports_st
    .group_by("state")
    .aggregate("airport_count")
    .nest(
        by_facility=lambda t: (
            t.group_by("fac_type")
            .aggregate("airport_count")
        )
    )
)
```

**Pros:**
- Very explicit
- Separates concerns (aggregate vs nest)
- Easier to read

**Cons:**
- .nest() would need to return a modified version of the already-aggregated result
- Doesn't match Malloy's unified syntax
- More API surface area

---

## Recommendation: Option B (Lambda-based)

### Full Example

```python
from boring_semantic_layer.semantic_api import to_semantic_table

# Define semantic table
airports_st = (
    to_semantic_table(airports_tbl, "airports")
    .with_dimensions(
        state=lambda t: t.state,
        county=lambda t: t.county,
        fac_type=lambda t: t.fac_type,
        major=lambda t: t.major
    )
    .with_measures(
        airport_count=lambda t: t.count()
    )
)

# Query with nested views
result = (
    airports_st
    .filter(lambda t: t.state.isin(['CA', 'NY', 'MN']))
    .group_by("state")
    .aggregate(
        "airport_count",
        nest=dict(
            top_5_counties=lambda t: (
                t.group_by("county")
                .aggregate("airport_count")
                .limit(5)
                .nest(
                    major_facilities=lambda t: (
                        t.filter(lambda x: x.major == 'Y')
                        .group_by("name")
                    ),
                    by_facility=lambda t: (
                        t.group_by("fac_type")
                        .aggregate("airport_count")
                    )
                )
            )
        )
    )
    .execute()
)
```

### Implementation Notes

1. **Lambda receives scoped table** - The lambda `lambda t:` receives a scoped version of the semantic table with the current aggregation context
2. **Nest returns SemanticTable** - Each nested query is a full semantic table query
3. **Compilation to Ibis** - Uses `ibis.struct()` and `.collect()` under the hood
4. **Result structure** - Returns nested dict/list structures (or DataFrame with struct columns)

### Type Signature

```python
from typing import Callable, Dict, Any

class SemanticTable:
    def aggregate(
        self,
        *measure_names: str,
        nest: Dict[str, Callable[[SemanticTable], Any]] | None = None,
        **aliased: str
    ) -> SemanticTable:
        """
        Aggregate measures with optional nested views.

        Parameters
        ----------
        *measure_names : str
            Names of measures to aggregate
        nest : dict of str -> callable, optional
            Nested view specifications. Each key is the name of the nested view,
            and each value is a lambda that receives a SemanticTable and returns
            an aggregation query.
        **aliased : str
            Measure name aliases

        Examples
        --------
        >>> result = (
        ...     airports_st
        ...     .group_by("state")
        ...     .aggregate(
        ...         "airport_count",
        ...         nest=dict(
        ...             by_facility=lambda t: (
        ...                 t.group_by("fac_type")
        ...                 .aggregate("airport_count")
        ...             )
        ...         )
        ...     )
        ... )
        """
        ...

    def nest(self, **nested_views: Callable[[SemanticTable], Any]) -> SemanticTable:
        """
        Add nested views to an aggregated query (alternative API).

        This is syntactic sugar for passing nest= to aggregate().

        Parameters
        ----------
        **nested_views : callable
            Named nested view specifications

        Examples
        --------
        >>> result = (
        ...     airports_st
        ...     .group_by("state")
        ...     .aggregate("airport_count")
        ...     .nest(
        ...         by_facility=lambda t: t.group_by("fac_type").aggregate("airport_count")
        ...     )
        ... )
        """
        ...
```

## Comparison with Malloy

| Feature | Malloy | BSL v2 |
|---------|--------|--------|
| **Nested views** | `nest: name is {...}` | `nest=dict(name=lambda t: ...)` |
| **Nesting depth** | Unlimited | Unlimited |
| **Filters** | `where: condition` | `.filter(lambda t: condition)` |
| **Limits** | `limit: 5` | `.limit(5)` |
| **Measures** | `aggregate: measure` | `.aggregate("measure")` |
| **Dimensions** | `group_by: dim` | `.group_by("dim")` |
| **Named views** | Define in source | Define as Python variables |

## Alternative: Named Nested Views

```python
# Define reusable nested views
by_facility = lambda t: t.group_by("fac_type").aggregate("airport_count")
major_facilities = lambda t: t.filter(lambda x: x.major == 'Y').group_by("name")

top_5_counties = lambda t: (
    t.group_by("county")
    .aggregate("airport_count")
    .limit(5)
    .nest(
        major_facilities=major_facilities,
        by_facility=by_facility
    )
)

# Use in query
result = (
    airports_st
    .group_by("state")
    .aggregate("airport_count", nest=dict(top_5_counties=top_5_counties))
)
```

## Next Steps

1. Implement `nest` parameter in `.aggregate()`
2. Add `.nest()` convenience method
3. Handle ibis struct/collect compilation
4. Add comprehensive tests
5. Document patterns and examples
