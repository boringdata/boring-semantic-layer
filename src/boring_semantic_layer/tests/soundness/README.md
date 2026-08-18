# Soundness regression suites

Every test in this directory pins a **confirmed silent-wrong-answer defect**
(or its loud-error replacement) against ground truth, found across a series
of adversarial soundness evaluations (July 2026, "rounds" 2–5). The files are
organized by the invariant they guard, not by when the bug was found; each
module docstring lists the original finding IDs for traceability.

Treat these suites as the behavioral contract of the compiler: a failure here
means a query can return a **wrong number without an error**. Never weaken a
test to make a refactor pass — if a failure looks like a test problem, the
original finding ID in the docstring points to the report describing the
defect it pins.

| File | Invariant | Origin |
|---|---|---|
| `test_fanout_preagg_regressions.py` | Fan-out-safe pre-aggregation: measure re-aggregation, filter routing, group keys, NULL-key re-joins, deferred joins, serialization round-trip stability | round 2 (A/B/C/D/E/F findings) |
| `test_query_input_semantics.py` | `query()` input handling: time filters/grains/ranges, dict filters, dimension shadowing, `limit`/`order_by` semantics | round 4 (R4-2…R4-14) |
| `test_calc_window_totals.py` | Windows over inline base-column reductions keep the user's window spec | round 4 |
| `test_join_many_participation.py` | `join_many` measures respect join participation (orphan rows excluded) | round 4 |
| `test_nest_execution.py` | `nest=` scopes see measure names; no silent row-struct fallback | round 4 |
| `test_join_namespacing_and_inline_totals.py` | Joined-model column namespacing (colliding dims, prefixed group keys) and inline `t.count()` totals | round 5 (R5-1…R5-3) |
| `test_join_lineage_and_grain.py` | Source, grain, and namespace preservation in joins | repairs suite |
| `test_query_repairs.py` | Query-layer semantic repairs | repairs suite |
| `test_nested_array_aggregation.py` | Automatic nested-array aggregation ordering and traversal | repairs suite |
| `test_expr_lineage.py` | Expression-layer join, filter, and grain lineage | repairs suite |

Related suites that stayed outside this directory: `test_preagg_stress.py`
(stress battery), `test_bi_traps.py` (classic BI trap scenarios),
`test_upstream_ibis_pins.py` (upstream behavior pins).
