# BSL Refactor Review — 2026-05-08

A pre-ADR survey of structural issues in the semantic-model implementation, framed from a relational-algebra and software-engineering lens. Companion to ADR 0001; intended to seed follow-up ADRs.

## Algebraic shape

The op tree is structurally clean — leaf (`SemanticTableOp`), unary (`Filter/Project/GroupBy/Aggregate/OrderBy/Limit/Mutate/Unnest`), binary (`Join`) — but compositionality leaks in three specific places:

1. **σ ∘ γ scope ambiguity** (`ops.py:2304`). `SemanticAggregateOp.to_untagged()` switches behavior on `isinstance(self.source, SemanticFilterOp)` to skip dimension enrichment. That's a special case substituting for an invariant. Operators don't declare a "scope" (pre-agg dim symbols vs. post-agg measure symbols), so legal vs. illegal compositions can't be checked statically — only crashed at compile.

2. **⋈ ∘ γ pre-agg fragility** (`ops.py:2417–2700`). `_to_untagged_with_preagg` compiles calc measures *before* per-table partitioning. Cross-table calc measures (e.g. `t1.revenue / t2.cnt`) are not commutative with this rewrite; the chasm-fallback `try/except` at `2481–2482` is the planner admitting it. The 280-line method bundles four passes — filter-push, partition, preagg, merge — into one control-flow blob.

3. **Mutate poisons two unrelated ops** (`ops.py:2264–2279`, `2442–2447`, `3185–3250`). `SemanticMutateOp` is a post-agg node, but its existence forces `SemanticAggregateOp` and the join planner to carry `collect_mutates_to_join`, `has_prior_aggregate`, and `mutated_gb_keys` branches. ADR 0001 is correct: dropping mutate is the single highest-leverage cleanup.

A subtler closure gap: calc measures permit `t.all(t.some_measure)` only at aggregation-scope construction time (`compile_all.py:169–174`, `ops.py:2486–2498`). Inline aggregations inside calc measures on a not-yet-aggregated model don't compose — undocumented in the AST.

## Engineering smells

- **`ops.py` is a 5,349-line god module.** It mixes 15 operator classes, the join/pre-agg planner, dimension-enrichment helpers, and a bespoke `_RenamedResolver`. There's a clean split waiting: `ops/relations.py` (the algebra), `ops/planner.py` (pre-agg + join rewrites), `ops/metadata.py` (dim/measure forwarding).
- **Repeated `get_dimensions/get_measures/get_calculated_measures`** at `1225–1235`, `1326–1336`, `2139–2149`, `3367–3392`, `4224–4232`. Pure pass-through except in leaf and Join. A `MetadataForwarding` mixin removes ~60 LOC and turns the contract into one method.
- **Three resolver/proxy classes solve the same problem differently:** `_RenamedResolver` (`ops.py:76`), `_Resolver` (`convert.py`), `_ColumnPrefixProxy` + `_PendingMethodCall` (`measure_scope.py:31, 64`). Pluggable column-lookup on a single base would let chained-attribute work (`t.flights.carrier`) live in one place.
- **`_mutate_dimensions_with_dependencies`** (`ops.py:550`) is recomputed at `1167`, `1315`, `2295` — three independent ibis graph walks per query. Memoize on the leaf op.
- **`expr.py` (1,765 LOC)** duplicates `.get_dimensions/.get_measures` across 8 wrapper classes and ships a 57-entry `_BLOCKED_IBIS_METHODS` denylist — a denylist of inherited ibis API is a smell that the wrapper hierarchy should compose, not subclass.
- **Filter has two ingestion paths.** `predicate.py` is a clean predicate AST but only `query.py` uses it; `SemanticFilterOp` still takes opaque callables. Single path → earlier column-existence checks, uniform serialization.

## Top refactors (ranked by leverage)

| # | Change | Touches | Why |
|---|---|---|---|
| 1 | **Execute ADR 0001 — drop `SemanticMutateOp`** | `ops.py:2264, 2442, 3185–3250`, `expr.py`, `serialization/extract.py`, `serialization/reconstruct.py` | Removes the largest source of cross-op coupling. Aggregate and join planner stop knowing about post-agg derivations. |
| 2 | **Split `_to_untagged_with_preagg` into staged passes** (`FilterPusher → AggPartitioner → PreAggregator → JoinMerger`) | `ops.py:2417–2700` | Makes each pass independently testable; surfaces the calc-measure-vs-partition ordering as an explicit pipeline contract instead of nested control flow. |
| 3 | **`MetadataForwarding` mixin for unary ops** | `ops.py:1225, 1326, 2139, 4224` | Replaces 5 copies with one default; leaf and Join override. |
| 4 | **Unify the three resolver/proxy classes into one `DeferredResolver` base** | `ops.py:76`, `convert.py`, `measure_scope.py:31` | One place to fix chained attribute access, prefix collisions, and rename pre/post hooks. |
| 5 | **Carve `ops.py` into a package** (`relations`, `planner`, `metadata`, `resolver`) | `ops.py` | The module is the project's bottleneck for navigation and review. |
| 6 | **Add scope invariants on operators** — each op declares the symbol set its expressions resolve against (pre-agg dims, post-agg cols) | `ops.py` filter/aggregate/mutate | Replaces `isinstance(source, SemanticFilterOp)` checks with a typed contract. Makes ADR 0001's "calc measure vs. `to_untagged()`" boundary self-enforcing. |
| 7 | **Memoize derived-dimension enrichment on the leaf op** | `ops.py:550, 1167, 1315, 2295` | Three graph walks → one. ~5 LOC. |
| 8 | **One filter ingestion path through `predicate.py`** | `predicate.py`, `ops.py SemanticFilterOp`, `query.py` | Removes opaque-callable filters; uniform validation and serialization. |

## Suggested order

1. ADR 0001 (#1) first — it's the clearance pass that simplifies #2 and #6.
2. Then #3 + #4 + #7 in one batch — pure mechanical cleanup, low risk.
3. Then #2 and #6 together — the planner refactor benefits from scope invariants.
4. #5 and #8 are housekeeping; do whenever.

## Bottom line

The codebase isn't structurally broken — the algebra is the right shape and the calc-measure direction is correct. The pain is concentrated in `ops.py` and in the mutate/aggregate/join coupling. Executing ADR 0001 plus the pre-agg pipeline split would likely net ~400 LOC removed and one entire class of edge cases gone.
