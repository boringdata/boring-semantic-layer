# ADR 0001: Drop `SemanticMutateOp` in favor of calculated measures

- **Status:** Proposed
- **Date:** 2026-05-08
- **Deciders:** BSL maintainers
- **Related code:** `src/boring_semantic_layer/ops.py` (`SemanticMutateOp`, `SemanticTableOp.calc_measures`), `src/boring_semantic_layer/expr.py` (`SemanticMutate`, `.mutate()` on `SemanticTable`/`SemanticAggregate`)

## Context

BSL exposes two ways to derive a column that depends on already-aggregated values:

1. **Calculated measures** — declared at model definition time via `with_measures(...)` and classified as `calc` (rather than `base`) by `_classify_measure`. They are stored on `SemanticTableOp.calc_measures` and compiled through the `compile_grouped_with_all` / `infer_calc_dtype` pipeline. Typical shapes:
   - cross-measure ratios: `avg_distance = total_distance / flight_count`
   - percent-of-total via the `.all()` lift: `market_share = t.flight_count / t.all(t.flight_count) * 100`
   - any `MeasureRef | AllOf | BinOp | MethodCall | int | float` AST (validated by `validate_calc_ast`).

2. **`SemanticMutateOp`** — a post-aggregation node, built by `SemanticTable.mutate(**post)` and `SemanticAggregate.mutate(**post)`. It runs an arbitrary user lambda over the *aggregated* result table and adds new columns via `ibis.Table.mutate`. In practice it is used for:
   - the same cross-measure ratios calc measures already cover (`.aggregate("a","b").mutate(c=lambda t: t.a / t.b)`);
   - percent-of-total via `t.all(t.x)` in post-agg form;
   - bucketing / `xo.case().when(...).end()` over aggregated values;
   - window functions on the aggregated frame: `cumsum`, `lag`, `lead`, `rank`, `percent_rank`, `mean().over(window(order_by=...))`.

The two systems were introduced independently. Calc measures came later, after `SemanticMutateOp` was already wired into pre-aggregation rewrites (`collect_mutates_to_join`, `_to_untagged_with_preagg(..., mutates=...)`), serialization (`SemanticMutateOp` registered in `serialization/extract.py` and `serialization/reconstruct.py`), and the public chained API. The result is two overlapping mechanisms with different visibility, lifetimes, and reusability:

| Aspect                                | `calc_measures`                         | `.mutate(...)`                                    |
|---------------------------------------|-----------------------------------------|---------------------------------------------------|
| Defined where                         | On the model (`with_measures`)          | On a query, after `.aggregate()`                  |
| Reusable across queries               | Yes                                     | No (anonymous, query-local)                       |
| In `model.measures` / catalog         | Yes                                     | No                                                |
| Round-trips through `to_tagged`       | Yes (structured)                        | Yes, but as an opaque resolver tree               |
| Window functions / `.over(...)`       | Not supported                           | Supported                                         |
| `xo.case().when(...).end()`           | Not supported (AST guarded)             | Supported                                         |
| Pre-aggregation join rewrites         | Native participation                    | Threaded through `collect_mutates_to_join`        |
| User-visible surface area             | `with_measures(...)`                    | `SemanticTable.mutate`, `SemanticAggregate.mutate`, `SemanticMutate` (own subclass with full chained API) |

Maintaining both has a real cost: the `SemanticMutate` class re-implements much of `SemanticTable`'s chained surface (`group_by`, `mutate`, `with_dimensions`, `with_measures`, …), the join planner has dedicated logic to walk past mutate nodes (`collect_mutates_to_join`, `has_prior_aggregate`), and pre-agg rewrites need to re-apply mutated columns at the right grain (`mutated_gb_keys`, `_rejoin_one`). Calc measures alone do not currently cover every shape `mutate` accepts.

## Decision

**Drop `SemanticMutateOp` and the associated `.mutate(**post)` API on semantic tables.** Make calculated measures the single way to derive post-aggregation columns inside the semantic layer. For shapes calc measures cannot represent today (window functions, `xo.case` over aggregated values, ad-hoc Ibis transforms), users drop into Ibis explicitly via `to_untagged()` instead of staying inside the semantic chain.

Concretely:

1. Remove `SemanticMutateOp` (`ops.py`), `SemanticMutate` (`expr.py`), and the `.mutate()` methods on `SemanticTable`, `SemanticGroupBy`-result types, and `SemanticAggregate`.
2. Remove the mutate-aware branches in the join planner (`collect_mutates_to_join`, the `SemanticMutateOp` arms in `has_prior_aggregate`, `_to_untagged_with_preagg(..., mutates=...)`, and the `mutated_gb_keys` paths it feeds).
3. Remove `SemanticMutateOp` registrations from `serialization/extract.py` and `serialization/reconstruct.py`. Old tags that contain a mutate node fail to deserialize with a clear error message pointing at the migration recipe below.
4. Extend `validate_calc_ast` / `_classify_measure` only as far as needed to absorb shapes we want to keep (see "Open questions").
5. Update `docs/md/doc/query-methods.md`, `bucketing.md`, `sessionized.md`, `windowing.md`, `percentage-total.md`, and `reference.md` to remove the `.mutate()` examples or rewrite them as `with_measures(...)` plus, where necessary, a `to_untagged()` escape hatch.

## Migration

| Today (`mutate`)                                                                                       | After                                                                                          |
|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `.aggregate("total","cnt").mutate(avg=lambda t: t.total/t.cnt)`                                         | Define `avg=lambda t: t.total / t.cnt` in `with_measures` once; just request it.               |
| `.aggregate("x").mutate(share=lambda t: t.x / t.all(t.x))`                                              | Define `share` as a calc measure with `t.all(t.x)` (already idiomatic — see `percentage-total.md`). |
| `.aggregate("x").mutate(rank=lambda t: t.x.rank(), pct=lambda t: t.x.percent_rank())`                   | `.aggregate("x").to_untagged().mutate(rank=_.x.rank(), pct=_.x.percent_rank())`               |
| `.aggregate("x").mutate(ma=lambda t: t.x.mean().over(window(order_by="d", preceding=2)))`               | `.aggregate("x").to_untagged().mutate(...)` — windows stay in Ibis.                           |
| `.aggregate("c").mutate(bucket=lambda t: xo.case().when(t.c>=3,"hi").else_("lo").end())`                | If `xo.case` becomes a permitted calc-measure shape, define as a calc measure; otherwise drop to `to_untagged()`. |

A short codemod / lint can flag remaining `.mutate(` chained off semantic objects and emit the equivalent suggestion.

## Consequences

### Positive

- One concept to learn ("define it on the model"), one place to reason about types, dependencies, and pre-agg rewrites.
- Calc measures are reusable across queries and surfaced in `model.measures` / catalog tooling; mutate columns were anonymous and per-query.
- The join planner loses an entire category of edge cases: `collect_mutates_to_join`, `has_prior_aggregate`'s mutate arm, and the `mutated_gb_keys` fix-ups in `_to_untagged_with_preagg` all go away, simplifying the code that's already the most-touched in BSL.
- The `SemanticMutate` class duplicates the `SemanticTable` chained surface; deleting it removes a class whose methods must stay in lockstep.
- Serialized tags become smaller and more uniform — every derived expression is a calc measure on the model, not a node in the chain.

### Negative

- Window functions (`over`, `cumsum`, `lag`, `lead`, `rank`, `percent_rank`) and `xo.case` over aggregated values currently work via `mutate`. Without a calc-measure extension to cover them, users must call `.to_untagged()` and finish in Ibis. That is a real ergonomic regression for the `windowing.md` and `bucketing.md` patterns.
- `mutate` is *more general* than calc measures. Anything a user expressed as an arbitrary post-agg lambda must either (a) be expressible as a calc-measure AST, or (b) move outside the semantic chain. There is no third option after this change.
- Existing tags written with `SemanticMutateOp` no longer deserialize. We need a deprecation window or a one-shot rewrite tool for any persisted tag.
- Several integration tests and doc snippets need rewriting (`test_real_world_scenarios.py`, `test_preagg_stress.py`, `test_malloy_inspired.py`, the docs listed above).

### Neutral

- Pre-aggregation correctness (the `mutated_gb_keys` machinery) is no longer ours to maintain — but it was only ours because `mutate` introduced post-agg derived dimensions in the first place.

## Alternatives considered

1. **Keep both, document the split.** Cheap, but enshrines a confusing two-axis decision ("is this a measure or a mutate?") that we already field as a recurring user question. Rejected because the cost is paid forever.

2. **Drop calc measures, keep `mutate`.** `mutate` is strictly more expressive. Rejected because calc measures are *declarative*, reusable, catalog-visible, and integrate with `.all()` rewrites — we want those properties to be the default, not opt-in.

3. **Drop `mutate`, extend calc measures to cover window functions and `xo.case` first.** Strictly better than this ADR if we are willing to do the work. The compiler and `validate_calc_ast` would need to accept window expressions and conditional ASTs, and `compile_grouped_with_all` would need to know how to lower them. This is the recommended follow-up and could become its own ADR; this ADR's decision does not preclude it.

4. **Keep `SemanticMutateOp` only at the post-aggregation boundary, hide the public `.mutate` API.** Strips the user-facing surface but keeps the internal node. Rejected: the bulk of the maintenance cost is the join-planner branches and the serialization registrations, not the public method.

## Open questions

- **Scope of calc-measure extension before removal.** Do we add window-expression and `xo.case` support to calc measures *before* dropping `mutate` (so the migration table's last two rows stay inside the semantic chain), or do we ship the removal first and direct users to `to_untagged()` for those cases? Recommendation: extend calc measures for `xo.case` (cheap, no aggregation-frame question) and accept `to_untagged()` for windows in the first cut.
- **Deprecation window.** Hard removal in the next minor, or one release of `DeprecationWarning` from `.mutate(...)` first? Recommendation: one release of warning, with the warning message pointing at this ADR.
- **Tag-format compatibility.** Do we ship a one-shot `migrate_tags` script that walks old tags and either rewrites the mutate node into calc measures (where possible) or fails loudly with the offending expression? Recommendation: yes, in `scripts/`.
