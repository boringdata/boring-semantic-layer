# ADR 0001: Unify calculated measures and post-aggregation `mutate` on a single ibis-expression primitive

- **Status:** Proposed (revised 2026-05-08; supersedes the earlier "drop `SemanticMutateOp`" framing)
- **Date:** 2026-05-08
- **Deciders:** BSL maintainers
- **Related code:** `src/boring_semantic_layer/ops.py` (`SemanticTableOp.calc_measures`, `SemanticMutateOp`, `_to_untagged_with_preagg`, `collect_mutates_to_join`, `has_prior_aggregate`, `mutated_gb_keys`), `src/boring_semantic_layer/expr.py` (`SemanticMutate`, `.mutate()` chained API), `src/boring_semantic_layer/measure_scope.py` (`MeasureRef`, `AllOf`, `BinOp`, `MethodCall`, `validate_calc_ast`), `src/boring_semantic_layer/compile_all.py` (`compile_grouped_with_all`, `infer_calc_dtype`), `src/boring_semantic_layer/serialization/extract.py`, `src/boring_semantic_layer/serialization/reconstruct.py`.

## Context

BSL currently has two independent mechanisms for deriving a column that depends on already-aggregated values:

1. **Calculated measures** — declared on the model via `with_measures(...)` and classified as `calc` (vs. `base`) by `_classify_measure`. Stored on `SemanticTableOp.calc_measures` and compiled through `compile_grouped_with_all` / `infer_calc_dtype`. The expression language is a curated AST: `MeasureRef | AllOf | BinOp | MethodCall | int | float`, validated by `validate_calc_ast`.
2. **`SemanticMutateOp`** — a post-aggregation chain operator built by `SemanticTable.mutate(**post)` and `SemanticAggregate.mutate(**post)`. Runs an arbitrary user lambda over the aggregated result and adds new columns via `ibis.Table.mutate`. The expression language is *all of ibis*.

The two systems were introduced independently. Calc measures came after `SemanticMutateOp` was already wired into the join planner (`collect_mutates_to_join`, `has_prior_aggregate`, `mutated_gb_keys`, `_to_untagged_with_preagg(..., mutates=...)`), into serialization (`serialization/extract.py`, `serialization/reconstruct.py`), and into the public chained API (`SemanticMutate` re-implements much of `SemanticTable`'s surface).

A first reading framed the problem as "two overlapping mechanisms — drop one." Closer inspection shows that **neither system is the right primitive**, because each got two things right and two things wrong, and the right things are *orthogonal*:

|                                | Calculated measures                                      | `SemanticMutateOp`                                        |
|--------------------------------|----------------------------------------------------------|-----------------------------------------------------------|
| **Placement** (defined where)  | ✅ on the model — reusable, catalog-visible              | ❌ per-query — anonymous, not in `model.measures`         |
| **Expression language**        | ❌ curated AST; no `xo.case`, no windows                 | ✅ full ibis — windows, `xo.case`, arbitrary transforms   |
| **Planner integration**        | ✅ pre-agg pushdown, `AllOf` lift, structured tags        | ❌ opaque — special-cased through `collect_mutates_to_join` |
| **Compilation**                | ❌ hand-rolled (`compile_grouped_with_all`, `infer_calc_dtype`) | ✅ ibis compiles it for free                              |

Calc measures are right about *placement* and *integration*; `SemanticMutateOp` is right about *expression language* and *compilation*. Maintaining both is not just redundant — it enshrines the suboptimal tradeoff on each axis. Growing the calc-measure AST node-by-node (add `Case`, add `Window`, …) chases mutate's expressivity at the cost of a permanently growing hand-rolled compiler. Defanging mutate keeps the curated-AST limitation in place forever.

The right primitive is one that combines mutate's expression language with calc measures' placement and integration: **ibis expressions, declared on the model, classified by analysis rather than by AST tag.**

## Decision

**Unify calculated measures and `mutate` on a single primitive: ibis expressions declared on the model, with planner properties (pushability, `AllOf` lift, post-agg-only) recovered by analysis on the ibis tree rather than by curated-AST tagging.** Drop `SemanticMutateOp` as a chain operator. Keep the `.mutate(**post)` API as sugar that registers an ad-hoc calc measure on a query.

Concretely:

1. **Generalize `with_measures(...)`** to accept ibis expressions / `Deferred`s in addition to (or in place of) the curated AST. The classification `base` vs. `calc` becomes a property of the *expression*, not of the AST node type.
2. **Replace `validate_calc_ast`** with `analyze_calc_expr`, which walks the ibis tree and returns a classification record: `{pushable: bool, references_AllOf: bool, has_window: bool, post_agg_only: bool, depends_on: set[MeasureName]}`. Anything not classifiable falls back to "post-agg-only, do not push" with a warning — never an error.
3. **Replace `compile_grouped_with_all` + `infer_calc_dtype`** with ibis compilation plus an `AllOf`-lift pass that pattern-matches the structural shape (an aggregation appearing as a scalar inside another aggregation context) on the ibis tree. Type inference becomes `expr.type()`.
4. **Remove `SemanticMutateOp` (`ops.py`), `SemanticMutate` (`expr.py`), and the chained `.mutate()` operator.** `SemanticAggregate.mutate(**post)` survives as sugar that calls a new `with_calc(**post)` on the active query — which registers an ad-hoc calc measure scoped to that query and requests it. Same chain ergonomics, no new operator node.
5. **Remove mutate-aware branches in the join planner**: `collect_mutates_to_join`, the `SemanticMutateOp` arms in `has_prior_aggregate`, `_to_untagged_with_preagg(..., mutates=...)`, and the `mutated_gb_keys` paths. The pre-agg planner reads pushability off the calc measure's classification record directly.
6. **Remove `SemanticMutateOp` registrations from `serialization/extract.py` and `serialization/reconstruct.py`.** Calc measures serialize as resolver trees through the existing utils.py machinery; the curated-AST-specific fields (`expr_struct` etc.) collapse into one. Old tags containing a `SemanticMutateOp` node fail to deserialize with a clear error pointing at the migration tool.
7. **`t.all(...)`** keeps its current surface (a method on the resolver proxy) but emits an ibis-tree-recognizable marker instead of an `AllOf` AST node. The analyzer detects the marker and applies the existing window-aggregation lift.

## Migration

| Today                                                                                                  | After                                                                                                              |
|--------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `with_measures(avg=lambda t: t.total / t.cnt)` (calc measure today)                                    | Unchanged shape — accepted as an ibis expression; analyzer classifies as `pushable`.                               |
| `with_measures(share=lambda t: t.x / t.all(t.x))`                                                       | Unchanged — `t.all(...)` keeps its surface; analyzer detects the marker and lifts to a window aggregation.         |
| `.aggregate("a","b").mutate(c=lambda t: t.a / t.b)`                                                     | Define `c` once on the model; just request it.                                                                     |
| `.aggregate("c").mutate(bucket=lambda t: xo.case().when(t.c>=3,"hi").else_("lo").end())`                | Define `bucket` as a calc measure with `xo.case` — now permitted (analyzer classifies as `post_agg_only`).         |
| `.aggregate("x").mutate(rank=lambda t: t.x.rank(), pct=lambda t: t.x.percent_rank())`                   | Define `rank`, `pct` as calc measures — analyzer classifies as `has_window`, `post_agg_only`.                      |
| `.aggregate("x").mutate(ma=lambda t: t.x.mean().over(window(order_by="d", preceding=2)))`               | Define `ma` as a calc measure with the windowed expression.                                                        |
| `.aggregate("x").mutate(adhoc=...)` where `adhoc` truly is per-query                                    | `.aggregate("x").with_calc(adhoc=...)` registers an ad-hoc calc measure on the query — same ergonomics, one path.  |
| Existing serialized tags containing `SemanticMutateOp`                                                  | One-shot rewrite tool (`scripts/migrate_tags.py`) lowers mutate columns to calc measures (now possible because the calc-measure expression language is full ibis). Tags it cannot rewrite fail loudly with the offending expression. |

A short codemod / lint flags remaining `.mutate(` chained off semantic objects and emits the `with_calc` or `with_measures` equivalent.

## Consequences

### Positive

- **One primitive** for post-aggregation derivation. Definition placement is uniform (on the model, or query-local via `with_calc` sugar). One concept to learn, one place to reason about types, dependencies, and pre-agg rewrites.
- **Full ibis expressivity** is retained. Windows, `xo.case`, and arbitrary post-agg transforms all work declaratively, and *participate in serialization, catalog tooling, and pre-agg analysis* in a way mutate columns never did.
- **No hand-rolled compiler.** `compile_grouped_with_all` and `infer_calc_dtype` are replaced by ibis compilation plus an analyzer. Type inference becomes `expr.type()`.
- **Planner branches collapse.** `collect_mutates_to_join`, `has_prior_aggregate`'s mutate arm, `mutated_gb_keys`, and the `_rejoin_one` mutate fixups all go away. Pushability is read off the analyzer's classification record.
- **Serialization simplifies.** Calc measures serialize through one mechanism (resolver trees). Two parallel formats (curated-AST `expr_struct` vs. opaque mutate resolver) collapse to one.
- **Catalog visibility for everything mutate previously hid.** Window expressions, bucket labels, and `xo.case` derivations show up in `model.measures` and tooling.
- **Obviates two would-be follow-up ADRs** (xo.case in calc measures, windows in calc measures). Going to ibis-as-the-language once is strictly less work over time than node-by-node AST extension.

### Negative

- **The analyzer is harder than tag-matching.** Detecting `AllOf` was `isinstance(node, AllOf)`; detecting "agg-of-agg" requires walking ibis trees and recognizing the structural pattern. The other classifications (window detection, single-table-ref check) are mechanical but not free. This is a genuine new piece of code roughly the size of the existing `validate_calc_ast` plus the AllOf-lift section of `compile_grouped_with_all`.
- **Migration of existing calc-measure ASTs.** `MeasureRef("x")`, `AllOf(...)`, `BinOp(...)` keep working through a thin compat shim that lowers them to ibis-tree shapes the analyzer recognizes. The shim is deprecated once external users have migrated.
- **`t.all(...)` mechanic shifts** from "method on proxy returning curated AST node" to "method on proxy returning ibis-recognizable marker." Same external surface; existing user code is unaffected.
- **Existing tags with `SemanticMutateOp` no longer deserialize** without the migration tool. The tool can rewrite most cases (because the new calc-measure expression language *is* full ibis), but truly opaque cases fail loudly.
- **Classification at compile time.** The analyzer walks each measure's expression once. Negligible cost for typical models.
- **Doc churn.** `query-methods.md`, `bucketing.md`, `sessionized.md`, `windowing.md`, `percentage-total.md`, `reference.md` all need rewrites — but in a *better* direction (declarative on the model rather than chained `.mutate()`).
- **Test churn.** `test_real_world_scenarios.py`, `test_preagg_stress.py`, `test_malloy_inspired.py`, and the calc-measure tests need updates.

### Neutral

- Pre-aggregation correctness (formerly the `mutated_gb_keys` machinery) is no longer the operator's responsibility — but it was only the operator's responsibility because `SemanticMutateOp` introduced post-agg derived columns the planner couldn't see into. With analysis-based classification, the planner reads pushability off the calc measure's expression directly; correctness is a property of the analyzer, not of a special-case branch.

## Alternatives considered

1. **Keep both, document the split.** Rejected for the same reason as the previous version of this ADR — enshrines a confusing two-axis decision ("is this a measure or a mutate?") that is already a recurring user question.
2. **Drop calc measures, keep `mutate`.** Mutate's expression language is right; its placement (query-local, anonymous, not in `model.measures`) is wrong. Rejected.
3. **Drop `mutate`, keep the curated calc-measure AST** (the previous version of this ADR). Trades the planner cleanup for ergonomic regression on windows and `xo.case`. Rejected because the curated AST has to grow eventually anyway, and growing it is strictly more total work than going to ibis-as-the-language once.
4. **Extend the curated calc-measure AST node-by-node** (add `Case`, `Window`, `When`, …). Each extension requires coordinated changes to `validate_calc_ast`, the compiler, the AllOf-lift pass, and serialization. Strictly more work over time than this unification; rejected.
5. **Defang `SemanticMutateOp`** to a terminal-only post-agg escape hatch with no chained API and no pre-agg participation. Saves the planner cleanup but keeps two systems and the curated-AST limitation forever. Rejected.
6. **Keep `SemanticMutateOp` only at the post-aggregation boundary, hide the public `.mutate` API.** Strips the user-facing surface but leaves the planner branches and serialization registrations intact. Rejected.

## Open questions

- **Analyzer scope for v1.** What patterns does the first cut recognize? Recommendation: (a) "all column refs target one source table and there is no window" → `pushable`; (b) "an aggregation node appears as a scalar inside an enclosing aggregation context" → `references_AllOf`, lift to window aggregation; (c) "any window node anywhere" → `has_window`, `post_agg_only`. Anything not classifiable falls back to `post_agg_only` with a deprecation warning, never an error.
- **Compat-shim duration for the curated AST.** Recommendation: one minor version of acceptance with a deprecation warning ("`MeasureRef`/`AllOf`/`BinOp` will be removed in vX.Y; use ibis expressions directly"), then removal.
- **`t.all(...)` API.** Keep the proxy method (current surface) or expose a top-level `xo.all(...)` marker? Recommendation: keep the proxy method; have it emit an ibis-recognizable marker. No user-visible change.
- **Tag migration tool.** Ship `scripts/migrate_tags.py` that rewrites old tags. Most `SemanticMutateOp` cases lower to calc measures cleanly; the rest fail loudly with the offending expression and a pointer to `to_untagged()` as the explicit escape hatch.
- **Sequencing.** Recommended order: (1) land the analyzer alongside the curated AST (both paths active, analyzer used for new measures); (2) migrate internal calc measures to ibis expressions, exercise the analyzer in tests; (3) drop `SemanticMutateOp`, planner branches, and serialization registrations; (4) deprecate the curated AST shim; (5) remove the shim one minor version later.
