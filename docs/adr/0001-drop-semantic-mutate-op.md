# ADR 0001: Unify calculated measures and post-aggregation `mutate` on a single ibis-expression primitive

- **Status:** Implemented — Phases 1+2 landed on `hussain/feat/calc-measure-analyzer` (merged to `main`); Phase 3 landed on `hussain/feat/drop-semantic-mutate-op-phase3`.
- **Date:** 2026-05-08 (revised 2026-05-09 to reflect landed work; revised 2026-06-10 for Phase 3 completion)
- **Deciders:** BSL maintainers
- **Related code (current state):** `src/boring_semantic_layer/calc_analyzer.py` (new), `src/boring_semantic_layer/calc_compiler.py` (new), `src/boring_semantic_layer/nested_compile.py` (new — extracted from deleted `compile_all.py`), `src/boring_semantic_layer/ops.py` (`CalcMeasure`, `_classify_measure`, `_build_aggregation_plan`, `_compile_aggregation`, `_apply_calc_specs`; `SemanticMutateOp` — deleted in Phase 3), `src/boring_semantic_layer/expr.py` (`SemanticMutate` deleted; `.mutate()` survives as a desugaring alias), `src/boring_semantic_layer/measure_scope.py` (`MeasureScope`/`ColumnScope` thin proxies; curated AST removed), `src/boring_semantic_layer/serialization/extract.py` (resolver-tree calc serialization), `src/boring_semantic_layer/tests/test_mutate_compositions.py` (Phase 3 composition pins).

## Context

BSL had two independent mechanisms for deriving a column that depends on already-aggregated values:

1. **Calculated measures** — declared on the model via `with_measures(...)` and classified as `calc` (vs. `base`) by `_classify_measure`. Stored on `SemanticTableOp.calc_measures` and compiled through a hand-rolled pipeline. The expression language was a curated AST: `MeasureRef | AllOf | BinOp | MethodCall | int | float`, validated by `validate_calc_ast`.
2. **`SemanticMutateOp`** — a post-aggregation chain operator built by `SemanticTable.mutate(**post)` and `SemanticAggregate.mutate(**post)`. Runs an arbitrary user lambda over the aggregated result and adds new columns via `ibis.Table.mutate`. The expression language is *all of ibis*.

Each system got two things right and two things wrong, and the right things were *orthogonal*:

|                                | Calculated measures                                      | `SemanticMutateOp`                                        |
|--------------------------------|----------------------------------------------------------|-----------------------------------------------------------|
| **Placement** (defined where)  | ✅ on the model — reusable, catalog-visible              | ❌ per-query — anonymous, not in `model.measures`         |
| **Expression language**        | ❌ curated AST; no `xo.case`, no windows                 | ✅ full ibis — windows, `xo.case`, arbitrary transforms   |
| **Planner integration**        | ✅ pre-agg pushdown, `AllOf` lift, structured tags        | ❌ opaque — special-cased through `collect_mutates_to_join` |
| **Compilation**                | ❌ hand-rolled (`compile_grouped_with_all`, `infer_calc_dtype`) | ✅ ibis compiles it for free                              |

Calc measures were right about *placement* and *integration*; `SemanticMutateOp` was right about *expression language* and *compilation*. Maintaining both enshrined the suboptimal tradeoff on each axis. Growing the calc-measure AST node-by-node would have chased mutate's expressivity at the cost of a permanently growing hand-rolled compiler. Defanging mutate would have kept the curated-AST limitation in place forever.

The right primitive is one that combines mutate's expression language with calc measures' placement and integration: **ibis expressions, declared on the model, classified by analysis rather than by AST tag.**

## Decision

**Unify calculated measures and `mutate` on a single primitive: ibis expressions declared on the model, with planner properties (pushability, `AllOf` lift, post-agg-only) recovered by analysis on the ibis tree rather than by curated-AST tagging.** Drop `SemanticMutateOp` as a chain operator. Per-query ad-hoc derivations go through the existing `with_measures(...)` method on `SemanticAggregate` (which the Phase 1+2 cutover already wired through the analyzer) — no new method, no parallel registration path.

The decision is being executed in three phases:

- **Phase 1 — Analyzer + ibis-native compiler.** Land the structural classifier (`analyze_calc_expr`) and the calc compiler (`IbisCalcScope`, `apply_calc_measures`, `lift_inline_reductions`, `compile_calc_measure`) alongside the curated-AST path.
- **Phase 2 — Hard cutover.** Replace the curated AST with the analyzer; remove `compile_grouped_with_all`, `validate_calc_ast`, and the curated AST classes. Calc measures are stored as `CalcMeasure(expr=callable)` and re-evaluated against `IbisCalcScope` at query time. `with_measures(...)` on `SemanticTable` *and* on `SemanticAggregate` (`expr.py:1589`) already routes through `_classify_measure`, so the same lambda surface that defines model-level measures also covers query-local ones.
- **Phase 3 — Drop `SemanticMutateOp`.** Remove the chain operator, its planner branches, and its serialization. The chained `.mutate(**post)` API either (a) is removed and users migrate to `.with_measures(**post).aggregate(..., *post.keys())`, or (b) survives as a thin alias that desugars to exactly that — one line, no operator node, no new method name. **Recommendation: (b)** — preserves chain ergonomics for existing user code while collapsing the operator graph to one path.

## Implementation status

### Phases 1+2 — Landed (branch `hussain/feat/calc-measure-analyzer`)

Net diff: **~-727 lines** in production code. Test suite: 978 passed, 1 preexisting unrelated xorq failure.

What's wired:

- **`calc_analyzer.py`** — `analyze_calc_expr` walks an ibis tree (skipping `Relation` subtrees) and returns `CalcExprAnalysis(pushable, references_AllOf, has_window, post_agg_only, depends_on, inline_aggs)`. Single-pass `_scan_tree` recognizes plain `Reduction`, real `WindowFunction`, and the agg-of-agg / empty-window-over-reduction patterns that mean "totals."
- **`calc_compiler.py`** — `IbisCalcScope` (dual-table dispatch over base + virtual aggregated + virtual totals), `evaluate_calc_lambda`, `classify_calc_lambda`, `lift_inline_reductions`, `apply_calc_measures`, `compile_calc_measure`. Topological ordering of calc-of-calc chains via `topological_order_from_deps`.
- **`ops.py`** — `CalcMeasure` is the new storage shape. `_classify_measure` runs the lambda once against `IbisCalcScope`, walks the result, and routes to base or calc. `_build_aggregation_plan` / `_compile_aggregation` replace `compile_grouped_with_all`. The pre-agg path's `_apply_calc_specs` and the deferred-join arm both go through `apply_calc_measures`.
- **`measure_scope.py`** — curated AST classes (`MeasureRef`, `AllOf`, `BinOp`, `MethodCall`, `AggregationExpr`, `_PendingMethodCall`, `DeferredColumn`, `validate_calc_ast`) deleted. `MeasureScope` and `ColumnScope` survive as thin pass-through proxies for post-aggregation chain contexts and for nested-access helpers.
- **`compile_all.py`** — deleted. Nested-array helpers extracted to `nested_compile.py`.
- **`serialization/extract.py`** — `serialize_calc_measures` walks each `CalcMeasure.expr` via `expr_to_structured` and stores the resolver tree plus `description`, `requires_unnest`, and `depends_on`. `deserialize_calc_measures` rebuilds `CalcMeasure(expr=Deferred(...), depends_on=...)`. Backwards-compat for the old bare-tuple format kept at one site.
- **`utils.py`** — `serialize_resolver` / `deserialize_resolver` handle the `Item` resolver (needed for `t["prefixed.name"]`). All FrozenSlotted resolvers built via `object.__new__` go through `_finalize_frozen_slotted` so the rebuilt resolver hashes equal to a freshly-constructed one — fixes a latent bug that would surface as `AttributeError: __precomputed_hash__` when a deserialized resolver was used as a dict key.

What this gained beyond literal cutover:

- **Non-sum `t.all(...)` works correctly.** `t.all(measure_ref)` resolves to a `Field(totals_vt, name)`; the compiler builds a real no-group-by totals aggregation by re-running `agg_specs` on the base, applies non-AllOf calc measures to it, cross-joins it with prefixed column names, and rewrites totals references. Non-sum chains (`avg_distance / t.all(avg_distance)`) now match the right answer (overall mean, not sum-of-per-group-means). Pinned by `test_apply_calc_measures_join_with_mean_totals` (mean) and parametrized `test_apply_calc_measures_non_sum_totals` (median, min, max).
- **Inline reductions inside `t.all(...)`.** `t.value.sum() / t.all(t.value.sum())` compiles end-to-end via `lift_inline_reductions`: each unique reduction over the base is named, added to both per-group and totals aggregations, and rewritten in-place (bare → `Field(vt, anon)`, windowed → `Field(totals_vt, anon)`). Pinned by `test_lift_inline_reductions_routes_window_to_totals`.
- **Calc-of-calc.** Topologically ordered inside `apply_calc_measures` — each calc is added to the result via its own `mutate(...)` so subsequent calcs see it as a column. `depends_on` is captured at classification time and survives serialization.
- **Joined models.** `IbisCalcScope` does unique-suffix matching: `t.flight_count` resolves to `flights.flight_count` when there's exactly one such suffix. No need to rewrite stored lambdas for prefixed names.
- **Clear errors instead of opaque ibis `IntegrityError`.** `TotalsNotAvailableError` when `t.all(...)` is referenced but no totals can be built; post-rewrite assertion in `compile_calc_measure` listing the unresolved column names.

### Phase 3 — Landed (branch `hussain/feat/drop-semantic-mutate-op-phase3`, 2026-06-10)

`SemanticMutateOp`, `SemanticMutate`, and every planner branch in the readiness table below are deleted. `.mutate()` survives as the recommended desugaring alias (option b). Test suite: 1015 passed (995 baseline + 20 new composition regression tests in `test_mutate_compositions.py`), 0 regressions.

What shipped, including deviations from the plan below:

- **`SemanticAggregate.mutate(**post)`** rebuilds the aggregate with `aggs = {**aggs, **post}` — no new node, no new storage shape. Inline classification routes each entry: measure refs / windows / `t.all(...)` → calc spec; expressions over group keys (e.g. `t.carrier.upper()`) → a new *dimension-grain* route in `_compile_aggregation` that applies them to the aggregated result via `mutate` (ibis dereferences group-key fields). Chained `.mutate().mutate()` folds into the same aggregate; sibling and group-key references resolve because `_build_aggregation_plan` augments the classification scope with the aggregate's own entry names and keys, and `_compile_aggregation` surfaces the group-by keys on the virtual aggregated table (windows can `order_by` a key).
- **Pre-aggregation `.mutate()` lowers to `with_dimensions`, not `with_measures`** — a deliberate deviation from the resolved question below. Pre-agg mutate columns are row-grain derived columns used as group-by keys; dimensions are exactly that, and `_mutate_dimensions_with_dependencies` materializes them at query time. On join-backed models the registration is lazy (preserves `_source_join` and the pre-agg fan-out machinery); on flat models the columns are materialized eagerly so `.mutate(...).execute()` still shows them.
- **Post-aggregation chains** (`.filter(...)/.order_by(...)/.limit(...)` then `.mutate(...)`) resolve each lambda against the current result table in chain order and wrap the result in a `SemanticModel` — same observable semantics as the old operator (windows see filtered/limited rows), no `SemanticLimit.with_measures` needed.
- **`mutated_gb_keys` replacement:** the pre-agg grain computation now materializes *unprefixed derived dimensions* per-table when their expression resolves on that table's columns, falling back to join keys + dimension bridge otherwise. This fixes the grain for `with_dimensions`-registered keys generally, not just mutate-lowered ones.
- **Serialization:** new chains serialize as plain `SemanticAggregateOp` tags. `_reconstruct_aggregate` now rebuilds query-local lambdas from their serialized resolver structs (model measures still replay by name). Old tags containing `SemanticMutateOp` do not deserialize; this is a hard cutoff for persisted pre-Phase-3 mutate tags.

Original Phase 3 work plan (all items done unless noted):

1. **Reduce `.mutate(**post)` to a thin alias for `.with_measures(**post).aggregate(*current, *post.keys())`** on `SemanticAggregate`. No new method name, no new operator node, no new storage shape — `with_measures` already routes through `_classify_measure`, which already routes through the analyzer. The mutate method becomes ~3 lines of desugaring; existing user code keeps working unchanged.
2. **Remove `SemanticMutateOp`** from `ops.py` (currently `ops.py:3307`).
3. **Remove `SemanticMutate`** from `expr.py` (currently `expr.py:1615`). The three `.mutate()` methods on `SemanticTable` (`expr.py:221`), `SemanticAggregate` (`expr.py:1352`), and `SemanticMutate` (`expr.py:1659`) either get the desugaring described in (1) or are deleted.
4. **Remove mutate-aware planner branches** (enumerated below).
5. **Remove `SemanticMutateOp` registrations** from `serialization/extract.py:63,74,140`, `serialization/reconstruct.py:185`, `convert.py:24,400`, `format.py:17,149`, `chart/utils.py:131,137`. Existing tags containing `SemanticMutateOp` fail to deserialize with the generic unknown-op error. No migration tool: tags are re-generated from current model definitions, and users with persisted tags either re-tag or pin the prior BSL version.
6. **Lint / deprecation pass.** Flag remaining `.mutate(` chained off semantic objects in user code if (3) opts to delete rather than alias; emit the `with_measures` equivalent.

#### Phase 3 readiness checklist — planner branches that go away

Each line is a concrete deletion target. Counts are from current `main`-vs-branch state.

| File:line | What it does | What replaces it |
|---|---|---|
| `ops.py:380` | `_semantic_repr` arm for `SemanticMutateOp` | Deleted with the op |
| `ops.py:2364` | `has_prior_aggregate(SemanticMutateOp)` traversal | Deleted; no node to traverse |
| `ops.py:2374` | `is_post_agg = has_prior_aggregate(self.source)` driving the post-agg branch in `SemanticAggregateOp.to_untagged` | Stays — but the only mutate-recursion case (line 2364) goes away; remaining cases (`SemanticAggregateOp`, `SemanticGroupByOp`) still apply |
| `ops.py:2377–2392` | `collect_mutates_to_join` walks the chain collecting `SemanticMutateOp.post` dicts | Deleted; no chained mutates to collect |
| `ops.py:2433` | `collected_mutates = collect_mutates_to_join(self.source)` | Deleted |
| `ops.py:2439` | `_to_untagged_with_preagg(..., mutates=collected_mutates)` | Drop the `mutates` parameter |
| `ops.py:2553–2558` | `mutated_gb_keys` heuristic — group-by keys that aren't dims/measures/calcs are assumed to be mutate-introduced | Deleted; group-by keys with derivations are calc measures via `with_measures`, so they appear in `merged_calc_measures` |
| `ops.py:2569–2581` | Apply mutate ops to full joined table for dim-bridge use | Deleted |
| `ops.py:2706–2716` | Apply mutated group-by keys to per-table raw tables for grain computation | Deleted |
| `ops.py:2768–2770` | Local-dim handling for mutated group-by keys | Deleted |
| `ops.py:3307–3372` | `SemanticMutateOp` class itself | Deleted |
| `expr.py:32, 221, 222, 1352, 1353, 1615–1698, 1659, 1660, 1675–1698` | `SemanticMutate` class, `.mutate()` methods, imports | Deleted or reduced to alias |
| `convert.py:24, 400–402` | `_convert_semantic_mutate` to-ibis conversion | Deleted |
| `format.py:17, 149–150` | `_format_semantic_mutate` repr | Deleted |
| `chart/utils.py:131, 137` | Chart introspection skipping `SemanticMutateOp` nodes | Deleted (no nodes to skip) |
| `serialization/extract.py:63, 74, 140` | Registration + lazy stash for `SemanticMutateOp` tag | Deleted |
| `serialization/reconstruct.py:185–190` | Reconstructor for `SemanticMutateOp` | Deleted; old tags hit the generic unknown-op error |

The intellectually load-bearing piece — proving you can recover pushability/AllOf-lift/post-agg classification by analyzing an ibis tree — is done. Phase 3 is mechanical deletion guided by the table above plus regression tests for each composition that the deletions touch.

#### Composition gotchas to pin with tests before deletion

Each row is a chained mutate composition that exists today; the right column says how the equivalent `with_measures` chain behaves. None requires new semantics — they all fall out of where `with_measures` already builds its scope — but each needs a regression test before mutate is removed.

| Composition | Today's behavior | After (`with_measures` chain) |
|---|---|---|
| `.aggregate(...).mutate(c=lambda t: ...).filter(p)` | Filter sees `c` (mutate runs first by chain order) | `.with_measures(c=...).aggregate(..., "c").filter(p)` — same: filter applies to the aggregated table containing `c` |
| `.aggregate(...).mutate(a=...).filter(p).mutate(b=...)` | `b` sees filtered table including `a` | `.with_measures(a=...).aggregate(..., "a").filter(p).with_measures(b=...)` — `SemanticFilter.with_measures` (`expr.py:1117`) scopes on `self.op().to_untagged()` which is the filtered table, so `b` sees rows surviving `p` |
| `.aggregate(...).mutate(c=...).order_by("c")` | OrderBy operates on `c` | `.with_measures(c=...).aggregate(..., "c").order_by("c")` — same: `c` is a column on the aggregated table |
| `.aggregate(...).mutate(c=...).limit(10)` | Limit applied after `c` is added | Same — limit is post-aggregate either way |
| `.aggregate(...).mutate(c=...).limit(10).mutate(d=lambda t: t.c * 2)` | `d` sees the post-limit table | `.with_measures(c=...).aggregate(..., "c").limit(10).with_measures(d=...)` — but **note**: `SemanticLimit.with_measures` does *not* exist today (`expr.py` only defines it on `SemanticTable`/`SemanticFilter`/`SemanticAggregate`/`SemanticMutate`). Phase 3 either adds `SemanticLimit.with_measures` or rejects this composition. Recommended: add it, scoped on `self.op().to_untagged()` for consistency with `SemanticFilter.with_measures` |
| `.mutate(c=...).group_by(...).aggregate(...)` | Mutate runs *before* aggregation; `c` is a dimension-grain column | `.with_measures(c=...).group_by(...).aggregate(..., "c")` — the analyzer classifies pre-agg derivations as `pushable`, so this Just Works on the existing path |
| `.aggregate(...).mutate(c=...)` followed by use as a join input | Mutate result becomes a `SemanticMutateOp` node the join planner had to special-case | After Phase 3, the result is a `SemanticAggregate` (subclass of `SemanticTable`) with `c` in its measures dict. The join planner already handles `SemanticAggregate`, so the `SemanticMutateOp` arm in `collect_mutates_to_join` simply has nothing to collect |
| `.join_one(...).mutate(c=...)` (mutate after join, before aggregate) | Mutate adds a column that participates as a group-by candidate via `mutated_gb_keys` | After Phase 3: define `c` via `with_measures` on either side before the join, or on the join result; either way it lands in `merged_calc_measures` and the planner sees it without the `mutated_gb_keys` heuristic |

The `mutated_gb_keys` heuristic at `ops.py:2553` deserves a specific call-out: *the only reason it exists* is that `SemanticMutateOp` introduced columns the planner couldn't classify as dims/measures/calcs. Once mutate columns become calc measures, the heuristic has no work to do — every group-by key resolves through `merged_*` lookups directly. This collapses lines 2553–2770 (~60 LOC of conditional handling for mutated keys) without replacement.

## Migration

| Today                                                                                                  | After Phase 3                                                                                                       |
|--------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `with_measures(avg=lambda t: t.total / t.cnt)` (calc measure today)                                    | Already works — landed in Phase 1+2; analyzer classifies as `pushable`.                                             |
| `with_measures(share=lambda t: t.x / t.all(t.x))`                                                       | Already works — landed in Phase 1+2; the analyzer detects the totals pattern, the compiler builds a real totals table. |
| `with_measures(avg=lambda t: t.x.mean(), ratio=lambda t: t.avg / t.all(t.avg))` (non-sum totals)        | Already works — landed in Phase 1+2; totals re-aggregation uses the formula, not a windowed sum.                    |
| `.aggregate("a","b").mutate(c=lambda t: t.a / t.b)`                                                     | Define `c` on the model and request it: `.with_measures(c=...).aggregate("a","b","c")`. Or keep the chained shape if Phase 3 ships `.mutate` as an alias. |
| `.aggregate("c").mutate(bucket=lambda t: xo.case().when(t.c>=3,"hi").else_("lo").end())`                | `.with_measures(bucket=...).aggregate("c","bucket")` — already permitted in Phase 1+2 (analyzer classifies as `post_agg_only`). |
| `.aggregate("x").mutate(rank=lambda t: t.x.rank(), pct=lambda t: t.x.percent_rank())`                   | `.with_measures(rank=..., pct=...).aggregate("x","rank","pct")` — already permitted (analyzer classifies as `has_window`). |
| `.aggregate("x").mutate(ma=lambda t: t.x.mean().over(window(order_by="d", preceding=2)))`               | `.with_measures(ma=...).aggregate("x","ma")` — already permitted.                                                   |
| `.aggregate("x").mutate(adhoc=...)` where `adhoc` truly is per-query                                    | `.with_measures(adhoc=...).aggregate("x","adhoc")` registers a query-local measure on the temporary aggregated model. Same shape, same lambda. |
| Existing serialized tags containing `SemanticMutateOp`                                                  | Fail to deserialize. Users re-tag from current model definitions or pin the prior BSL version. |

## Consequences

### Positive — realized in Phase 1+2

- **One classification path.** Calc-measure pushability/AllOf-lift/post-agg routing is read off `analyze_calc_expr`. No more parallel "is this a curated AST node" + "what does ibis make of it" reasoning.
- **Full ibis expressivity for calc measures.** Windows, `xo.case`, `xo.ifelse`, `.fillna(...).cast(...)`, struct/array methods — all work declaratively *and* participate in serialization, catalog tooling, and pre-agg analysis.
- **No hand-rolled compiler.** `compile_grouped_with_all` and `infer_calc_dtype` deleted. Type inference falls out of `expr.type()` (with a debug-logged fallback for joined-model edge cases).
- **Serialization simplified.** Calc measures serialize through one mechanism (resolver trees). Two parallel formats collapsed to one.
- **Catalog visibility for everything mutate previously hid would be the next benefit** — partially realized: anything declarable on the model now shows up in `model.measures`. Phase 3 closes the gap for genuinely per-query derivations.
- **Obviates two would-be follow-up ADRs** (`xo.case` in calc measures, windows in calc measures). Done in one cut.

### Positive — pending Phase 3

- **Planner branches collapse.** `collect_mutates_to_join`, `has_prior_aggregate`'s mutate arm, `mutated_gb_keys`, and `_to_untagged_with_preagg(..., mutates=...)` all go away.
- **`expr.py` shrinks.** `SemanticMutate` (entire class) and `SemanticMutateOp` (entire op) disappear. The `.mutate(**post)` method either survives as a 3-line alias for `with_measures(**post).aggregate(...)` or is deleted entirely.
- **No new public method.** The user-facing surface stays at `with_measures` — already familiar, already analyzer-routed. No `with_calc` to learn.
- **Serialization simplifies further.** Tags carrying `SemanticMutateOp` go away; old tags fail loudly rather than keeping a compatibility reader.

### Behavior changes for users

The Phase 1+2 cutover is an internal refactor in shape, but it has three semantic edges users will hit and that the ADR commits to as stable surface area.

1. **Calc lambdas execute twice per query.** Once at definition (classification — `_classify_measure` runs the lambda against `IbisCalcScope` to walk the resulting ibis tree) and once at query time (compilation — `apply_calc_measures` re-runs the lambda against the real aggregated table). Implications:
   - **Pure expressions** (`lambda t: t.a / t.b`) — no observable change.
   - **Lambdas that read external state** (config, env, globals) — both reads happen; if the values differ between definition and query they observe the *latter*. In practice the classification result is discarded once the lambda is stored, but the side effects are not.
   - **Lambdas with side effects** (logging, counters, network calls) — fire twice. **Don't put side effects in calc lambdas.** This was technically also true for `mutate(...)` lambdas (they ran during planning *and* execution if the table was re-executed) but only the analyzer makes the double-execution unconditional and predictable.
   - The current ``_classify_measure`` swallows generic exceptions and falls through to base classification (`ops.py:790`), so a lambda that raises during classification still gets called at query time. Don't rely on classification-time exceptions to short-circuit anything.

2. **`IbisCalcScope` dispatch order is the public contract** users program against in calc lambdas. The order is:
   1. **Base column wins on collision.** If `t.foo` matches a column on the base table, it returns the base column — even when `foo` is also registered as a measure. This preserves historical `t.distance.sum()` semantics where `distance` was both a column and a measure name.
   2. **Then known measure (with suffix matching).** `t.flight_count` resolves to the measure named `flight_count`; on a joined model with prefixed names like `flights.flight_count`, the unique-suffix match bridges the short name automatically.
   3. **Then ibis Table methods.** `t.count`, `t.filter`, etc. fall through to the underlying table for parity with ibis usage inside calc bodies.
   4. **Otherwise `UnknownMeasureRefError`** with a `difflib`-derived "did you mean?" suggestion.

   `t.all(x)` follows a separate contract: string measure name → totals reference; string column name → `column.sum().over(window())` with a logger warning saying "use a measure for non-sum semantics"; ibis Reduction → window-wrap; ibis Field on the virtual aggregated table → totals reference.

   **Footgun.** A model defining a measure with the same name as a base column shadows the column inside `t.foo`. This is rarely what users want when defining ratios — `t.distance.mean() / t.all(t.distance.mean())` has clear intent, but if the user wrote `t.distance / t.all(t.distance)` expecting "ratio of mean to total mean," they'd get the column instead. ADR commits to documenting this in user docs and adding a startup-time warning when a measure shadows a column.

3. **Nested-array measures + `t.all(...)` is unsupported.** `apply_calc_measures` raises `TotalsNotAvailableError` (calc_compiler.py:498) when a calc references `t.all(...)` on a model with nested-array measures. Reason: nested-array measures compile at multiple grains and join — there is no single "totals aggregation" that respects all grains. Users hitting this either (a) restructure the calc to reference a flat-grained intermediate measure, or (b) lower it manually via the `to_untagged()` escape hatch. ADR commits to the error rather than a silently-wrong answer; lifting the limitation is future work that requires designing per-grain totals semantics.

### Negative — accepted

- **The analyzer is harder than tag-matching.** Detecting `AllOf` was `isinstance(node, AllOf)`; detecting "agg-of-agg" requires walking ibis trees and recognizing the structural pattern. The analyzer is roughly the size of the deleted `validate_calc_ast` plus the AllOf-lift section of the deleted compiler — net code is still smaller (-727 LOC).
- **Calc lambdas now run twice.** Once at construction (for classification) and once at query time (against the real table). For typical models this is negligible — calc lambdas are tiny — but lambdas with side effects would be observed twice. Documented as an intentional tradeoff.
- **`IbisCalcScope` is load-bearing public-ish surface area.** When users write calcs they're effectively programming against the scope's dispatch rules (column-first, then known-measure suffix lookup, then `t.all(...)` totals). Documented.
- **Hard cutover, no compat shim.** Calc measures using the old curated-AST shapes (`MeasureRef("x")`, `AllOf(...)`, `BinOp(...)`) no longer work directly. In practice the user-facing API was always the lambda form (`lambda t: t.x / t.all(t.x)`); only internal tests touched the AST classes. Test suite migrated in the same branch.

### Negative — Phase 3

- **Existing tags with `SemanticMutateOp` will not deserialize.** Users re-tag from current model definitions or pin the prior BSL version. No migration tool: the failure mode is loud and the fix is to regenerate current tags.
- **Doc churn** turned out minimal: because `.mutate()` survives as an alias with identical user-facing behavior, every documented chain keeps working. Rewriting docs toward the declarative `with_measures` style remains a (non-blocking) editorial follow-up.
- **Test churn** was limited to one assertion (`test_dependency_graph` — pre-agg mutate now registers a dimension, so the graph gains a node) plus 20 new pinning tests in `test_mutate_compositions.py`.

### Neutral

- Pre-aggregation correctness (formerly the `mutated_gb_keys` machinery) is no longer the operator's responsibility — it was only the operator's responsibility because `SemanticMutateOp` introduced post-agg derived columns the planner couldn't see into. With analysis-based classification, the planner reads pushability off the calc measure's expression directly; correctness is a property of the analyzer.

## Alternatives considered

1. **Keep both, document the split.** Rejected — enshrines a confusing two-axis decision ("is this a measure or a mutate?") that was already a recurring user question.
2. **Drop calc measures, keep `mutate`.** Mutate's expression language is right; its placement (query-local, anonymous, not in `model.measures`) is wrong. Rejected.
3. **Drop `mutate`, keep the curated calc-measure AST** (the previous version of this ADR). Trades the planner cleanup for ergonomic regression on windows and `xo.case`. Rejected — the curated AST has to grow eventually anyway, and growing it is strictly more total work than going to ibis-as-the-language once.
4. **Extend the curated calc-measure AST node-by-node** (add `Case`, `Window`, `When`, …). Each extension requires coordinated changes to `validate_calc_ast`, the compiler, the AllOf-lift pass, and serialization. Strictly more work over time than this unification; rejected.
5. **Defang `SemanticMutateOp`** to a terminal-only post-agg escape hatch with no chained API and no pre-agg participation. Saves the planner cleanup but keeps two systems and the curated-AST limitation forever. Rejected.
6. **Keep `SemanticMutateOp` only at the post-aggregation boundary, hide the public `.mutate` API.** Strips the user-facing surface but leaves the planner branches and serialization registrations intact. Rejected.
7. **Soft cutover with a curated-AST compat shim** (the original Phase 1 plan). Rejected during implementation — the AST classes had no external users in practice (only internal tests), so the shim was pure carrying cost. Hard cutover saved the deprecation cycle.

## Open questions

### Resolved in Phase 1+2

- ~~**Analyzer scope for v1.**~~ Implemented per the recommendation: column-refs-on-one-source → `pushable`; agg-of-agg / empty-window-over-reduction → `references_AllOf`; any window node → `has_window`. Unrecognized inputs warn and fall back to `post_agg_only`.
- ~~**`t.all(...)` API.**~~ Kept the proxy method (`scope.all(...)`); emits a `Field(totals_vt, ...)` reference the analyzer recognizes. No user-visible change.
- ~~**`t.all(...)` over non-sum measures.**~~ Resolved with a real totals table. Per-group means cross-joined with overall mean; totals re-aggregation uses the formula, not a windowed sum.
- ~~**Inline reductions inside `t.all(...)`.**~~ Resolved via `lift_inline_reductions`.
- ~~**Calc-of-calc dependency ordering.**~~ Topological order from `CalcMeasure.depends_on`, captured at classification time and preserved through serialization.
- ~~**Compat-shim duration for the curated AST.**~~ Hard cutover; no shim.

### Resolved during ADR review

- ~~**`with_measures` semantics for chained calls after a filter.**~~ Settled. `SemanticFilter.with_measures` (`expr.py:1117`) builds its `MeasureScope` on `self.op().to_untagged()`, which is the *filtered* ibis table; lambdas registered there see the filtered rows. Phase 3's `.mutate(...)` desugaring inherits this — no new semantics to invent. Worth a regression test (`test_with_measures_after_filter_sees_filtered_table`) so the contract is pinned before mutate is removed.

### Resolved during Phase 3 readiness pass

- ~~**`.mutate(...)` chain method: alias or delete?**~~ **Alias.** Keep `.mutate(**post)` as a 3-line desugaring to `self.with_measures(**post).aggregate(*current_aggs, *post.keys())` on `SemanticAggregate`. The composition gotcha table above shows every chained-mutate shape lowers cleanly to the equivalent `with_measures` chain, so the alias is unambiguous. Preserves chain ergonomics; no operator/storage-path divergence. The `.mutate()` method on `SemanticTable` (pre-aggregate) lowers to a slightly different alias — `self.with_measures(**post)` — because pre-aggregate mutate is just "register measures on this model"; calling it as `with_measures` is the rename. *(Implementation note: Phase 3 lowered pre-agg mutate to `with_dimensions(**post)` instead — pre-agg derivations are row-grain columns used as group-by keys, which is what dimensions are; measures cannot be grouped by. See the Phase 3 landed section.)*

- ~~**Old tag handling.**~~ Hard cutoff. Tags containing `SemanticMutateOp` fail with the generic unknown-op error. Users re-tag from current model definitions or pin the prior BSL version.

- ~~**Pre-agg correctness coverage audit.**~~ Required before deletion. The pre-agg paths (`_to_untagged_with_preagg`, `_to_untagged_with_deferred_joins`) currently special-case mutate via `mutated_gb_keys` and `collect_mutates_to_join`. The audit consists of: (a) enumerate every mutate-aware branch (the planner-readiness table above is this list); (b) for each branch, identify the equivalent `with_measures` test case in `test_real_world_scenarios.py` / `test_preagg_stress.py`; (c) where coverage is missing, add the test *before* deleting the branch. The Phase 1+2 baseline is 978 passing tests (1 preexisting unrelated xorq `read_parquet` failure) — any Phase 3 deletion that doesn't keep that count at 978 or higher blocks the merge. Concrete coverage gaps known today: chained-after-limit (`SemanticLimit.with_measures` doesn't exist yet — Phase 3 must add it), and the `mutated_gb_keys` interaction with cross-table dimension bridges (`ops.py:2566–2581`).

### Resolved in Phase 3

- ~~**Sequencing inside Phase 3.**~~ Executed as recommended, with one delta: `SemanticLimit.with_measures` proved unnecessary because post-agg chain mutate resolves directly against the result table (the after-limit composition is covered by `test_mutate_after_limit`). Old `SemanticMutateOp` tags intentionally fail after the hard cutoff. Whether to keep the alias long-term remains open — it currently costs ~40 lines across two methods and zero operator surface.

- **Classification-result caching.** The analyzer runs once per calc per `_compile_aggregation` call — already memoized within a single query (`classify_calc_lambdas` in `calc_compiler.py:540`). Cross-query caching is out of scope: the classification depends on the model's known-measure set, which varies between models that share calc lambdas (e.g. via copy-paste). Anyone hitting hot-path overhead from re-classification has bigger problems (their model construction is in the request path); the right fix is to cache the *built* `SemanticAggregateOp` tree, not the classification record. ADR commits to no cross-query analyzer cache; the classification-cost story is "negligible for typical models, not the bottleneck for hot paths anyway."
