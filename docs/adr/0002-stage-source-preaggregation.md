# ADR 0002: Stage source-aware pre-aggregation

## Status

Accepted.

## Context

Source-aware aggregation prevents fan-out by evaluating every measure on its
own semantic source before the source results are joined. The implementation
previously lived in one 1,089-line method on `SemanticAggregateOp`. That method
mixed join preparation, filter provenance, aggregate classification, per-source
grain selection, compilation, result combination, calculated measures, and
final validation.

The behavior was well covered, but the intermediate contracts were implicit.
Changing one concern required understanding every mutable local carried through
the method.

## Decision

Move the source-aware planner to `boring_semantic_layer.preagg` and express it
as an explicit pipeline:

1. `MetadataStage`
2. `JoinPreparationStage`
3. `FilterPlanningStage`
4. `AggregationPlanningStage`
5. `SourcePreAggregationStage`
6. `ResultCombinationStage`
7. `CalculatedMeasureStage`
8. `FinalProjectionStage`

Source pre-aggregation is itself decomposed into preparation, measure binding,
grain planning, and compilation stages.

Stage inputs and outputs are frozen, slotted attrs records. Mapping fields use
read-only proxies because several Ibis expression records are intentionally not
hashable. The per-source loop uses one mutable attrs builder to accumulate
result tables and measure bookkeeping; it is scoped to that stage and freezes
to `PreAggregationProducts` before result combination.

`SemanticAggregateOp._to_untagged_with_preagg` remains as the compatibility
boundary and delegates to `PreAggregationPlanner`.

## Invariants

- A stage may mutate local variables but not an input record.
- Cross-stage state must be represented by an attrs output record.
- Source compilation may append only through `PreAggregationProductsBuilder`.
- The builder must freeze before source results are combined.
- Unsupported ownership, grain, or predicate shapes continue to fail closed.
- The final stage must reject missing requested columns rather than silently
  dropping them.

## Consequences

- Each planning responsibility has a named, directly testable contract.
- Source partitioning and filter ownership can be inspected without executing
  the entire query.
- The largest planning stage is substantially smaller than the former method.
- The planner has more structural code and records than the monolith.
- Low-level Ibis and xorq helpers still live in `ops`; `preagg` is imported
  lazily to keep that dependency explicit without an import-time cycle.
