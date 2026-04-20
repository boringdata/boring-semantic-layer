# boring-semantic-layer v0.3.12

## Highlights

This release focuses on join planning and reliability.

- Added deferred `join_one` support for dimension lookups after aggregation (#220)
- Added grain-aware `join_one` behavior via `is_entity` dimensions (#219)
- Pruned unused dimension joins from generated queries for leaner SQL (#228)
- Fixed `demo_bsl_v2.py` so the example script works again (#236)
- Added regression coverage for read-only DuckDB joined dimension introspection (#237)

## Why upgrade

If you use joined semantic models, this release improves correctness and efficiency:

- better handling of dimension lookups across joins
- better join behavior when entity grain is defined explicitly
- fewer unnecessary joins in compiled queries
- better protection against regressions around read-only DuckDB joined introspection

## Included changes

- `3c7c098` feat: deferred join_one — dimension lookups after aggregation (#220)
- `52acdc9` feat: prune unused dimension joins from queries (#228)
- `0059738` fix: repair demo_bsl_v2 script (#236)
- `0fbf06e` feat: grain-aware join_one via is_entity dimensions (#219)
- `7ac2031` test: cover read-only joined dimensions (#237)

## Install

```bash
pip install -U boring-semantic-layer==0.3.12
```

## Notes

- The read-only DuckDB issue reported in #232 appears to have already been fixed by earlier join/backend work on `main`; this release keeps the regression test in the published package history.
