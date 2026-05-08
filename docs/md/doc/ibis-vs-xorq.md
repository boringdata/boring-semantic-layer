# Plain Ibis vs Xorq Backends

BSL is built on top of Ibis, but internally it uses [xorq](https://github.com/xorq-labs/xorq) — a fork of Ibis that adds tagged-metadata serialization, a catalog/rebuild system, and a structured resolver tree. This page explains:

- when BSL operates on a plain `ibis` table vs a xorq-wrapped one,
- which BSL features work on plain ibis and which require xorq,
- how serialization and round-trip differ between the two,
- gotchas you may hit at the boundary.

> `xorq>=0.3.19` is a **hard** dependency of BSL (see `pyproject.toml`). The split below isn't about whether xorq is installed — it's about whether the *table* you handed BSL is a xorq-vendored ibis table or a plain `ibis-framework` table.

## The boundary

Every `SemanticModel` runs the table you pass through one helper at construction time:

```python
# src/boring_semantic_layer/ops.py:224
def _ensure_xorq_table(table):
    """Convert plain ibis Table to xorq-vendored ibis if possible."""
    if "xorq.vendor.ibis" not in type(table).__module__:
        try:
            from xorq.common.utils.ibis_utils import from_ibis
            return from_ibis(table)
        except Exception:
            return table
    return table
```

There are three resulting states:

| You pass in | What BSL holds internally |
|---|---|
| `xo.connect(...).read_*()` (xorq-vendored already) | xorq table, no-op |
| `ibis.duckdb.connect()...` (xorq supports the backend) | xorq table, via `from_ibis()` |
| `ibis.bigquery.connect()...` / Databricks / other backend xorq doesn't register | **plain ibis table** — `from_ibis()` raised, BSL silently fell back |

The fallback path is what `tests/test_plain_ibis.py` exercises. Backend detection elsewhere in the codebase uses the same module-string check:

```python
# src/boring_semantic_layer/ops.py:36
if type(expr.op()).__module__.startswith("xorq.vendor.ibis"):
    ...
```

## What works on plain ibis

`SemanticTable.to_untagged()` produces a plain ibis expression — no xorq calls in the output path. So everything that flows through `to_untagged()` works regardless of which side of the boundary you're on:

- `.aggregate()`, `.filter()`, `.group_by()`, `.order_by()`, `.limit()`, `.mutate()`
- `.join_one()`, `.join_many()`, `.join_cross()` — including the multi-way join column-ambiguity fix in `SemanticJoinOp.to_untagged()`
- `.execute()`, `.sql()`, `.compile()`, `.to_pandas()`, `.to_pyarrow()`, `.to_polars()`, `.to_csv()`, `.to_parquet()`
- Calc measures (`percent_of_total`, `t.all()` patterns) when *executed* — the rewrite happens in untagged ibis.

`tests/test_plain_ibis.py` covers each of these against an `ibis.duckdb.connect()` and against a monkey-patched `_ensure_xorq_table` that forces the plain-ibis fallback (simulating an unsupported backend like BigQuery).

One subtlety: `SemanticTable.execute()` calls `_rebind_to_canonical_backend(to_untagged(self))` first (`expr.py:261`). That helper rewrites every `xorq.vendor.ibis` `DatabaseTable` to share a single backend, fixing "Multiple backends found" errors when you composed two separately-converted tables. It is a **no-op on plain ibis** expressions (`ops.py:285-291`), so plain-ibis users don't pay for it but also don't benefit from it.

## What requires xorq

Anything that involves **tag metadata** on the expression tree:

### `to_tagged()` / `from_tagged()`

`SemanticTable.to_tagged()` walks the BSL op tree, extracts dimensions / measures / filter predicates / join predicates / calc measures into a structured representation, and stamps them onto a xorq `Tag` node attached to the underlying expression. `from_tagged()` reverses it.

```python
# src/boring_semantic_layer/serialization/__init__.py:128
result = try_import_xorq().bind(do_convert)
if isinstance(result, Failure):
    error = result.failure()
    if isinstance(error, ImportError):
        raise ImportError(
            "Xorq conversion requires the 'xorq' optional dependency. ..."
        ) from error
    raise error
```

Even when xorq is installed, `to_tagged()` will fail (less cleanly) if the underlying table is plain ibis — there's no `Tag` op in vanilla ibis to hang the metadata off. The plain-ibis test `test_to_tagged_works_or_errors_cleanly` only asserts that the failure is not an internal `AssertionError` from xorq's guts.

### Catalog rebuild (`from_tag_node`)

BSL registers a tag handler with xorq via the `xorq.from_tag_node` entry point:

```toml
# pyproject.toml
[project.entry-points."xorq.from_tag_node"]
bsl = "boring_semantic_layer.serialization.tag_handler:bsl_tag_handler"
```

This lets xorq's catalog reconstruct a base `SemanticModel` from a tag node it discovered, without BSL-specific imports inside xorq itself. `serialization/tag_handler.py` defines:

- `extract_metadata(tag_node)` — returns sidecar dim/measure names for the catalog
- `from_tag_node(tag_node)` — rebuilds the **base** `SemanticTableOp` (not the query chain on top of it), so the caller can issue fresh `.query()` calls
- `reemit(tag_node, rebuild_subexpr)` — used during catalog rebuild to translate the source while preserving the tag metadata above it

None of this exists on the plain-ibis side.

### `Deferred.resolve(table)`

`xorq.vendor.ibis.common.deferred.Deferred.resolve(table)` only accepts a **xorq-vendored** table. If you serialized a plain-ibis `Deferred` and try to resolve it against a plain ibis table, you'll get a type mismatch.

BSL handles this transparently by routing plain-ibis `Deferred` callables through xorq's `_` to get xorq types out:

```python
# src/boring_semantic_layer/utils.py:504
def expr_to_structured(fn):
    from xorq.vendor.ibis.common.deferred import Deferred as XorqDeferred
    @safe
    def do_convert():
        from xorq.vendor.ibis import _
        if isinstance(fn, XorqDeferred):
            return serialize_resolver(fn._resolver)
        # For ibis Deferred (not xorq vendor), resolve through xorq _
        if _is_deferred(fn):
            result = fn.resolve(_)
            ...
```

This is why dimensions/measures defined as plain `ibis.Deferred` (e.g. `_.amount.sum()` from `import ibis; ibis._`) still serialize correctly — BSL coerces them to xorq types at the serialization boundary.

## Serialization (v2.0): structured, pickle-free

Two flavors of serialization exist in BSL:

| Flavor | Where | Backend needed |
|---|---|---|
| **String** form (`to_ibis_string`, `from_ibis_string`) | YAML configs (`yaml.py`), legacy paths | None — works against ibis Deferred |
| **Tagged** form (`to_tagged`, `from_tagged`) | Round-trip through xorq catalog, `compile_all` | xorq |

The tagged form, since v2.0 (Feb 2026), uses **structured tuple representation** instead of pickle:

- `dimensions` → `expr_struct`: tuple-of-pairs of `(name, structured_resolver_tuple)`
- `filter` predicate → `predicate_struct`
- `aggregate` → `aggs_struct`
- `mutate` → `post_struct`
- `order_by` → `value_struct`
- `join.on` → `on_struct` (binary predicate serialized via two named `Variable`s)
- `calc_measures` → `expr_struct` of the cross-measure expression tree

The structured form is a hashable nested tuple compatible with xorq's `FrozenOrderedDict` tag metadata. `_unpickle_callable` is retained only for reading v1.0 tags — new tags never write pickle.

`serialize_resolver` / `deserialize_resolver` (`utils.py`) walk the `Deferred._resolver` tree:

- `Variable`, `Just`, `Attr`, `Item`, `Call`, `BinaryOperator`, `UnaryOperator`, `Sequence`, `Mapping`
- `Just(callable)` → `("fn", module, qualname)` (resolved via `importlib`)
- `Just(ibis_literal)` → `("ibis_literal", value, dtype_str)`
- Frozen-slotted resolver objects need `__precomputed_hash__` set after `object.__setattr__` reconstruction (see `_finalize_frozen_slotted`).

## Round-trip semantics

| Path | Plain ibis | Xorq |
|---|---|---|
| BSL → SQL string (`.sql()`) | ✓ | ✓ |
| BSL → pandas (`.execute()`) | ✓ | ✓ |
| BSL → YAML | ✓ (string form) | ✓ |
| YAML → BSL | ✓ | ✓ |
| BSL → tagged xorq expr → BSL | ✗ | ✓ |
| Catalog `from_tag_node` rebuild | ✗ | ✓ |

YAML is one-way at the file level — there is no `.to_yaml()` on `SemanticTable` — but the dimensions/measures themselves serialize fine without xorq because they're stored as ibis-Deferred string expressions (`_.amount.sum()`).

The full BSL → tagged → BSL loop is what `tests/test_xorq_string_serialization.py`, `test_xorq_convert.py`, `test_xorq_rebuild.py`, `test_xorq_tag_handler.py`, and `test_malloy_xorq_roundtrip.py` exercise. None of those tests have a plain-ibis equivalent because the loop is structurally a xorq feature.

## Gotchas

**Calc measures only round-trip via xorq.** Calc measures (`percent_of_total`, custom cross-measure expressions) execute fine on plain ibis — they get rewritten in `to_untagged()`. But to **persist** a query containing them and reload it later, you need the tagged form.

**Multi-backend expressions on xorq.** `from_ibis()` creates a fresh `Backend` instance per call. If you build a model from two separately-converted tables and then try to execute, you'll hit "Multiple backends found." `_rebind_to_canonical_backend` is the workaround — it picks the first `DatabaseTable.source` and rebinds the rest. This problem doesn't exist on plain ibis because there's no `from_ibis()` step.

**`_ensure_xorq_table()` swallows failures.** It catches `Exception` from `from_ibis()` and silently returns the original table. If a backend you expected xorq to handle silently falls through to plain ibis, you'll only notice when `to_tagged()` later refuses to work. For unsupported backends this is by design (issue #242); for misconfiguration it's a debugging trap.

**Pre-aggregation join direction matters.** On joins involving `join_many`, BSL pre-aggregates the fact side and re-joins to the dimension. The dim_bridge must be on the **left** side of the left join — otherwise unmatched dimension rows get dropped. This is in `SemanticJoinOp.to_untagged()` and applies to both backends; see CLAUDE.md project memory for context.

**Use `join_one` for reference tables.** `join_many` (LEFT JOIN) inflates `mean()` measures across multiple reference tables. For 1:1 / N:1 lookups, prefer `join_one` (INNER JOIN). Backend-agnostic.

## When to use which

- **Plain ibis is enough** if you only need to define a semantic model, query it, and ship results (pandas / SQL / parquet). This includes BigQuery, Databricks, and other backends xorq doesn't yet wrap.
- **You need xorq** as soon as you want to persist a *query* (not just its result), rebuild from a catalog, ship a tagged expression to another process, or use the xorq cube-cache (`aggregate_cache_storage` parameter on `to_tagged`).
