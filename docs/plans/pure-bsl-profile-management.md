# Plan: finish pure-BSL profile management on top of PR #278

## Context

PR #278 (`hussain/optional-xorq`) already does the broad optional-xorq work:

- moves `xorq` out of core dependencies into an optional extra;
- makes `src/boring_semantic_layer/_xorq.py` importable without xorq by falling back to plain ibis symbols;
- gates or skips many xorq-specific tests;
- adds a no-xorq CI path;
- starts profile no-xorq support by adding `HAS_XORQ` and plain-ibis env-var expansion.

This follow-up should therefore **not** redo top-level import-boundary or packaging optionalization work. It should focus only on making `src/boring_semantic_layer/profile.py` cleanly BSL-owned instead of “xorq-first when available, plain ibis when absent.”

## Goal

Make profile management pure BSL by default:

- BSL profile YAML/dict configs are parsed and normalized by BSL.
- BSL profile configs create plain `ibis.<backend>.connect(**kwargs)` connections by default, regardless of whether xorq is installed.
- xorq profile-directory compatibility remains available as a lazy fallback when xorq is installed, preserving current profile discovery behavior without making pure-BSL profile loading depend on xorq.
- Environment-variable expansion and parquet table bootstrap behavior are implemented and validated in BSL.

## Current PR #278 profile state

`src/boring_semantic_layer/profile.py` in PR #278 is improved, but still has profile-specific xorq coupling:

- imports `HAS_XORQ` and `Profile as XorqProfile` from `._xorq` at module import time;
- defaults to `search_locations or ["bsl_dir", "local", "xorq_dir"]`, so explicit `[]` is not respected and `xorq_dir` remains part of the default behavior;
- local/BSL YAML configs still try xorq first whenever xorq is installed;
- `xorq_dir` catches every exception and silently continues;
- env expansion covers only top-level string connection kwargs, not nested values or `tables` sources;
- `tables` config is still interpreted in `_load_parquet_tables()` and malformed entries are silently skipped.

## Acceptance criteria

- `profile.py` does not import `._xorq` at module import time for normal BSL profile loading.
- `get_connection()` keeps current default search locations `['bsl_dir', 'local', 'xorq_dir']` for feature compatibility, but `xorq_dir` is lazy and skipped when xorq is not installed.
- `search_locations=[]` is respected and does not fall back to defaults.
- BSL YAML/dict profiles connect via plain ibis by default even when xorq is installed.
- `xorq_dir` lookup lazy-imports xorq only when search reaches that fallback location.
- `xorq_dir` skips only no-xorq / known not-found cases; malformed profiles, auth failures, or backend connection failures surface as `ProfileError`.
- Profile config normalization is separated from connection effects through a small internal normalized spec.
- Env-var expansion is recursive for connection kwargs and table sources, with clear `ProfileError` messages for unresolved braced placeholders like `${VAR}`. Bare `$` in credentials/URLs is not rejected unless the implementation deliberately supports and tests `$VAR` strict expansion safely.
- Table bootstrap config is normalized before loading; malformed `tables` entries raise `ProfileError` instead of being silently skipped, with this compatibility change documented.
- Profile docs mention that BSL profiles use ibis by default and `xorq_dir` is explicit compatibility.
- Before implementation, run a xorq-installed compatibility probe proving plain-ibis profile connections still work with BSL semantic construction/tagging paths that current xorq users rely on, or narrow the ibis-first change if the probe fails.

## Implementation plan

### Phase 1 — Make lookup pure-BSL by default

File: `src/boring_semantic_layer/profile.py`

1. Remove top-level `from ._xorq import HAS_XORQ, Profile as XorqProfile`.
2. Preserve current default search order for feature compatibility, but stop using truthiness so explicit `[]` is respected:

   ```python
   if search_locations is None:
       search_locations = ["bsl_dir", "local", "xorq_dir"]
   ```

   This keeps xorq profile-directory lookup as a fallback for users who have xorq installed, while avoiding any xorq import until BSL/local profiles have missed.

3. Add a private lazy compatibility adapter for the fallback location:

   ```python
   def _load_from_xorq_profile_dir(name: str) -> BaseBackend | None:
       try:
           from ._xorq import HAS_XORQ, Profile as XorqProfile
       except ImportError:
           return None
       if not HAS_XORQ or XorqProfile is None:
           return None

       try:
           xorq_profile = XorqProfile.load(name)
       except _KNOWN_XORQ_PROFILE_NOT_FOUND_ERRORS:
           return None
       except Exception:
           # If the xorq not-found probe is inconclusive, preserve search-fallback
           # behavior by treating load() failures as a miss. Do not apply this to
           # get_con(): once a profile loads, connection failures are real errors.
           return None

       try:
           return xorq_profile.get_con()
       except Exception as exc:
           raise ProfileError(f"Failed to connect using xorq profile {name!r}") from exc
   ```

4. Hard gate before implementation: identify xorq’s real “profile not found” exception(s) from `XorqProfile.load(name)`. Do not ship the guessed `FileNotFoundError` / `KeyError` list without this probe.

   If the probe is inconclusive, preserve search UX by treating unknown `load()` exceptions in the search-fallback path as `None`/continue so a genuinely missing profile still reaches the aggregate `Profile '<name>' not found...` error. Keep `.get_con()` failures strict: once a xorq profile loads, connection/auth/backend failures are real and should surface as `ProfileError`.

### Phase 2 — Normalize profile config once

Add a small internal normalized model. It does not need to be public API.

```python
from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any

@dataclass(frozen=True)
class _ConnectionSpec:
    backend: str
    kwargs: Mapping[str, Any]
    parquet_tables: Mapping[str, str]
```

Add helpers:

```python
def _profile_connection_spec(raw: Mapping[str, Any]) -> _ConnectionSpec:
    """Validate raw profile config, expand env vars, and split BSL metadata from ibis kwargs."""


def _normalize_parquet_tables(raw_tables: Any) -> dict[str, str]:
    """Normalize tables config to table_name -> parquet source."""


def _expand_env_vars(value: Any, *, path: str) -> Any:
    """Recursively expand env vars in strings, mappings, lists, and tuples."""
```

Rules:

- `type` is required and must be a non-empty string.
- `tables` is BSL metadata and is never passed to ibis connect.
- All other keys become ibis connection kwargs.
- Env expansion applies to connection kwargs and parquet table sources.
- Non-string scalar values are preserved.
- Missing `${VAR}` raises `ProfileError` with the variable name and profile path, e.g. `database` or `tables.flights.source`.
- Bare `$` characters in passwords/URLs must remain valid literals unless the implementation explicitly supports `$VAR` substitution with an escape rule and tests. Preferred safe policy: strict expansion for `${VAR}` only; leave bare `$word` untouched to avoid breaking credentials.

### Phase 3 — Make connection creation ibis-first after compatibility probe

Before changing connection creation, run a xorq-installed compatibility probe:

- a BSL/local DuckDB profile loads through plain ibis;
- at least one non-DuckDB backend used by current xorq-profile users is checked or explicitly documented as not covered by the compatibility probe;
- `SemanticModel` construction still accepts the resulting tables and applies the existing `_ensure_xorq_table()` boundary when xorq is installed;
- existing xorq serialization/tagging smoke paths still work or fail with a known pre-existing limitation.

If this probe shows plain-ibis profile connections break a current xorq user workflow, adjust the implementation to preserve a documented opt-in xorq connection mode rather than silently regressing compatibility.

Then replace xorq-first `_create_connection_from_config()` with a direct BSL flow:

```python
spec = _profile_connection_spec(config)
connect = _get_ibis_connect(spec.backend)
try:
    connection = connect(**dict(spec.kwargs))
except Exception as exc:
    raise ProfileError(f"Failed to connect using backend {spec.backend!r}: {exc}") from exc

if spec.parquet_tables:
    _load_parquet_tables(connection, spec.parquet_tables, spec.backend)
return connection
```

`_get_ibis_connect()` should resolve `ibis.<backend>.connect` and raise `ProfileError` for unknown backend types. Reuse/refactor PR #278's existing `_connect_plain_ibis` logic instead of duplicating backend-resolution helpers.

### Phase 4 — Make parquet table loading effect-only

After `_normalize_parquet_tables()`, `_load_parquet_tables()` should not interpret arbitrary shapes. It should only loop over normalized `table_name -> source` values and call `connection.read_parquet(source, table_name=table_name)`.

Malformed config should fail earlier with `ProfileError`, for example:

```yaml
bad:
  type: duckdb
  tables:
    flights:
      path: flights.parquet  # invalid: expected source
```

Supported shapes:

```yaml
tables:
  flights: data/flights.parquet
  carriers:
    source: data/carriers.parquet
```

### Phase 5 — Tests

Add or update profile tests for:

- default search locations preserve `xorq_dir` as a final fallback but do not import xorq unless BSL/local profiles miss;
- `search_locations=[]` is respected;
- explicit `search_locations=["xorq_dir"]` imports xorq profile compatibility directly;
- BSL YAML/dict profile configs use plain ibis even when xorq is installed;
- xorq-installed semantic/tagging smoke paths remain compatible with plain-ibis profile connections, or an opt-in xorq connection mode is documented;
- missing `${VAR}` env refs fail with `ProfileError` and include variable/path context;
- bare `$` in credentials remains literal under the chosen env expansion policy;
- env vars expand inside parquet table sources;
- malformed `tables` config raises `ProfileError`;
- supported string and `{source: ...}` table configs still work;
- explicit `xorq_dir` failure semantics do not swallow non-not-found errors.

Suggested commands on top of PR #278:

```bash
uv run pytest src/boring_semantic_layer/tests/test_profile.py -q
uv run pytest src/boring_semantic_layer/tests/test_dependency_groups.py -q
```

### Phase 6 — Docs

Update profile docs to say:

- BSL profile files use ibis connections by default.
- `type` maps to an ibis backend name.
- `xorq_dir` lookup is a lazy compatibility fallback when xorq is installed.
- The default search order remains BSL config dir, local `profiles.yml`, then xorq profile dir fallback.

## Out of scope

- Reworking `_xorq.py` fallback behavior from PR #278.
- Reworking package optional dependency metadata from PR #278.
- Making semantic-model internals more or less xorq-dependent.
- Changing xorq serialization/tagging behavior.
