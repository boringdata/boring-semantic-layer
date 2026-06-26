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
- xorq profile-directory compatibility remains available only through an explicit, isolated `xorq_dir` lookup path.
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
- `get_connection()` uses default search locations `['bsl_dir', 'local']` unless the caller explicitly includes `xorq_dir`.
- `search_locations=[]` is respected and does not fall back to defaults.
- BSL YAML/dict profiles connect via plain ibis by default even when xorq is installed.
- `xorq_dir` lookup lazy-imports xorq only when explicitly requested.
- `xorq_dir` skips only no-xorq / known not-found cases; malformed profiles, auth failures, or backend connection failures surface as `ProfileError`.
- Profile config normalization is separated from connection effects through a small internal normalized spec.
- Env-var expansion is recursive for connection kwargs and table sources, with clear `ProfileError` messages for unresolved vars.
- Table bootstrap config is normalized before loading; malformed `tables` entries raise `ProfileError` instead of being silently skipped.
- Profile docs mention that BSL profiles use ibis by default and `xorq_dir` is explicit compatibility.

## Implementation plan

### Phase 1 — Make lookup pure-BSL by default

File: `src/boring_semantic_layer/profile.py`

1. Remove top-level `from ._xorq import HAS_XORQ, Profile as XorqProfile`.
2. Change default search handling from:

   ```python
   search_locations = search_locations or ["bsl_dir", "local", "xorq_dir"]
   ```

   to:

   ```python
   if search_locations is None:
       search_locations = ["bsl_dir", "local"]
   ```

3. Add a private lazy compatibility adapter:

   ```python
   def _load_from_xorq_profile_dir(name: str) -> BaseBackend | None:
       try:
           from ._xorq import HAS_XORQ, Profile as XorqProfile
       except ImportError:
           return None
       if not HAS_XORQ or XorqProfile is None:
           return None

       try:
           return XorqProfile.load(name).get_con()
       except FileNotFoundError:
           return None
       except KeyError:
           return None
       except Exception as exc:
           raise ProfileError(f"Failed to load xorq profile {name!r}") from exc
   ```

4. Before finalizing exception handling, inspect xorq’s real “profile not found” exception. Only known not-found cases should return `None`; all other failures should be visible.

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
- Missing `$VAR` / `${VAR}` raises `ProfileError` with the variable name and profile path, e.g. `database` or `tables.flights.source`.
- Literal-dollar behavior should be tested. The env regex should only reject valid env placeholder patterns, not every `$` character.

### Phase 3 — Make connection creation ibis-first

Replace xorq-first `_create_connection_from_config()` with a direct BSL flow:

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

`_get_ibis_connect()` should resolve `ibis.<backend>.connect` and raise `ProfileError` for unknown backend types.

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

- default search locations do not include `xorq_dir`;
- `search_locations=[]` is respected;
- explicit `search_locations=["xorq_dir"]` is the only path that imports xorq profile compatibility;
- BSL YAML/dict profile configs use plain ibis even when xorq is installed;
- missing env vars fail with `ProfileError` and include variable/path context;
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
- `xorq_dir` lookup is explicit compatibility for users who opt into xorq profile storage.
- The default search order is now BSL config dir, then local `profiles.yml`.

## Out of scope

- Reworking `_xorq.py` fallback behavior from PR #278.
- Reworking package optional dependency metadata from PR #278.
- Making semantic-model internals more or less xorq-dependent.
- Changing xorq serialization/tagging behavior.
