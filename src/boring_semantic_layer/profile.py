from __future__ import annotations

import os
from pathlib import Path

from ibis import BaseBackend
from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile

from .utils import read_yaml_file


class ProfileError(Exception):
    """Raised when profile loading fails."""


def get_connection(
    profile: str | dict | BaseBackend | None = None,
    profile_file: str | Path | None = None,
    search_locations: list[str] | None = None,
) -> BaseBackend:
    """Get xorq database connection from profile name, dict config, or env vars."""
    search_locations = search_locations or ["bsl_dir", "local", "xorq_dir"]

    # Resolve from env vars if not provided
    profile = profile or os.environ.get("BSL_PROFILE")
    profile_file = profile_file or os.environ.get("BSL_PROFILE_FILE")

    # Case 1: Already a connection - return as-is
    if isinstance(profile, BaseBackend):
        return profile

    # Case 2: Dict config
    if isinstance(profile, dict):
        return _connect_from_dict(profile, profile_file, search_locations)

    # Case 3: String profile name or YAML path
    if isinstance(profile, str):
        return _connect_from_string(profile, profile_file, search_locations)

    # Case 4: No profile but have profile_file - use first profile in file
    if profile_file:
        return _load_from_file(Path(profile_file))

    raise ProfileError(
        "No profile specified. Provide a profile parameter or set BSL_PROFILE environment variable."
    )


def _connect_from_dict(
    config: dict, profile_file: str | Path | None, search_locations: list[str]
) -> BaseBackend:
    """Handle dict profile: either reference {"name": ...} or inline {"type": ...}."""
    if "name" in config:
        # Reference to named profile: {"name": "my_db", "file": "profiles.yml"}
        return get_connection(config["name"], config.get("file") or profile_file, search_locations)
    # Inline config: {"type": "duckdb", "database": ":memory:"}
    return _create_connection_from_config(config)


def _connect_from_string(
    profile: str, profile_file: str | Path | None, search_locations: list[str]
) -> BaseBackend:
    """Handle string profile: YAML file path or profile name to search."""
    # Direct YAML file path
    if profile.endswith((".yml", ".yaml")) and Path(profile).exists():
        return _load_from_file(Path(profile))

    # Explicit profile file provided
    if profile_file:
        return _load_from_file(Path(profile_file), profile)

    # Search for profile name in configured locations
    return _search_profile(profile, search_locations)


def _search_profile(name: str, search_locations: list[str]) -> BaseBackend:
    """Search for profile name in configured locations."""
    local_profile = Path.cwd() / "profiles.yml"
    bsl_profile = Path.home() / ".config" / "bsl" / "profiles" / f"{name}.yml"

    for location in search_locations:
        if location == "bsl_dir" and bsl_profile.exists():
            return _load_from_file(bsl_profile, name)
        if location == "local" and local_profile.exists():
            return _load_from_file(local_profile, name)
        if location == "xorq_dir":
            try:
                return XorqProfile.load(name).get_con()
            except Exception:
                continue

    raise ProfileError(
        f"Profile '{name}' not found. Create {local_profile} or ~/.config/bsl/profiles/{name}.yml"
    )


def _load_from_file(yaml_file: Path, profile_name: str | None = None) -> BaseBackend:
    """Load profile from YAML file."""
    profiles = read_yaml_file(yaml_file)
    if not profiles:
        raise ProfileError(f"Profile file {yaml_file} is empty")

    name = profile_name or next(iter(profiles))
    config = profiles.get(name)
    if config is None:
        raise ProfileError(
            f"Profile '{name}' not found in {yaml_file}. Available: {', '.join(profiles)}"
        )
    if not isinstance(config, dict):
        raise ProfileError(f"Profile '{name}' must be a dict, got: {type(config)}")

    return _create_connection_from_config(config)


def _create_connection_from_config(config: dict) -> BaseBackend:
    """Create xorq connection from config dict with 'type' field."""
    config = config.copy()
    conn_type = config.get("type")
    if not conn_type:
        raise ProfileError("Profile must specify 'type' field")

    parquet_tables = config.pop("tables", None)

    # Use xorq (handles env var substitution automatically)
    kwargs_tuple = tuple(sorted((k, v) for k, v in config.items() if k != "type"))
    xorq_profile = XorqProfile(con_name=conn_type, kwargs_tuple=kwargs_tuple)
    connection = xorq_profile.get_con()

    # Load parquet tables if specified
    if parquet_tables:
        _load_parquet_tables(connection, parquet_tables, conn_type)

    return connection


def _load_parquet_tables(connection: BaseBackend, tables_config: dict, conn_type: str) -> None:
    """Load parquet files as tables into the connection."""
    if not hasattr(connection, "read_parquet"):
        raise ProfileError(
            f"Backend '{conn_type}' does not support loading parquet files.\n"
            f"The 'tables' configuration in profiles is only supported for backends with read_parquet() method.\n"
            f"Try using 'duckdb' or another backend that supports parquet files."
        )

    for table_name, source in tables_config.items():
        if isinstance(source, dict):
            source = source.get("source")
            if not source:
                continue
        elif not isinstance(source, str):
            continue

        try:
            connection.read_parquet(source, table_name=table_name)
        except Exception as e:
            raise ProfileError(
                f"Failed to load parquet file '{source}' as table '{table_name}': {e}"
            ) from e
