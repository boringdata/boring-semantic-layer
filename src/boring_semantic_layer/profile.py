from __future__ import annotations

import os
from pathlib import Path
from typing import Any

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
    if search_locations is None:
        search_locations = ["bsl_dir", "local", "xorq_dir"]

    # Get profile from parameter or environment
    if profile is None:
        profile = os.environ.get("BSL_PROFILE")

    # Get profile_file from parameter or environment
    if profile_file is None:
        profile_file = os.environ.get("BSL_PROFILE_FILE") or os.environ.get("BSL_PROFILE_PATH")

    # Auto-select first profile if profile_file is provided but no profile name
    if profile is None and profile_file:
        try:
            profiles_config = read_yaml_file(Path(profile_file))
            if profiles_config:
                profile = list(profiles_config.keys())[0]
        except Exception:
            pass

    # If still no profile, raise error
    if profile is None:
        raise ProfileError(
            "No profile specified. Provide a profile parameter or set BSL_PROFILE environment variable."
        )

    # Return if already a connection
    if isinstance(profile, BaseBackend):
        return profile

    # Handle dict config
    if isinstance(profile, dict):
        # Special case: dict with 'name' and 'file' keys (e.g., {"name": "test_db", "file": "profile.yml"})
        if "name" in profile:
            profile_name = profile["name"]
            profile_file = profile.get("file") or profile_file
            return get_connection(
                profile_name,
                profile_file=profile_file,
                search_locations=search_locations,
            )
        # Regular inline connection config (e.g., {"type": "duckdb", "database": ":memory:"})
        return _create_connection_from_config(profile)

    # Load from file or search
    if not isinstance(profile, str):
        raise ProfileError(f"Profile must be string, dict, or BaseBackend, got {type(profile)}")

    # Direct YAML file path
    if profile.endswith((".yml", ".yaml")) and Path(profile).exists():
        return _load_from_file(Path(profile))

    # Load from specific profile file
    if profile_file:
        return _load_from_file(Path(profile_file), profile)

    # Search for profile name in configured locations
    local_profile = Path.cwd() / "profiles.yml"
    bsl_profile = Path.home() / ".config" / "bsl" / "profiles" / f"{profile}.yml"

    for location in search_locations:
        if location == "bsl_dir" and bsl_profile.exists():
            return _load_from_file(bsl_profile, profile)

        if location == "local" and local_profile.exists():
            return _load_from_file(local_profile, profile)

        if location == "xorq_dir":
            try:
                xorq_profile = XorqProfile.load(profile)
                return xorq_profile.get_con()
            except Exception:
                continue

    raise ProfileError(
        f"Profile '{profile}' not found. "
        f"Create {local_profile} or ~/.config/bsl/profiles/{profile}.yml"
    )


def get_tables(
    connection: BaseBackend,
    table_names: list[str] | None = None,
) -> dict[str, Any]:
    """Get tables from connection as dict."""
    names = table_names or connection.list_tables()
    return {name: connection.table(name) for name in names}


def _load_from_file(yaml_file: Path, profile_name: str | None = None) -> BaseBackend:
    """Load profile from YAML file."""
    try:
        profiles_config = read_yaml_file(yaml_file)
    except (FileNotFoundError, ValueError) as e:
        raise ProfileError(str(e)) from e

    config = _get_profile_config(profiles_config, profile_name, yaml_file)
    return _create_connection_from_config(config)


def _get_profile_config(profiles_config: dict, profile_name: str | None, yaml_file: Path) -> dict:
    """Extract specific profile config from profiles dict."""
    if not profiles_config:
        raise ProfileError(f"Profile file {yaml_file} is empty")

    # Use first profile if no name specified
    if profile_name is None:
        profile_name = list(profiles_config.keys())[0]

    config = profiles_config.get(profile_name)
    if config is None:
        available = ", ".join(profiles_config.keys())
        raise ProfileError(
            f"Profile '{profile_name}' not found in {yaml_file}. Available profiles: {available}"
        )

    if not isinstance(config, dict):
        raise ProfileError(f"Profile '{profile_name}' must be a dict, got: {type(config)}")

    return config


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
