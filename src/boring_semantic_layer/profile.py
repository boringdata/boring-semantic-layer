from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from ibis import BaseBackend
from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile

from .utils import read_yaml_file


def _substitute_env_vars(value: Any) -> Any:
    """Recursively substitute ${VAR} or $VAR with environment variables."""
    if isinstance(value, str):
        pattern = re.compile(r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)")

        def replacer(match):
            var_name = match.group(1) or match.group(2)
            return os.environ.get(var_name, match.group(0))

        return pattern.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]
    return value


class ProfileError(Exception):
    """Raised when profile loading fails."""

    @classmethod
    def not_found(cls, name: str, local_profile: Path, search_locations: list[str]) -> ProfileError:
        """Create ProfileError for profile not found."""
        return cls(
            f"Profile '{name}' not found. "
            f"Create {local_profile} or ~/.config/bsl/profiles/{name}.yml"
        )


def get_connection(
    profile: str | dict | BaseBackend | None = None,
    profile_file: str | Path | None = None,
    use_xorq: bool = True,
    search_locations: list[str] | None = None,
) -> BaseBackend:
    """Get database connection from profile.

    Resolves connection from multiple sources (in order):
    1. Explicit profile parameter (string, dict, or BaseBackend)
    2. Environment variables (BSL_PROFILE, BSL_PROFILE_FILE, BSL_PROFILE_PATH)

    Args:
        profile: Profile name, config dict, BaseBackend, or None (uses env vars)
        profile_file: Optional path to profile YAML file (or from env vars)
        use_xorq: If True, use xorq backend (default). If False, use pure ibis.
        search_locations: Order of locations to search for profiles (default: ["bsl_dir", "local", "xorq_dir"])

    Returns:
        BaseBackend: Database connection

    Raises:
        ProfileError: If profile cannot be loaded or doesn't exist

    Example:
        >>> con = get_connection()  # Uses env vars
        >>> con = get_connection("my_profile", use_xorq=False)  # Pure ibis
        >>> con = get_connection({"type": "duckdb", "database": ":memory:"})
    """
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
                use_xorq=use_xorq,
                search_locations=search_locations,
            )
        # Regular inline connection config (e.g., {"type": "duckdb", "database": ":memory:"})
        return _create_connection_from_config(profile, use_xorq=use_xorq)

    # Load from file or search
    if not isinstance(profile, str):
        raise ProfileError(f"Profile must be string, dict, or BaseBackend, got {type(profile)}")

    # Direct YAML file path
    if profile.endswith((".yml", ".yaml")) and Path(profile).exists():
        return _load_from_file(Path(profile), use_xorq=use_xorq)

    # Load from specific profile file
    if profile_file:
        return _load_from_file(Path(profile_file), profile, use_xorq=use_xorq)

    # Search for profile name in configured locations
    local_profile = Path.cwd() / "profiles.yml"
    bsl_profile = Path.home() / ".config" / "bsl" / "profiles" / f"{profile}.yml"

    for location in search_locations:
        if location == "bsl_dir" and bsl_profile.exists():
            return _load_from_file(bsl_profile, profile, use_xorq=use_xorq)

        if location == "local" and local_profile.exists():
            return _load_from_file(local_profile, profile, use_xorq=use_xorq)

        if location == "xorq_dir" and use_xorq:
            try:
                xorq_profile = XorqProfile.load(profile)
                return xorq_profile.get_con()
            except Exception:
                continue

    raise ProfileError.not_found(profile, local_profile, search_locations)


def get_tables(
    connection: BaseBackend,
    table_names: list[str] | None = None,
) -> dict[str, Any]:
    """Get tables from a connection as a dictionary.

    Helper function for loading tables from a connection, with optional filtering.
    Handles conversion of xorq tables to pure ibis tables.

    Args:
        connection: Database connection
        table_names: Optional list of table names to load. If None, loads all tables.

    Returns:
        dict: Dictionary mapping table names to ibis Table objects

    Example:
        >>> con = get_connection("my_profile")
        >>> tables = get_tables(con)  # {"users": Table, "orders": Table}
        >>> tables = get_tables(con, ["users"])  # Only users table
    """
    import ibis

    names = table_names or connection.list_tables()
    tables = {}

    for name in names:
        table = connection.table(name)
        # Check if this is a xorq vendored ibis table that needs conversion
        if type(table).__module__.startswith("xorq.vendor.ibis"):
            # TODO: REMOVE !!!!!
            table = ibis.memtable(table.execute())
        tables[name] = table

    return tables


def _load_from_file(
    yaml_file: Path, profile_name: str | None = None, use_xorq: bool = True
) -> BaseBackend:
    """Load profile from YAML file."""
    try:
        profiles_config = read_yaml_file(yaml_file)
    except (FileNotFoundError, ValueError) as e:
        raise ProfileError(str(e)) from e

    config = _get_profile_config(profiles_config, profile_name, yaml_file)
    return _create_connection_from_config(config, use_xorq=use_xorq)


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


def _create_connection_from_config(config: dict, use_xorq: bool = True) -> BaseBackend:
    """Create connection from configuration dict.

    Args:
        config: Configuration dictionary with 'type' and connection parameters
        use_xorq: If True, use xorq for connection (default). If False, use pure ibis.
    """
    import ibis

    config = config.copy()
    conn_type = config.get("type")
    if not conn_type:
        raise ProfileError("Profile must specify 'type' field")

    parquet_tables = config.pop("tables", None)

    if use_xorq:
        # Use xorq (handles env var substitution automatically)
        kwargs_tuple = tuple(sorted((k, v) for k, v in config.items() if k != "type"))
        xorq_profile = XorqProfile(con_name=conn_type, kwargs_tuple=kwargs_tuple)
        connection = xorq_profile.get_con()
    else:
        # Use pure ibis
        config.pop("type")
        config = _substitute_env_vars(config)

        # Create ibis connection
        try:
            backend = getattr(ibis, conn_type, None)
            if not backend or not hasattr(backend, "connect"):
                raise ProfileError(f"Unsupported ibis backend: {conn_type}")
            connection = backend.connect(**config)
        except Exception as e:
            raise ProfileError(f"Failed to create {conn_type} connection: {e}") from e

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
