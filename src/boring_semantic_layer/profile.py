from __future__ import annotations

from pathlib import Path

import yaml
from ibis import BaseBackend
from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile


class ProfileError(Exception):
    """Raised when profile loading fails."""


def load_profile(
    profile_name: str | BaseBackend,
    profile_file: str | Path | None = None,
    search_locations: list[str] | None = None,
) -> BaseBackend:
    if isinstance(profile_name, BaseBackend):
        return profile_name

    name_path = Path(profile_name)
    if name_path.suffix in (".yml", ".yaml") and name_path.exists():
        return _load_from_file(name_path)

    if profile_file:
        return _load_from_file(Path(profile_file), profile_name)

    if search_locations is None:
        search_locations = ["bsl_dir", "local", "xorq_dir"]

    local_profile = Path.cwd() / "profiles.yml"
    bsl_profile = Path.home() / ".config" / "bsl" / "profiles" / f"{profile_name}.yml"

    for location in search_locations:
        if location == "bsl_dir":
            if bsl_profile.exists():
                return _load_from_yaml(bsl_profile, profile_name)

        elif location == "local":
            if local_profile.exists():
                return _load_from_yaml(local_profile, profile_name)

        elif location == "xorq_dir":
            try:
                xorq_profile = XorqProfile.load(profile_name)
                return xorq_profile.get_con()
            except Exception:
                continue

    raise ProfileError(_profile_not_found_error(profile_name, local_profile, search_locations))


def _load_from_file(yaml_file: Path, profile_name: str | None = None) -> BaseBackend:
    if not yaml_file.exists():
        raise ProfileError(f"Profile file not found: {yaml_file}")
    return _load_from_yaml(yaml_file, profile_name)


def _load_from_yaml(yaml_file: Path, profile_name: str | None = None) -> BaseBackend:
    with open(yaml_file) as f:
        config = yaml.safe_load(f)

    if not config:
        raise ProfileError(f"Empty or invalid YAML file: {yaml_file}")

    # Support both formats:
    # 1. New format with "profiles:" key
    # 2. Legacy format with profiles at root level
    profiles = config.get("profiles", config)

    if not profiles:
        raise ProfileError(f"No profiles found in {yaml_file}")

    if profile_name:
        if profile_name not in profiles:
            available = ", ".join(profiles.keys())
            raise ProfileError(
                f"Profile '{profile_name}' not found in {yaml_file}\n"
                f"Available profiles: {available}"
            )
        profile_config = profiles[profile_name]
    else:
        # Auto-select first profile when loading from file path
        # (backward compatibility for legacy behavior)
        profile_config = next(iter(profiles.values()))

    return _create_connection_from_config(profile_config)


def _create_connection_from_config(config: dict) -> BaseBackend:
    """Create a connection from a profile configuration dict."""
    import os

    import ibis

    config = config.copy()

    # Handle environment variable substitution
    for key, value in config.items():
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                raise ProfileError(f"Environment variable {env_var} not set")
            config[key] = env_value

    conn_type = config.pop("type", None)
    if not conn_type:
        raise ProfileError("Profile configuration must specify 'type' field")

    tables_config = config.pop("tables", None)

    try:
        # Use ibis backend-specific connection instead of generic connect
        # to avoid duplicate keyword argument issues
        connection = ibis.__getattribute__(conn_type).connect(**config)
    except Exception as e:
        raise ProfileError(f"Failed to create {conn_type} connection: {e}") from e

    if tables_config:
        _load_parquet_tables(connection, tables_config, conn_type)

    return connection


def _profile_not_found_error(name: str, local_profile: Path, search_locations: list[str]) -> str:
    message = f"Profile '{name}' not found.\n\n"
    message += "Searched in:\n"

    if "bsl_dir" in search_locations:
        bsl_dir = Path.home() / ".config" / "bsl" / "profiles"
        message += f"  - BSL profiles directory: {bsl_dir}\n"

    if "local" in search_locations:
        message += f"  - Local profiles file: {local_profile}\n"

    if "xorq_dir" in search_locations:
        message += "  - Xorq profiles directory\n"

    message += "\nTo create a profile, add it to profiles.yml or create a profile file:\n\n"
    message += "profiles:\n"
    message += f"  {name}:\n"
    message += "    type: duckdb\n"
    message += "    database: ':memory:'"

    return message


def _load_parquet_tables(connection: BaseBackend, tables_config: dict, conn_type: str) -> None:
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


def load_tables_from_profile(
    profile_name: str,
    profile_file: str | Path | None = None,
    table_names: list[str] | None = None,
) -> dict:
    """
    Load tables from a profile and return them as a dictionary.

    This is a helper function for yaml.py to get tables from a profile connection.

    Args:
        profile_name: Name of the profile or path to profile file
        profile_file: Optional path to the profile file
        table_names: Optional list of specific table names to load

    Returns:
        Dictionary mapping table names to table objects
    """
    connection = load_profile(profile_name, profile_file=profile_file)

    # Get all table names from the connection
    available_tables = connection.list_tables()

    # Filter to requested tables if specified
    if table_names:
        tables_to_load = [t for t in table_names if t in available_tables]
        missing = set(table_names) - set(available_tables)
        if missing:
            raise ProfileError(
                f"Tables not found in profile '{profile_name}': {', '.join(missing)}\n"
                f"Available tables: {', '.join(available_tables)}"
            )
    else:
        tables_to_load = available_tables

    # Return dict of table name -> table object
    return {name: connection.table(name) for name in tables_to_load}
