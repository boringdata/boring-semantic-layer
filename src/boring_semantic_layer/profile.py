from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ibis import BaseBackend
from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile

from .utils import read_yaml_file


class ProfileError(Exception):
    """Raised when profile loading fails."""

    @classmethod
    def not_found(cls, name: str, local_profile: Path, search_locations: list[str]) -> ProfileError:
        """Create ProfileError for profile not found."""
        return cls(
            f"Profile '{name}' not found. "
            f"Create {local_profile} or ~/.config/bsl/profiles/{name}.yml"
        )


class ProfileLoader:
    """Loads database connections from profile configurations."""

    def __init__(self, search_locations: list[str] | None = None):
        """Initialize ProfileLoader with optional search location order."""
        self.search_locations = search_locations or ["bsl_dir", "local", "xorq_dir"]

    def get_connection(
        self,
        profile: str | dict | BaseBackend,
        profile_file: str | Path | None = None,
    ) -> BaseBackend:
        """Get database connection from profile."""
        # Return if already a connection
        if isinstance(profile, BaseBackend):
            return profile

        # Create from inline dict config
        if isinstance(profile, dict):
            return self._create_ibis_connection(profile)

        # Load from file or search
        if not isinstance(profile, str):
            raise ProfileError(f"Profile must be string, dict, or BaseBackend, got {type(profile)}")

        # Direct YAML file path
        if profile.endswith((".yml", ".yaml")) and Path(profile).exists():
            return self._load_from_file(Path(profile))

        # Load from specific profile file
        if profile_file:
            return self._load_from_file(Path(profile_file), profile)

        # Search for profile name
        return self._search_and_load(profile)

    def load_tables(
        self,
        profile: str | dict | None = None,
        profile_file: str | Path | None = None,
    ) -> dict[str, Any]:
        """Load tables from profile."""
        # Get profile from parameter or environment
        profile = profile or os.environ.get("BSL_PROFILE")
        if not profile:
            return {}

        # Extract table filter from dict config
        table_filter = None
        if isinstance(profile, dict):
            table_filter = profile.get("tables")
            profile_file = profile.get("file") or profile_file
            profile = profile.get("name", profile)

        # Get connection and return tables
        connection = self.get_connection(profile, profile_file=profile_file)
        return self._get_tables_from_connection(connection, table_filter)

    def _search_and_load(self, profile_name: str) -> BaseBackend:
        """Search for profile in configured locations."""
        local_profile = Path.cwd() / "profiles.yml"
        bsl_profile = Path.home() / ".config" / "bsl" / "profiles" / f"{profile_name}.yml"

        for location in self.search_locations:
            if location == "bsl_dir" and bsl_profile.exists():
                return self._load_from_file(bsl_profile, profile_name)

            if location == "local" and local_profile.exists():
                return self._load_from_file(local_profile, profile_name)

            if location == "xorq_dir":
                try:
                    xorq_profile = XorqProfile.load(profile_name)
                    return xorq_profile.get_con()
                except Exception:
                    continue

        raise ProfileError.not_found(profile_name, local_profile, self.search_locations)

    def _load_from_file(self, yaml_file: Path, profile_name: str | None = None) -> BaseBackend:
        """Load profile from YAML file."""
        try:
            profiles_config = read_yaml_file(yaml_file)
        except (FileNotFoundError, ValueError) as e:
            raise ProfileError(str(e)) from e

        config = self._get_profile_config(profiles_config, profile_name, yaml_file)
        return self._create_ibis_connection(config)

    def _get_profile_config(
        self, profiles_config: dict, profile_name: str | None, yaml_file: Path
    ) -> dict:
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
                f"Profile '{profile_name}' not found in {yaml_file}. "
                f"Available profiles: {available}"
            )

        if not isinstance(config, dict):
            raise ProfileError(f"Profile '{profile_name}' must be a dict, got: {type(config)}")

        return config

    def _create_ibis_connection(self, config: dict) -> BaseBackend:
        """Create connection from configuration dict."""
        # Currently using legacy ibis connection
        # TODO: Switch to xorq connection (see _create_xorq_connection below)
        return _create_ibis_connection_legacy(self, config)

    def _get_tables_from_connection(
        self, connection: BaseBackend, table_filter: list[str] | None = None
    ) -> dict[str, Any]:
        """Get tables from connection, optionally filtered by name list."""
        table_names = table_filter or connection.list_tables()
        return {name: connection.table(name) for name in table_names}

    @staticmethod
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


# Default loader instance
loader = ProfileLoader()


# =============================================================================
# TO MIGRATE: Temporary code - will be replaced when fully migrated to xorq
# =============================================================================


def _substitute_env_vars(value: Any) -> Any:
    """Recursively substitute environment variables in config values.

    Supports both ${VAR} and $VAR formats.
    Raises ProfileError if referenced environment variable is not set.

    NOTE: This function will be replaced by xorq's environment variable handling.
    """
    import re

    if isinstance(value, str):
        # Support both ${VAR} and $VAR formats - process ${VAR} first to avoid conflicts
        for pattern, fmt in [
            (r"\$\{([^}]+)\}", lambda v: f"${{{v}}}"),  # ${VAR}
            (r"\$([A-Z_][A-Z0-9_]*)", lambda v: f"${v}"),  # $VAR
        ]:
            for var_name in re.findall(pattern, value):
                if var_name not in os.environ:
                    raise ProfileError(
                        f"Environment variable not set: {var_name}\n\n"
                        f"Set it before running:\n"
                        f"  export {var_name}=..."
                    )
                value = value.replace(fmt(var_name), os.environ[var_name])
        return value

    if isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]

    return value


def _create_ibis_connection_legacy(loader: ProfileLoader, config: dict) -> BaseBackend:
    """Currently used: Create native ibis connection directly.

    TODO: Replace with _create_xorq_connection when ready.
    """
    import ibis

    config = _substitute_env_vars(config.copy())

    conn_type = config.get("type")
    if not conn_type:
        raise ProfileError("Profile must specify 'type' field")

    parquet_tables = config.pop("tables", None)
    kwargs = {k: v for k, v in config.items() if k != "type"}

    # Create native ibis connection directly
    backend_module = getattr(ibis, conn_type, None)
    if not backend_module:
        raise ProfileError(f"Unknown connection type: {conn_type}")

    connect_method = getattr(backend_module, "connect", None)
    if not connect_method:
        raise ProfileError(f"Backend '{conn_type}' does not have a connect() method")

    connection = connect_method(**kwargs)

    # Load parquet tables if specified
    if parquet_tables:
        loader._load_parquet_tables(connection, parquet_tables, conn_type)

    return connection


def _create_xorq_connection(loader: ProfileLoader, config: dict) -> BaseBackend:
    """Future: Create connection using xorq for better caching and management.

    Uncomment and use this when ready to migrate to xorq.
    """
    config = config.copy()
    conn_type = config.get("type")
    if not conn_type:
        raise ProfileError("Profile must specify 'type' field")

    parquet_tables = config.pop("tables", None)

    # Use xorq for connection management
    kwargs_tuple = tuple(sorted((k, v) for k, v in config.items() if k != "type"))
    xorq_profile = XorqProfile(con_name=conn_type, kwargs_tuple=kwargs_tuple)
    connection = xorq_profile.get_con()

    # Load parquet tables if specified
    if parquet_tables:
        loader._load_parquet_tables(connection, parquet_tables, conn_type)

    return connection
