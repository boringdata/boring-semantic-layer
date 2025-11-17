"""Profile support for BSL - manages database connections using profiles."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from ibis import BaseBackend

# Check if xorq is available (for optional caching)
try:
    from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile

    XORQ_AVAILABLE = True
except ImportError:
    XORQ_AVAILABLE = False


class ProfileError(Exception):
    """Raised when profile loading fails."""

    pass


class Profile:
    """Profile configuration for database connections."""

    def __init__(self, con_name: str, kwargs: dict[str, Any] | None = None):
        self._con_name = con_name
        self._kwargs = kwargs.copy() if kwargs else {}

    @property
    def con_name(self) -> str:
        return self._con_name

    @property
    def kwargs_dict(self) -> dict[str, Any]:
        return self._kwargs.copy()

    def get_con(self, use_xorq_backend: bool = False) -> BaseBackend:
        """Create and return a database connection."""
        if use_xorq_backend:
            # Use xorq backend directly (with caching)
            if not XORQ_AVAILABLE:
                raise ProfileError(
                    "xorq backend requested but xorq is not installed.\n"
                    "Install with: pip install boring-semantic-layer[xorq]"
                )
            # Create xorq backend with caching
            xorq_profile = XorqProfile(self._con_name, tuple(self._kwargs.items()))
            return xorq_profile.get_con()

        # Use ibis backend (no caching)
        config = {"type": self._con_name, **self._kwargs}
        return _create_connection_from_config(config)

    @classmethod
    def load(cls, name: str, profile_file: str | Path | None = None) -> Profile:
        """Load a profile from BSL saved profiles."""
        # Case 1: profile_file is specified
        if profile_file:
            profile_file_path = Path(profile_file)
            if not profile_file_path.exists():
                raise ProfileError(f"Profile file not found: {profile_file_path}")
            return cls._load_from_yaml(profile_file_path, name)

        # Case 2: Check BSL profiles directory
        bsl_profile = Path.home() / ".config" / "bsl" / "profiles" / f"{name}.yml"
        if bsl_profile.exists():
            return cls._load_from_yaml(bsl_profile, name)

        # Case 3: Check local profiles.yml
        local_profile = Path.cwd() / "profiles.yml"
        if local_profile.exists():
            return cls._load_from_yaml(local_profile, name)

        # Profile not found
        raise ProfileError(_profile_not_found_error(name, local_profile))

    @classmethod
    def _load_from_yaml(cls, yaml_file: Path, profile_name: str | None) -> Profile:
        """Load a profile from a YAML file."""
        try:
            with open(yaml_file) as f:
                profiles_config = yaml.safe_load(f)

            if not isinstance(profiles_config, dict):
                raise ProfileError(
                    f"Profile file must contain a dict, got: {type(profiles_config)}"
                )

            # If no profile_name specified, load the first profile
            if profile_name is None:
                if not profiles_config:
                    raise ProfileError(f"Profile file {yaml_file} is empty")
                profile_name = list(profiles_config.keys())[0]

            if profile_name not in profiles_config:
                available = list(profiles_config.keys())
                raise ProfileError(
                    f"Profile '{profile_name}' not found in {yaml_file}\n"
                    f"Available profiles: {', '.join(available)}"
                )

            config = profiles_config[profile_name]
            if not isinstance(config, dict):
                raise ProfileError(f"Profile config must be a dict, got: {type(config)}")

            conn_type = config.get("type")
            if not conn_type:
                raise ProfileError("Profile must specify 'type' field")

            # Create Profile with all config except 'type'
            kwargs = {k: v for k, v in config.items() if k != "type"}
            return cls(con_name=conn_type, kwargs=kwargs)

        except ProfileError:
            raise
        except Exception as e:
            raise ProfileError(
                f"Failed to load profile '{profile_name}' from {yaml_file}: {type(e).__name__}: {e}"
            ) from e


def _profile_not_found_error(name: str, local_profile: Path) -> str:
    message = (
        f"Profile '{name}' not found.\n\n"
        "Searched in:\n"
        f"  1. BSL profiles: ~/.config/bsl/profiles/{name}.yml (not found)\n"
        f"  2. Current directory: {local_profile} (not found)\n\n"
        f"Create a profiles.yml file at {local_profile} with:\n"
        f"  {name}:\n"
        f"    type: duckdb\n"
        f"    database: ':memory:'"
    )

    return message


def load_tables_from_profile(
    profile_name: str,
    table_names: list[str] | None = None,
    profile_file: str | Path | None = None,
    use_xorq_backend: bool = False,
) -> dict[str, Any]:
    """Load tables from a BSL profile."""
    # Load the connection from profile
    con = load_profile(profile_name, profile_file=profile_file, use_xorq_backend=use_xorq_backend)

    # Get table names to load
    if table_names is None:
        # Load all available tables
        if not hasattr(con, "list_tables"):
            raise ProfileError(
                f"Backend '{con.name}' does not support listing tables. "
                f"Please specify table_names explicitly when calling load_tables_from_profile()."
            )
        table_names = con.list_tables()

    # Load tables
    tables = {}
    for table_name in table_names:
        try:
            tables[table_name] = con.table(table_name)
        except Exception as e:
            raise ProfileError(
                f"Failed to load table '{table_name}' from profile '{profile_name}': {e}"
            ) from e

    return tables


def load_profile(
    profile_name: str, profile_file: str | Path | None = None, use_xorq_backend: bool = False
) -> BaseBackend:
    """Load a connection from a BSL profile."""
    # Check if profile_name is a file path (convenience feature)
    name_path = Path(profile_name)
    if name_path.suffix in (".yml", ".yaml") and name_path.exists():
        # Load the first profile from the YAML file
        profile = Profile._load_from_yaml(name_path, profile_name=None)
        return profile.get_con(use_xorq_backend=use_xorq_backend)

    # Otherwise, use standard Profile.load()
    profile = Profile.load(profile_name, profile_file=profile_file)
    return profile.get_con(use_xorq_backend=use_xorq_backend)


def _create_connection_from_config(config: dict[str, Any]) -> BaseBackend:
    import ibis

    # Substitute environment variables
    config = _substitute_env_vars(config)

    conn_type = config.pop("type", None)
    if not conn_type:
        raise ProfileError("Profile must specify 'type' field")

    # Extract tables configuration if present (for parquet loading)
    tables_config = config.pop("tables", None)

    try:
        # Get the backend module
        backend_module = getattr(ibis, conn_type, None)
        if not backend_module or not hasattr(backend_module, "connect"):
            raise ProfileError(
                f"Backend '{conn_type}' not supported or not installed.\n"
                f"Available backends: {', '.join(dir(ibis))}"
            )

        # Connect based on backend type
        if conn_type == "duckdb":
            # DuckDB takes database as first positional arg
            database = config.pop("database", ":memory:")
            connection = backend_module.connect(database, **config)
        elif conn_type == "postgres" and "connection_string" in config:
            connection = backend_module.connect(config["connection_string"])
        else:
            connection = backend_module.connect(**config)

        # Load parquet files if tables configuration is present
        if tables_config:
            _load_parquet_tables(connection, tables_config, conn_type)

        return connection

    except ProfileError:
        raise
    except Exception as e:
        raise ProfileError(f"Failed to connect to {conn_type}: {type(e).__name__}: {e}") from e


def _load_parquet_tables(connection: BaseBackend, tables_config: dict, conn_type: str) -> None:
    if not hasattr(connection, "read_parquet"):
        raise ProfileError(
            f"Backend '{conn_type}' does not support loading parquet files.\n"
            f"The 'tables' configuration in profiles is only supported for backends with read_parquet() method.\n"
            f"Remove the 'tables' section from your profile or use a different backend."
        )

    for table_name, table_config in tables_config.items():
        if isinstance(table_config, dict) and "source" in table_config:
            source = table_config["source"]
        elif isinstance(table_config, str):
            source = table_config
        else:
            continue

        try:
            connection.read_parquet(source, table_name=table_name)
        except Exception as e:
            raise ProfileError(
                f"Failed to load parquet file '{source}' as table '{table_name}': {e}"
            ) from e


def _substitute_env_vars(value: Any) -> Any:
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
