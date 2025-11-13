"""
Profile support for BSL - manages database connections using xorq profiles.

This module provides simple integration with xorq's Profile system with support
for both saved xorq profiles and local profile.yml files.

Profile Resolution Order:
1. If profile_name is a path to a YAML file, load it directly
2. If ./profile.yml exists in current directory, look for profile there
3. Otherwise, load from xorq's saved profiles (~/.config/xorq/profiles/)
"""

import os
from pathlib import Path
from typing import Any

import yaml
from ibis import BaseBackend


class ProfileError(Exception):
    """Raised when profile loading fails."""

    pass


def load_profile(profile_name: str, profile_file: str | Path | None = None) -> BaseBackend:
    """Load a connection from a saved xorq profile or local profile.yml file.

    Resolution order:
    1. If profile_name is a path to .yml/.yaml file, load from that file
    2. If profile_file is specified, load profile_name from that file
    3. If ./profile.yml exists, load profile_name from there
    4. Otherwise, load from xorq's saved profiles (~/.config/xorq/profiles/)

    Args:
        profile_name: Name of the profile or path to a profile YAML file
        profile_file: Optional path to a profile YAML file to load from

    Returns:
        Backend connection (xorq backend)

    Raises:
        ProfileError: If profile loading fails or xorq is not installed

    Examples:
        # Load from saved xorq profile (traditional)
        >>> con = load_profile('my_duckdb')

        # Load from local profile.yml in current directory
        >>> con = load_profile('dev_db')  # Looks in ./profile.yml first

        # Load from specific profile file
        >>> con = load_profile('dev_db', profile_file='config/profiles.yml')

        # Load directly from a YAML file path
        >>> con = load_profile('./my_profile.yml')

    Profile YAML Format:
        # profile.yml
        dev_db:
          type: duckdb
          database: dev.db

        prod_db:
          type: postgres
          host: ${POSTGRES_HOST}
          port: 5432
          database: ${POSTGRES_DB}
          user: ${POSTGRES_USER}
          password: ${POSTGRES_PASSWORD}
    """
    try:
        from xorq.vendor.ibis.backends.profiles import Profile
    except ImportError as e:
        raise ProfileError(
            "Profile support requires xorq. "
            "Install with: pip install 'boring-semantic-layer[xorq]'"
        ) from e

    # Case 1: profile_name is a path to a YAML file
    profile_name_path = Path(profile_name)
    if profile_name_path.suffix in (".yml", ".yaml") and profile_name_path.exists():
        return _load_from_yaml_file(profile_name_path, profile_name_path.stem)

    # Case 2: profile_file is specified
    if profile_file:
        profile_file_path = Path(profile_file)
        if not profile_file_path.exists():
            raise ProfileError(f"Profile file not found: {profile_file_path}")
        return _load_from_yaml_file(profile_file_path, profile_name)

    # Case 3: Check for ./profile.yml in current directory
    local_profile = Path.cwd() / "profile.yml"
    if local_profile.exists():
        return _load_from_yaml_file(local_profile, profile_name)

    # Case 4: Load from xorq's saved profiles and convert to external ibis
    try:
        profile = Profile.load(profile_name)
        # Get xorq connection
        xorq_con = profile.get_con()
        # Convert to external ibis connection using same parameters
        return _xorq_connection_to_ibis(xorq_con)
    except Exception as e:
        raise ProfileError(
            f"Failed to load profile '{profile_name}': {type(e).__name__}: {e}\n\n"
            f"Searched in:\n"
            f"  1. Current directory: {local_profile} (not found)\n"
            f"  2. Xorq profiles: ~/.config/xorq/profiles/\n\n"
            f"Create a profile.yml file or save a profile using:\n"
            f"  import xorq.api as xo\n"
            f"  con = xo.duckdb.connect('...')\n"
            f"  con._profile.save(alias='{profile_name}')"
        ) from e


def _xorq_connection_to_ibis(xorq_con) -> BaseBackend:
    """Convert a xorq connection to an external ibis connection.

    Args:
        xorq_con: Xorq backend connection

    Returns:
        External ibis backend connection

    Raises:
        ProfileError: If conversion fails
    """
    import ibis

    try:
        # Get profile from xorq connection
        profile = xorq_con._profile
        con_name = profile.con_name
        kwargs = dict(profile.kwargs_tuple)

        # For DuckDB, we need to use the underlying connection directly
        # to avoid the "different configuration" error
        if con_name == "duckdb":
            # Use external ibis with xorq's underlying DuckDB connection
            backend_module = getattr(ibis, con_name)
            new_con = backend_module.connect()
            # Replace the connection object with xorq's underlying connection
            new_con.con = xorq_con.con
            return new_con

        # Create external ibis connection with same parameters for other backends
        backend_module = getattr(ibis, con_name)

        # Special handling for certain backends
        if con_name == "postgres" and "connection_string" in kwargs:
            return backend_module.connect(kwargs["connection_string"])
        else:
            return backend_module.connect(**kwargs)

    except Exception as e:
        raise ProfileError(
            f"Failed to convert xorq connection to ibis: {type(e).__name__}: {e}"
        ) from e


def _load_from_yaml_file(yaml_file: Path, profile_name: str) -> BaseBackend:
    """Load a profile from a YAML file.

    Args:
        yaml_file: Path to YAML file containing profiles
        profile_name: Name of profile to load from the file

    Returns:
        Backend connection

    Raises:
        ProfileError: If loading fails
    """
    try:
        with open(yaml_file) as f:
            profiles_config = yaml.safe_load(f)

        if not isinstance(profiles_config, dict):
            raise ProfileError(f"Profile file must contain a dict, got: {type(profiles_config)}")

        if profile_name not in profiles_config:
            available = list(profiles_config.keys())
            raise ProfileError(
                f"Profile '{profile_name}' not found in {yaml_file}\n"
                f"Available profiles: {', '.join(available)}"
            )

        profile_config = profiles_config[profile_name]
        return _create_connection_from_config(profile_config)

    except ProfileError:
        raise
    except Exception as e:
        raise ProfileError(
            f"Failed to load profile '{profile_name}' from {yaml_file}: {type(e).__name__}: {e}"
        ) from e


def _create_connection_from_config(config: dict[str, Any]) -> BaseBackend:
    """Create a connection from a profile configuration dict.

    Args:
        config: Profile configuration with 'type' and connection parameters

    Returns:
        Backend connection

    Raises:
        ProfileError: If connection fails
    """
    import ibis

    # Substitute environment variables
    config = _substitute_env_vars(config)

    conn_type = config.pop("type", None)
    if not conn_type:
        raise ProfileError("Profile must specify 'type' field")

    try:
        # Get the backend module
        backend_module = getattr(ibis, conn_type, None)
        if not backend_module or not hasattr(backend_module, "connect"):
            raise ProfileError(
                f"Backend '{conn_type}' not supported or not installed.\n"
                f"Available backends: {', '.join(dir(ibis))}"
            )

        # Special handling for certain backends
        if conn_type == "duckdb":
            # DuckDB connect takes database as first positional arg
            database = config.pop("database", ":memory:")
            return backend_module.connect(database, **config)
        elif conn_type == "postgres" and "connection_string" in config:
            # Postgres can use connection string
            return backend_module.connect(config["connection_string"])
        else:
            # Generic: pass all config as kwargs
            return backend_module.connect(**config)

    except ProfileError:
        raise
    except Exception as e:
        raise ProfileError(f"Failed to connect to {conn_type}: {type(e).__name__}: {e}") from e


def _substitute_env_vars(value: Any) -> Any:
    """Recursively substitute ${VAR_NAME} or $VAR_NAME with environment variables.

    Args:
        value: String, dict, list, or other value to process

    Returns:
        Value with environment variables substituted

    Raises:
        ProfileError: If a referenced environment variable is not set
    """
    import re

    if isinstance(value, str):
        # Support both ${VAR} and $VAR formats
        # Find all ${VAR_NAME} patterns
        pattern = r"\$\{([^}]+)\}"
        matches = re.findall(pattern, value)

        for var_name in matches:
            if var_name not in os.environ:
                raise ProfileError(
                    f"Environment variable not set: {var_name}\n\n"
                    f"Set it before running:\n"
                    f"  export {var_name}=..."
                )
            value = value.replace(f"${{{var_name}}}", os.environ[var_name])

        # Also support $VAR format (without braces)
        pattern2 = r"\$([A-Z_][A-Z0-9_]*)"
        matches2 = re.findall(pattern2, value)

        for var_name in matches2:
            if var_name not in os.environ:
                raise ProfileError(
                    f"Environment variable not set: {var_name}\n\n"
                    f"Set it before running:\n"
                    f"  export {var_name}=..."
                )
            value = value.replace(f"${var_name}", os.environ[var_name])

        return value

    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]

    else:
        return value


def load_tables_from_profile(
    profile_name: str,
    table_names: list[str] | None = None,
) -> dict[str, Any]:
    """Load tables from a profile connection.

    Args:
        profile_name: Name of the saved xorq profile
        table_names: Optional list of table names to load.
                    If None, lists all available tables.

    Returns:
        Dict mapping table names to table expressions

    Raises:
        ProfileError: If loading fails

    Example:
        >>> from boring_semantic_layer.profile import load_tables_from_profile
        >>> tables = load_tables_from_profile('my_duckdb', ['flights', 'carriers'])
        >>> # Or load all tables
        >>> tables = load_tables_from_profile('my_duckdb')
    """
    try:
        connection = load_profile(profile_name)

        # If no table names specified, list all available
        if table_names is None:
            table_names = connection.list_tables()

        # Load each table
        result = {}
        for table_name in table_names:
            if table_name not in connection.list_tables():
                raise ProfileError(
                    f"Table '{table_name}' not found in profile '{profile_name}'.\n"
                    f"Available tables: {', '.join(connection.list_tables())}"
                )
            result[table_name] = connection.table(table_name)

        return result

    except ProfileError:
        raise
    except Exception as e:
        raise ProfileError(
            f"Failed to load tables from profile '{profile_name}': {type(e).__name__}: {e}"
        ) from e
