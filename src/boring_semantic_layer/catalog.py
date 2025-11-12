"""
Catalog support for BSL - manages database connections and table discovery.
"""

import os
import re
from pathlib import Path
from typing import Any

import ibis
from ibis import BaseBackend


class CatalogError(Exception):
    """Raised when catalog configuration or connection fails."""

    pass


def _substitute_env_vars(value: Any) -> Any:
    """Recursively substitute ${VAR_NAME} with environment variables.

    Args:
        value: String, dict, list, or other value to process

    Returns:
        Value with environment variables substituted

    Raises:
        CatalogError: If a referenced environment variable is not set
    """
    if isinstance(value, str):
        # Find all ${VAR_NAME} patterns
        pattern = r"\$\{([^}]+)\}"
        matches = re.findall(pattern, value)

        for var_name in matches:
            if var_name not in os.environ:
                raise CatalogError(
                    f"Environment variable not set: {var_name}\n\n"
                    f"Set it before running:\n"
                    f"  export {var_name}=...",
                )
            value = value.replace(f"${{{var_name}}}", os.environ[var_name])

        return value

    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]

    else:
        return value


def create_connection(connection_config: dict[str, Any]) -> BaseBackend:
    """Create an Ibis connection from catalog configuration.

    Args:
        connection_config: Connection configuration dict with 'type' and connection parameters

    Returns:
        Ibis backend connection

    Raises:
        CatalogError: If connection fails

    Example configs:
        # DuckDB
        {"type": "duckdb", "database": ":memory:"}

        # Snowflake
        {"type": "snowflake", "account": "...", "user": "...", "password": "...", "database": "..."}

        # Postgres
        {"type": "postgres", "host": "...", "database": "...", "user": "...", "password": "..."}

        # Any Ibis backend - just pass the parameters as kwargs
    """
    # Substitute environment variables
    config = _substitute_env_vars(connection_config)

    conn_type = config.pop("type", None)
    if not conn_type:
        raise CatalogError("Connection must specify 'type' field")

    try:
        # Get the backend module dynamically
        backend_module = getattr(ibis, conn_type, None)
        if not backend_module or not hasattr(backend_module, "connect"):
            raise CatalogError(
                f"Backend '{conn_type}' not supported or not installed.\n\n"
                f"Available backends: {', '.join(dir(ibis))}",
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

    except CatalogError:
        raise
    except Exception as e:
        raise CatalogError(
            f"Failed to connect to {conn_type}:\n{type(e).__name__}: {e}",
        ) from e


def discover_tables(
    connection: BaseBackend,
    discover_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Discover tables from database using pattern matching.

    Args:
        connection: Ibis backend connection
        discover_config: Optional discovery configuration with 'include' and 'exclude' patterns

    Returns:
        Dict mapping table names to ibis table expressions

    Example discover_config:
        {
            "catalog": "analytics",  # Optional: specific catalog
            "database": "public",    # Optional: specific database/schema
            "include": ["FACT_*", "DIM_*"],
            "exclude": ["*_TEMP", "*_STAGING"]
        }
    """
    tables = {}

    # Get database specification if provided
    database = None
    if discover_config:
        catalog = discover_config.get("catalog")
        db = discover_config.get("database")

        # Build database parameter for list_tables
        if catalog and db:
            database = (catalog, db)
        elif db:
            database = db

    # Get all available tables
    if database:
        all_tables = connection.list_tables(database=database)
    else:
        all_tables = connection.list_tables()

    # Apply filters if provided
    if discover_config:
        include_patterns = discover_config.get("include", [])
        exclude_patterns = discover_config.get("exclude", [])

        # Convert glob patterns to regex
        def glob_to_regex(pattern: str) -> str:
            # Escape special chars except *
            pattern = pattern.replace(".", r"\.")
            pattern = pattern.replace("*", ".*")
            return f"^{pattern}$"

        # Filter tables
        filtered_tables = all_tables

        if include_patterns:
            include_regexes = [re.compile(glob_to_regex(p)) for p in include_patterns]
            filtered_tables = [
                t for t in filtered_tables if any(r.match(t) for r in include_regexes)
            ]

        if exclude_patterns:
            exclude_regexes = [re.compile(glob_to_regex(p)) for p in exclude_patterns]
            filtered_tables = [
                t for t in filtered_tables if not any(r.match(t) for r in exclude_regexes)
            ]

        all_tables = filtered_tables

    # Load tables
    for table_name in all_tables:
        tables[table_name] = connection.table(table_name)

    return tables


def load_catalog(catalog_config: dict[str, Any], base_path: Path | None = None) -> dict[str, Any]:
    """Load catalog and discover tables.

    Args:
        catalog_config: Catalog configuration dict with 'type' and connection parameters
                       Example: {type: "snowflake", account: "...", database: "...", ...}
                       Optional keys: discover, tables
        base_path: Base directory for resolving relative file paths (defaults to cwd)

    Returns:
        Dict mapping table names to ibis table expressions

    Raises:
        CatalogError: If catalog loading fails
    """
    if base_path is None:
        base_path = Path.cwd()
    if "type" not in catalog_config:
        raise CatalogError("Catalog must specify 'type' field")

    # Extract connection params (everything except discover/tables)
    connection_config = {k: v for k, v in catalog_config.items() if k not in ["discover", "tables"]}
    discover_config = catalog_config.get("discover")

    # Create connection
    connection = create_connection(connection_config)

    # Discover tables
    tables = discover_tables(connection, discover_config)

    # Also handle explicit table sources (for DuckDB with files)
    if "tables" in catalog_config:
        for table_name, table_config in catalog_config["tables"].items():
            if "source" in table_config:
                source = table_config["source"]

                # Substitute env vars in source path
                source = _substitute_env_vars(source)

                # Load table from source
                if source.startswith("http://") or source.startswith("https://"):
                    # Remote file
                    if source.endswith(".parquet"):
                        tables[table_name] = connection.read_parquet(source)
                    elif source.endswith(".csv"):
                        tables[table_name] = connection.read_csv(source)
                    else:
                        raise CatalogError(
                            f"Unsupported remote file format: {source}",
                        )
                else:
                    # Local file - resolve relative to base_path
                    path = Path(source).expanduser()
                    if not path.is_absolute():
                        path = (base_path / path).resolve()
                    else:
                        path = path.resolve()
                    if not path.exists():
                        raise CatalogError(f"File not found: {path}")

                    if path.suffix == ".parquet":
                        tables[table_name] = connection.read_parquet(str(path))
                    elif path.suffix == ".csv":
                        tables[table_name] = connection.read_csv(str(path))
                    else:
                        raise CatalogError(
                            f"Unsupported file format: {path.suffix}",
                        )

    return tables
