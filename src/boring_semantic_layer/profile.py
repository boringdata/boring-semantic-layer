from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from ibis import BaseBackend
from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile


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

    def load(
        self,
        profile_name: str | BaseBackend,
        profile_file: str | Path | None = None,
    ) -> BaseBackend:
        """Load a profile and return the database connection."""
        # Already a connection
        if isinstance(profile_name, BaseBackend):
            return profile_name

        # Direct file path
        name_path = Path(profile_name)
        if name_path.suffix in (".yml", ".yaml") and name_path.exists():
            return self._load_from_file(name_path)

        # Specific profile file provided
        if profile_file:
            return self._load_from_file(Path(profile_file), profile_name)

        # Search in configured locations
        return self._search_and_load(profile_name)

    def load_tables(
        self,
        profile_config: str | dict | None = None,
        profile_file: str | Path | None = None,
    ) -> dict[str, Any]:
        """Load tables from profile (string name, inline dict config, or named dict with filter)."""
        # Check BSL_PROFILE env var if no profile provided
        if profile_config is None:
            profile_config = os.environ.get("BSL_PROFILE")
            if not profile_config:
                return {}

        # Get connection and optional table filter
        table_filter = None

        if isinstance(profile_config, str):
            connection = self.load(profile_config, profile_file=profile_file)
        elif isinstance(profile_config, dict):
            if "type" in profile_config:
                # Inline connection config
                connection = self._create_connection(profile_config.copy())
            elif "name" in profile_config:
                # Named profile with options
                connection = self.load(
                    profile_config["name"], profile_file=profile_config.get("file") or profile_file
                )
                table_filter = profile_config.get("tables")
            else:
                raise ProfileError(
                    "Profile dict must specify either 'type' (inline config) or 'name' (named profile)"
                )
        else:
            raise ProfileError(f"Profile config must be string or dict, got {type(profile_config)}")

        # Load tables
        table_names = table_filter or connection.list_tables()
        return {name: connection.table(name) for name in table_names}

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
        """Load profile from a YAML file."""
        if not yaml_file.exists():
            raise ProfileError(f"Profile file not found: {yaml_file}")

        try:
            with open(yaml_file) as f:
                profiles_config = yaml.safe_load(f)

            if not isinstance(profiles_config, dict):
                raise ProfileError(
                    f"Profile file must contain a dict of profiles, got: {type(profiles_config)}"
                )

            if profile_name is None:
                if not profiles_config:
                    raise ProfileError(f"Profile file {yaml_file} is empty")
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

            return self._create_connection(config)

        except ProfileError:
            raise
        except Exception as e:
            raise ProfileError(
                f"Failed to load profile '{profile_name}' from {yaml_file}: {type(e).__name__}: {e}"
            ) from e

    def _create_connection(self, config: dict) -> BaseBackend:
        """Create connection from profile configuration dict."""
        conn_type = config.get("type")
        if not conn_type:
            raise ProfileError("Profile must specify 'type' field")

        parquet_tables = config.pop("tables", None)
        kwargs_tuple = tuple(sorted((k, v) for k, v in config.items() if k != "type"))

        xorq_profile = XorqProfile(con_name=conn_type, kwargs_tuple=kwargs_tuple)
        con = xorq_profile.get_con()

        if parquet_tables:
            self._load_parquet_tables(con, parquet_tables, conn_type)

        return con

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
