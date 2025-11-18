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

        conn_type = config.get("type")
        if not conn_type:
            raise ProfileError("Profile must specify 'type' field")

        parquet_tables = config.pop("tables", None)

        kwargs = {k: v for k, v in config.items() if k != "type"}
        kwargs_tuple = tuple(sorted(kwargs.items()))
        xorq_profile = XorqProfile(con_name=conn_type, kwargs_tuple=kwargs_tuple)

        con = xorq_profile.get_con()

        if parquet_tables:
            _load_parquet_tables(con, parquet_tables, conn_type)

        return con

    except ProfileError:
        raise
    except Exception as e:
        raise ProfileError(
            f"Failed to load profile '{profile_name}' from {yaml_file}: {type(e).__name__}: {e}"
        ) from e


def _profile_not_found_error(name: str, local_profile: Path, search_locations: list[str]) -> str:
    searched = []
    for i, location in enumerate(search_locations, 1):
        if location == "bsl_dir":
            searched.append(f"  {i}. BSL profiles: ~/.config/bsl/profiles/{name}.yml")
        elif location == "local":
            searched.append(f"  {i}. Current directory: {local_profile}")
        elif location == "xorq_dir":
            searched.append(f"  {i}. xorq profiles directory")

    message = f"Profile '{name}' not found.\n\nSearched in:\n"
    message += "\n".join(searched)
    message += f"\n\nCreate a profiles.yml file at {local_profile} with:\n"
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
