"""
YAML loader for Boring Semantic Layer models using the semantic API.
"""

from collections.abc import Mapping
from typing import Any

import yaml

from .api import to_semantic_table
from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure
from .utils import safe_eval


def _parse_dimensions(dimensions: Mapping[str, Any]) -> dict[str, Dimension]:
    result: dict[str, Dimension] = {}

    from ibis import _

    for name, config in dimensions.items():
        if isinstance(config, str):
            deferred = safe_eval(config, context={"_": _}, allowed_names={"_"}).unwrap()
            result[name] = Dimension(
                expr=lambda t, d=deferred: d.resolve(t),
                description=None,
            )
        elif isinstance(config, dict):
            if "expr" not in config:
                raise ValueError(
                    f"Dimension '{name}' must specify 'expr' field when using dict format",
                )

            expr_str = config["expr"]
            description = config.get("description")
            is_time_dimension = config.get("is_time_dimension", False)
            smallest_time_grain = config.get("smallest_time_grain")

            deferred = safe_eval(expr_str, context={"_": _}, allowed_names={"_"}).unwrap()
            result[name] = Dimension(
                expr=lambda t, d=deferred: d.resolve(t),
                description=description,
                is_time_dimension=is_time_dimension,
                smallest_time_grain=smallest_time_grain,
            )
        else:
            raise ValueError(
                f"Invalid dimension format for '{name}'. Must be a string or dict",
            )

    return result


def _parse_measures(measures: Mapping[str, Any]) -> dict[str, Measure]:
    result: dict[str, Measure] = {}

    from ibis import _

    for name, config in measures.items():
        if isinstance(config, str):
            deferred = safe_eval(config, context={"_": _}, allowed_names={"_"}).unwrap()
            result[name] = Measure(
                expr=lambda t, d=deferred: d.resolve(t),
                description=None,
            )
        elif isinstance(config, dict):
            if "expr" not in config:
                raise ValueError(
                    f"Measure '{name}' must specify 'expr' field when using dict format",
                )

            expr_str = config["expr"]
            description = config.get("description")

            deferred = safe_eval(expr_str, context={"_": _}, allowed_names={"_"}).unwrap()
            result[name] = Measure(
                expr=lambda t, d=deferred: d.resolve(t),
                description=description,
            )
        else:
            raise ValueError(
                f"Invalid measure format for '{name}'. Must be a string or dict",
            )

    return result


def _parse_joins(
    joins_config: dict[str, Mapping[str, Any]],
    tables: Mapping[str, Any],
    yaml_configs: Mapping[str, Any],
    current_model_name: str,
    models: dict[str, SemanticModel],
) -> SemanticModel:
    result_model = models[current_model_name]

    for alias, join_config in joins_config.items():
        join_model_name = join_config.get("model")
        if not join_model_name:
            raise ValueError(f"Join '{alias}' must specify 'model' field")

        # Get the model to join
        if join_model_name in models:
            join_model = models[join_model_name]
        elif join_model_name in tables:
            table = tables[join_model_name]
            if isinstance(table, SemanticModel | SemanticTable):
                join_model = table
            else:
                raise TypeError(
                    f"Join '{alias}' references '{join_model_name}' which is not a SemanticModel or SemanticTable",
                )
        else:
            available_models = list(yaml_configs.keys()) + [
                k for k in tables if isinstance(tables.get(k), SemanticModel | SemanticTable)
            ]
            if join_model_name in yaml_configs:
                raise ValueError(
                    f"Model '{join_model_name}' referenced in join '{alias}' is defined in the same YAML file "
                    f"but not yet loaded. Ensure models are loaded in the correct order.",
                )
            else:
                raise KeyError(
                    f"Model '{join_model_name}' referenced in join '{alias}' not found.\n"
                    f"Available models: {', '.join(sorted(available_models))}",
                )

        join_type = join_config.get("type", "one")

        if join_type == "one":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'one' must specify 'left_on' and 'right_on' fields",
                )
            result_model = result_model.join_one(
                join_model,
                left_on=left_on,
                right_on=right_on,
            )
        elif join_type == "many":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'many' must specify 'left_on' and 'right_on' fields",
                )
            result_model = result_model.join_many(
                join_model,
                left_on=left_on,
                right_on=right_on,
            )
        elif join_type == "cross":
            result_model = result_model.join_cross(join_model)
        else:
            raise ValueError(
                f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'",
            )

    return result_model


def _resolve_table_references(tables: Mapping[str, Any]) -> dict[str, Any]:
    resolved_tables = {}
    if isinstance(tables, dict):
        from .profile import load_profile

        for table_name, table_ref in tables.items():
            if isinstance(table_ref, tuple) and len(table_ref) in (2, 3):
                if len(table_ref) == 2:
                    profile_name, remote_table_name = table_ref
                    profile_file_for_table = None
                else:
                    profile_name, remote_table_name, profile_file_for_table = table_ref

                con = load_profile(profile_name, profile_file=profile_file_for_table)
                resolved_tables[table_name] = con.table(remote_table_name)
            else:
                resolved_tables[table_name] = table_ref
        return resolved_tables
    return tables


def _merge_tables_with_conflict_check(
    existing_tables: dict[str, Any],
    new_tables: dict[str, Any],
    source_description: str,
) -> dict[str, Any]:
    conflicts = set(existing_tables.keys()) & set(new_tables.keys())
    if conflicts:
        raise ValueError(
            f"Table name conflict from {source_description}: {', '.join(sorted(conflicts))}\n"
            f"Tables with these names already exist in the tables dictionary.\n"
            f"Ensure different profiles or table sources don't provide tables with the same name."
        )
    return {**existing_tables, **new_tables}


def _load_profile_tables(profile: str | None, profile_path: str | None) -> dict[str, Any]:
    import os

    from .profile import load_tables_from_profile

    if profile is None:
        profile = os.environ.get("BSL_PROFILE")

    if profile is not None:
        return load_tables_from_profile(profile, profile_file=profile_path)
    return {}


def _load_yaml_profile_section(yaml_configs: dict) -> dict[str, Any]:
    from .profile import ProfileError, _create_connection_from_config, load_tables_from_profile

    profile_config = yaml_configs["profile"]

    if isinstance(profile_config, str):
        return load_tables_from_profile(profile_config)

    if isinstance(profile_config, dict):
        if "type" in profile_config:
            try:
                connection = _create_connection_from_config(profile_config.copy())
                table_names = connection.list_tables()
                return {name: connection.table(name) for name in table_names}
            except ProfileError:
                raise
            except Exception as e:
                raise ValueError(
                    f"Failed to load inline profile configuration.\n"
                    f"Error: {e}\n\n"
                    f"Check that:\n"
                    f"  - Connection details are correct\n"
                    f"  - Required environment variables are set\n"
                    f"  - Table sources (files/URLs) are accessible"
                ) from e
        else:
            profile_name = profile_config.get("name")
            profile_file = profile_config.get("file")
            table_names = profile_config.get("tables")

            if not profile_name:
                raise ValueError("Profile section must specify 'name' or 'type' field")

            return load_tables_from_profile(
                profile_name,
                table_names=table_names,
                profile_file=profile_file,
            )

    raise ValueError("Profile section must be a string or dict")


def _load_model_profile(config: dict, table_name: str) -> dict[str, Any]:
    from .profile import load_tables_from_profile

    model_profile = config["profile"]

    if isinstance(model_profile, str):
        return load_tables_from_profile(model_profile, table_names=[table_name])

    if isinstance(model_profile, dict):
        profile_name = model_profile.get("name")
        profile_file = model_profile.get("file")

        if not profile_name:
            raise ValueError("Profile section must specify 'name' field")

        return load_tables_from_profile(
            profile_name,
            table_names=[table_name],
            profile_file=profile_file,
        )

    raise ValueError("Profile must be a string or dict")


def from_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """Load semantic models from a YAML file with optional profile-based table loading."""
    if tables is None:
        tables = {}

    tables = _resolve_table_references(tables)

    profile_tables = _load_profile_tables(profile, profile_path)
    if profile_tables:
        tables = _merge_tables_with_conflict_check(
            tables, profile_tables, f"profile parameter '{profile}'"
        )

    with open(yaml_path) as f:
        yaml_configs = yaml.safe_load(f)

    if "profile" in yaml_configs and not tables:
        profile_tables = _load_yaml_profile_section(yaml_configs)
        if profile_tables:
            tables = _merge_tables_with_conflict_check(
                tables, profile_tables, "YAML profile section"
            )

    models: dict[str, SemanticModel] = {}

    # First pass: create models without joins
    for name, config in yaml_configs.items():
        # Skip special sections
        if name == "profile":
            continue

        if not isinstance(config, dict):
            continue

        table_name = config.get("table")
        if not table_name:
            raise ValueError(f"Model '{name}' must specify 'table' field")

        # Check if this model has its own profile (table-level)
        if "profile" in config:
            profile_tables = _load_model_profile(config, table_name)
            tables = _merge_tables_with_conflict_check(
                tables, profile_tables, f"table-level profile for model '{name}'"
            )

        if table_name not in tables:
            available = ", ".join(
                sorted(k for k in tables if hasattr(tables[k], "execute")),
            )
            raise KeyError(
                f"Table '{table_name}' not found in tables.\nAvailable tables: {available}",
            )

        table = tables[table_name]

        # Parse dimensions and measures
        dimensions = _parse_dimensions(config.get("dimensions", {}))
        measures = _parse_measures(config.get("measures", {}))

        # Create the semantic table
        semantic_table = to_semantic_table(table, name=name)

        # Add dimensions and measures
        if dimensions:
            semantic_table = semantic_table.with_dimensions(**dimensions)
        if measures:
            semantic_table = semantic_table.with_measures(**measures)

        models[name] = semantic_table

    # Second pass: add joins now that all models exist
    for name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue

        if "joins" in config and config["joins"]:
            models[name] = _parse_joins(
                config["joins"],
                tables,
                yaml_configs,
                name,
                models,
            )

    return models
