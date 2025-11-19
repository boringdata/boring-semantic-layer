"""
YAML loader for Boring Semantic Layer models using the semantic API.
"""

from collections.abc import Mapping
from typing import Any

from ibis import _

from .api import to_semantic_table
from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure
from .profile import loader
from .utils import read_yaml_file, safe_eval


def _parse_dimension_or_measure(
    name: str, config: str | dict, metric_type: str
) -> Dimension | Measure:
    """Parse a single dimension or measure configuration."""
    # Parse expression and description
    if isinstance(config, str):
        expr_str = config
        description = None
        extra_kwargs = {}
    elif isinstance(config, dict):
        if "expr" not in config:
            raise ValueError(
                f"{metric_type.capitalize()} '{name}' must specify 'expr' field when using dict format"
            )
        expr_str = config["expr"]
        description = config.get("description")
        extra_kwargs = {}
        if metric_type == "dimension":
            extra_kwargs["is_time_dimension"] = config.get("is_time_dimension", False)
            extra_kwargs["smallest_time_grain"] = config.get("smallest_time_grain")
    else:
        raise ValueError(f"Invalid {metric_type} format for '{name}'. Must be a string or dict")

    # Create the metric
    deferred = safe_eval(expr_str, context={"_": _}).unwrap()
    base_kwargs = {"expr": deferred, "description": description}
    return (
        Dimension(**base_kwargs, **extra_kwargs)
        if metric_type == "dimension"
        else Measure(**base_kwargs)
    )


def _parse_joins(
    joins_config: dict[str, Mapping[str, Any]],
    tables: Mapping[str, Any],
    yaml_configs: Mapping[str, Any],
    current_model_name: str,
    models: dict[str, SemanticModel],
) -> SemanticModel:
    """Parse join configuration and apply joins to a semantic model."""
    result_model = models[current_model_name]

    # Process each join definition
    for alias, join_config in joins_config.items():
        join_model_name = join_config.get("model")
        if not join_model_name:
            raise ValueError(f"Join '{alias}' must specify 'model' field")

        # Look up the model to join - check in order: models, tables, yaml_configs
        if join_model_name in models:
            # Already loaded model from this YAML
            join_model = models[join_model_name]
        elif join_model_name in tables:
            # Table passed via tables parameter
            table = tables[join_model_name]
            if isinstance(table, SemanticModel | SemanticTable):
                join_model = table
            else:
                raise TypeError(
                    f"Join '{alias}' references '{join_model_name}' which is not a semantic model/table"
                )
        elif join_model_name in yaml_configs:
            # Defined in YAML but not yet loaded - wrong order
            raise ValueError(
                f"Model '{join_model_name}' in join '{alias}' not yet loaded. Check model order."
            )
        else:
            # Not found anywhere
            available = sorted(
                list(models.keys())
                + [k for k in tables if isinstance(tables.get(k), SemanticModel | SemanticTable)]
            )
            raise KeyError(
                f"Model '{join_model_name}' in join '{alias}' not found. Available: {', '.join(available)}"
            )

        # Apply the join based on type
        join_type = join_config.get("type", "one")  # Default to one-to-one

        if join_type == "cross":
            # Cross join - no keys needed
            result_model = result_model.join_cross(join_model)
        elif join_type in ("one", "many"):
            # One-to-one or one-to-many join - requires keys
            left_on, right_on = join_config.get("left_on"), join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' type '{join_type}' requires 'left_on' and 'right_on'"
                )

            # Select join method based on type
            join_method = result_model.join_one if join_type == "one" else result_model.join_many
            result_model = join_method(join_model, left_on=left_on, right_on=right_on)
        else:
            raise ValueError(f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'")

    return result_model


def _resolve_table_references(tables: Mapping[str, Any], profile_loader) -> dict[str, Any]:
    """Resolve tuple table references like ("profile", "table") or ("profile", "table", "file")."""
    resolved = {}
    for name, ref in tables.items():
        # Handle tuple references: (profile, table_name) or (profile, table_name, profile_file)
        if isinstance(ref, tuple) and len(ref) in (2, 3):
            profile_name, remote_table = ref[0], ref[1]
            profile_file = ref[2] if len(ref) == 3 else None
            con = profile_loader.load(profile_name, profile_file=profile_file)
            resolved[name] = con.table(remote_table)
        else:
            resolved[name] = ref
    return resolved


def from_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """Load semantic models from a YAML file with optional profile-based table loading."""
    tables = _resolve_table_references(tables or {}, loader)

    # Load tables from profile parameter (or BSL_PROFILE env var if profile is None)
    tables = {**tables, **loader.load_tables(profile, profile_file=profile_path)}

    yaml_configs = read_yaml_file(yaml_path)

    # Load from YAML profile section if no tables loaded yet
    if "profile" in yaml_configs and not tables:
        tables = {**tables, **loader.load_tables(yaml_configs["profile"])}

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
            # Load only the specific table needed
            all_tables = loader.load_tables(config["profile"])
            profile_tables = {table_name: all_tables[table_name]}
            # Check for duplicate table names before merging
            duplicates = set(tables.keys()) & set(profile_tables.keys())
            if duplicates:
                raise ValueError(
                    f"Table name conflict: {', '.join(sorted(duplicates))} already exists. "
                    f"Tables loaded from profiles must have unique names."
                )
            tables = {**tables, **profile_tables}

        if table_name not in tables:
            available = ", ".join(
                sorted(k for k in tables if hasattr(tables[k], "execute")),
            )
            raise KeyError(
                f"Table '{table_name}' not found in tables.\nAvailable tables: {available}",
            )

        table = tables[table_name]

        # Parse dimensions and measures
        dimensions = {
            name: _parse_dimension_or_measure(name, cfg, "dimension")
            for name, cfg in config.get("dimensions", {}).items()
        }
        measures = {
            name: _parse_dimension_or_measure(name, cfg, "measure")
            for name, cfg in config.get("measures", {}).items()
        }

        # Create the semantic table and add dimensions/measures
        semantic_table = to_semantic_table(table, name=name)
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
