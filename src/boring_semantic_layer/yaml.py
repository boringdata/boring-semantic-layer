"""
YAML loader for Boring Semantic Layer models using the semantic API.
"""

from collections.abc import Mapping
from typing import Any

from ibis import _

from .api import to_semantic_table
from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure
from .profile import get_connection, get_tables
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


def _load_tables_from_references(
    table_refs: dict[str, tuple[str, str] | tuple[str, str, str] | Any],
) -> dict[str, Any]:
    """Load tables from mixed references (tuples for remote profiles, or direct table objects).

    Supports loading tables from remote profiles using tuple notation:
    - (profile_name, table_name) - Load table from profile
    - (profile_name, table_name, profile_file) - Load table from specific profile file

    Args:
        table_refs: Dictionary mapping table names to either:
            - Tuple[str, str]: (profile_name, table_name) - Load from profile
            - Tuple[str, str, str]: (profile_name, table_name, profile_file) - Load from specific file
            - Table object: Already loaded ibis table

    Returns:
        dict[str, Table]: Dictionary mapping names to loaded ibis tables

    Example:
        >>> table_refs = {
        ...     "prod_users": ("prod_db", "users"),  # Load from prod_db profile
        ...     "staging_orders": ("staging", "orders", "staging.yml"),  # Load from specific file
        ...     "local_data": ibis_table,  # Use existing table
        ... }
        >>> tables = _load_tables_from_references(table_refs)
        >>> # {"prod_users": <Table>, "staging_orders": <Table>, "local_data": <Table>}
    """
    resolved = {}
    for name, ref in table_refs.items():
        if isinstance(ref, tuple) and len(ref) in (2, 3):
            profile_name, remote_table = ref[0], ref[1]
            profile_file = ref[2] if len(ref) == 3 else None
            con = get_connection(profile_name, profile_file=profile_file)
            resolved[name] = con.table(remote_table)
        else:
            resolved[name] = ref
    return resolved


def _load_table_for_yaml_model(
    model_config: dict[str, Any],
    existing_tables: dict[str, Any],
    table_name: str,
) -> dict[str, Any]:
    """Load table for a semantic model definition, handling profile configs.

    Args:
        model_config: Model configuration dict (may contain 'profile' key)
        existing_tables: Already loaded tables
        table_name: Name of the table to load

    Returns:
        dict: Updated tables dictionary with new table loaded if needed

    Raises:
        ValueError: If table name conflicts with existing tables
        KeyError: If required table not found

    Example:
        >>> config = {"table": "users", "profile": {"name": "prod_db"}}
        >>> tables = _load_table_for_yaml_model(config, {}, "users")
    """
    tables = existing_tables.copy()

    # Load table from model-specific profile if needed
    if "profile" in model_config:
        profile_config = model_config["profile"]
        connection = get_connection(profile_config)
        model_tables = get_tables(connection, [table_name])

        # Check for duplicates
        duplicates = set(tables.keys()) & set(model_tables.keys())
        if duplicates:
            raise ValueError(f"Table name conflict: {', '.join(sorted(duplicates))} already exists")

        tables.update(model_tables)

    # Verify table exists
    if table_name not in tables:
        available = ", ".join(sorted(tables.keys()))
        raise KeyError(f"Table '{table_name}' not found. Available: {available}")

    return tables


def from_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """Load semantic models from a YAML file with optional profile-based table loading."""
    tables = _load_tables_from_references(tables or {})
    yaml_configs = read_yaml_file(yaml_path)

    # Load tables from profile if not provided
    if not tables:
        profile_config = profile or yaml_configs.get("profile")
        if profile_config or profile_path:
            connection = get_connection(
                profile_config or profile_path,
                profile_file=profile_path if profile_config else None,
            )
            tables = get_tables(connection)

    # Filter to only model definitions (exclude 'profile' key and non-dict values)
    model_configs = {
        name: config
        for name, config in yaml_configs.items()
        if name != "profile" and isinstance(config, dict)
    }

    models: dict[str, SemanticModel] = {}

    # First pass: create models
    for name, config in model_configs.items():
        table_name = config.get("table")
        if not table_name:
            raise ValueError(f"Model '{name}' must specify 'table' field")

        # Load table if needed and verify it exists
        tables = _load_table_for_yaml_model(config, tables, table_name)
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
    for name, config in model_configs.items():
        if "joins" in config and config["joins"]:
            models[name] = _parse_joins(
                config["joins"],
                tables,
                yaml_configs,
                name,
                models,
            )

    return models
