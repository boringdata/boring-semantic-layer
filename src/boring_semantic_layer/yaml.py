"""
YAML loader for Boring Semantic Layer models using the semantic API.
"""

from collections.abc import Mapping
from typing import Any

from ibis import _

from .api import to_semantic_table
from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure
from .profile import get_connection
from .utils import read_yaml_file, safe_eval


def _parse_dimension_or_measure(
    name: str, config: str | dict, metric_type: str
) -> Dimension | Measure:
    """Parse a single dimension or measure configuration.

    Supports two formats:
    1. Simple format (backwards compatible): name: expression_string
    2. Extended format with descriptions and metadata:
        name:
          expr: expression_string
          description: "description text"
          is_entity: true/false (dimensions only)
          is_event_timestamp: true/false (dimensions only)
          is_time_dimension: true/false (dimensions only)
          smallest_time_grain: "TIME_GRAIN_DAY" (dimensions only)
    """
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
            extra_kwargs["is_entity"] = config.get("is_entity", False)
            extra_kwargs["is_event_timestamp"] = config.get("is_event_timestamp", False)
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


def _parse_filter(filter_expr: str) -> callable:
    """Parse a filter expression from YAML.

    Example YAML:
        flights:
          table: flights_tbl
          filter: _.origin.isin(['SFO', 'LAX', 'JFK'])
    """
    from ibis import _

    deferred = safe_eval(filter_expr, context={"_": _}, allowed_names={"_"}).unwrap()
    return lambda t, d=deferred: d.resolve(t)


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
        how = join_config.get("how")  # Optional join method override

        if join_type == "cross":
            # Cross join - no keys needed
            result_model = result_model.join_cross(join_model)
        elif join_type == "one":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'one' must specify 'left_on' and 'right_on' fields",
                )

            # Convert left_on/right_on to lambda condition
            def make_join_condition(left_col, right_col):
                return lambda left, right: getattr(left, left_col) == getattr(right, right_col)

            on_condition = make_join_condition(left_on, right_on)
            result_model = result_model.join_one(
                join_model,
                on=on_condition,
                how=how if how else "inner",
            )
        elif join_type == "many":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'many' must specify 'left_on' and 'right_on' fields",
                )

            # Convert left_on/right_on to lambda condition
            def make_join_condition(left_col, right_col):
                return lambda left, right: getattr(left, left_col) == getattr(right, right_col)

            on_condition = make_join_condition(left_on, right_on)
            result_model = result_model.join_many(
                join_model,
                on=on_condition,
                how=how if how else "left",
            )
        else:
            raise ValueError(f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'")

    return result_model


def _load_tables_from_references(
    table_refs: dict[str, tuple[str, str] | tuple[str, str, str] | Any],
) -> dict[str, Any]:
    """Load tables from tuples (profile, table) or pass through table objects."""
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
    """Load table from model config profile if specified, verify it exists."""
    tables = existing_tables.copy()

    # Load table from model-specific profile if needed
    if "profile" in model_config:
        profile_config = model_config["profile"]
        connection = get_connection(profile_config)
        if table_name in tables:
            raise ValueError(f"Table name conflict: {table_name} already exists")
        tables[table_name] = connection.table(table_name)

    # Verify table exists
    if table_name not in tables:
        available = ", ".join(sorted(tables.keys()))
        raise KeyError(f"Table '{table_name}' not found. Available: {available}")

    return tables


def from_config(
    config: Mapping[str, Any],
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """
    Load semantic tables from a configuration dictionary.

    This is useful when you have already loaded your configuration through
    custom logic (e.g., Kedro catalog, external config management) and want
    to construct SemanticTable objects without going through YAML file loading.

    Args:
        config: Configuration dictionary with model definitions
        tables: Optional mapping of table names to ibis table expressions
        profile: Optional profile name to load tables from
        profile_path: Optional path to profile file

    Returns:
        Dict mapping model names to SemanticModel instances

    Example config format:
        {
            "flights": {
                "table": "flights_tbl",
                "description": "Flight data model",
                "dimensions": {
                    "origin": {"expr": "_.origin", "description": "Origin airport"},
                    "destination": "_.destination",
                },
                "measures": {
                    "flight_count": "_.count()",
                    "avg_distance": "_.distance.mean()",
                },
            }
        }

    Example usage with pre-loaded tables:
        >>> import ibis
        >>> con = ibis.duckdb.connect()
        >>> flights_tbl = con.table("flights")
        >>> config = {"flights": {"table": "flights_tbl", "dimensions": {...}}}
        >>> models = from_config(config, tables={"flights_tbl": flights_tbl})
    """
    tables = _load_tables_from_references(dict(tables) if tables else {})

    # Load tables from profile if not provided
    if not tables:
        profile_config = profile or config.get("profile")
        if profile_config or profile_path:
            connection = get_connection(
                profile_config or profile_path,
                profile_file=profile_path if profile_config else None,
            )
            tables = {name: connection.table(name) for name in connection.list_tables()}

    # Filter to only model definitions (exclude 'profile' key and non-dict values)
    model_configs = {
        name: cfg for name, cfg in config.items() if name != "profile" and isinstance(cfg, dict)
    }

    models: dict[str, SemanticModel] = {}

    # First pass: create models
    for name, model_config in model_configs.items():
        table_name = model_config.get("table")
        if not table_name:
            raise ValueError(f"Model '{name}' must specify 'table' field")

        # Load table if needed and verify it exists
        tables = _load_table_for_yaml_model(model_config, tables, table_name)
        table = tables[table_name]

        # Parse dimensions and measures
        dimensions = {
            dim_name: _parse_dimension_or_measure(dim_name, dim_cfg, "dimension")
            for dim_name, dim_cfg in model_config.get("dimensions", {}).items()
        }
        measures = {
            measure_name: _parse_dimension_or_measure(measure_name, measure_cfg, "measure")
            for measure_name, measure_cfg in model_config.get("measures", {}).items()
        }

        # Create the semantic table and add dimensions/measures
        semantic_table = to_semantic_table(table, name=name)
        if dimensions:
            semantic_table = semantic_table.with_dimensions(**dimensions)
        if measures:
            semantic_table = semantic_table.with_measures(**measures)

        # Apply filter if specified
        if "filter" in model_config:
            filter_predicate = _parse_filter(model_config["filter"])
            semantic_table = semantic_table.filter(filter_predicate)

        models[name] = semantic_table

    # Second pass: add joins now that all models exist
    for name, model_config in model_configs.items():
        if "joins" in model_config and model_config["joins"]:
            models[name] = _parse_joins(
                model_config["joins"],
                tables,
                config,
                name,
                models,
            )

    return models


def from_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """
    Load semantic tables from a YAML file with optional profile-based table loading.

    This is a convenience wrapper around from_config() that loads the YAML file first.

    Args:
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names to ibis table expressions
        profile: Optional profile name to load tables from
        profile_path: Optional path to profile file

    Returns:
        Dict mapping model names to SemanticModel instances

    Example YAML format:
        flights:
          table: flights_tbl
          description: "Flight data model"
          dimensions:
            origin:
              expr: _.origin
              description: "Origin airport code"
              is_entity: true
            destination: _.destination
            carrier: _.carrier
            arr_time:
              expr: _.arr_time
              description: "Arrival time"
              is_event_timestamp: true
              is_time_dimension: true
              smallest_time_grain: "TIME_GRAIN_DAY"
          measures:
            flight_count: _.count()
            avg_distance: _.distance.mean()
            total_distance:
              expr: _.distance.sum()
              description: "Total distance flown"
          joins:
            carriers:
              model: carriers
              type: one
              left_on: carrier
              right_on: code
    """
    yaml_configs = read_yaml_file(yaml_path)
    return from_config(yaml_configs, tables=tables, profile=profile, profile_path=profile_path)
