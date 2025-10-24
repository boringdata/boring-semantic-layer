"""
YAML loader for Boring Semantic Layer models using the semantic API.
"""

from typing import Any, Dict, Optional
import yaml
from ibis import _

from .api import to_semantic_table
from .ops import SemanticTable, Dimension, Measure


def _parse_dimensions(dimensions: Dict[str, Any]) -> Dict[str, Dimension]:
    """Parse dimension expressions from YAML configurations.

    Supports two formats:
    1. Simple format (backwards compatible): name: expression_string
    2. Extended format with descriptions and time dimension info:
        name:
          expr: expression_string
          description: "description text"
          is_time_dimension: true/false
          smallest_time_grain: "TIME_GRAIN_DAY"
    """
    result: Dict[str, Dimension] = {}

    for name, config in dimensions.items():
        if isinstance(config, str):
            # Simple format: name: _.column_name
            deferred = eval(config, {"_": _, "__builtins__": {}})
            result[name] = Dimension(
                expr=lambda t, d=deferred: d.resolve(t),
                description=None,
            )
        elif isinstance(config, dict):
            if "expr" not in config:
                raise ValueError(
                    f"Dimension '{name}' must specify 'expr' field when using dict format"
                )

            expr_str = config["expr"]
            description = config.get("description")
            is_time_dimension = config.get("is_time_dimension", False)
            smallest_time_grain = config.get("smallest_time_grain")

            deferred = eval(expr_str, {"_": _, "__builtins__": {}})
            result[name] = Dimension(
                expr=lambda t, d=deferred: d.resolve(t),
                description=description,
                is_time_dimension=is_time_dimension,
                smallest_time_grain=smallest_time_grain,
            )
        else:
            raise ValueError(
                f"Invalid dimension format for '{name}'. Must be a string or dict"
            )

    return result


def _parse_measures(measures: Dict[str, Any]) -> Dict[str, Measure]:
    """Parse measure expressions from YAML configurations.

    Supports two formats:
    1. Simple format (backwards compatible): name: expression_string
    2. Extended format with descriptions:
        name:
          expr: expression_string
          description: "description text"
    """
    result: Dict[str, Measure] = {}

    for name, config in measures.items():
        if isinstance(config, str):
            # Simple format: name: _.count()
            deferred = eval(config, {"_": _, "__builtins__": {}})
            result[name] = Measure(
                expr=lambda t, d=deferred: d.resolve(t),
                description=None,
            )
        elif isinstance(config, dict):
            if "expr" not in config:
                raise ValueError(
                    f"Measure '{name}' must specify 'expr' field when using dict format"
                )

            expr_str = config["expr"]
            description = config.get("description")

            deferred = eval(expr_str, {"_": _, "__builtins__": {}})
            result[name] = Measure(
                expr=lambda t, d=deferred: d.resolve(t),
                description=description,
            )
        else:
            raise ValueError(
                f"Invalid measure format for '{name}'. Must be a string or dict"
            )

    return result


def _parse_joins(
    joins_config: Dict[str, Dict[str, Any]],
    tables: Dict[str, Any],
    yaml_configs: Dict[str, Any],
    current_model_name: str,
    models: Dict[str, SemanticTable],
) -> SemanticTable:
    """Parse join configurations for a model.

    Note: The alias in the join config is used to look up the model to join.
    The actual prefix in queries will be based on the joined model's name property.
    """
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
            if isinstance(table, SemanticTable):
                join_model = table
            else:
                raise TypeError(
                    f"Join '{alias}' references '{join_model_name}' which is not a SemanticTable"
                )
        else:
            available_models = list(yaml_configs.keys()) + [
                k for k in tables.keys() if isinstance(tables.get(k), SemanticTable)
            ]
            if join_model_name in yaml_configs:
                raise ValueError(
                    f"Model '{join_model_name}' referenced in join '{alias}' is defined in the same YAML file "
                    f"but not yet loaded. Ensure models are loaded in the correct order."
                )
            else:
                raise KeyError(
                    f"Model '{join_model_name}' referenced in join '{alias}' not found.\n"
                    f"Available models: {', '.join(sorted(available_models))}"
                )

        join_type = join_config.get("type", "one")

        if join_type == "one":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'one' must specify 'left_on' and 'right_on' fields"
                )
            result_model = result_model.join_one(
                join_model,
                left_on=left_on,
                right_on=right_on
            )
        elif join_type == "many":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'many' must specify 'left_on' and 'right_on' fields"
                )
            result_model = result_model.join_many(
                join_model,
                left_on=left_on,
                right_on=right_on
            )
        elif join_type == "cross":
            result_model = result_model.join_cross(join_model)
        else:
            raise ValueError(
                f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'"
            )

    return result_model


def from_yaml(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> Dict[str, SemanticTable]:
    """
    Load semantic tables from a YAML file.

    Args:
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names to ibis table expressions

    Returns:
        Dict mapping model names to SemanticTable instances

    Example YAML format:
        flights:
          table: flights_tbl
          description: "Flight data model"
          dimensions:
            origin: _.origin
            destination: _.destination
            carrier: _.carrier
            arr_time:
              expr: _.arr_time
              description: "Arrival time"
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
    if tables is None:
        tables = {}

    with open(yaml_path, "r") as f:
        yaml_configs = yaml.safe_load(f)

    models: Dict[str, SemanticTable] = {}

    # First pass: create models without joins
    for name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue

        table_name = config.get("table")
        if not table_name:
            raise ValueError(f"Model '{name}' must specify 'table' field")

        if table_name not in tables:
            available = ", ".join(
                sorted(k for k in tables.keys() if hasattr(tables[k], "execute"))
            )
            raise KeyError(
                f"Table '{table_name}' not found in tables.\n"
                f"Available tables: {available}"
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
                models
            )

    return models
