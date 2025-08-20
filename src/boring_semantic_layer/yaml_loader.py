"""
YAML loader for Boring Semantic Layer models.
"""

from typing import Any, Dict, Optional
import yaml
from attrs import evolve

try:
    import xorq.vendor.ibis as ibis_mod
except ImportError:
    import ibis as ibis_mod

from .joins import Join
from .semantic_model import DimensionSpec, MeasureSpec, SemanticModel


def _parse_expressions(expressions: Dict[str, str], spec_class) -> Dict[str, Any]:
    """Parse dimension or measure expressions from YAML configurations.

    Supports two formats:
    1. Simple format (backwards compatible): name: expression_string
    2. Extended format with descriptions:
        name: expression_string
        description: "description text"
    """
    result: Dict[str, Any] = {}
    for name, config in expressions.items():
        if isinstance(config, str):
            deferred = eval(config, {"_": ibis_mod._, "__builtins__": {}})

            def expr_func(t, d=deferred):
                return d.resolve(t)

            result[name] = expr_func
        elif isinstance(config, dict):
            if "expr" not in config:
                raise ValueError(
                    f"Expression '{name}' must specify 'expr' field when using dict format"
                )

            expr_str = config["expr"]
            description = config.get("description", "")

            deferred = eval(expr_str, {"_": ibis_mod._, "__builtins__": {}})

            def expr_func(t, d=deferred):
                return d.resolve(t)

            # Create appropriate spec class with description
            result[name] = spec_class(expr=expr_func, description=description)
        else:
            raise ValueError(
                f"Invalid expression format for '{name}'. Must either be a string or a dictionary"
            )
    return result


def _parse_joins(
    joins_config: Dict[str, Dict[str, Any]],
    tables: Dict[str, Any],
    yaml_configs: Dict[str, Any],
    current_model_name: str,
) -> Dict[str, Join]:
    """Parse join configurations for a model."""
    joins: Dict[str, Join] = {}
    for alias, join_config in joins_config.items():
        join_model_name = join_config.get("model")
        if not join_model_name:
            raise ValueError(f"Join '{alias}' must specify 'model' field")

        if join_model_name in tables:
            model = tables[join_model_name]
            if not isinstance(model, SemanticModel):
                raise TypeError(
                    f"Join '{alias}' references '{join_model_name}' which is not a SemanticModel"
                )
        else:
            available_models = list(yaml_configs.keys()) + [
                k for k in tables.keys() if isinstance(tables[k], SemanticModel)
            ]
            if join_model_name in yaml_configs:
                raise ValueError(
                    f"Model '{join_model_name}' referenced in join '{alias}' is defined in the same YAML file "
                    f"but not yet loaded. Use SemanticModel.from_yaml() without model_name to load all models together."
                )
            else:
                raise KeyError(
                    f"Model '{join_model_name}' referenced in join '{alias}' not found.\n"
                    f"Available models: {', '.join(sorted(available_models))}"
                )

        join_type = join_config.get("type", "one")
        if join_type in ["one", "many"]:
            with_expr_str = join_config.get("with")
            if not with_expr_str:
                raise ValueError(
                    f"Join '{alias}' of type '{join_type}' must specify 'with' field"
                )
            with_expr = eval(with_expr_str, {"_": ibis_mod._, "__builtins__": {}})

            def with_func(t, e=with_expr):
                return e.resolve(t)

            if join_type == "one":
                joins[alias] = Join.one(alias, model, with_func)
            else:
                joins[alias] = Join.many(alias, model, with_func)
        elif join_type == "cross":
            joins[alias] = Join.cross(alias, model)
        else:
            raise ValueError(
                f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'"
            )
    return joins


def from_yaml(
    cls,
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> Dict[str, SemanticModel]:
    """
    Load semantic models from a YAML file.

    Args:
        cls: SemanticModel class reference
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names or model instances
    Returns:
        Dict mapping model names to SemanticModel instances
    """
    if tables is None:
        tables = {}

    with open(yaml_path, "r") as f:
        yaml_configs = yaml.safe_load(f)

    models: Dict[str, SemanticModel] = {}

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

        dimensions = _parse_expressions(config.get("dimensions", {}), DimensionSpec)
        measures = _parse_expressions(config.get("measures", {}), MeasureSpec)

        models[name] = cls(
            name=name,
            table=table,
            dimensions=dimensions,
            measures=measures,
            joins={},
            description=config.get("description"),
            primary_key=config.get("primary_key"),
            time_dimension=config.get("time_dimension"),
            smallest_time_grain=config.get("smallest_time_grain"),
        )

    # Second pass: add joins now that all models exist
    for name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue

        if "joins" in config and config["joins"]:
            extended_tables = {**tables, **models}
            joins = _parse_joins(config["joins"], extended_tables, yaml_configs, name)
            models[name] = evolve(models[name], joins=joins)
    return models
