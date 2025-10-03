"""
YAML loader for Boring Semantic Layer semantic API models.
"""

from typing import Any, Dict, Optional, Callable, Tuple
import yaml

try:
    import xorq.vendor.ibis as ibis_mod
except ImportError:
    import ibis as ibis_mod

from .api import (
    to_semantic_table,
    with_dimensions,
    with_measures,
    join_,
    SemanticTableExpr,
)


def _parse_expression(expr_str: str) -> Callable:
    """Parse an expression string into a lambda function."""
    deferred = eval(expr_str, {"_": ibis_mod._, "__builtins__": {}})

    def expr_func(t, d=deferred):
        return d.resolve(t)

    return expr_func


def _parse_expressions(
    expressions: Dict[str, Any],
) -> Tuple[Dict[str, Callable], Dict[str, str]]:
    """Parse dimension or measure expressions from YAML configurations.

    Returns:
        Tuple of (expressions dict, descriptions dict)
    """
    result: Dict[str, Callable] = {}
    descriptions: Dict[str, str] = {}

    for name, config in expressions.items():
        if isinstance(config, str):
            # Simple format: name: expression_string
            result[name] = _parse_expression(config)
            descriptions[name] = ""
        elif isinstance(config, dict):
            # Extended format with descriptions
            if "expr" not in config:
                raise ValueError(
                    f"Expression '{name}' must specify 'expr' field when using dict format"
                )

            expr_str = config["expr"]
            description = config.get("description", "")

            result[name] = _parse_expression(expr_str)
            descriptions[name] = description
        else:
            raise ValueError(
                f"Invalid expression format for '{name}'. Must either be a string or a dictionary"
            )

    return result, descriptions


def _parse_joins(
    joins_config: Dict[str, Dict[str, Any]],
    semantic_tables: Dict[str, SemanticTableExpr],
    current_model_name: str,
) -> Dict[str, Tuple[SemanticTableExpr, str, Callable]]:
    """Parse join configurations for a model.

    Returns dict mapping alias to (target_table, join_type, on_func)
    """
    joins: Dict[str, Tuple[SemanticTableExpr, str, Callable]] = {}

    for alias, join_config in joins_config.items():
        join_model_name = join_config.get("model")
        if not join_model_name:
            raise ValueError(f"Join '{alias}' must specify 'model' field")

        if join_model_name not in semantic_tables:
            available_models = list(semantic_tables.keys())
            raise KeyError(
                f"Model '{join_model_name}' referenced in join '{alias}' not found.\n"
                f"Available models: {', '.join(sorted(available_models))}"
            )

        target_table = semantic_tables[join_model_name]
        join_type = join_config.get("type", "one")

        if join_type in ["one", "many"]:
            with_expr_str = join_config.get("with")
            if not with_expr_str:
                raise ValueError(
                    f"Join '{alias}' of type '{join_type}' must specify 'with' field"
                )

            # Parse the join condition
            with_expr = eval(with_expr_str, {"_": ibis_mod._, "__builtins__": {}})

            # Create join function based on type
            if join_type == "one":
                how = "inner"
            else:  # many
                how = "left"

            def on_func(left, right, expr=with_expr):
                # Resolve the expression on the left table to get the join key
                left_key = expr.resolve(left)
                # Assume joining on primary key or same named column
                # This is a simplification - might need more complex logic
                if hasattr(left_key, "get_name"):
                    key_name = left_key.get_name()
                    return left_key == getattr(right, key_name)
                else:
                    # Fall back to comparing with 'code' or first column
                    # This would need to be more sophisticated in practice
                    raise ValueError(f"Cannot determine join condition for {alias}")

            joins[alias] = (target_table, how, on_func)

        elif join_type == "cross":
            joins[alias] = (target_table, "cross", None)
        else:
            raise ValueError(
                f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'"
            )

    return joins


def _load_yaml_impl(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, SemanticTableExpr], Dict[str, Dict[str, Any]]]:
    """
    Internal implementation that loads semantic tables and metadata.

    Args:
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names to Ibis tables

    Returns:
        Tuple of (semantic_tables dict, metadata dict)
    """
    if tables is None:
        tables = {}

    with open(yaml_path, "r") as f:
        yaml_configs = yaml.safe_load(f)

    semantic_tables: Dict[str, SemanticTableExpr] = {}
    metadata: Dict[str, Dict[str, Any]] = {}

    # First pass: create semantic tables without joins
    for name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue

        table_name = config.get("table")
        if not table_name:
            raise ValueError(f"Model '{name}' must specify 'table' field")

        if table_name not in tables:
            available = ", ".join(sorted(tables.keys()))
            raise KeyError(
                f"Table '{table_name}' not found in tables.\n"
                f"Available tables: {available}"
            )

        base_table = tables[table_name]

        # Parse dimensions and measures
        dimensions, dim_descriptions = _parse_expressions(config.get("dimensions", {}))
        measures, meas_descriptions = _parse_expressions(config.get("measures", {}))

        # Create semantic table
        sem_table = to_semantic_table(base_table)

        if dimensions:
            sem_table = with_dimensions(sem_table, **dimensions)

        if measures:
            sem_table = with_measures(sem_table, **measures)

        semantic_tables[name] = sem_table

        # Store metadata for later use
        metadata[name] = {
            "description": config.get("description"),
            "primary_key": config.get("primary_key"),
            "time_dimension": config.get("time_dimension"),
            "smallest_time_grain": config.get("smallest_time_grain"),
            "dimension_descriptions": dim_descriptions,
            "measure_descriptions": meas_descriptions,
            "joins_config": config.get("joins", {}),
        }

    # Second pass: add joins now that all semantic tables exist
    for name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue

        if "joins" in config and config["joins"]:
            base_table = semantic_tables[name]
            joins = _parse_joins(config["joins"], semantic_tables, name)

            # Apply joins to the semantic table
            for alias, (target_table, how, on_func) in joins.items():
                if how == "cross":
                    joined = join_(base_table, target_table, how="cross", on=None)
                else:
                    joined = join_(base_table, target_table, how=how, on=on_func)

                # Update the semantic table
                semantic_tables[name] = joined

                # Store join metadata (could be used for documentation/introspection)
                if "joins" not in metadata[name]:
                    metadata[name]["joins"] = {}
                metadata[name]["joins"][alias] = {
                    "target": target_table,
                    "type": how,
                }

    # Return both tables and metadata separately
    # Since SemanticTableExpr is immutable, we can't attach metadata directly
    return semantic_tables, metadata


def from_yaml_with_metadata(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, SemanticTableExpr], Dict[str, Dict[str, Any]]]:
    """
    Load semantic table expressions from a YAML file, returning both tables and metadata.

    Returns:
        Tuple of (semantic_tables dict, metadata dict)
    """
    return _load_yaml_impl(yaml_path, tables)


def from_yaml(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> Dict[str, SemanticTableExpr]:
    """Load semantic table expressions from a YAML file."""
    semantic_tables, _ = _load_yaml_impl(yaml_path, tables)
    return semantic_tables
