"""
YAML loader for Boring Semantic Layer semantic API models.
Simplified version supporting single-table Malloy-style approach.
"""

from typing import Any, Dict, Optional, Callable
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
from .utils import parse_expression


def _parse_dimensions(dimensions_config: Dict[str, Any]) -> Dict[str, Callable]:
    """Parse dimension expressions from YAML configurations."""
    result: Dict[str, Callable] = {}

    for name, config in dimensions_config.items():
        if isinstance(config, str):
            # Simple format: name: expression_string
            result[name] = parse_expression(config)
        elif isinstance(config, dict):
            # Extended format (ignore descriptions for now)
            if "expr" not in config:
                raise ValueError(
                    f"Dimension '{name}' must specify 'expr' field when using dict format"
                )
            result[name] = parse_expression(config["expr"])
        else:
            raise ValueError(
                f"Invalid dimension format for '{name}'. Must either be a string or a dictionary"
            )

    return result


def _parse_measures(measures_config: Dict[str, Any]) -> Dict[str, Callable]:
    """Parse measure expressions from YAML configurations."""
    result: Dict[str, Callable] = {}

    for name, config in measures_config.items():
        if isinstance(config, str):
            # Simple format: name: expression_string
            result[name] = parse_expression(config)
        elif isinstance(config, dict):
            # Extended format (ignore descriptions for now)
            if "expr" not in config:
                raise ValueError(
                    f"Measure '{name}' must specify 'expr' field when using dict format"
                )
            result[name] = parse_expression(config["expr"])
        else:
            raise ValueError(
                f"Invalid measure format for '{name}'. Must either be a string or a dictionary"
            )

    return result


def _create_join_condition(left_expr: Callable, alias: str) -> Callable:
    """Create a join condition function for the given left expression and alias."""

    def on_func(left, right, expr=left_expr):
        left_key = expr.resolve(left)
        # Standard join patterns
        if alias == "carriers":
            return left_key == right.code
        else:
            # Generic fallback - try common patterns
            if hasattr(right, "id"):
                return left_key == right.id
            elif hasattr(right, "code"):
                return left_key == right.code
            else:
                raise ValueError(f"Cannot determine join condition for {alias}")

    return on_func


def load_table(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> SemanticTableExpr:
    """
    Load a single semantic table from YAML file (Malloy-style).

    Expects YAML to define one main table with inline joins.

    Args:
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names to Ibis tables

    Returns:
        SemanticTableExpr: Single semantic table with joins applied
    """
    if tables is None:
        tables = {}

    with open(yaml_path, "r") as f:
        yaml_config = yaml.safe_load(f)

    # Expect single root model
    if not isinstance(yaml_config, dict) or len(yaml_config) != 1:
        raise ValueError(
            f"YAML file must contain exactly one root model definition, got: {list(yaml_config.keys()) if isinstance(yaml_config, dict) else type(yaml_config)}"
        )

    model_name, config = next(iter(yaml_config.items()))

    if not isinstance(config, dict):
        raise ValueError(f"Model '{model_name}' configuration must be a dictionary")

    # Get base table
    table_name = config.get("table")
    if not table_name:
        raise ValueError(f"Model '{model_name}' must specify 'table' field")

    if table_name not in tables:
        available = ", ".join(sorted(tables.keys()))
        raise KeyError(
            f"Table '{table_name}' not found in tables.\nAvailable tables: {available}"
        )

    base_table = tables[table_name]

    # Parse main table dimensions and measures
    dimensions = _parse_dimensions(config.get("dimensions", {}))
    measures = _parse_measures(config.get("measures", {}))

    # Get table description
    description = config.get("description", "")

    # Create base semantic table with description
    sem_table = to_semantic_table(base_table, description=description)

    if dimensions:
        sem_table = with_dimensions(sem_table, **dimensions)

    if measures:
        sem_table = with_measures(sem_table, **measures)

    # Handle joins
    joins_config = config.get("joins", {})
    for alias, join_config in joins_config.items():
        join_table_name = join_config.get("table")
        if not join_table_name:
            raise ValueError(f"Join '{alias}' must specify 'table' field")

        if join_table_name not in tables:
            available = ", ".join(sorted(tables.keys()))
            raise KeyError(
                f"Join table '{join_table_name}' not found in tables.\n"
                f"Available tables: {available}"
            )

        join_table = tables[join_table_name]
        join_type = join_config.get("type", "one")

        if join_type in ["one", "many"]:
            # Parse join condition
            with_expr_str = join_config.get("with")
            if not with_expr_str:
                raise ValueError(
                    f"Join '{alias}' of type '{join_type}' must specify 'with' field"
                )

            with_expr = eval(with_expr_str, {"_": ibis_mod._, "__builtins__": {}})

            # Parse join table dimensions and measures
            join_dimensions = _parse_dimensions(join_config.get("dimensions", {}))
            join_measures = _parse_measures(join_config.get("measures", {}))

            # Create semantic table for the joined table
            join_sem_table = to_semantic_table(join_table)
            if join_dimensions:
                join_sem_table = with_dimensions(join_sem_table, **join_dimensions)
            if join_measures:
                join_sem_table = with_measures(join_sem_table, **join_measures)

            # Create join condition
            on_func = _create_join_condition(with_expr, alias)

            # Apply the join
            how = "inner" if join_type == "one" else "left"
            sem_table = join_(sem_table, join_sem_table, how=how, on=on_func)

        elif join_type == "cross":
            # Parse join table dimensions and measures
            join_dimensions = _parse_dimensions(join_config.get("dimensions", {}))
            join_measures = _parse_measures(join_config.get("measures", {}))

            # Create semantic table for the joined table
            join_sem_table = to_semantic_table(join_table)
            if join_dimensions:
                join_sem_table = with_dimensions(join_sem_table, **join_dimensions)
            if join_measures:
                join_sem_table = with_measures(join_sem_table, **join_measures)

            # Apply cross join
            sem_table = join_(sem_table, join_sem_table, how="cross", on=None)
        else:
            raise ValueError(
                f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'"
            )

    return sem_table
