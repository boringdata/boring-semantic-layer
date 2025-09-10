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

from typing import Callable
import ibis


def parse_expression(expr_str: str) -> Callable:
    """Parse an expression string into a lambda function.

    Supports two formats:
    1. Unbound expressions using '_': "_.count()", "_.field.mean()"
    2. Lambda expressions: "lambda t: t.count()"

    Args:
        expr_str: Expression string to parse

    Returns:
        Callable that takes a table and returns an Ibis expression
    """
    # Handle lambda expressions
    if expr_str.strip().startswith("lambda"):
        return eval(expr_str, {"__builtins__": {}})

    # Handle unbound expressions
    deferred = eval(expr_str, {"_": ibis._, "__builtins__": {}})

    def expr_func(t, d=deferred):
        return d.resolve(t)

    return expr_func


def _parse_expressions(expressions_config: Dict[str, Any], expr_type: str = "expression") -> Dict[str, Callable]:
    """Parse dimension or measure expressions from YAML configurations.
    
    Args:
        expressions_config: Dictionary of expression configurations
        expr_type: Type of expressions being parsed ("dimension" or "measure") for error messages
        
    Returns:
        Dictionary mapping expression names to callable functions
    """
    result: Dict[str, Callable] = {}

    for name, config in expressions_config.items():
        if isinstance(config, str):
            # Simple format: name: expression_string
            result[name] = parse_expression(config)
        elif isinstance(config, dict):
            # Extended format (descriptions supported but ignored for now)
            if "expr" not in config:
                raise ValueError(
                    f"{expr_type.capitalize()} '{name}' must specify 'expr' field when using dict format"
                )
            result[name] = parse_expression(config["expr"])
        else:
            raise ValueError(
                f"Invalid {expr_type} format for '{name}'. Must either be a string or a dictionary"
            )

    return result


def _parse_dimensions(dimensions_config: Dict[str, Any]) -> Dict[str, Callable]:
    """Parse dimension expressions from YAML configurations."""
    return _parse_expressions(dimensions_config, "dimension")


def _parse_measures(measures_config: Dict[str, Any]) -> Dict[str, Callable]:
    """Parse measure expressions from YAML configurations."""
    return _parse_expressions(measures_config, "measure")


def _extract_descriptions(expressions_config: Dict[str, Any]) -> Dict[str, str]:
    """Extract description metadata from expression configurations.
    
    Args:
        expressions_config: Dictionary of expression configurations
        
    Returns:
        Dictionary mapping expression names to descriptions
    """
    descriptions = {}
    
    for name, config in expressions_config.items():
        if isinstance(config, str):
            # Simple format: no description available
            descriptions[name] = ""
        elif isinstance(config, dict):
            # Extended format: extract description
            descriptions[name] = config.get("description", "")
        else:
            descriptions[name] = ""
    
    return descriptions


def _create_join_condition(left_expr: Callable, alias: str, join_config: Optional[Dict[str, Any]] = None) -> Callable:
    """Create a join condition function for the given left expression and alias.
    
    Args:
        left_expr: Expression function for the left side of the join
        alias: Alias name for the joined table
        join_config: Join configuration that may contain 'right_on' field
        
    Returns:
        Join condition function
    """
    # Check if explicit right column is specified
    right_column = None
    if join_config and "right_on" in join_config:
        right_column = join_config["right_on"]

    def on_func(left, right, expr=left_expr):
        left_key = expr.resolve(left)
        
        # Use explicit right column if provided
        if right_column:
            if hasattr(right, right_column):
                return left_key == getattr(right, right_column)
            else:
                raise ValueError(f"Right table does not have column '{right_column}' for join '{alias}'")
        
        # Auto-detect join column using common patterns
        common_join_columns = ["id", "code", "key", "name", alias.lower()]
        
        for col in common_join_columns:
            if hasattr(right, col):
                return left_key == getattr(right, col)
        
        # If no common pattern found, try to get the left key name and match it
        try:
            if hasattr(left_key, "get_name"):
                left_key_name = left_key.get_name()
                if hasattr(right, left_key_name):
                    return left_key == getattr(right, left_key_name)
        except:
            pass  # Ignore errors in name detection
            
        raise ValueError(
            f"Cannot determine join condition for '{alias}'. "
            f"Consider specifying 'right_on' field in join configuration. "
            f"Available columns tried: {common_join_columns}"
        )

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

    # Create base semantic table
    sem_table = to_semantic_table(base_table, name=model_name)

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

            with_expr = parse_expression(with_expr_str)

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
            on_func = _create_join_condition(with_expr, alias, join_config)

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


def from_yaml(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> Dict[str, SemanticTableExpr]:
    """
    Load semantic table expressions from a YAML file.
    
    This function supports multiple models in a single YAML file,
    returning a dictionary mapping model names to SemanticTableExpr instances.
    
    Args:
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names to Ibis tables
        
    Returns:
        Dict mapping model names to SemanticTableExpr instances
    """
    if tables is None:
        tables = {}

    with open(yaml_path, "r") as f:
        yaml_configs = yaml.safe_load(f)

    if not isinstance(yaml_configs, dict):
        raise ValueError("YAML file must contain a dictionary of model definitions")

    results = {}
    
    # Handle single model case (load_table style)
    if len(yaml_configs) == 1:
        model_name, config = next(iter(yaml_configs.items()))
        if isinstance(config, dict) and "table" in config:
            results[model_name] = load_table(yaml_path, tables)
            return results
    
    # First pass: create models without joins
    for model_name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue
            
        table_name = config.get("table")
        if not table_name:
            raise ValueError(f"Model '{model_name}' must specify 'table' field")

        if table_name not in tables:
            available = ", ".join(sorted(tables.keys()))
            raise KeyError(
                f"Table '{table_name}' not found in tables.\n"
                f"Available tables: {available}"
            )

        base_table = tables[table_name]
        
        # Parse dimensions and measures
        dimensions = _parse_dimensions(config.get("dimensions", {}))
        measures = _parse_measures(config.get("measures", {}))
        
        # Create semantic table
        sem_table = to_semantic_table(base_table, name=model_name)
        
        if dimensions:
            sem_table = with_dimensions(sem_table, **dimensions)
            
        if measures:
            sem_table = with_measures(sem_table, **measures)
            
        results[model_name] = sem_table
    
    # Second pass: handle joins between models
    for model_name, config in yaml_configs.items():
        if not isinstance(config, dict):
            continue
        
        joins_config = config.get("joins", {})
        if not joins_config:
            continue
            
        base_model = results[model_name]
        
        for alias, join_config in joins_config.items():
            join_type = join_config.get("type", "one")
            
            # Handle model-to-model joins
            if "model" in join_config:
                model_ref = join_config["model"]
                if model_ref not in results:
                    available = ", ".join(sorted(results.keys()))
                    raise KeyError(f"Model '{model_ref}' not found. Available models: {available}")
                
                joined_model = results[model_ref]
                
                if join_type in ["one", "many"]:
                    with_expr_str = join_config.get("with")
                    if not with_expr_str:
                        raise ValueError(f"Join '{alias}' must specify 'with' field")
                    
                    with_expr = parse_expression(with_expr_str)
                    on_func = _create_join_condition(with_expr, alias, join_config)
                    how = "inner" if join_type == "one" else "left"
                    
                    base_model = join_(base_model, joined_model, how=how, on=on_func)
                    
                elif join_type == "cross":
                    base_model = join_(base_model, joined_model, how="cross", on=None)
                else:
                    raise ValueError(f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'")
            
            # Handle table-to-table joins (existing functionality)
            elif "table" in join_config:
                # Use existing load_table logic for inline joins
                # This is more complex and would require refactoring
                pass
        
        # Update the result with the joined model
        results[model_name] = base_model
    
    return results


def from_yaml_with_metadata(
    yaml_path: str,
    tables: Optional[Dict[str, Any]] = None,
) -> tuple[Dict[str, SemanticTableExpr], Dict[str, Dict[str, Any]]]:
    """
    Load semantic table expressions from a YAML file, returning both tables and metadata.
    
    Args:
        yaml_path: Path to the YAML configuration file
        tables: Optional mapping of table names to Ibis tables
        
    Returns:
        Tuple of (semantic_tables dict, metadata dict)
    """
    # Load the semantic tables
    semantic_tables = from_yaml(yaml_path, tables)
    
    # Load metadata
    with open(yaml_path, "r") as f:
        yaml_configs = yaml.safe_load(f)
    
    metadata = {}
    for model_name, config in yaml_configs.items():
        if isinstance(config, dict):
            # Process joins config to create the expected metadata structure
            joins_config = config.get("joins", {})
            joins_metadata = {}
            for alias, join_config in joins_config.items():
                joins_metadata[alias] = {
                    "target": join_config.get("model") or join_config.get("table"),
                    "type": join_config.get("type", "one"),
                }
            
            metadata[model_name] = {
                "description": config.get("description"),
                "primary_key": config.get("primary_key"),
                "time_dimension": config.get("time_dimension"),
                "smallest_time_grain": config.get("smallest_time_grain"),
                "dimension_descriptions": _extract_descriptions(config.get("dimensions", {})),
                "measure_descriptions": _extract_descriptions(config.get("measures", {})),
                "joins_config": joins_config,
                "joins": joins_metadata,  # Add the expected joins structure
            }
    
    return semantic_tables, metadata