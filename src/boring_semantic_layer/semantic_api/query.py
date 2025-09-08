"""
Query interface for Semantic API using yaml_loader expression parsing approach.
Input is a simple list of key-value operations applied in order to the semantic table.
"""

from typing import Any, Dict, List

try:
    import xorq.vendor.ibis as ibis_mod
except ImportError:
    import ibis as ibis_mod

from .api import SemanticTableExpr
from .utils import parse_expression

_ = ibis_mod._


# === MAIN QUERY EXECUTION FUNCTION ===


def query(
    semantic_table: SemanticTableExpr, operations: List[Dict[str, Any]]
) -> SemanticTableExpr:
    """
    Execute a list of operations on a semantic table in the order provided.

    Args:
        semantic_table: The semantic table to query
        operations: List of operations to apply in order

    Returns:
        SemanticTableExpr: The resulting semantic table after all operations

    Example:
        operations = [
            {"group_by": ["carrier"]},
            {"aggregate": {"flight_count": "_.count()", "avg_delay": "_.dep_delay.mean()"}},
            {"filter": "_.flight_count > 10"},
            {"order_by": ["flight_count desc"]},
            {"limit": 5}
        ]

        result_table = query(semantic_table, operations)
        df = result_table.execute()
    """
    result_table = semantic_table

    for operation in operations:
        # Each operation should have exactly one key-value pair
        if len(operation) != 1:
            raise ValueError(
                f"Each operation must have exactly one key-value pair, got: {operation}"
            )

        op_name, op_value = next(iter(operation.items()))

        # Apply the operation based on the key
        if op_name == "group_by":
            if isinstance(op_value, list):
                # Simple field names
                result_table = result_table.group_by(*op_value)
            elif isinstance(op_value, dict):
                # Expression-based grouping
                inline_dims = {}
                for name, expr_str in op_value.items():
                    inline_dims[name] = parse_expression(expr_str)
                result_table = result_table.group_by(**inline_dims)
            else:
                raise ValueError(
                    f"group_by value must be list or dict, got: {type(op_value)}"
                )

        elif op_name == "aggregate":
            if isinstance(op_value, dict):
                agg_dict = {}
                for name, expr_str in op_value.items():
                    agg_dict[name] = parse_expression(expr_str)
                result_table = result_table.aggregate(**agg_dict)
            else:
                raise ValueError(f"aggregate value must be dict, got: {type(op_value)}")

        elif op_name == "mutate":
            if isinstance(op_value, dict):
                mutate_dict = {}
                for name, expr_str in op_value.items():
                    mutate_dict[name] = parse_expression(expr_str)
                result_table = result_table.mutate(**mutate_dict)
            else:
                raise ValueError(f"mutate value must be dict, got: {type(op_value)}")

        elif op_name == "filter":
            if isinstance(op_value, str):
                filter_func = parse_expression(op_value)
                result_table = result_table.filter(filter_func)
            else:
                raise ValueError(f"filter value must be string, got: {type(op_value)}")

        elif op_name == "select":
            if isinstance(op_value, list):
                result_table = result_table.select(*op_value)
            else:
                raise ValueError(f"select value must be list, got: {type(op_value)}")

        elif op_name == "order_by":
            if isinstance(op_value, list):
                order_keys = []
                for field in op_value:
                    if isinstance(field, str) and field.endswith(" desc"):
                        field_name = field[:-5].strip()
                        order_keys.append(ibis_mod.desc(field_name))
                    else:
                        order_keys.append(field)
                result_table = result_table.order_by(*order_keys)
            else:
                raise ValueError(f"order_by value must be list, got: {type(op_value)}")

        elif op_name == "limit":
            if isinstance(op_value, int) and op_value > 0:
                result_table = result_table.limit(op_value)
            else:
                raise ValueError(
                    f"limit value must be positive integer, got: {op_value}"
                )

        else:
            raise ValueError(f"Unknown operation: {op_name}")

    return result_table
