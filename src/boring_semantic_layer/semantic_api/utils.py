"""Utility functions for the semantic API."""

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
