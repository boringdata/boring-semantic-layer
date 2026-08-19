"""Sandboxed evaluation of user-supplied expression strings.

An AST allowlist (nodes, module calls, method calls) guards the strings
accepted from YAML models and agent tooling. This is a separate trust
boundary from the serialization callable allowlist in
``serialization._trust``.
"""

from __future__ import annotations

import ast
from typing import Any

from returns.result import Result, safe
from toolz import curry


class SafeEvalError(Exception):
    pass


SAFE_NODES = {
    ast.Expression,
    ast.Load,
    ast.Name,
    ast.Constant,
    ast.Attribute,
    ast.Call,
    ast.Subscript,
    ast.Index,
    ast.Slice,
    ast.UnaryOp,
    ast.UAdd,
    ast.USub,
    ast.Not,
    ast.Invert,  # Bitwise NOT (~)
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.BitOr,  # Bitwise OR (|) - for combining conditions in pandas/ibis
    ast.BitAnd,  # Bitwise AND (&) - for combining conditions in pandas/ibis
    ast.BitXor,  # Bitwise XOR (^)
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.List,
    ast.Tuple,
    ast.Dict,
    ast.keyword,
    ast.IfExp,
    ast.Lambda,  # Allow lambda expressions in YAML/agent-supplied strings
    ast.arguments,  # Required for lambda function arguments
    ast.arg,  # Required for individual lambda arguments
}


# Helpers exposed by the Ibis modules in agent
# query contexts.  In particular, backend/IO namespaces (duckdb, postgres,
# read_*, connect, ...) are intentionally absent.
SAFE_MODULE_CALLS = frozenset(
    {
        "and_",
        "asc",
        "cases",
        "coalesce",
        "cume_dist",
        "date",
        "dense_rank",
        "desc",
        "greatest",
        "ifelse",
        "interval",
        "least",
        "literal",
        "now",
        "ntile",
        "or_",
        "param",
        "percent_rank",
        "random",
        "rank",
        "row_number",
        "time",
        "timestamp",
        "today",
        "uuid",
        "window",
    }
)


# Pure expression operations plus the BSL query-building operations accepted
# in agent-generated query chains.  Calls that execute, compile, perform IO,
# access a backend, or expose an underlying operation are deliberately absent.
SAFE_METHOD_CALLS = frozenset(
    {
        "abs",
        "aggregate",
        "all",
        "any",
        "approx_median",
        "approx_nunique",
        "arbitrary",
        "argmax",
        "argmin",
        "as_table",
        "between",
        "capitalize",
        "cast",
        "ceil",
        "coalesce",
        "collect",
        "contains",
        "count",
        "cummax",
        "cummean",
        "cummin",
        "cumsum",
        "date",
        "day",
        "day_of_week",
        "distinct",
        "drop",
        "endswith",
        "epoch_seconds",
        "fill_null",
        "filter",
        "find",
        "first",
        "floor",
        "group_by",
        "hour",
        "identical_to",
        "ifelse",
        "isin",
        "isnull",
        "lag",
        "last",
        "lead",
        "length",
        "like",
        "limit",
        "lower",
        "lpad",
        "lstrip",
        "max",
        "mean",
        "median",
        "microsecond",
        "millisecond",
        "min",
        "minute",
        "mode",
        "month",
        "mutate",
        "name",
        "notin",
        "notnull",
        "nullif",
        "nunique",
        "order_by",
        "over",
        "quarter",
        "quantile",
        "re_extract",
        "re_replace",
        "re_search",
        "re_split",
        "rename",
        "repeat",
        "replace",
        "reverse",
        "right",
        "round",
        "rpad",
        "rstrip",
        "second",
        "select",
        "sign",
        "split",
        "startswith",
        "std",
        "strftime",
        "strip",
        "substr",
        "sum",
        "time",
        "timestamp",
        "translate",
        "truncate",
        "typeof",
        "unique",
        "unnest",
        "upper",
        "var",
        "week_of_year",
        "with_dimensions",
        "with_measures",
        "year",
    }
)

SAFE_QUERY_METHOD_CALLS = frozenset(
    {
        "aggregate",
        "distinct",
        "drop",
        "filter",
        "group_by",
        "limit",
        "mutate",
        "order_by",
        "rename",
        "select",
        "unnest",
        "with_dimensions",
        "with_measures",
    }
)

SAFE_QUERY_ATTRIBUTES = frozenset({"dimensions", "measures"})

_SAFE_MODULE_ROOTS = frozenset({"ibis", "xorq_ibis", "xo"})


class _SafeEvalValidator(ast.NodeVisitor):
    """Validate the small, expression-only DSL accepted by ``safe_eval``."""

    def __init__(self, allowed_names: set[str]):
        self._allowed_names = allowed_names
        self._lambda_names: list[set[str]] = []

    @staticmethod
    def _root_name(node: ast.AST) -> str | None:
        while True:
            if isinstance(node, ast.Name):
                return node.id
            if isinstance(node, ast.Attribute):
                node = node.value
            elif isinstance(node, ast.Call):
                node = node.func
            elif isinstance(node, ast.Subscript):
                node = node.value
            else:
                return None

    def _is_expression_root(self, name: str | None) -> bool:
        lambda_names = set().union(*self._lambda_names) if self._lambda_names else set()
        return name == "_" or name in lambda_names or name in _SAFE_MODULE_ROOTS

    def generic_visit(self, node: ast.AST) -> None:
        if type(node) not in SAFE_NODES:
            raise SafeEvalError(
                f"Unsafe node type: {type(node).__name__}. Only whitelisted operations are allowed."
            )
        super().generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        if node.id.startswith("_") and node.id != "_":
            raise SafeEvalError(f"Private name '{node.id}' is not allowed")
        lambda_names = set().union(*self._lambda_names) if self._lambda_names else set()
        if node.id not in self._allowed_names and node.id not in lambda_names:
            raise SafeEvalError(
                f"Name '{node.id}' is not in the allowed names: {self._allowed_names}"
            )

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if node.attr.startswith("_"):
            raise SafeEvalError(f"Private attribute '{node.attr}' is not allowed")
        if (
            isinstance(node.value, ast.Name)
            and node.value.id in _SAFE_MODULE_ROOTS
            and node.attr not in SAFE_MODULE_CALLS
        ):
            raise SafeEvalError(f"Module attribute '{node.value.id}.{node.attr}' is not allowed")
        if (
            isinstance(node.value, ast.Name)
            and not self._is_expression_root(node.value.id)
            and node.attr not in SAFE_QUERY_METHOD_CALLS | SAFE_QUERY_ATTRIBUTES
        ):
            raise SafeEvalError(
                f"Attribute '{node.value.id}.{node.attr}' is not an allowed query operation"
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if not isinstance(node.func, ast.Attribute):
            raise SafeEvalError("Only allowlisted DSL method calls are allowed")

        root_name = self._root_name(node.func)
        if isinstance(node.func.value, ast.Name) and node.func.value.id in _SAFE_MODULE_ROOTS:
            if node.func.attr not in SAFE_MODULE_CALLS:
                raise SafeEvalError(
                    f"Module call '{node.func.value.id}.{node.func.attr}' is not allowed"
                )
        elif (
            root_name is not None
            and not self._is_expression_root(root_name)
            and node.func.attr not in SAFE_QUERY_METHOD_CALLS
        ):
            raise SafeEvalError(f"Query method call '{node.func.attr}' is not allowed")
        elif node.func.attr not in SAFE_METHOD_CALLS:
            raise SafeEvalError(f"Method call '{node.func.attr}' is not allowed")

        for keyword in node.keywords:
            if keyword.arg is None or keyword.arg.startswith("_"):
                raise SafeEvalError("Private or expanded keyword arguments are not allowed")
        self.generic_visit(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
        args = node.args
        if args.vararg or args.kwarg or args.kwonlyargs or args.defaults or args.kw_defaults:
            raise SafeEvalError("Lambda defaults and variadic arguments are not allowed")
        lambda_names = {arg.arg for arg in [*args.posonlyargs, *args.args]}
        if any(name.startswith("_") for name in lambda_names):
            raise SafeEvalError("Private lambda argument names are not allowed")
        self._lambda_names.append(lambda_names)
        try:
            self.visit(node.body)
        finally:
            self._lambda_names.pop()


def _validate_ast(node: ast.AST, allowed_names: set[str]) -> None:
    _SafeEvalValidator(allowed_names).visit(node)


def _parse_expr(expr_str: str) -> ast.AST:
    try:
        return ast.parse(expr_str, mode="eval")
    except SyntaxError:
        # Try wrapping in parentheses to allow multiline method chaining
        # This handles cases like:
        #   model.filter(...)
        #   .group_by(...)
        # which is valid Python when wrapped in parens
        try:
            return ast.parse(f"({expr_str})", mode="eval")
        except SyntaxError as e:
            raise SafeEvalError(f"Invalid Python syntax: {e}") from e


def _compile_validated(tree: ast.AST) -> Any:
    return compile(tree, "<safe_eval>", "eval")


@curry
def _eval_in_context(context: dict, code: Any) -> Any:
    return eval(code, context)  # noqa: S307


def safe_eval(
    expr_str: str,
    context: dict[str, Any] | None = None,
    allowed_names: set[str] | None = None,
) -> Result[Any, Exception]:
    context = context or {}
    names = set(context) if allowed_names is None else set(allowed_names)
    # ``_`` is the one intentionally public DSL identifier beginning with an
    # underscore.  All caller-provided private names remain inaccessible.
    names = {name for name in names if name == "_" or not name.startswith("_")}
    # Keep builtins empty even if an untrusted caller supplied a conflicting
    # ``__builtins__`` context entry.
    eval_context = {**context, "__builtins__": {}}

    @safe
    def do_eval():
        tree = _parse_expr(expr_str)
        _validate_ast(tree, names)
        code = _compile_validated(tree)
        return _eval_in_context(eval_context, code)

    return do_eval()
