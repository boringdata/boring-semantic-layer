from __future__ import annotations

import ast
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from returns.maybe import Maybe, Nothing, Some
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
    ast.Lambda,  # Allow lambda expressions for ibis_string_to_expr
    ast.arguments,  # Required for lambda function arguments
    ast.arg,  # Required for individual lambda arguments
}


def _validate_ast(node: ast.AST, allowed_names: set[str] | None = None) -> None:
    if type(node) not in SAFE_NODES:
        raise SafeEvalError(
            f"Unsafe node type: {type(node).__name__}. Only whitelisted operations are allowed."
        )

    if isinstance(node, ast.Name) and allowed_names is not None and node.id not in allowed_names:
        raise SafeEvalError(f"Name '{node.id}' is not in the allowed names: {allowed_names}")

    for child in ast.iter_child_nodes(node):
        _validate_ast(child, allowed_names)


def _parse_expr(expr_str: str) -> ast.AST:
    try:
        return ast.parse(expr_str, mode="eval")
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
    eval_context = {"__builtins__": {}, **(context or {})}

    @safe
    def do_eval():
        tree = _parse_expr(expr_str)
        _validate_ast(tree, allowed_names)
        code = _compile_validated(tree)
        return _eval_in_context(eval_context, code)

    return do_eval()


def _extract_lambda_from_source(source: str) -> str:
    if "lambda" not in source:
        return source

    lambda_start = source.index("lambda")
    lambda_expr = source[lambda_start:]

    for end_marker in [" #", "  #", ",\n", "\n"]:
        if end_marker in lambda_expr:
            end_idx = lambda_expr.index(end_marker)
            return lambda_expr[:end_idx].strip().rstrip(",")

    return lambda_expr.strip().rstrip(",")


def lambda_to_string(fn: Callable) -> Result[str, Exception]:
    @safe
    def do_extract():
        source_lines = inspect.getsourcelines(fn)[0]
        source = "".join(source_lines).strip()
        return _extract_lambda_from_source(source)

    return do_extract()


def _check_deferred(fn: Any) -> Maybe[str]:
    from ibis.common.deferred import Deferred

    return Some(str(fn)) if isinstance(fn, Deferred) else Nothing


def _check_closure_vars(fn: Callable) -> Maybe[str]:
    from ibis.common.deferred import Deferred
    from returns.result import Success

    closure_vars = inspect.getclosurevars(fn)

    if not closure_vars.nonlocals:
        return Nothing

    for name, value in closure_vars.nonlocals.items():
        if isinstance(value, Deferred):
            return Some(str(value))
        if callable(value) and name == "expr":
            result = expr_to_ibis_string(value)
            if isinstance(result, Success):
                return Some(result.unwrap())

    return Nothing


@safe
def _try_ibis_introspection(fn: Callable) -> Maybe[str]:
    from xorq.vendor.ibis import _
    from xorq.vendor.ibis.common.deferred import Deferred

    result = fn(_)
    return Some(str(result)) if isinstance(result, Deferred) else Nothing


def _extract_ibis_from_lambda_str(lambda_str: str) -> Maybe[str]:
    if ":" not in lambda_str:
        return Nothing

    body = lambda_str.split(":", 1)[1].strip()
    param_part = lambda_str.split(":")[0]
    param_names = param_part.replace("lambda", "").strip().split(",")
    first_param = param_names[0].strip()
    ibis_expr = body.replace(f"{first_param}.", "_.")

    return Some(ibis_expr)


def _try_source_extraction(fn: Callable) -> Maybe[str]:
    from returns.result import Success

    lambda_str_result = lambda_to_string(fn)
    return (
        _extract_ibis_from_lambda_str(lambda_str_result.unwrap())
        if isinstance(lambda_str_result, Success)
        else Nothing
    )


def expr_to_ibis_string(fn: Callable) -> Result[str, Exception]:
    @safe
    def do_convert():
        if not callable(fn):
            deferred_check = _check_deferred(fn)
            if isinstance(deferred_check, Some):
                return deferred_check.unwrap()
            raise ValueError(f"Expected callable or Deferred, got {type(fn)}")

        checks = [
            lambda: _try_ibis_introspection(fn).value_or(Nothing),
            lambda: _check_closure_vars(fn),
            lambda: _try_source_extraction(fn),
        ]

        for check in checks:
            result = check()
            if isinstance(result, Some):
                return result.unwrap()

        return None

    return do_convert()


def ibis_string_to_expr(expr_str: str) -> Result[Callable, Exception]:
    from returns.result import Failure, Success

    @safe
    def do_convert():
        t_expr = expr_str.replace("_.", "t.")
        lambda_str = f"lambda t: {t_expr}"

        import ibis
        from ibis import _

        try:
            from xorq.vendor import ibis as xorq_ibis

            eval_context = {
                "ibis": ibis,
                "_": _,
                "xorq_ibis": xorq_ibis,
            }
            allowed_names = {"ibis", "_", "xorq_ibis", "t"}
        except ImportError:
            eval_context = {
                "ibis": ibis,
                "_": _,
            }
            allowed_names = {"ibis", "_", "t"}

        result = safe_eval(lambda_str, context=eval_context, allowed_names=allowed_names)
        if isinstance(result, Success):
            return result.unwrap()
        elif isinstance(result, Failure):
            raise result.failure()
        else:
            raise ValueError(f"Unexpected result type: {type(result)}")

    return do_convert()


def _is_url(path: str | Path | None) -> bool:
    """Check if a path is a URL."""
    if path is None:
        return False
    from urllib.parse import urlparse

    parsed = urlparse(str(path))
    return parsed.scheme in ("http", "https")


def _fetch_url_content(url: str) -> str:
    """Fetch content from a URL.

    Args:
        url: The URL to fetch

    Returns:
        The content as a string

    Raises:
        ValueError: If the fetch fails
    """
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise ValueError(f"HTTP Error {e.code}: {e.reason} for URL: {url}") from e
    except urllib.error.URLError as e:
        raise ValueError(f"URL Error: {e.reason} for URL: {url}") from e
    except Exception as e:
        raise ValueError(f"Failed to fetch URL {url}: {e}") from e


def read_yaml_file(yaml_path: str | Path) -> dict:
    """Read and parse YAML file into dict. Supports local files and URLs.

    Args:
        yaml_path: Path to local file or URL (http:// or https://)

    Returns:
        Parsed YAML content as dict
    """
    try:
        if _is_url(yaml_path):
            content_str = _fetch_url_content(str(yaml_path))
            content = yaml.safe_load(content_str)
        else:
            yaml_path = Path(yaml_path)
            if not yaml_path.exists():
                raise FileNotFoundError(f"YAML file not found: {yaml_path}")

            with open(yaml_path) as f:
                content = yaml.safe_load(f)

        if not isinstance(content, dict):
            raise ValueError(f"YAML file must contain a dict, got: {type(content)}")

        return content
    except (FileNotFoundError, ValueError):
        raise
    except Exception as e:
        raise ValueError(f"Failed to parse YAML file {yaml_path}: {e}") from e


__all__ = [
    "safe_eval",
    "SafeEvalError",
    "expr_to_ibis_string",
    "ibis_string_to_expr",
    "read_yaml_file",
]
