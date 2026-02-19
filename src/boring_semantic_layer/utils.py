from __future__ import annotations

import ast
import importlib
import inspect
import operator
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
    from returns.result import Success

    from xorq.vendor.ibis import _
    from xorq.vendor.ibis.common.deferred import Deferred

    result = fn(_)
    if not isinstance(result, Deferred):
        return Nothing
    expr_str = str(result)
    # Validate by attempting deserialization — if the string can't round-trip,
    # it's useless (catches invalid syntax, internal function names like
    # _finish_searched_case/ifelse that aren't in the eval context, etc.)
    if not isinstance(ibis_string_to_expr(expr_str), Success):
        return Nothing
    return Some(expr_str)


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
            from xorq import api as xo
            from xorq.vendor import ibis as xorq_ibis

            eval_context = {
                "ibis": ibis,
                "_": _,
                "xorq_ibis": xorq_ibis,
                "xo": xo,
            }
            allowed_names = {"ibis", "_", "xorq_ibis", "xo", "t"}
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


def _is_ibis_literal_node(value) -> bool:
    try:
        from xorq.vendor.ibis.expr.operations.generic import Literal
        return isinstance(value, Literal)
    except ImportError:
        return False


def serialize_resolver(resolver) -> tuple:
    """Walk a Resolver tree and produce a hashable nested-tuple representation."""
    from xorq.vendor.ibis.common.deferred import (
        Attr,
        BinaryOperator,
        Call,
        Just,
        JustUnhashable,
        Mapping as MappingResolver,
        Sequence,
        UnaryOperator,
        Variable,
    )

    if isinstance(resolver, Variable):
        return ("var", resolver.name)

    if isinstance(resolver, Just):
        value = resolver.value
        # ibis Literal node (e.g., from case().when(..., 1))
        if _is_ibis_literal_node(value):
            py_value = value.args[0]
            dtype_str = str(value.args[1])
            return ("ibis_literal", py_value, dtype_str)
        # callable (operator functions, deferrable functions like ifelse, _finish_searched_case)
        if callable(value):
            module = getattr(value, "__module__", None)
            qualname = getattr(value, "__qualname__", None)
            if module and qualname:
                return ("fn", module, qualname)
            raise ValueError(f"Cannot serialize callable without __module__/__qualname__: {value!r}")
        # primitive value (int, float, str, bool, None)
        return ("just", value)

    if isinstance(resolver, JustUnhashable):
        value = resolver.value.obj
        if _is_ibis_literal_node(value):
            py_value = value.args[0]
            dtype_str = str(value.args[1])
            return ("ibis_literal", py_value, dtype_str)
        raise ValueError(f"Cannot serialize unhashable value: {value!r}")

    if isinstance(resolver, Attr):
        return ("attr", serialize_resolver(resolver.obj), serialize_resolver(resolver.name))

    if isinstance(resolver, Call):
        func_tuple = serialize_resolver(resolver.func)
        args_tuple = tuple(serialize_resolver(a) for a in resolver.args)
        kwargs_tuple = tuple(
            (k, serialize_resolver(v)) for k, v in resolver.kwargs.items()
        )
        return ("call", func_tuple, args_tuple, kwargs_tuple)

    if isinstance(resolver, BinaryOperator):
        op_name = resolver.func.__name__
        return ("binop", op_name, serialize_resolver(resolver.left), serialize_resolver(resolver.right))

    if isinstance(resolver, UnaryOperator):
        op_name = resolver.func.__name__
        return ("unop", op_name, serialize_resolver(resolver.arg))

    if isinstance(resolver, Sequence):
        type_name = resolver.typ.__name__
        items = tuple(serialize_resolver(v) for v in resolver.values)
        return ("seq", type_name, items)

    if isinstance(resolver, MappingResolver):
        type_name = resolver.typ.__name__
        items = tuple((k, serialize_resolver(v)) for k, v in resolver.values.items())
        return ("map", type_name, items)

    raise ValueError(f"Unknown resolver type: {type(resolver).__name__}")


_OPERATOR_MAP = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "truediv": operator.truediv,
    "floordiv": operator.floordiv,
    "pow": operator.pow,
    "mod": operator.mod,
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "le": operator.le,
    "gt": operator.gt,
    "ge": operator.ge,
    "and_": operator.and_,
    "or_": operator.or_,
    "xor": operator.xor,
    "rshift": operator.rshift,
    "lshift": operator.lshift,
    "inv": operator.inv,
    "neg": operator.neg,
    "invert": operator.invert,
}


def _resolve_qualname(module_obj, qualname: str):
    """Resolve a dotted qualname like 'ClassName.method' on a module."""
    parts = qualname.split(".")
    obj = module_obj
    for part in parts:
        if part == "<lambda>":
            raise ValueError(f"Cannot resolve lambda qualname: {qualname}")
        obj = getattr(obj, part)
    return obj


def deserialize_resolver(data: tuple):
    """Reconstruct a Resolver tree from a nested-tuple representation."""
    from xorq.vendor.ibis.common.deferred import (
        Attr,
        BinaryOperator,
        Call,
        Just,
        Mapping as MappingResolver,
        Sequence,
        UnaryOperator,
        Variable,
    )

    tag = data[0]

    if tag == "var":
        return Variable(data[1])

    if tag == "just":
        return Just(data[1])

    if tag == "fn":
        _, module_name, qualname = data
        mod = importlib.import_module(module_name)
        func = _resolve_qualname(mod, qualname)
        return Just(func)

    if tag == "ibis_literal":
        _, py_value, dtype_str = data
        from xorq.vendor import ibis
        lit_expr = ibis.literal(py_value, type=ibis.dtype(dtype_str))
        # Return the Literal node (not the expression) since that's what goes into Just
        return Just(lit_expr.op())

    if tag == "attr":
        _, obj_data, name_data = data
        obj_resolver = deserialize_resolver(obj_data)
        name_resolver = deserialize_resolver(name_data)
        # Attr stores obj and name as resolvers already, so construct directly
        attr = object.__new__(Attr)
        object.__setattr__(attr, "obj", obj_resolver)
        object.__setattr__(attr, "name", name_resolver)
        return attr

    if tag == "call":
        _, func_data, args_data, kwargs_data = data
        func_resolver = deserialize_resolver(func_data)
        args_resolvers = tuple(deserialize_resolver(a) for a in args_data)
        from xorq.vendor.ibis.common.collections import FrozenDict
        kwargs_resolvers = FrozenDict(
            {k: deserialize_resolver(v) for k, v in kwargs_data}
        )
        call = object.__new__(Call)
        object.__setattr__(call, "func", func_resolver)
        object.__setattr__(call, "args", args_resolvers)
        object.__setattr__(call, "kwargs", kwargs_resolvers)
        return call

    if tag == "binop":
        _, op_name, left_data, right_data = data
        func = _OPERATOR_MAP[op_name]
        left = deserialize_resolver(left_data)
        right = deserialize_resolver(right_data)
        binop = object.__new__(BinaryOperator)
        object.__setattr__(binop, "func", func)
        object.__setattr__(binop, "left", left)
        object.__setattr__(binop, "right", right)
        return binop

    if tag == "unop":
        _, op_name, arg_data = data
        func = _OPERATOR_MAP[op_name]
        arg = deserialize_resolver(arg_data)
        unop = object.__new__(UnaryOperator)
        object.__setattr__(unop, "func", func)
        object.__setattr__(unop, "arg", arg)
        return unop

    if tag == "seq":
        _, type_name, items_data = data
        typ = {"tuple": tuple, "list": list}[type_name]
        values = tuple(deserialize_resolver(v) for v in items_data)
        seq = object.__new__(Sequence)
        object.__setattr__(seq, "typ", typ)
        object.__setattr__(seq, "values", values)
        return seq

    if tag == "map":
        _, type_name, items_data = data
        typ = {"dict": dict}[type_name]
        from xorq.vendor.ibis.common.collections import FrozenDict
        values = FrozenDict(
            {k: deserialize_resolver(v) for k, v in items_data}
        )
        mapping = object.__new__(MappingResolver)
        object.__setattr__(mapping, "typ", typ)
        object.__setattr__(mapping, "values", values)
        return mapping

    raise ValueError(f"Unknown resolver tag: {tag}")


def _is_deferred(obj) -> bool:
    """Duck-type check for Deferred (works for both ibis and xorq vendor)."""
    return hasattr(obj, "_resolver") and hasattr(obj, "resolve")


def expr_to_structured(fn: Callable) -> Result[tuple, Exception]:
    """Convert a callable/Deferred expression to a structured tuple representation."""
    from xorq.vendor.ibis.common.deferred import Deferred as XorqDeferred

    @safe
    def do_convert():
        from xorq.vendor.ibis import _

        if isinstance(fn, XorqDeferred):
            return serialize_resolver(fn._resolver)
        # For ibis Deferred (not xorq vendor), resolve through xorq _ to get xorq types
        if _is_deferred(fn):
            result = fn.resolve(_)
            if _is_deferred(result):
                return serialize_resolver(result._resolver)
        if callable(fn):
            result = fn(_)
            if _is_deferred(result):
                return serialize_resolver(result._resolver)
            raise ValueError(f"Callable did not produce a Deferred, got {type(result)}")
        raise ValueError(f"Expected callable or Deferred, got {type(fn)}")

    return do_convert()


def structured_to_expr(data: tuple) -> Result:
    """Reconstruct a Deferred from a structured tuple representation."""
    from xorq.vendor.ibis.common.deferred import Deferred

    @safe
    def do_convert():
        resolver = deserialize_resolver(data)
        return Deferred(resolver)

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
    "expr_to_structured",
    "structured_to_expr",
    "serialize_resolver",
    "deserialize_resolver",
    "read_yaml_file",
]
