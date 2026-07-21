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


# Helpers exposed by the Ibis modules in ``ibis_string_to_expr`` and agent
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

    from ._xorq import Deferred, _

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

        def _build(flavor_ibis):
            """Evaluate the lambda with ``ibis``/``_`` bound to one flavor."""
            eval_context = {"ibis": flavor_ibis, "_": flavor_ibis._}
            allowed_names = {"ibis", "_", "t"}
            try:
                from ._xorq import api as xo, ibis as xorq_ibis

                eval_context.update({"xorq_ibis": xorq_ibis, "xo": xo})
                allowed_names |= {"xorq_ibis", "xo"}
            except ImportError:
                pass

            result = safe_eval(lambda_str, context=eval_context, allowed_names=allowed_names)
            if isinstance(result, Success):
                return result.unwrap()
            elif isinstance(result, Failure):
                raise result.failure()
            else:
                raise ValueError(f"Unexpected result type: {type(result)}")

        # Eager evaluation validates the string up front; the returned wrapper
        # re-binds ``ibis``/``_`` to the flavor (plain vs xorq-vendored) of the
        # table it is called with, so eager constructors like ``ibis.literal``
        # compose with either flavor instead of silently mis-comparing.
        fns = {id(ibis): _build(ibis)}

        def _flavored(t):
            from .nested_compile import get_ibis_module

            flavor = get_ibis_module(t)
            key = id(flavor)
            if key not in fns:
                fns[key] = _build(flavor)
            return fns[key](t)

        return _flavored

    return do_convert()


def _is_ibis_literal_node(value) -> bool:
    try:
        from ._xorq import Literal
        return isinstance(value, Literal)
    except ImportError:
        return False


def serialize_resolver(resolver) -> tuple:
    """Walk a Resolver tree and produce a hashable nested-tuple representation."""
    from ._xorq import (
        Attr,
        BinaryOperator,
        Call,
        Item,
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

    if isinstance(resolver, Item):
        return ("item", serialize_resolver(resolver.obj), serialize_resolver(resolver.name))

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


def _finalize_frozen_slotted(obj, *fields) -> None:
    """Set ``__precomputed_hash__`` on a FrozenSlotted built via ``object.__new__``.

    xorq's vendored ibis FrozenSlotted base implements ``__hash__`` by
    returning a precomputed value that ``__init__`` would normally set
    via ``hash((cls, tuple(field_values)))``. When we bypass
    ``__init__`` to skip validation during deserialization we must
    mirror that exactly — note the inner ``tuple(...)`` wrap, which is
    significant: ``hash((cls, *fields))`` produces a different value.
    Without this the rebuilt resolver raises ``AttributeError`` the
    first time it is hashed (e.g. as a key in ``op.replace``
    substitutions).
    """
    object.__setattr__(obj, "__precomputed_hash__", hash((type(obj), tuple(fields))))


def deserialize_resolver(data: tuple):
    """Reconstruct a Resolver tree from a nested-tuple representation."""
    from ._xorq import (
        Attr,
        BinaryOperator,
        Call,
        Item,
        Just,
        Mapping as MappingResolver,
        Sequence,
        UnaryOperator,
        Variable,
    )

    match data:
        case ("var", name):
            return Variable(name)

        case ("just", value):
            return Just(value)

        case ("fn", module_name, qualname):
            mod = importlib.import_module(module_name)
            func = _resolve_qualname(mod, qualname)
            return Just(func)

        case ("ibis_literal", py_value, dtype_str):
            from ._xorq import ibis
            lit_expr = ibis.literal(py_value, type=ibis.dtype(dtype_str))
            return Just(lit_expr.op())

        case ("attr", obj_data, name_data):
            obj_resolver = deserialize_resolver(obj_data)
            name_resolver = deserialize_resolver(name_data)
            attr = object.__new__(Attr)
            object.__setattr__(attr, "obj", obj_resolver)
            object.__setattr__(attr, "name", name_resolver)
            _finalize_frozen_slotted(attr, obj_resolver, name_resolver)
            return attr

        case ("item", obj_data, name_data):
            obj_resolver = deserialize_resolver(obj_data)
            name_resolver = deserialize_resolver(name_data)
            item = object.__new__(Item)
            object.__setattr__(item, "obj", obj_resolver)
            object.__setattr__(item, "name", name_resolver)
            _finalize_frozen_slotted(item, obj_resolver, name_resolver)
            return item

        case ("call", func_data, args_data, kwargs_data):
            func_resolver = deserialize_resolver(func_data)
            args_resolvers = tuple(deserialize_resolver(a) for a in args_data)
            from ._xorq import FrozenDict
            kwargs_resolvers = FrozenDict(
                {k: deserialize_resolver(v) for k, v in kwargs_data}
            )
            call = object.__new__(Call)
            object.__setattr__(call, "func", func_resolver)
            object.__setattr__(call, "args", args_resolvers)
            object.__setattr__(call, "kwargs", kwargs_resolvers)
            _finalize_frozen_slotted(call, func_resolver, args_resolvers, kwargs_resolvers)
            return call

        case ("binop", op_name, left_data, right_data):
            func = _OPERATOR_MAP.get(op_name)
            if func is None:
                raise ValueError(f"Unknown binary operator: {op_name!r}")
            left = deserialize_resolver(left_data)
            right = deserialize_resolver(right_data)
            binop = object.__new__(BinaryOperator)
            object.__setattr__(binop, "func", func)
            object.__setattr__(binop, "left", left)
            object.__setattr__(binop, "right", right)
            _finalize_frozen_slotted(binop, func, left, right)
            return binop

        case ("unop", op_name, arg_data):
            func = _OPERATOR_MAP.get(op_name)
            if func is None:
                raise ValueError(f"Unknown unary operator: {op_name!r}")
            arg = deserialize_resolver(arg_data)
            unop = object.__new__(UnaryOperator)
            object.__setattr__(unop, "func", func)
            object.__setattr__(unop, "arg", arg)
            _finalize_frozen_slotted(unop, func, arg)
            return unop

        case ("seq", type_name, items_data):
            typ = {"tuple": tuple, "list": list}[type_name]
            values = tuple(deserialize_resolver(v) for v in items_data)
            seq = object.__new__(Sequence)
            object.__setattr__(seq, "typ", typ)
            object.__setattr__(seq, "values", values)
            _finalize_frozen_slotted(seq, typ, values)
            return seq

        case ("map", type_name, items_data):
            typ = {"dict": dict}[type_name]
            from ._xorq import FrozenDict
            values = FrozenDict(
                {k: deserialize_resolver(v) for k, v in items_data}
            )
            mapping = object.__new__(MappingResolver)
            object.__setattr__(mapping, "typ", typ)
            object.__setattr__(mapping, "values", values)
            _finalize_frozen_slotted(mapping, typ, values)
            return mapping

        case _:
            raise ValueError(f"Unknown resolver tag: {data[0]}")


def _is_deferred(obj) -> bool:
    """Duck-type check for Deferred (works for both ibis and xorq vendor)."""
    return hasattr(obj, "_resolver") and hasattr(obj, "resolve")


def expr_to_structured(fn: Callable) -> Result[tuple, Exception]:
    """Convert a callable/Deferred expression to a structured tuple representation."""
    from ._xorq import Deferred as XorqDeferred
    from .ops import _CallableWrapper

    @safe
    def do_convert():
        from ._xorq import _

        expr = fn._fn if isinstance(fn, _CallableWrapper) else fn
        if isinstance(expr, XorqDeferred):
            return serialize_resolver(expr._resolver)
        # For ibis Deferred (not xorq vendor), resolve through xorq _ to get xorq types
        if _is_deferred(expr):
            result = expr.resolve(_)
            if _is_deferred(result):
                return serialize_resolver(result._resolver)
        if callable(expr):
            result = expr(_)
            if _is_deferred(result):
                return serialize_resolver(result._resolver)
            raise ValueError(f"Callable did not produce a Deferred, got {type(result)}")
        raise ValueError(f"Expected callable or Deferred, got {type(expr)}")

    return do_convert()


def structured_to_expr(data: tuple) -> Result:
    """Reconstruct a Deferred from a structured tuple representation."""
    from ._xorq import Deferred

    @safe
    def do_convert():
        resolver = deserialize_resolver(data)
        return Deferred(resolver)

    return do_convert()


def join_predicate_to_structured(fn: Callable) -> Result[tuple, Exception]:
    """Convert a binary join predicate to a structured tuple representation.

    Binary predicates like ``lambda l, r: l.col == r.col`` are serialized by
    calling the function with two named Deferred variables (``left``, ``right``)
    and serializing the resulting resolver tree.
    """
    from ._xorq import Deferred, Variable

    @safe
    def do_convert():
        from .ops import _CallableWrapper

        raw_fn = fn._fn if isinstance(fn, _CallableWrapper) else fn
        left = Deferred(Variable("left"))
        right = Deferred(Variable("right"))
        result = raw_fn(left, right)
        if not hasattr(result, "_resolver"):
            raise ValueError(
                f"Join predicate did not produce a Deferred, got {type(result)}"
            )
        return serialize_resolver(result._resolver)

    return do_convert()


def structured_to_join_predicate(data: tuple) -> Result[Callable, Exception]:
    """Reconstruct a binary join predicate from a structured tuple representation."""
    from ._xorq import Deferred

    @safe
    def do_convert():
        resolver = deserialize_resolver(data)
        deferred = Deferred(resolver)
        return lambda left, right: deferred.resolve(left=left, right=right)

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
    "join_predicate_to_structured",
    "structured_to_join_predicate",
    "serialize_resolver",
    "deserialize_resolver",
    "read_yaml_file",
]
