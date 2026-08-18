"""The resolver-tree expression codec.

Serializes ibis ``Deferred`` resolver trees (and join predicates and
scalar literals) into hashable structured tuples compatible with xorq's
tag metadata, and reconstructs them. The callable trust boundary lives in
``._trust`` and is enforced on both directions.
"""

from __future__ import annotations

import operator
from collections.abc import Callable
from typing import Any

from returns.result import Result, safe

from ._trust import (
    _check_callable_ref,
    _load_trusted_callable,
)


def _is_ibis_literal_node(value) -> bool:
    try:
        from .._xorq import Literal

        return isinstance(value, Literal)
    except ImportError:
        return False


#: Marker for a constant that is not one of xorq's native tag scalar types.
#: Tag metadata can only hold str/int/float/bool/None (see
#: ``serialization.freeze``), so anything else — dates, ``Decimal``,
#: ``bytes`` — is carried as ``(_SCALAR_TAG, kind, payload)`` and rebuilt on
#: read. Previously these reached ``freeze()`` and were flattened with
#: ``str()``: a ``date`` predicate came back comparing against a string, and
#: a ``Decimal`` came back as a type error from the query compiler.
_SCALAR_TAG = "__bsl_scalar__"


def _encode_scalar(value: Any) -> Any:
    """Represent a constant in a form tag metadata can hold losslessly."""
    import datetime
    import decimal
    import uuid

    if isinstance(value, str | bool | int | float | type(None)):
        # numpy scalars subclass int/float; normalize so they survive as
        # native Python values rather than as repr strings.
        if type(value) is not bool and isinstance(value, int) and type(value) is not int:
            return int(value)
        if isinstance(value, float) and type(value) is not float:
            return float(value)
        return value
    # datetime before date: datetime is a date subclass.
    if isinstance(value, datetime.datetime):
        return (_SCALAR_TAG, "datetime", value.isoformat())
    if isinstance(value, datetime.date):
        return (_SCALAR_TAG, "date", value.isoformat())
    if isinstance(value, datetime.time):
        return (_SCALAR_TAG, "time", value.isoformat())
    if isinstance(value, datetime.timedelta):
        return (_SCALAR_TAG, "timedelta", repr(value.total_seconds()))
    if isinstance(value, decimal.Decimal):
        return (_SCALAR_TAG, "decimal", str(value))
    if isinstance(value, uuid.UUID):
        return (_SCALAR_TAG, "uuid", str(value))
    if isinstance(value, bytes):
        import base64

        return (_SCALAR_TAG, "bytes", base64.b64encode(value).decode("ascii"))
    if isinstance(value, list | tuple):
        kind = "list" if isinstance(value, list) else "tuple"
        return (_SCALAR_TAG, kind, tuple(_encode_scalar(item) for item in value))
    # numpy scalars that subclass nothing familiar (e.g. np.datetime64)
    if hasattr(value, "item") and type(value).__module__.startswith("numpy"):
        return _encode_scalar(value.item())
    raise ValueError(
        f"Cannot serialize constant of type {type(value).__name__} ({value!r}): "
        "tag metadata holds only scalars, dates, Decimal, UUID and bytes. "
        "Previously such values were silently stringified."
    )


def _decode_scalar(value: Any) -> Any:
    """Inverse of :func:`_encode_scalar`; untagged values pass through."""
    if not (isinstance(value, tuple | list) and len(value) == 3 and value[0] == _SCALAR_TAG):
        return value
    import datetime
    import decimal
    import uuid

    _, kind, payload = value
    match kind:
        case "datetime":
            return datetime.datetime.fromisoformat(payload)
        case "date":
            return datetime.date.fromisoformat(payload)
        case "time":
            return datetime.time.fromisoformat(payload)
        case "timedelta":
            return datetime.timedelta(seconds=float(payload))
        case "decimal":
            return decimal.Decimal(payload)
        case "uuid":
            return uuid.UUID(payload)
        case "bytes":
            import base64

            return base64.b64decode(payload.encode("ascii"))
        case "list":
            return [_decode_scalar(item) for item in payload]
        case "tuple":
            return tuple(_decode_scalar(item) for item in payload)
        case _:
            raise ValueError(f"Unknown encoded-constant kind: {kind!r}")


def serialize_resolver(resolver) -> tuple:
    """Walk a Resolver tree and produce a hashable nested-tuple representation."""
    from .._xorq import (
        Attr,
        BinaryOperator,
        Call,
        Item,
        Just,
        JustUnhashable,
        Sequence,
        UnaryOperator,
        Variable,
    )
    from .._xorq import (
        Mapping as MappingResolver,
    )

    if isinstance(resolver, Variable):
        return ("var", resolver.name)

    if isinstance(resolver, Just):
        value = resolver.value
        # ibis Literal node (e.g., from case().when(..., 1))
        if _is_ibis_literal_node(value):
            py_value = value.args[0]
            dtype_str = str(value.args[1])
            return ("ibis_literal", _encode_scalar(py_value), dtype_str)
        # callable (operator functions, deferrable functions like ifelse, _finish_searched_case)
        if callable(value):
            module = getattr(value, "__module__", None)
            qualname = getattr(value, "__qualname__", None)
            _check_callable_ref(module, qualname)
            return ("fn", module, qualname)
        # primitive value (int, float, str, bool, None) or an encodable constant
        return ("just", _encode_scalar(value))

    if isinstance(resolver, JustUnhashable):
        value = resolver.value.obj
        if _is_ibis_literal_node(value):
            py_value = value.args[0]
            dtype_str = str(value.args[1])
            return ("ibis_literal", _encode_scalar(py_value), dtype_str)
        raise ValueError(f"Cannot serialize unhashable value: {value!r}")

    if isinstance(resolver, Attr):
        return ("attr", serialize_resolver(resolver.obj), serialize_resolver(resolver.name))

    if isinstance(resolver, Item):
        return ("item", serialize_resolver(resolver.obj), serialize_resolver(resolver.name))

    if isinstance(resolver, Call):
        func_tuple = serialize_resolver(resolver.func)
        args_tuple = tuple(serialize_resolver(a) for a in resolver.args)
        kwargs_tuple = tuple((k, serialize_resolver(v)) for k, v in resolver.kwargs.items())
        return ("call", func_tuple, args_tuple, kwargs_tuple)

    if isinstance(resolver, BinaryOperator):
        op_name = resolver.func.__name__
        return (
            "binop",
            op_name,
            serialize_resolver(resolver.left),
            serialize_resolver(resolver.right),
        )

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
    from .._xorq import (
        Attr,
        BinaryOperator,
        Call,
        Item,
        Just,
        Sequence,
        UnaryOperator,
        Variable,
    )
    from .._xorq import (
        Mapping as MappingResolver,
    )

    match data:
        case ("var", name):
            return Variable(name)

        case ("just", value):
            return Just(_decode_scalar(value))

        case ("fn", module_name, qualname):
            return Just(_load_trusted_callable(module_name, qualname))

        case ("ibis_literal", py_value, dtype_str):
            from .._xorq import ibis

            lit_expr = ibis.literal(_decode_scalar(py_value), type=ibis.dtype(dtype_str))
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
            from .._xorq import FrozenDict

            kwargs_resolvers = FrozenDict({k: deserialize_resolver(v) for k, v in kwargs_data})
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
            from .._xorq import FrozenDict

            values = FrozenDict({k: deserialize_resolver(v) for k, v in items_data})
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
    from .._xorq import Deferred as XorqDeferred

    @safe
    def do_convert():
        from .._xorq import _

        # ops._CallableWrapper exposes the wrapped callable as ._fn;
        # duck-type so this bottom-layer module doesn't import ops. Guard
        # against Deferred first — getattr on a Deferred never falls back,
        # it builds a new deferred attribute access.
        expr = fn if _is_deferred(fn) else getattr(fn, "_fn", fn)
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
    from .._xorq import Deferred

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
    from .._xorq import Deferred, Variable

    @safe
    def do_convert():
        # See expr_to_structured: unwrap _CallableWrapper by duck-typing,
        # guarding against Deferred's synthetic attribute access.
        raw_fn = fn if _is_deferred(fn) else getattr(fn, "_fn", fn)
        left = Deferred(Variable("left"))
        right = Deferred(Variable("right"))
        result = raw_fn(left, right)
        if not hasattr(result, "_resolver"):
            raise ValueError(f"Join predicate did not produce a Deferred, got {type(result)}")
        return serialize_resolver(result._resolver)

    return do_convert()


def structured_to_join_predicate(data: tuple) -> Result[Callable, Exception]:
    """Reconstruct a binary join predicate from a structured tuple representation."""
    from .._xorq import Deferred

    @safe
    def do_convert():
        resolver = deserialize_resolver(data)
        deferred = Deferred(resolver)
        return lambda left, right: deferred.resolve(left=left, right=right)

    return do_convert()
