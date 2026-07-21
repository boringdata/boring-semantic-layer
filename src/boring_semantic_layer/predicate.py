"""Internal Predicate AST for filter expressions.

A small algebra over filters that gives every supported operator a single
canonical representation. Replaces the ad-hoc walk-the-dict pattern that
existed in three places:

- ``query.Filter._parse_json_filter`` (pre-aggregation, attribute access)
- ``query._build_post_agg_predicate`` (post-aggregation, bracket access)
- ``query._extract_filter_fields`` (collect referenced fields)

JSON filter specs (and string specs, eventually) parse into ``Predicate``;
the same compiler turns ``Predicate`` into an ibis expression for either
pre- or post-aggregation tables. ``SemanticFilterOp`` continues to accept
opaque callables — reflecting callables into ``Predicate`` is a later
step.
"""

from __future__ import annotations

import datetime
from collections.abc import Callable, Iterable, Mapping
from typing import Any, ClassVar, Literal

import ibis
from attrs import field, frozen

_COMPARE_OPS: dict[str, Callable[[Any, Any], Any]] = {
    "eq": lambda x, y: x == y,
    "ne": lambda x, y: x != y,
    "lt": lambda x, y: x < y,
    "le": lambda x, y: x <= y,
    "gt": lambda x, y: x > y,
    "ge": lambda x, y: x >= y,
    "like": lambda x, y: x.like(y),
    "not_like": lambda x, y: ~x.like(y),
    "ilike": lambda x, y: x.ilike(y),
    "not_ilike": lambda x, y: ~x.ilike(y),
}

# JSON filter operator strings that map to a Compare node. Includes
# legacy aliases (``=``, ``equals``) accepted by the existing parser.
_DICT_COMPARE_OPS: dict[str, str] = {
    "=": "eq",
    "eq": "eq",
    "equals": "eq",
    "!=": "ne",
    ">": "gt",
    ">=": "ge",
    "<": "lt",
    "<=": "le",
    "like": "like",
    "not like": "not_like",
    "ilike": "ilike",
    "not ilike": "not_ilike",
}


@frozen
class Compare:
    """Two-arg comparison: field <op> value."""

    op: Literal["eq", "ne", "lt", "le", "gt", "ge", "like", "not_like", "ilike", "not_ilike"]
    field: str
    value: Any

    def fields(self) -> set[str]:
        return {self.field}


@frozen
class In:
    """Membership test: ``field in values`` (or ``not in`` when negated)."""

    field: str
    values: tuple = field(converter=tuple)
    negate: bool = False

    def fields(self) -> set[str]:
        return {self.field}


@frozen
class IsNull:
    """Null check (or not-null when negated)."""

    field: str
    negate: bool = False

    def fields(self) -> set[str]:
        return {self.field}


@frozen
class And:
    """Conjunction of one or more predicates."""

    children: tuple = field(converter=tuple)

    def fields(self) -> set[str]:
        return set().union(*(c.fields() for c in self.children))


@frozen
class Or:
    """Disjunction of one or more predicates."""

    children: tuple = field(converter=tuple)

    def fields(self) -> set[str]:
        return set().union(*(c.fields() for c in self.children))


@frozen
class Not:
    """Negation."""

    predicate: Any

    def fields(self) -> set[str]:
        return self.predicate.fields()


@frozen
class Custom:
    """Escape hatch for callables that can't be reflected into the AST."""

    fn: Callable

    def fields(self) -> set[str]:
        return set()


Predicate = Compare | In | IsNull | And | Or | Not | Custom


_COMPOUND_OPS: ClassVar = frozenset({"AND", "OR"})


def from_dict(spec: dict[str, Any]) -> Predicate:
    """Parse a JSON-style filter spec into a ``Predicate``.

    Mirrors the schema that ``query.Filter`` accepts. Unknown operators
    raise ``ValueError`` rather than falling through silently.
    """
    if not isinstance(spec, dict):
        raise ValueError(f"Filter spec must be a dict, got {type(spec).__name__}")

    op = spec.get("operator")
    if op is None:
        raise KeyError(
            "Missing required keys in filter: 'field' and 'operator' are required"
        )

    if op == "AND":
        return And(children=tuple(from_dict(c) for c in _require_conditions(spec, op)))
    if op == "OR":
        return Or(children=tuple(from_dict(c) for c in _require_conditions(spec, op)))

    field_name = spec.get("field")
    if field_name is None:
        raise KeyError(
            "Missing required keys in filter: 'field' and 'operator' are required"
        )

    if op == "is null":
        _reject_value_keys(spec, op)
        return IsNull(field=field_name, negate=False)
    if op == "is not null":
        _reject_value_keys(spec, op)
        return IsNull(field=field_name, negate=True)

    if op == "in":
        return In(field=field_name, values=_require_values(spec, op), negate=False)
    if op == "not in":
        return In(field=field_name, values=_require_values(spec, op), negate=True)

    canonical = _DICT_COMPARE_OPS.get(op)
    if canonical is None:
        raise ValueError(f"Unsupported operator: {op}")

    if "value" not in spec:
        raise ValueError(f"Operator {op!r} requires 'value' field")
    return Compare(op=canonical, field=field_name, value=spec["value"])


def _require_conditions(spec: dict, op: str) -> Iterable[dict]:
    conditions = spec.get("conditions")
    if not conditions:
        raise ValueError(f"Compound filter {op!r} must have non-empty 'conditions' list")
    return conditions


def _require_values(spec: dict, op: str) -> tuple:
    values = spec.get("values")
    if values is None:
        raise ValueError(f"Operator {op!r} requires 'values' field")
    if isinstance(values, (str, bytes)):
        raise ValueError(
            f"Operator {op!r} requires a list of values, got the string "
            f"{values!r}; iterating it would match individual characters. "
            f"Use 'values': [{values!r}] instead"
        )
    return tuple(values)


def _reject_value_keys(spec: dict, op: str) -> None:
    if any(k in spec for k in ("value", "values")):
        raise ValueError(f"Operator {op!r} should not have 'value' or 'values' fields")


def _convert_literal(value: Any, ibis_module) -> Any:
    """Convert complete ISO date/timestamp strings to typed ibis literals.

    Backends like Athena require typed date literals or fail with
    TYPE_MISMATCH. Only complete ISO dates/datetimes are coerced: ibis's
    lenient parser fills fields missing from partial strings like "2024"
    or "12:30" with *today's* date, so coercing them would make results
    depend on the day the query runs. Other strings pass through
    unchanged.
    """
    if not isinstance(value, str) or not _is_complete_iso_datetime(value):
        return value
    for dtype in ("timestamp", "date"):
        try:
            return ibis_module.literal(value, type=dtype)
        except (ValueError, TypeError):
            pass
    return value


def _is_complete_iso_datetime(value: str) -> bool:
    for parse in (datetime.date.fromisoformat, datetime.datetime.fromisoformat):
        try:
            parse(value)
        except ValueError:
            continue
        return True
    return False


def _field_accessor(
    table,
    name: str,
    *,
    post_agg: bool,
    strict_qualified: bool = False,
):
    """Resolve a field name on the table.

    Pre-aggregation: resolve a qualified name exactly through bracket access.
    Semantic resolver proxies understand names such as ``items.status`` and
    bind them to the owning model's dimension, including any physical rename
    caused by a join collision.  Stripping the prefix first is unsound: when
    both inputs contain ``status`` it silently binds to the left input.

    The unprefixed fallback preserves the convenience spelling
    ``standalone_model.field`` when a standalone model exposes only bare
    semantic names.  Joined/scoped resolvers resolve a declared qualified
    dimension in the exact lookup and therefore never take that fallback.

    Post-aggregation: use bracket access to preserve dotted names like
    ``orders.total_amount`` that survive into the aggregated table.
    """
    if post_agg:
        return table[name]
    if "." in name:
        declared_dimensions = getattr(table, "_dims", {})
        if isinstance(declared_dimensions, Mapping) and name in declared_dimensions:
            # Do not mask an error raised while evaluating an explicitly
            # qualified dimension by falling back to an unrelated bare
            # column with the same suffix.
            return table[name]
        try:
            return table[name]
        except (AttributeError, KeyError, TypeError) as exc:
            if strict_qualified:
                raise KeyError(
                    f"Qualified filter field {name!r} did not resolve exactly"
                ) from exc
            _prefix, unprefixed = name.split(".", 1)
            return getattr(table, unprefixed)
    return getattr(table, name)


def compile(  # noqa: A001
    pred: Predicate,
    table,
    *,
    post_agg: bool = False,
    ibis_module=ibis,
    strict_qualified: bool = False,
) -> Any:
    """Compile *pred* into an ibis expression against *table*.

    *table* may be a Deferred (``ibis._``), a semantic resolver proxy, or an
    actual relation. ``ibis_module`` controls the flavor of literal
    construction (plain ibis vs xorq vendored).
    """
    if isinstance(pred, And):
        compiled = [
            compile(
                c,
                table,
                post_agg=post_agg,
                ibis_module=ibis_module,
                strict_qualified=strict_qualified,
            )
            for c in pred.children
        ]
        result = compiled[0]
        for c in compiled[1:]:
            result = result & c
        return result
    if isinstance(pred, Or):
        compiled = [
            compile(
                c,
                table,
                post_agg=post_agg,
                ibis_module=ibis_module,
                strict_qualified=strict_qualified,
            )
            for c in pred.children
        ]
        result = compiled[0]
        for c in compiled[1:]:
            result = result | c
        return result
    if isinstance(pred, Not):
        return ~compile(
            pred.predicate,
            table,
            post_agg=post_agg,
            ibis_module=ibis_module,
            strict_qualified=strict_qualified,
        )
    if isinstance(pred, IsNull):
        col = _field_accessor(
            table,
            pred.field,
            post_agg=post_agg,
            strict_qualified=strict_qualified,
        )
        return col.notnull() if pred.negate else col.isnull()
    if isinstance(pred, In):
        col = _field_accessor(
            table,
            pred.field,
            post_agg=post_agg,
            strict_qualified=strict_qualified,
        )
        values = [_convert_literal(v, ibis_module) for v in pred.values]
        return col.notin(values) if pred.negate else col.isin(values)
    if isinstance(pred, Compare):
        col = _field_accessor(
            table,
            pred.field,
            post_agg=post_agg,
            strict_qualified=strict_qualified,
        )
        value = _convert_literal(pred.value, ibis_module)
        return _COMPARE_OPS[pred.op](col, value)
    if isinstance(pred, Custom):
        return pred.fn(table)
    raise TypeError(f"Unknown predicate node: {type(pred).__name__}")


def fields(pred: Predicate) -> set[str]:
    """Return the set of field names referenced by *pred*."""
    return pred.fields()
