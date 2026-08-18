"""Typed exception hierarchy for boring-semantic-layer.

Every error BSL raises on bad user input derives from :class:`BSLError`,
so callers can catch one family instead of guessing between bare
``ValueError``/``KeyError``s. Each subclass also inherits the stdlib
exception it historically replaced, so existing ``except ValueError:``
call sites (and tests) keep working during the migration.

Bottom layer: this module imports nothing from the package.
"""

from __future__ import annotations

from difflib import get_close_matches
from typing import Any


class BSLError(Exception):
    """Base class for all boring-semantic-layer errors."""


class DefinitionError(BSLError, ValueError):
    """A model definition is invalid (dimension/measure/join/YAML config)."""


class QueryError(BSLError, ValueError):
    """A query is invalid against an otherwise valid model."""


class UnknownFieldError(QueryError, KeyError):
    """A query referenced a dimension, measure, or column that doesn't exist.

    Also a ``KeyError`` because unknown-name lookups historically raised
    ``KeyError``.
    """

    def __str__(self) -> str:  # KeyError.__str__ repr()s its arg; undo that.
        return Exception.__str__(self)


class CompilationError(BSLError):
    """The compiler could not lower a semantically valid query."""


class SerializationError(BSLError, ValueError):
    """A payload could not be serialized or deserialized."""


class BackendError(BSLError, RuntimeError):
    """A backend interaction (conversion, rebinding, execution) failed."""


def suggest(name: str, candidates: Any, *, n: int = 3, cutoff: float = 0.6) -> list[str]:
    """Return up to *n* close matches for *name* among *candidates*.

    The single typo-suggestion implementation — error messages across the
    package should build their "did you mean" hints from this.
    """
    return get_close_matches(name, sorted(candidates), n=n, cutoff=cutoff)


def format_suggestions(name: str, candidates: Any) -> str:
    """Return a ``" Did you mean: a, b?"`` suffix, or ``""`` if no match."""
    matches = suggest(name, candidates)
    if not matches:
        return ""
    return f" Did you mean: {', '.join(matches)}?"


def suggest_kinded(
    name: str,
    kinded_candidates: list[tuple[str, Any]],
    *,
    n: int = 3,
    cutoff: float = 0.80,
) -> str | None:
    """Build a "Did you mean: measure 'x', column 'y'?" hint, or ``None``.

    *kinded_candidates* pairs a kind label with its candidate names, so the
    hint can say what kind of thing each match is.
    """
    matches: list[tuple[str, str]] = []
    for kind, candidates in kinded_candidates:
        for match in get_close_matches(name, list(candidates), n=n, cutoff=cutoff):
            matches.append((kind, match))
    if not matches:
        return None
    formatted = ", ".join(f"{kind} {match!r}" for kind, match in matches)
    return f"Did you mean: {formatted}?"


def unwrap_or_raise(result: Any, *, context: str, error: type[BSLError] = DefinitionError) -> Any:
    """Unwrap a ``returns.Result``, re-raising failures with real context.

    ``Result.unwrap()`` raises a contentless ``UnwrapFailedError``; this
    helper surfaces the original exception's message plus *context* (which
    should name the model/field/expression being processed) and chains the
    original exception as ``__cause__``.
    """
    from returns.result import Success

    if isinstance(result, Success):
        return result.unwrap()
    cause = result.failure()
    raise error(f"{context}: {cause}") from cause
