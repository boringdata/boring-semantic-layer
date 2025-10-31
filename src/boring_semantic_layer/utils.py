"""Utility types and functions: Result and Option monads."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from attrs import field, frozen

logger = logging.getLogger(__name__)

T = TypeVar("T")
E = TypeVar("E")
A = TypeVar("A")
B = TypeVar("B")


@frozen
class ValidationError:
    """Structured error for validation failures."""

    message: str
    table_name: str | None = None
    column_names: frozenset[str] = field(factory=frozenset, converter=frozenset)

    def __str__(self) -> str:
        parts = [self.message]
        if self.table_name:
            parts.append(f"table={self.table_name}")
        if self.column_names:
            cols = ", ".join(sorted(self.column_names))
            parts.append(f"columns={{{cols}}}")
        return " | ".join(parts)


@frozen
class Success(Generic[T]):
    """Successful result containing a value."""

    value: T

    def map(self, fn: Callable[[T], A]) -> Result[A, Any]:
        try:
            return Success(fn(self.value))
        except Exception as e:
            return Failure(e)

    def flatmap(self, fn: Callable[[T], Result[A, E]]) -> Result[A, E]:
        try:
            return fn(self.value)
        except Exception as e:
            return Failure(e)

    def map_error(self, fn: Callable[[Any], E]) -> Result[T, E]:
        return self

    def unwrap_or(self, default: T) -> T:
        return self.value

    def unwrap_or_else(self, fn: Callable[[], T]) -> T:
        return self.value

    def is_success(self) -> bool:
        return True

    def is_failure(self) -> bool:
        return False


@frozen
class Failure(Generic[E]):
    """Failed result containing an error."""

    error: E

    def map(self, fn: Callable[[Any], A]) -> Result[A, E]:
        return self

    def flatmap(self, fn: Callable[[Any], Result[A, E]]) -> Result[A, E]:
        return self

    def map_error(self, fn: Callable[[E], Any]) -> Result[Any, Any]:
        try:
            return Failure(fn(self.error))
        except Exception as e:
            return Failure(e)

    def unwrap_or(self, default: T) -> T:
        return default

    def unwrap_or_else(self, fn: Callable[[], T]) -> T:
        return fn()

    def is_success(self) -> bool:
        return False

    def is_failure(self) -> bool:
        return True


Result = Success[T] | Failure[E]


def success(value: T) -> Success[T]:
    return Success(value)


def failure(error: E) -> Failure[E]:
    return Failure(error)


def try_result(fn: Callable[[], T]) -> Result[T, Exception]:
    try:
        return Success(fn())
    except Exception as e:
        return Failure(e)


@frozen
class Some(Generic[T]):
    """Optional value that is present."""

    value: T

    def map(self, fn: Callable[[T], A]) -> Option[A]:
        try:
            result = fn(self.value)
            return Some(result) if result is not None else Nothing()
        except Exception as e:
            logger.debug(f"Option.map failed: {e!r}")
            return Nothing()

    def flatmap(self, fn: Callable[[T], Option[A]]) -> Option[A]:
        try:
            return fn(self.value)
        except Exception as e:
            logger.debug(f"Option.flatmap failed: {e!r}")
            return Nothing()

    def filter(self, predicate: Callable[[T], bool]) -> Option[T]:
        try:
            return self if predicate(self.value) else Nothing()
        except Exception as e:
            logger.debug(f"Option.filter failed: {e!r}")
            return Nothing()

    def unwrap_or(self, default: T) -> T:
        return self.value

    def unwrap_or_else(self, fn: Callable[[], T]) -> T:
        return self.value

    def is_some(self) -> bool:
        return True

    def is_nothing(self) -> bool:
        return False


class Nothing:
    """Optional value that is absent (singleton)."""

    _instance: Nothing | None = None

    def __new__(cls) -> Nothing:
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    def map(self, fn: Callable[[Any], A]) -> Nothing:
        return self

    def flatmap(self, fn: Callable[[Any], Option[A]]) -> Nothing:
        return self

    def filter(self, predicate: Callable[[Any], bool]) -> Nothing:
        return self

    def unwrap_or(self, default: T) -> T:
        return default

    def unwrap_or_else(self, fn: Callable[[], T]) -> T:
        return fn()

    def is_some(self) -> bool:
        return False

    def is_nothing(self) -> bool:
        return True


Option = Some[T] | Nothing

_NOTHING_SINGLETON = Nothing()


def some(value: T) -> Some[T]:
    return Some(value)


def nothing() -> Nothing:
    return _NOTHING_SINGLETON


def option_of(value: T | None) -> Option[T]:
    return Some(value) if value is not None else Nothing()
