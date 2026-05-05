"""Hashable callable wrapper used to put lambdas/Deferred into FrozenDict.

Both raw callables and user-side ``Deferred`` instances are not hashable
in their bare form, but ibis Op classes store dimension/measure exprs in
``FrozenDict``. ``_CallableWrapper`` gives them an identity-based hash so
they can be persisted in op fields.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from attrs import frozen


@frozen
class _CallableWrapper:
    """Hashable wrapper for Callable and Deferred.

    Both raw callables (lambda) and user Deferred (_.foo) are not hashable
    and cannot be stored in FrozenDict. This wrapper provides hashability
    using identity-based hashing.
    """

    _fn: Any

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def __hash__(self):
        # should this be dask.base.tokenize()?
        return hash(id(self._fn))

    @property
    def unwrap(self):
        return self._fn


def _ensure_wrapped(fn: Any) -> _CallableWrapper:
    """Wrap Callable or Deferred for hashability."""
    return fn if isinstance(fn, _CallableWrapper) else _CallableWrapper(fn)


def _infer_unnest(fn: Callable, table: Any) -> tuple[str, ...]:
    """Infer required unnest operations from the table.

    Examples:
        to_semantic_table(tbl).with_measures(...) -> ()  # Session level
        to_semantic_table(tbl).unnest("hits").with_measures(...) -> ("hits",)
        unnested.unnest("product").with_measures(...) -> ("product",)
    """
    from ..expr import SemanticUnnest

    if isinstance(table, SemanticUnnest):
        op = table.op()
        # SemanticUnnestOp always has column attribute
        return (op.column,)

    return ()
