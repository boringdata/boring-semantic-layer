"""Trust boundary for callables referenced by serialized payloads.

A ``("fn", module, qualname)`` payload arm may only resolve inside the
allowlisted module roots; both the writer and the reader enforce this so
no attacker-chosen module is ever imported.
"""

from __future__ import annotations

import importlib


class UntrustedCallableError(ValueError):
    """A serialized expression names a callable outside the trusted set.

    Serialized models are data, not code: a tag payload can travel through
    a xorq catalog, a git repo or any other artifact store, and is not
    necessarily written by whoever reads it. Restoring an arbitrary
    ``(module, qualname)`` pair means importing an attacker-chosen module
    and handing the result to ``Call.resolve()``, which calls it — i.e.
    arbitrary code execution. Only functions from the expression libraries
    BSL builds on can be restored.
    """


#: Module roots whose callables may be named in a serialized expression.
#: These are the libraries that actually appear in ibis resolver trees:
#: deferrable API functions (``ifelse``, ``coalesce``, ``_finish_searched_case``)
#: and the ``operator`` functions behind binary/unary nodes.
_TRUSTED_CALLABLE_ROOTS: frozenset[str] = frozenset(
    {
        "ibis",
        "xorq",
        "operator",
        "_operator",
        "boring_semantic_layer",
    }
)

_EXTRA_TRUSTED_CALLABLE_ROOTS: set[str] = set()


def trust_callable_module(root: str) -> None:
    """Allow callables from an additional top-level module in serialized models.

    Only do this for modules you control, and only when every model you
    deserialize comes from a source you trust as much as your own code:
    a serialized expression naming a callable is equivalent to a function
    call, so widening this set widens what a malicious payload can invoke.
    """
    _EXTRA_TRUSTED_CALLABLE_ROOTS.add(root.split(".", 1)[0])


def _trusted_roots() -> frozenset[str]:
    return _TRUSTED_CALLABLE_ROOTS | frozenset(_EXTRA_TRUSTED_CALLABLE_ROOTS)


def _module_root(module_name: str | None) -> str:
    return (module_name or "").split(".", 1)[0]


def _check_callable_ref(module_name: str | None, qualname: str | None) -> None:
    """Reject a ``(module, qualname)`` pair that must not cross the wire.

    Applied on *both* sides: serialization refuses to emit a reference that
    deserialization would refuse to load, so the failure surfaces where the
    model is authored rather than in someone else's process.
    """
    if not module_name or not qualname:
        raise UntrustedCallableError(
            f"Callable reference is incomplete: module={module_name!r} qualname={qualname!r}"
        )
    root = _module_root(module_name)
    if root not in _trusted_roots():
        raise UntrustedCallableError(
            f"Refusing to (de)serialize callable {module_name}.{qualname}: "
            f"module root {root!r} is not trusted. Serialized expressions may "
            f"only reference {sorted(_trusted_roots())}. Express the logic with "
            "ibis operations, or call "
            "boring_semantic_layer.serialization.trust_callable_module() if you own "
            "the module and trust every model you load."
        )
    for part in qualname.split("."):
        if part.startswith("__") or "<" in part or not part.isidentifier():
            raise UntrustedCallableError(
                f"Refusing to (de)serialize callable {module_name}.{qualname}: "
                f"qualname component {part!r} is not a plain public identifier "
                "(lambdas, closures and dunder attributes cannot be restored)."
            )


def _resolve_qualname(module_obj, qualname: str):
    """Resolve a dotted qualname like 'ClassName.method' on a module."""
    parts = qualname.split(".")
    obj = module_obj
    for part in parts:
        if part == "<lambda>":
            raise ValueError(f"Cannot resolve lambda qualname: {qualname}")
        obj = getattr(obj, part)
    return obj


def _load_trusted_callable(module_name: str, qualname: str):
    """Import and return a callable named by a serialized expression.

    The pair is validated before the import — an unimportable module is a
    side effect in itself, so an untrusted name must never reach
    ``import_module``. After resolution the *result* is checked too: a
    qualname is a ``getattr`` chain, so ``("fn", "ibis", "os.system")``
    would otherwise walk out of a trusted module into an untrusted one.
    """
    _check_callable_ref(module_name, qualname)
    mod = importlib.import_module(module_name)
    func = _resolve_qualname(mod, qualname)
    if not callable(func):
        raise UntrustedCallableError(
            f"{module_name}.{qualname} resolved to a non-callable "
            f"{type(func).__name__}; refusing to use it as an expression function."
        )
    origin = getattr(func, "__module__", None)
    if _module_root(origin) not in _trusted_roots():
        raise UntrustedCallableError(
            f"{module_name}.{qualname} resolves to an object defined in "
            f"{origin!r}, which is outside the trusted module set. This is how "
            "an attribute chain escapes a trusted module — refusing to load it."
        )
    return func
