from attrs import frozen
from typing import Callable, Optional, TYPE_CHECKING, Literal

try:
    import xorq.vendor.ibis as ibis_mod
    IS_XORQ_USED = True
except ImportError:
    import ibis as ibis_mod
    IS_XORQ_USED = False

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._

How = Literal["inner", "left", "cross"]
Cardinality = Literal["one", "many", "cross"]

if TYPE_CHECKING:
    from .semantic_model import SemanticModel

@frozen(kw_only=True, slots=True)
class Join:
    """Definition of a join relationship in the semantic model."""

    alias: str
    model: "SemanticModel"
    on: Callable[[Expr, Expr], Expr]
    how: How = "inner"
    kind: Cardinality = "one"

    @classmethod
    def one(
        cls,
        alias: str,
        model: "SemanticModel",
        with_: Optional[Callable[[Expr], Expr]] = None,
    ) -> "Join":
        if with_ is None:
            raise ValueError(
                "Join.one requires a 'with_' callable for foreign key mapping"
            )
        if not callable(with_):
            raise TypeError(
                "'with_' must be a callable mapping the left table to a column expression"
            )
        if not model.primary_key:
            raise ValueError(
                f"Model does not have 'primary_key' defined for join: {alias}"
            )

        def on_expr(left, right):
            return with_(left) == getattr(right, model.primary_key)

        return cls(alias=alias, model=model, on=on_expr, how="inner", kind="one")

    @classmethod
    def many(
        cls,
        alias: str,
        model: "SemanticModel",
        with_: Optional[Callable[[Expr], Expr]] = None,
    ) -> "Join":
        if with_ is None:
            raise ValueError(
                "Join.many requires a 'with_' callable for foreign key mapping"
            )
        if not callable(with_):
            raise TypeError(
                "'with_' must be a callable mapping the left table to a column expression"
            )
        if not model.primary_key:
            raise ValueError(
                f"Model does not have 'primary_key' defined for join: {alias}"
            )

        def on_expr(left, right):
            return with_(left) == getattr(right, model.primary_key)

        return cls(alias=alias, model=model, on=on_expr, how="left", kind="many")

    @classmethod
    def cross(
        cls,
        alias: str,
        model: "SemanticModel",
    ) -> "Join":
        return cls(
            alias=alias,
            model=model,
            on=lambda left, right: None,
            how="cross",
            kind="cross",
        )