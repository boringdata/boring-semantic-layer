"""Round-trip recovery of joined models whose leaves are into_backend seams.

Regression for the pennybank failure: when a join's leaf table is an
``into_backend`` seam, leaf recovery cannot walk to a single base relation
and keeps the lowered leaf projection as the model's table. With colliding
join-key names across the joined tables, ``SemanticJoinOp.to_untagged``
renames left predicate columns to ``__bsl_jk_<name>`` temporaries inside
that projection, so the declared dimensions no longer resolved and
``from_tagged`` raised "Round-trip could not recover the left join table".

The fix inverts BSL's reserved temporaries on the recovered leaf
(``_strip_internal_join_temps``), restoring the schema the model was
authored against.
"""

from __future__ import annotations

import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.serialization import from_tagged, to_tagged

xorq = pytest.importorskip("xorq", reason="xorq not installed")

import pandas as pd  # noqa: E402
import xorq.api as xo  # noqa: E402


@pytest.fixture
def seamed_tables():
    """Three collision-heavy tables, each an into_backend seam.

    The seam matters: a leaf whose expression holds more than one relation
    (RemoteTable placeholder + in-memory payload) cannot be recovered by
    walking to a single base table, so recovery keeps the lowered leaf
    projection — the one carrying the ``__bsl_jk_`` temporaries.
    """
    frames = {
        "accounts": pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "customer_id": [10, 10, 20],
                "credit_limit": [1000.0, 2000.0, 500.0],
            }
        ),
        "customers": pd.DataFrame({"customer_id": [10, 20], "state": ["CA", "NY"]}),
        "transactions": pd.DataFrame(
            {
                "transaction_id": [1, 2, 3, 4],
                "account_id": [1, 1, 2, 3],
                "amount": [5.0, 6.0, 7.0, 8.0],
            }
        ),
    }
    con = xo.connect()
    return {
        name: xo.memtable(df, name=name).into_backend(con, f"{name}_rt")
        for name, df in frames.items()
    }


def _build_model(tables):
    accounts = (
        to_semantic_table(tables["accounts"], name="accounts")
        .with_dimensions(
            account_id=lambda t: t.account_id,
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(account_count=lambda t: t.count())
    )
    customers = to_semantic_table(tables["customers"], name="customers").with_dimensions(
        customer_id=lambda t: t.customer_id,
        state=lambda t: t.state,
    )
    transactions = to_semantic_table(tables["transactions"], name="transactions").with_measures(
        total_amount=lambda t: t.amount.sum(),
    )
    return accounts.join_one(customers, on=lambda a, c: a.customer_id == c.customer_id).join_many(
        transactions, on=lambda a, t: a.account_id == t.account_id
    )


def test_seamed_collision_join_round_trips(seamed_tables):
    tagged = to_tagged(_build_model(seamed_tables))

    recovered = from_tagged(tagged)

    dims = recovered.get_dimensions()
    assert "accounts.customer_id" in dims
    assert "customers.customer_id" in dims
    meas = recovered.get_measures()
    assert "accounts.account_count" in meas
    assert "transactions.total_amount" in meas


def test_seamed_collision_join_lowers_and_executes(seamed_tables):
    """The recovered model must lower against ORIGINAL column names.

    Execution through in-process ``into_backend`` seams only supports a
    bounded number of reads per seam, so this asserts one single-leg query
    (which fits the budget) and plan-lowering for a cross-leg query.
    """
    tagged = to_tagged(_build_model(seamed_tables))
    recovered = from_tagged(tagged)

    cross_leg = recovered.group_by("customers.state").aggregate(
        "accounts.account_count", "transactions.total_amount"
    )
    assert cross_leg.to_untagged() is not None

    result = recovered.aggregate("accounts.account_count").to_untagged().execute()
    assert list(result["accounts.account_count"]) == [3]


def test_recovered_leaf_keeps_user_temp_lookalike_column():
    """A user column literally named __bsl_jk_x must not be renamed away."""
    con = xo.connect()
    df = pd.DataFrame({"__bsl_jk_x": [1, 2], "x": [3, 4], "y": [5.0, 6.0]})
    weird = xo.memtable(df, name="weird").into_backend(con, "weird_rt")

    model = to_semantic_table(weird, name="weird").with_dimensions(
        jk=lambda t: t["__bsl_jk_x"],
        x=lambda t: t.x,
    )
    recovered = from_tagged(to_tagged(model))
    assert set(recovered.get_dimensions()) == {"jk", "x"}
