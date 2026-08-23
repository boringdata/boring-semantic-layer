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


def _seam_free_tables():
    """Shaped deferred views over ONE backend (no seams): the star-schema-
    first doctrine — derivations authored in bare xorq, thin BSL on top."""
    con = xo.connect()
    raw_accounts = con.register(
        pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "customer_id": [10, 10, 20],
                "close_date": [None, "2025-01-01", None],
            }
        ),
        "raw_accounts",
    )
    raw_txn = con.register(
        pd.DataFrame(
            {
                "transaction_id": [1, 2, 3, 4],
                "account_id": [1, 1, 2, 3],
                "amount": [-5.0, -6.0, 3.0, -8.0],
                "transaction_type": ["Purchase", "Purchase", "Purchase Return", "ACH Payment"],
            }
        ),
        "raw_transactions",
    )
    accounts_view = raw_accounts.mutate(is_open=xo._.close_date.isnull())
    txn_view = raw_txn.mutate(
        purchase_amount=(xo._.transaction_type == "Purchase").ifelse(-xo._.amount, 0.0),
    )
    return accounts_view, txn_view


def test_shaped_single_table_round_trips():
    """A model over a deferred shaped view must recover WITH its shaping.

    Regression: _reconstruct_table walked to the bare base relation,
    discarding authored mutates — `t.is_open.sum()` then failed with
    "'Table' object has no attribute 'is_open'" wrapped in the round-trip
    error, breaking the star-schema-view doctrine at the first step.
    """
    accounts_view, _ = _seam_free_tables()
    model = to_semantic_table(accounts_view, name="accounts").with_measures(
        open_account_count=lambda t: t.is_open.sum(),
    )
    recovered = from_tagged(to_tagged(model))
    result = recovered.aggregate("open_account_count").to_untagged().execute()
    assert list(result["open_account_count"]) == [2]


def test_shaped_join_leaves_round_trip():
    accounts_view, txn_view = _seam_free_tables()
    accounts = to_semantic_table(accounts_view, name="accounts").with_dimensions(
        account_id=lambda t: t.account_id,
        is_open=lambda t: t.is_open,
    )
    transactions = to_semantic_table(txn_view, name="transactions").with_measures(
        gross_purchases=lambda t: t.purchase_amount.sum(),
    )
    model = accounts.join_many(transactions, on=lambda a, t: a.account_id == t.account_id)

    recovered = from_tagged(to_tagged(model))
    result = (
        recovered.group_by("accounts.is_open")
        .aggregate("transactions.gross_purchases")
        .to_untagged()
        .execute()
        .sort_values("accounts.is_open")
        .reset_index(drop=True)
    )
    # closed account 2: purchase-return row only → 0.0; open accounts 1+3: 5+6+0=11.0
    assert list(result["transactions.gross_purchases"]) == [0.0, 11.0]


@pytest.mark.xfail(
    strict=True,
    reason="Known gap: a lowered QUERY entry over a shaped view still base-walks "
    "— BSL's query-time injected mutates and authored shaping are "
    "indistinguishable in the lowered tree. Needs a lowering-time base-boundary "
    "marker. MODEL entries (what the star-schema doctrine catalogs) are covered "
    "by the tests above.",
)
def test_query_entry_still_digs_under_the_aggregate():
    """Recovery of a tagged aggregate over a SHAPED view should replay the
    query against the shaped base — today the base walk discards the shaping."""
    accounts_view, _ = _seam_free_tables()
    model = (
        to_semantic_table(accounts_view, name="accounts")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(open_account_count=lambda t: t.is_open.sum())
    )
    tagged_query = to_tagged(model.group_by("customer_id").aggregate("open_account_count"))
    recovered = from_tagged(tagged_query)
    # reconstructed chain replays the query over the recovered base
    result = recovered.to_untagged().execute().sort_values("customer_id").reset_index(drop=True)
    assert list(result["open_account_count"]) == [1, 1]


def test_guard_message_names_the_underlying_error():
    """A field using a nonexistent API must surface the REAL exception, not
    only the canned pre-aggregation explanation (the pennybank Column.filter
    misdiagnosis)."""
    accounts_view, txn_view = _seam_free_tables()
    accounts = to_semantic_table(accounts_view, name="accounts").with_measures(
        bad=lambda t: t.account_id.filter(t.is_open).nunique(),
    )
    transactions = to_semantic_table(txn_view, name="transactions").with_measures(
        gross_purchases=lambda t: t.purchase_amount.sum(),
    )
    model = accounts.join_many(transactions, on=lambda a, t: a.account_id == t.account_id)
    with pytest.raises(ValueError, match="AttributeError.*filter"):
        from_tagged(to_tagged(model))


# ---------------------------------------------------------------------------
# Tagging an already-aggregated join query (not just the bare model)
# ---------------------------------------------------------------------------
#
# ``to_tagged()`` explicitly supports tagging a ``SemanticAggregateOp``
# directly (see its ``aggregate_cache_storage`` parameter, for smart-cube
# caching), which tags the FULLY pre-agg-compiled query rather than a bare
# model. Leaf recovery then has to isolate each join leg from that compiled
# tree instead of the original (unaggregated) join chain.


@pytest.fixture
def plain_tables():
    return {
        "accounts": pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "customer_id": [10, 10, 20],
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


def _build_plain_model(tables):
    accounts = (
        to_semantic_table(xo.memtable(tables["accounts"], name="accounts"), name="accounts")
        .with_dimensions(
            account_id=lambda t: t.account_id,
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(account_count=lambda t: t.count())
    )
    customers = to_semantic_table(
        xo.memtable(tables["customers"], name="customers"), name="customers"
    ).with_dimensions(
        customer_id=lambda t: t.customer_id,
        state=lambda t: t.state,
    )
    transactions = to_semantic_table(
        xo.memtable(tables["transactions"], name="transactions"), name="transactions"
    ).with_measures(total_amount=lambda t: t.amount.sum())
    return accounts, customers, transactions


def test_join_one_aggregate_query_round_trips(plain_tables):
    """A plain two-table join_one (no fan-out) survives tagging the query itself."""
    accounts, customers, _ = _build_plain_model(plain_tables)
    joined = accounts.join_one(customers, on=lambda a, c: a.customer_id == c.customer_id)
    query = joined.group_by("customers.state").aggregate("accounts.account_count")

    direct = query.to_untagged().execute().sort_values("customers.state").reset_index(drop=True)
    recovered = (
        from_tagged(to_tagged(query))
        .to_untagged()
        .execute()
        .sort_values("customers.state")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(direct, recovered)


def test_join_many_aggregate_query_fails_loud_not_silently_wrong(plain_tables):
    """A fan-out leg under an aggregated, tagged query cannot be recovered today.

    Pre-agg compilation for ``join_many`` rewrites the join into a decomposed
    tree of partial-aggregate/key-bridge joins with no single ``JoinChain``
    corresponding to the original leaves, so ``_split_join_expr`` cannot
    isolate each leg from the fully-compiled, tagged expression.
    ``_validate_join_leaf`` catches the resulting mismatch and raises —
    this pins that the failure stays LOUD (a clear, actionable error) rather
    than regressing into silently wrong numbers. Tagging the un-aggregated
    join, or the bare model, is unaffected (see the other tests in this
    file) and is the supported workaround.
    """
    accounts, customers, transactions = _build_plain_model(plain_tables)
    joined = accounts.join_one(customers, on=lambda a, c: a.customer_id == c.customer_id).join_many(
        transactions, on=lambda a, t: a.account_id == t.account_id
    )
    query = joined.group_by("customers.state").aggregate("transactions.total_amount")

    assert query.to_untagged().execute() is not None  # the direct query itself is fine

    with pytest.raises(ValueError, match="Round-trip could not recover"):
        from_tagged(to_tagged(query))
