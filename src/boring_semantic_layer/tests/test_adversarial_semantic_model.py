"""An intentionally hostile, end-to-end semantic model.

This is not a collection of isolated feature tests.  It combines the failure
modes that are most likely to produce believable, incorrect BI results:

* eleven sources at six different grains;
* a four-arm chasm plus two nested one-to-many chains;
* compound keys whose second component is deliberately non-unique;
* NULL and unmatched foreign keys at every many-side boundary;
* snowflaked dimensions reached through a fact table;
* additive, conditional, distinct, average, and calculated measures;
* cross-source filters, calculated measures, totals, and output windows.

Every result is checked against a pandas oracle built by explicitly following
the left-join participation path.  Keeping the oracle relational (instead of
copying constants from BSL output) makes this useful as a stress harness when
the fixture is extended.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

import ibis
import pandas as pd
import pytest
from ibis import _

from boring_semantic_layer import Dimension, to_semantic_table


@dataclass(frozen=True)
class AdversarialCommerce:
    """The joined model, its component models, and raw oracle inputs."""

    model: object
    sources: dict[str, object]
    frames: dict[str, pd.DataFrame]


def _fixture_frames() -> dict[str, pd.DataFrame]:
    """Build deterministic data with repeated compound-key components."""
    tenants = pd.DataFrame(
        {
            "tenant_id": [1, 2, 3],
            "region": ["east", "west", None],
            "market": ["enterprise", "smb", "enterprise"],
        }
    )
    accounts = pd.DataFrame(
        {
            # account_id=10 intentionally exists in two tenants.
            "tenant_id": [1, 1, 1, 2, 2, 3, 99, None],
            "account_id": [10, 20, 50, 10, 30, 40, 90, 91],
            "tier": ["gold", "silver", "dormant", "gold", None, "platinum", "ghost", "ghost"],
            "seats": [10, 5, 1, 8, 12, 20, 999, 999],
        }
    )
    orders = pd.DataFrame(
        {
            # order_id=101 also repeats across tenants.
            "tenant_id": [1, 1, 1, 2, 2, 2, 3, 3, 9, 1, None],
            "account_id": [10, 10, 20, 10, 30, 30, 40, 40, 99, 999, 10],
            "order_id": [101, 102, 103, 101, 104, 105, 106, 107, 999, 998, 997],
            "created_at": pd.to_datetime(
                [
                    "2025-01-05",
                    "2025-02-06",
                    "2025-01-20",
                    "2025-01-07",
                    "2025-02-08",
                    "2025-03-09",
                    "2025-03-10",
                    "2025-03-11",
                    "2025-01-01",
                    "2025-01-02",
                    "2025-01-03",
                ]
            ),
            "status": [
                "paid",
                "paid",
                "cancelled",
                "paid",
                "paid",
                "pending",
                "paid",
                None,
                "paid",
                "paid",
                "paid",
            ],
            "gross": [
                100.0,
                220.0,
                80.0,
                150.0,
                300.0,
                90.0,
                400.0,
                50.0,
                5_000.0,
                2_000.0,
                1_000.0,
            ],
            "discount": [10.0, 20.0, 0.0, 15.0, 30.0, 0.0, 40.0, 0.0, 0.0, 0.0, 0.0],
        }
    )

    products = pd.DataFrame(
        {
            # product_id values repeat by tenant; (3, 99) is deliberately absent.
            "tenant_id": [1, 1, 1, 2, 2, 2, 3, 3],
            "product_id": [1, 2, 3, 1, 2, 4, 1, 2],
            "category_id": [10, 20, 20, 10, 30, 30, 10, 40],
            "product_name": [
                "server",
                "seat",
                "training",
                "sensor",
                "plan",
                "service",
                "compute",
                "storage",
            ],
        }
    )
    categories = pd.DataFrame(
        {
            "tenant_id": [1, 1, 2, 2, 3, 3],
            "category_id": [10, 20, 10, 30, 10, 40],
            "category_name": ["Hardware", "Services", "Hardware", "Software", "Hardware", None],
        }
    )

    valid_order_keys = list(
        orders.iloc[:8][["tenant_id", "account_id", "order_id"]].itertuples(index=False, name=None)
    )
    lines_rows = []
    line_id = 1
    for order_index, (tenant_id, _account_id, order_id) in enumerate(valid_order_keys):
        for offset in range(1 + order_index % 3):
            product_options = {
                1: [1, 2, 3],
                2: [1, 2, 4],
                3: [1, 2, 99],
            }[int(tenant_id)]
            product_id = product_options[(order_index + offset) % len(product_options)]
            quantity = 1 + (line_id % 4)
            unit_price = float(12 + 3 * line_id)
            unit_cost = float(5 + line_id)
            lines_rows.append(
                (
                    tenant_id,
                    order_id,
                    line_id,
                    product_id,
                    quantity,
                    unit_price,
                    unit_cost,
                    line_id % 5 == 0,
                )
            )
            line_id += 1
    # Orphans that must not leak into any aggregate.
    lines_rows.extend(
        [
            (1, 9_999, line_id, 1, 100, 1_000.0, 1.0, False),
            (None, 101, line_id + 1, 1, 100, 1_000.0, 1.0, False),
        ]
    )
    lines = pd.DataFrame(
        lines_rows,
        columns=[
            "tenant_id",
            "order_id",
            "line_id",
            "product_id",
            "quantity",
            "unit_price",
            "unit_cost",
            "returned",
        ],
    )

    payment_rows = []
    payment_id = 1
    for order_index, (tenant_id, _account_id, order_id) in enumerate(valid_order_keys):
        installments = 2 if order_index % 3 == 0 else 1
        for installment in range(installments):
            amount = float(40 + 10 * order_index + 5 * installment)
            payment_rows.append(
                (
                    tenant_id,
                    order_id,
                    payment_id,
                    amount,
                    "captured" if installment == 0 else "failed",
                )
            )
            payment_id += 1
    payment_rows.extend([(9, 999, 900, 9_000.0, "captured"), (None, 101, 901, 9_000.0, "captured")])
    payments = pd.DataFrame(
        payment_rows,
        columns=["tenant_id", "order_id", "payment_id", "amount", "payment_status"],
    )

    refund_rows = []
    refund_id = 1
    for row_index, payment in payments.iloc[:-2].iterrows():
        if row_index % 2 == 0:
            refund_rows.append(
                (
                    payment.tenant_id,
                    payment.payment_id,
                    refund_id,
                    float(3 + row_index),
                    "approved" if row_index % 4 else "rejected",
                )
            )
            refund_id += 1
    refund_rows.extend([(9, 900, 900, 8_000.0, "approved"), (1, 9999, 901, 8_000.0, "approved")])
    refunds = pd.DataFrame(
        refund_rows,
        columns=["tenant_id", "payment_id", "refund_id", "amount", "refund_status"],
    )

    valid_accounts = accounts.iloc[:6]
    ticket_rows = []
    ticket_id = 1
    for account_index, account in valid_accounts.iterrows():
        for offset in range(1 + account_index % 2):
            ticket_rows.append(
                (
                    account.tenant_id,
                    account.account_id,
                    ticket_id,
                    ["high", "low", None][(account_index + offset) % 3],
                    1 + ((ticket_id * 3) % 12),
                )
            )
            ticket_id += 1
    ticket_rows.extend([(99, 90, 900, "high", 999), (1, 999, 901, "high", 999)])
    tickets = pd.DataFrame(
        ticket_rows,
        columns=["tenant_id", "account_id", "ticket_id", "priority", "resolution_hours"],
    )

    event_rows = []
    event_id = 1
    for ticket_index, ticket in tickets.iloc[:-2].iterrows():
        for offset in range(1 + ticket_index % 3):
            event_rows.append(
                (
                    ticket.tenant_id,
                    ticket.ticket_id,
                    event_id,
                    ["opened", "reply", "closed"][offset],
                    2 + event_id,
                )
            )
            event_id += 1
    event_rows.extend([(99, 900, 900, "reply", 999), (1, 9999, 901, "reply", 999)])
    ticket_events = pd.DataFrame(
        event_rows,
        columns=["tenant_id", "ticket_id", "event_id", "event_type", "agent_minutes"],
    )

    subscription_rows = []
    subscription_id = 1
    for account_index, account in valid_accounts.iterrows():
        for offset in range(1 + (account_index % 3 == 0)):
            subscription_rows.append(
                (
                    account.tenant_id,
                    account.account_id,
                    subscription_id,
                    ["active", "paused", "cancelled"][(account_index + offset) % 3],
                    float(1_200 + account_index * 100 + offset * 50),
                )
            )
            subscription_id += 1
    subscription_rows.extend([(99, 90, 900, "active", 99_000.0), (1, 999, 901, "active", 99_000.0)])
    subscriptions = pd.DataFrame(
        subscription_rows,
        columns=["tenant_id", "account_id", "subscription_id", "subscription_status", "arr"],
    )

    return {
        "tenants": tenants,
        "accounts": accounts,
        "orders": orders,
        "lines": lines,
        "products": products,
        "categories": categories,
        "payments": payments,
        "refunds": refunds,
        "tickets": tickets,
        "ticket_events": ticket_events,
        "subscriptions": subscriptions,
    }


def build_adversarial_commerce() -> AdversarialCommerce:
    """Create the ten-source model on an in-memory DuckDB connection."""
    frames = _fixture_frames()
    con = ibis.duckdb.connect(":memory:")
    tables = {name: con.create_table(f"nightmare_{name}", frame) for name, frame in frames.items()}

    tenants = (
        to_semantic_table(tables["tenants"], "tenants", description="Tenant grain root")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            region=lambda t: t.region,
            market=lambda t: t.market,
        )
        .with_measures(tenant_count=_.count())
    )
    accounts = (
        to_semantic_table(
            tables["accounts"], "accounts", description="Accounts repeat across tenants"
        )
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            account_id=Dimension(expr=lambda t: t.account_id, is_entity=True),
            tier=lambda t: t.tier,
        )
        .with_measures(account_count=_.count(), licensed_seats=_.seats.sum())
    )
    orders = (
        to_semantic_table(tables["orders"], "orders", description="Order fact")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            account_id=Dimension(expr=lambda t: t.account_id, is_entity=True),
            order_id=Dimension(expr=lambda t: t.order_id, is_entity=True),
            order_month={"expr": _.created_at.truncate("M"), "is_time_dimension": True},
            order_status=_.status,
        )
        .with_measures(
            order_count=_.count(),
            distinct_buyers=_.account_id.nunique(),
            gross_revenue=_.gross.sum(),
            net_revenue=(_.gross - _.discount).sum(),
            paid_revenue=(_.status == "paid").ifelse(_.gross - _.discount, 0).sum(),
            average_order_value=_.gross.mean(),
        )
        .with_measures(discount_rate=(_.gross_revenue - _.net_revenue) / _.gross_revenue.nullif(0))
    )
    lines = (
        to_semantic_table(tables["lines"], "lines", description="Order line fact")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            order_id=Dimension(expr=lambda t: t.order_id, is_entity=True),
            line_id=Dimension(expr=lambda t: t.line_id, is_entity=True),
            product_id=lambda t: t.product_id,
        )
        .with_measures(
            line_count=_.count(),
            distinct_products=_.product_id.nunique(),
            units=_.quantity.sum(),
            line_revenue=(_.quantity * _.unit_price).sum(),
            line_cost=(_.quantity * _.unit_cost).sum(),
            returned_units=_.returned.ifelse(_.quantity, 0).sum(),
        )
    )
    products = to_semantic_table(tables["products"], "products").with_dimensions(
        tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
        product_id=Dimension(expr=lambda t: t.product_id, is_entity=True),
        category_id=lambda t: t.category_id,
        product_name=lambda t: t.product_name,
    )
    categories = to_semantic_table(tables["categories"], "categories").with_dimensions(
        tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
        category_id=Dimension(expr=lambda t: t.category_id, is_entity=True),
        category_name=lambda t: t.category_name,
    )
    payments = (
        to_semantic_table(tables["payments"], "payments", description="Payment attempt fact")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            order_id=Dimension(expr=lambda t: t.order_id, is_entity=True),
            payment_id=Dimension(expr=lambda t: t.payment_id, is_entity=True),
            payment_status=lambda t: t.payment_status,
        )
        .with_measures(
            payment_count=_.count(),
            captured_count=(_.payment_status == "captured").ifelse(1, 0).sum(),
            collected=(_.payment_status == "captured").ifelse(_.amount, 0).sum(),
        )
    )
    refunds = (
        to_semantic_table(tables["refunds"], "refunds", description="Refund fact below payments")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            payment_id=Dimension(expr=lambda t: t.payment_id, is_entity=True),
            refund_id=Dimension(expr=lambda t: t.refund_id, is_entity=True),
            refund_status=lambda t: t.refund_status,
        )
        .with_measures(
            refund_count=_.count(),
            approved_refunds=(_.refund_status == "approved").ifelse(1, 0).sum(),
            refunded=(_.refund_status == "approved").ifelse(_.amount, 0).sum(),
        )
    )
    tickets = (
        to_semantic_table(tables["tickets"], "tickets", description="Support ticket fact")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            account_id=Dimension(expr=lambda t: t.account_id, is_entity=True),
            ticket_id=Dimension(expr=lambda t: t.ticket_id, is_entity=True),
            priority=lambda t: t.priority,
        )
        .with_measures(
            ticket_count=_.count(),
            resolution_hours=_.resolution_hours.sum(),
            sla_breaches=(_.resolution_hours > 8).ifelse(1, 0).sum(),
        )
    )
    ticket_events = (
        to_semantic_table(tables["ticket_events"], "ticket_events", description="Ticket event fact")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            ticket_id=Dimension(expr=lambda t: t.ticket_id, is_entity=True),
            event_id=Dimension(expr=lambda t: t.event_id, is_entity=True),
            event_type=lambda t: t.event_type,
        )
        .with_measures(event_count=_.count(), agent_minutes=_.agent_minutes.sum())
    )
    subscriptions = (
        to_semantic_table(tables["subscriptions"], "subscriptions", description="Subscription fact")
        .with_dimensions(
            tenant_id=Dimension(expr=lambda t: t.tenant_id, is_entity=True),
            account_id=Dimension(expr=lambda t: t.account_id, is_entity=True),
            subscription_id=Dimension(expr=lambda t: t.subscription_id, is_entity=True),
            subscription_status=lambda t: t.subscription_status,
        )
        .with_measures(
            subscription_count=_.count(),
            active_subscriptions=(_.subscription_status == "active").ifelse(1, 0).sum(),
            annual_recurring_revenue=(_.subscription_status == "active").ifelse(_.arr, 0).sum(),
        )
    )

    sources = {
        "tenants": tenants,
        "accounts": accounts,
        "orders": orders,
        "lines": lines,
        "products": products,
        "categories": categories,
        "payments": payments,
        "refunds": refunds,
        "tickets": tickets,
        "ticket_events": ticket_events,
        "subscriptions": subscriptions,
    }
    model = (
        tenants.join_many(accounts, on="tenant_id")
        .join_many(orders, on=["tenant_id", "account_id"])
        .join_many(lines, on=["tenant_id", "order_id"])
        .join_one(products, on=["tenant_id", "product_id"])
        .join_one(categories, on=["tenant_id", "category_id"])
        .join_many(payments, on=["tenant_id", "order_id"])
        .join_many(refunds, on=["tenant_id", "payment_id"])
        .join_many(tickets, on=["tenant_id", "account_id"])
        .join_many(ticket_events, on=["tenant_id", "ticket_id"])
        .join_many(subscriptions, on=["tenant_id", "account_id"])
        .with_measures(
            net_cash=lambda t: t["payments.collected"] - t["refunds.refunded"],
            revenue_per_account=lambda t: t["orders.net_revenue"]
            / t["accounts.account_count"].nullif(0),
            support_minutes_per_order=lambda t: t["ticket_events.agent_minutes"]
            / t["orders.order_count"].nullif(0),
            gross_margin=lambda t: (t["lines.line_revenue"] - t["lines.line_cost"])
            / t["lines.line_revenue"].nullif(0),
        )
    )
    return AdversarialCommerce(model=model, sources=sources, frames=frames)


@pytest.fixture(scope="module")
def commerce():
    return build_adversarial_commerce()


def _participating_frames(frames):
    """Return each fact decorated with its reachable root dimensions."""
    root_accounts = frames["accounts"].merge(frames["tenants"], on="tenant_id", how="inner")
    orders = frames["orders"].merge(
        root_accounts[["tenant_id", "account_id", "tier", "region", "market"]],
        on=["tenant_id", "account_id"],
        how="inner",
    )
    lines = frames["lines"].merge(
        orders[
            [
                "tenant_id",
                "order_id",
                "account_id",
                "tier",
                "region",
                "market",
                "status",
                "created_at",
            ]
        ],
        on=["tenant_id", "order_id"],
        how="inner",
    )
    payments = frames["payments"].merge(
        orders[["tenant_id", "order_id", "account_id", "tier", "region", "market", "status"]],
        on=["tenant_id", "order_id"],
        how="inner",
    )
    refunds = frames["refunds"].merge(
        payments[
            [
                "tenant_id",
                "payment_id",
                "order_id",
                "account_id",
                "tier",
                "region",
                "market",
                "status",
            ]
        ],
        on=["tenant_id", "payment_id"],
        how="inner",
    )
    tickets = frames["tickets"].merge(
        root_accounts[["tenant_id", "account_id", "tier", "region", "market"]],
        on=["tenant_id", "account_id"],
        how="inner",
    )
    ticket_events = frames["ticket_events"].merge(
        tickets[["tenant_id", "ticket_id", "account_id", "tier", "region", "market"]],
        on=["tenant_id", "ticket_id"],
        how="inner",
    )
    subscriptions = frames["subscriptions"].merge(
        root_accounts[["tenant_id", "account_id", "tier", "region", "market"]],
        on=["tenant_id", "account_id"],
        how="inner",
    )
    return {
        "accounts": root_accounts,
        "orders": orders,
        "lines": lines,
        "payments": payments,
        "refunds": refunds,
        "tickets": tickets,
        "ticket_events": ticket_events,
        "subscriptions": subscriptions,
    }


def _group_sum(frame, keys, **expressions):
    work = frame.copy()
    for name, expression in expressions.items():
        work[name] = expression(work)
    return work.groupby(keys, dropna=False)[list(expressions)].sum().reset_index()


def _outer_merge(parts, keys):
    return reduce(lambda left, right: left.merge(right, on=keys, how="outer"), parts)


def _normalize_result(frame, keys):
    frame = frame.copy()
    # DuckDB returns SQL NULL strings as None while pandas merges normally use
    # NaN.  Normalize both to one nullable-string representation before an
    # exact frame comparison.
    for key in keys:
        if pd.api.types.is_object_dtype(frame[key].dtype):
            frame[key] = frame[key].astype("string")
    for column in frame.columns.difference(keys):
        frame[column] = pd.to_numeric(frame[column])
    return frame.sort_values(keys, na_position="last").reset_index(drop=True)


def test_nightmare_model_exposes_all_sources_and_metadata(commerce):
    """The fixture itself should stay difficult as the implementation evolves."""
    model = commerce.model
    for source in commerce.sources:
        assert any(name.startswith(f"{source}.") for name in model.dimensions + model.measures)
    assert len(model.dimensions) >= 35
    assert len(model.measures) >= 30
    assert model.get_dimensions()["orders.order_month"].is_time_dimension is True
    assert model.get_dimensions()["ticket_events.event_id"].is_entity is True


def test_grand_totals_survive_six_grains_and_four_chasm_arms(commerce):
    """No source may be multiplied by any of its sibling or child facts."""
    p = _participating_frames(commerce.frames)
    actual = (
        commerce.model.aggregate(
            "tenants.tenant_count",
            "accounts.account_count",
            "accounts.licensed_seats",
            "orders.order_count",
            "orders.distinct_buyers",
            "orders.net_revenue",
            "lines.line_count",
            "lines.units",
            "lines.line_revenue",
            "payments.payment_count",
            "payments.collected",
            "refunds.refund_count",
            "refunds.refunded",
            "tickets.ticket_count",
            "tickets.sla_breaches",
            "ticket_events.event_count",
            "ticket_events.agent_minutes",
            "subscriptions.subscription_count",
            "subscriptions.annual_recurring_revenue",
            "net_cash",
            "revenue_per_account",
            "support_minutes_per_order",
            "gross_margin",
        )
        .execute()
        .iloc[0]
    )

    expected = {
        "tenants.tenant_count": len(commerce.frames["tenants"]),
        "accounts.account_count": len(p["accounts"]),
        "accounts.licensed_seats": p["accounts"].seats.sum(),
        "orders.order_count": len(p["orders"]),
        "orders.distinct_buyers": p["orders"].account_id.nunique(),
        "orders.net_revenue": (p["orders"].gross - p["orders"].discount).sum(),
        "lines.line_count": len(p["lines"]),
        "lines.units": p["lines"].quantity.sum(),
        "lines.line_revenue": (p["lines"].quantity * p["lines"].unit_price).sum(),
        "payments.payment_count": len(p["payments"]),
        "payments.collected": p["payments"]
        .amount.where(p["payments"].payment_status == "captured", 0)
        .sum(),
        "refunds.refund_count": len(p["refunds"]),
        "refunds.refunded": p["refunds"]
        .amount.where(p["refunds"].refund_status == "approved", 0)
        .sum(),
        "tickets.ticket_count": len(p["tickets"]),
        "tickets.sla_breaches": (p["tickets"].resolution_hours > 8).sum(),
        "ticket_events.event_count": len(p["ticket_events"]),
        "ticket_events.agent_minutes": p["ticket_events"].agent_minutes.sum(),
        "subscriptions.subscription_count": len(p["subscriptions"]),
        "subscriptions.annual_recurring_revenue": p["subscriptions"]
        .arr.where(p["subscriptions"].subscription_status == "active", 0)
        .sum(),
    }
    expected["net_cash"] = expected["payments.collected"] - expected["refunds.refunded"]
    expected["revenue_per_account"] = (
        expected["orders.net_revenue"] / expected["accounts.account_count"]
    )
    expected["support_minutes_per_order"] = (
        expected["ticket_events.agent_minutes"] / expected["orders.order_count"]
    )
    line_cost = (p["lines"].quantity * p["lines"].unit_cost).sum()
    expected["gross_margin"] = (expected["lines.line_revenue"] - line_cost) / expected[
        "lines.line_revenue"
    ]

    for column, value in expected.items():
        assert float(actual[column]) == pytest.approx(float(value)), column


def test_grouped_multi_fact_result_matches_independent_oracle(commerce):
    """Group on two ancestor dimensions while reading every fact arm."""
    keys = ["region", "tier"]
    p = _participating_frames(commerce.frames)
    expected = _outer_merge(
        [
            _group_sum(
                p["accounts"], keys, account_count=lambda x: 1, licensed_seats=lambda x: x.seats
            ),
            _group_sum(
                p["orders"],
                keys,
                order_count=lambda x: 1,
                net_revenue=lambda x: x.gross - x.discount,
            ),
            _group_sum(
                p["lines"],
                keys,
                units=lambda x: x.quantity,
                line_revenue=lambda x: x.quantity * x.unit_price,
            ),
            _group_sum(
                p["payments"],
                keys,
                payment_count=lambda x: 1,
                collected=lambda x: x.amount.where(x.payment_status == "captured", 0),
            ),
            _group_sum(
                p["refunds"],
                keys,
                refund_count=lambda x: 1,
                refunded=lambda x: x.amount.where(x.refund_status == "approved", 0),
            ),
            _group_sum(
                p["tickets"],
                keys,
                ticket_count=lambda x: 1,
                sla_breaches=lambda x: (x.resolution_hours > 8).astype(int),
            ),
            _group_sum(
                p["ticket_events"],
                keys,
                event_count=lambda x: 1,
                agent_minutes=lambda x: x.agent_minutes,
            ),
            _group_sum(
                p["subscriptions"],
                keys,
                subscription_count=lambda x: 1,
                annual_recurring_revenue=lambda x: x.arr.where(
                    x.subscription_status == "active", 0
                ),
            ),
        ],
        keys,
    ).fillna(0)
    actual = (
        commerce.model.group_by("tenants.region", "accounts.tier")
        .aggregate(
            "accounts.account_count",
            "accounts.licensed_seats",
            "orders.order_count",
            "orders.net_revenue",
            "lines.units",
            "lines.line_revenue",
            "payments.payment_count",
            "payments.collected",
            "refunds.refund_count",
            "refunds.refunded",
            "tickets.ticket_count",
            "tickets.sla_breaches",
            "ticket_events.event_count",
            "ticket_events.agent_minutes",
            "subscriptions.subscription_count",
            "subscriptions.annual_recurring_revenue",
        )
        .execute()
        .rename(columns=lambda c: c.split(".")[-1])
        .fillna(0)
    )
    expected = _normalize_result(expected, keys)
    actual = _normalize_result(actual, keys)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, rtol=1e-12)


def test_cross_source_filter_totals_and_output_windows(commerce):
    """Filter through a snowflake, then calculate share/rank after aggregation."""
    p = _participating_frames(commerce.frames)
    enriched_lines = (
        p["lines"]
        .merge(commerce.frames["products"], on=["tenant_id", "product_id"], how="left")
        .merge(commerce.frames["categories"], on=["tenant_id", "category_id"], how="left")
    )
    eligible = enriched_lines[
        (enriched_lines.status == "paid") & (enriched_lines.category_name == "Hardware")
    ]
    expected = _group_sum(
        eligible,
        ["region"],
        hardware_revenue=lambda x: x.quantity * x.unit_price,
        hardware_units=lambda x: x.quantity,
    )
    expected["revenue_share"] = expected.hardware_revenue / expected.hardware_revenue.sum()
    expected["revenue_rank"] = expected.hardware_revenue.rank(method="min").astype(int) - 1

    actual = (
        commerce.model.filter(
            lambda t: (t["orders.order_status"] == "paid")
            & (t["categories.category_name"] == "Hardware")
        )
        .group_by("tenants.region")
        .aggregate(
            hardware_revenue=lambda t: t["lines.line_revenue"],
            hardware_units=lambda t: t["lines.units"],
        )
        .mutate(revenue_share=lambda t: t.hardware_revenue / t.all(t.hardware_revenue))
        .mutate(revenue_rank=lambda t: t.hardware_revenue.rank())
        .execute()
        .rename(columns={"tenants.region": "region"})
    )
    expected = _normalize_result(expected, ["region"])
    actual = _normalize_result(actual, ["region"])
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, rtol=1e-12)


def test_deep_snowflake_time_grain_and_null_dimension(commerce):
    """A two-fact-key/two-dimension-key query keeps unmatched and NULL dims."""
    frames = commerce.frames
    truth = (
        frames["tenants"]
        .merge(frames["accounts"], on="tenant_id", how="left")
        .merge(frames["orders"], on=["tenant_id", "account_id"], how="left")
        .merge(frames["lines"], on=["tenant_id", "order_id"], how="left")
        .merge(frames["products"], on=["tenant_id", "product_id"], how="left")
        .merge(frames["categories"], on=["tenant_id", "category_id"], how="left")
    )
    truth["order_month"] = truth.created_at.dt.to_period("M").dt.to_timestamp()
    truth["line_revenue"] = truth.quantity * truth.unit_price
    expected = (
        truth.groupby(["order_month", "category_name"], dropna=False)
        .agg(
            line_revenue=("line_revenue", lambda x: x.sum(min_count=1)),
            units=("quantity", lambda x: x.sum(min_count=1)),
        )
        .reset_index()
    )
    actual = (
        commerce.model.group_by("orders.order_month", "categories.category_name")
        .aggregate("lines.line_revenue", "lines.units")
        .execute()
        .rename(columns=lambda c: c.split(".")[-1])
    )
    expected = _normalize_result(expected, ["order_month", "category_name"])
    actual = _normalize_result(actual, ["order_month", "category_name"])
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, rtol=1e-12)
