import pandas as pd
import ibis

from boring_semantic_layer import to_semantic_table


def test_cross_team_aggregated_measure_refs():
    marketing_df = pd.DataFrame(
        {"customer_id": [1, 2], "segment": ["A", "B"], "monthly_spend": [100, 200]}
    )
    support_df = pd.DataFrame(
        {"case_id": [10, 11], "customer_id": [1, 2], "priority": ["high", "low"]}
    )
    con = ibis.duckdb.connect(":memory:")
    m_tbl = con.create_table("marketing", marketing_df)
    s_tbl = con.create_table("support", support_df)

    # Base measures are *aggregates*
    marketing_st = (
        to_semantic_table(m_tbl, name="marketing")
        .with_dimensions(
            customer_id=lambda t: t.customer_id, segment=lambda t: t.segment
        )
        .with_measures(
            avg_monthly_spend=lambda t: t.monthly_spend.mean(),
        )
    )

    support_st = (
        to_semantic_table(s_tbl, name="support")
        .with_dimensions(
            case_id=lambda t: t.case_id,
            customer_id=lambda t: t.customer_id,
            priority=lambda t: t.priority,
        )
        .with_measures(
            case_count=lambda t: t.count(),
        )
    )

    # Calculated measure references other measures (both aggregates)
    cross_team = marketing_st.join_one(
        support_st, "customer_id", "customer_id"
    ).with_measures(avg_case_value=lambda t: t.avg_monthly_spend / t.case_count)

    # Sane query: pick the calculated measure at the requested grain
    df = (
        cross_team.group_by("segment")
        .aggregate("avg_case_value")
        .order_by("segment")
        .execute()
    )

    # One case per customer, so:
    # segment A: 100 / 1 = 100
    # segment B: 200 / 1 = 200
    assert df.to_dict(orient="list") == {
        "segment": ["A", "B"],
        "avg_case_value": [100.0, 200.0],
    }
