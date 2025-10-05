import pytest
import pandas as pd
import ibis
from ibis.common.exceptions import InputTypeError
from ibis.common.annotations import SignatureValidationError
from boring_semantic_layer.semantic_api import to_semantic_table

def test_cross_team_execute_binding_issue():
    marketing_df = pd.DataFrame({'customer_id':[1,2],'segment':['A','B'],'monthly_spend':[100,200]})
    support_df   = pd.DataFrame({'case_id':[10,11],'customer_id':[1,2],'priority':['high','low']})
    con = ibis.duckdb.connect(':memory:')
    m_tbl = con.create_table('marketing', marketing_df)
    s_tbl = con.create_table('support',   support_df)

    marketing_st = (
        to_semantic_table(m_tbl, name='marketing')
        .with_dimensions(customer_id=lambda t: t.customer_id, segment=lambda t: t.segment)
        .with_measures(monthly_spend=lambda t: t.monthly_spend.mean())
    )
    support_st = (
        to_semantic_table(s_tbl, name='support')
        .with_dimensions(case_id=lambda t: t.case_id, customer_id=lambda t: t.customer_id, priority=lambda t: t.priority)
        .with_measures(case_count=lambda t: t.count())
    )

    cross_team = (
        marketing_st.join(support_st, on=lambda m, s: m.customer_id == s.customer_id)
        .with_measures(avg_case_value=lambda t: t.monthly_spend.mean() / t.case_count)
    )

    cross_team.group_by('segment').aggregate(avg_case_value=lambda t: t.avg_case_value).execute()
