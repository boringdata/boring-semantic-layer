import pandas as pd
import ibis
import pytest

from boring_semantic_layer import to_semantic_table


def test_percent_of_total_grand_total():
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA", "DL", "DL", "DL"]})
    carriers = pd.DataFrame(
        {"code": ["AA", "UA", "DL"], "nickname": ["American", "United", "Delta"]}
    )
    f_tbl = con.create_table("flights", flights)
    c_tbl = con.create_table("carriers", carriers)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
    )
    carriers_st = to_semantic_table(c_tbl, "carriers").with_dimensions(
        code=lambda t: t.code, nickname=lambda t: t.nickname
    )

    joined = (
        flights_st.join(carriers_st, on=lambda f, c: f.carrier == c.code)
        .with_dimensions(nickname=lambda t: t.nickname)
        .with_measures(
            percent_of_total=lambda t: t.flight_count / t.all(t.flight_count)
        )
    )

    df = (
        joined.group_by("nickname")
        .aggregate("percent_of_total")
        .order_by("nickname")
        .execute()
    )

    expected = {"American": 2 / 6, "Delta": 3 / 6, "United": 1 / 6}
    got = dict(zip(df.nickname, df.percent_of_total))
    for k, v in expected.items():
        assert pytest.approx(v) == got[k]
    assert pytest.approx(sum(got.values())) == 1.0
