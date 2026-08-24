"""A semantic model is a definition, not a query — output requires a query.

Executing a bare model used to return the raw underlying table: every
physical column at row grain, undeclared columns included, computed
dimensions missing. Nothing about that result is semantic, and the same
hole existed for every output sink (``sql``, ``to_pandas``, ``to_csv``, …)
and for every pre-aggregation chain (filters, joins, order_by/limit,
``group_by`` without ``aggregate``, empty ``query()``).

The contract now: sinking to output requires an aggregation stage (or an
index, or a model explicitly materialized from a query result). Raw access
stays one explicit call away via ``.to_untagged()``.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.errors import QueryError


@pytest.fixture
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def model(con):
    tbl = con.create_table(
        "orders",
        pd.DataFrame(
            {
                "o_id": [1, 2, 3],
                "cust": ["a", "b", "a"],
                "amt": [10.0, 20.0, 30.0],
                "d": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-02-15"]),
            }
        ),
    )
    return (
        to_semantic_table(tbl, name="orders")
        .with_dimensions(
            cust=lambda t: t.cust,
            d={"expr": lambda t: t.d, "is_time_dimension": True},
        )
        .with_measures(revenue=lambda t: t.amt.sum(), n=lambda t: t.count())
    )


@pytest.fixture
def customers(con):
    tbl = con.create_table(
        "customers",
        pd.DataFrame({"cust": ["a", "b"], "region": ["west", "east"]}),
    )
    return to_semantic_table(tbl, name="customers").with_dimensions(
        cust=lambda t: t.cust, region=lambda t: t.region
    )


# ---------------------------------------------------------------------------
# Definition-side expressions refuse every output sink
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sink",
    ["execute", "to_pandas", "to_pyarrow", "to_polars", "to_pandas_batches", "sql", "compile"],
)
def test_bare_model_refuses_output_sinks(model, sink):
    with pytest.raises(QueryError, match="definition, not a query"):
        getattr(model, sink)()


def test_bare_model_refuses_file_sinks(model, tmp_path):
    with pytest.raises(QueryError, match="definition, not a query"):
        model.to_csv(tmp_path / "out.csv")
    with pytest.raises(QueryError, match="definition, not a query"):
        model.to_parquet(tmp_path / "out.parquet")


def test_error_names_the_model_and_its_fields(model):
    with pytest.raises(QueryError) as exc:
        model.execute()
    msg = str(exc.value)
    assert "'orders'" in msg
    assert "to_untagged" in msg
    assert "cust" in msg and "revenue" in msg


def test_filter_chain_is_still_a_definition(model):
    with pytest.raises(QueryError, match="definition, not a query"):
        model.filter(lambda t: t.amt > 5).execute()


def test_order_by_and_limit_do_not_complete_a_query(model):
    with pytest.raises(QueryError, match="definition, not a query"):
        model.order_by("cust").limit(2).execute()


def test_bare_join_is_a_definition(model, customers):
    joined = model.join_one(customers, on="cust")
    with pytest.raises(QueryError, match="definition, not a query"):
        joined.execute()


def test_group_by_without_aggregate_is_an_incomplete_query(model):
    with pytest.raises(QueryError, match=r"\.aggregate"):
        model.group_by("cust").execute()


def test_regroup_of_an_aggregate_is_incomplete_again(model):
    agg = model.group_by("cust").aggregate("revenue")
    with pytest.raises(QueryError, match=r"\.aggregate"):
        agg.group_by("cust").execute()


def test_empty_query_is_refused_eagerly(model):
    with pytest.raises(QueryError, match="at least one dimension or measure"):
        model.query()
    with pytest.raises(QueryError, match="at least one dimension or measure"):
        model.query(filters=[{"field": "cust", "operator": "=", "value": "a"}])


# ---------------------------------------------------------------------------
# The explicit escape hatch and completed queries keep working
# ---------------------------------------------------------------------------


def test_to_untagged_is_the_explicit_raw_escape(model):
    df = model.to_untagged().execute()
    assert len(df) == 3
    assert "o_id" in df.columns  # raw access is explicit, so raw columns are fine


def test_aggregate_executes(model):
    df = model.group_by("cust").aggregate("revenue").execute()
    assert sorted(df["revenue"].tolist()) == [20.0, 40.0]


def test_empty_aggregate_returns_distinct_group_values(model):
    df = model.group_by("cust").aggregate().execute()
    assert sorted(df["cust"].tolist()) == ["a", "b"]


def test_post_aggregation_chain_executes(model):
    df = (
        model.group_by("cust")
        .aggregate("revenue")
        .filter(lambda t: t.revenue > 5)
        .order_by("revenue")
        .limit(5)
        .execute()
    )
    assert len(df) == 2


def test_query_with_fields_executes(model):
    df = model.query(dimensions=["cust"], measures=["revenue"]).execute()
    assert set(df.columns) == {"cust", "revenue"}


def test_join_completed_by_a_query_executes(model, customers):
    df = (
        model.join_one(customers, on="cust")
        .group_by("customers.region")
        .aggregate("orders.revenue")
        .execute()
    )
    assert len(df) == 2


def test_as_table_over_an_aggregate_is_a_result_model(model):
    result = model.group_by("cust").aggregate("revenue").as_table()
    df = result.execute()
    assert sorted(df["revenue"].tolist()) == [20.0, 40.0]


def test_as_table_over_an_ordered_aggregate_is_a_result_model(model):
    result = model.group_by("cust").aggregate("revenue").order_by("revenue").as_table()
    assert result.execute()["revenue"].tolist() == [20.0, 40.0]


def test_as_table_over_a_definition_stays_a_definition(model):
    result = model.filter(lambda t: t.amt > 5).as_table()
    with pytest.raises(QueryError, match="definition, not a query"):
        result.execute()


def test_compare_periods_result_executes(model):
    df = model.compare_periods(
        dimensions=["cust"],
        measures=["revenue"],
        current_time_range={"start": "2024-02-01", "end": "2024-03-01"},
        previous_time_range={"start": "2024-01-01", "end": "2024-02-01"},
    ).execute()
    assert "revenue_delta" in df.columns


def test_index_result_operations_execute(model):
    df = model.index("cust").filter(lambda t: t.fieldValue == "a").execute()
    assert df["fieldValue"].tolist() == ["a"]


# ---------------------------------------------------------------------------
# Post-aggregation mutate is refused with guidance
# ---------------------------------------------------------------------------


def test_mutate_on_wrapped_aggregate_is_refused(model):
    agg = model.group_by("cust").aggregate("revenue")
    for wrapped in (
        agg.order_by("revenue"),
        agg.filter(lambda t: t.revenue > 5),
        agg.limit(2),
    ):
        with pytest.raises(QueryError, match="to_untagged"):
            wrapped.mutate(x=lambda t: t.revenue * 2)


def test_mutate_directly_on_aggregate_stays_on_the_measure_path(model):
    df = (
        model.group_by("cust")
        .aggregate("revenue")
        .mutate(share=lambda t: t.revenue / t.all(t.revenue))
        .execute()
    )
    assert sorted(round(v, 3) for v in df["share"]) == [0.333, 0.667]


def test_pre_aggregation_mutate_registers_a_dimension(model):
    df = model.mutate(big=lambda t: t.amt > 15).group_by("big").aggregate("n").execute()
    assert sorted(df["n"].tolist()) == [1, 2]


def test_pre_aggregation_mutate_result_is_still_a_definition(model):
    with pytest.raises(QueryError, match="definition, not a query"):
        model.mutate(big=lambda t: t.amt > 15).execute()
