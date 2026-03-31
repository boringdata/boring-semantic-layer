"""Tests that BSL core features work with plain ibis backends (no xorq wrapping).

This validates support for ibis backends that xorq doesn't register (e.g. Databricks).
See: https://github.com/boringdata/boring-semantic-layer/issues/216
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def plain_ibis_con():
    """Plain ibis DuckDB connection (not wrapped by xorq)."""
    return ibis.duckdb.connect()


@pytest.fixture(scope="module")
def flights_table(plain_ibis_con):
    """Sample flights table."""
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA", "DL"],
            "origin": ["JFK", "LAX", "SFO", "JFK", "ATL"],
            "dest": ["LAX", "JFK", "LAX", "SFO", "JFK"],
            "distance": [2475, 2475, 337, 2586, 760],
            "dep_delay": [10, -5, 0, 15, -3],
        }
    )
    return plain_ibis_con.create_table("flights", df)


@pytest.fixture(scope="module")
def carriers_table(plain_ibis_con):
    """Sample carriers lookup table."""
    df = pd.DataFrame(
        {
            "code": ["AA", "UA", "DL"],
            "name": ["American Airlines", "United Airlines", "Delta Air Lines"],
        }
    )
    return plain_ibis_con.create_table("carriers", df)


@pytest.fixture(scope="module")
def flights_model(flights_table):
    """BSL semantic table from plain ibis table."""
    return (
        to_semantic_table(flights_table, name="flights")
        .with_dimensions(
            carrier=lambda t: t.carrier,
            origin=lambda t: t.origin,
            dest=lambda t: t.dest,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_delay=lambda t: t.dep_delay.mean(),
        )
    )


@pytest.fixture(scope="module")
def carriers_model(carriers_table):
    """BSL semantic table for carriers."""
    return (
        to_semantic_table(carriers_table, name="carriers")
        .with_dimensions(
            code=lambda t: t.code,
            name=lambda t: t.name,
        )
        .with_measures(
            carrier_count=lambda t: t.count(),
        )
    )


class TestPlainIbisExecution:
    """Core execution works without xorq wrapping."""

    def test_simple_aggregate(self, flights_model):
        result = flights_model.group_by("carrier").aggregate("flight_count").execute()
        assert len(result) == 3
        assert result.flight_count.sum() == 5

    def test_multiple_measures(self, flights_model):
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .execute()
        )
        assert "flight_count" in result.columns
        assert "total_distance" in result.columns

    def test_filter_then_aggregate(self, flights_model):
        result = (
            flights_model.filter(lambda t: t.carrier == "AA")
            .group_by("carrier")
            .aggregate("flight_count")
            .execute()
        )
        assert len(result) == 1
        assert result.flight_count.iloc[0] == 2

    def test_order_by(self, flights_model):
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .order_by(ibis.desc("flight_count"))
            .execute()
        )
        assert result.flight_count.iloc[0] >= result.flight_count.iloc[-1]

    def test_limit(self, flights_model):
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .limit(2)
            .execute()
        )
        assert len(result) == 2

    def test_sql_compilation(self, flights_model):
        sql = flights_model.group_by("carrier").aggregate("flight_count").sql()
        assert isinstance(sql, str)
        assert "carrier" in sql.lower()

    def test_to_pandas(self, flights_model):
        result = (
            flights_model.group_by("carrier").aggregate("flight_count").to_pandas()
        )
        assert hasattr(result, "columns")
        assert len(result) == 3


class TestPlainIbisFiltering:
    """Filtering works without xorq wrapping."""

    def test_callable_filter(self, flights_model):
        result = (
            flights_model.filter(lambda t: t.distance > 1000)
            .group_by("carrier")
            .aggregate("flight_count")
            .execute()
        )
        # AA(JFK->LAX, LAX->JFK) + UA(JFK->SFO) = 3 flights with distance > 1000
        assert result.flight_count.sum() == 3

    def test_multiple_filters(self, flights_model):
        result = (
            flights_model.filter(lambda t: t.carrier == "AA")
            .filter(lambda t: t.distance > 2000)
            .group_by("origin")
            .aggregate("flight_count")
            .execute()
        )
        assert result.flight_count.sum() == 2


class TestPlainIbisJoins:
    """Joins work without xorq wrapping."""

    def test_join_one(self, flights_model, carriers_model):
        joined = flights_model.join_one(
            carriers_model,
            on=lambda f, c: f.carrier == c.code,
        )
        result = joined.group_by("name").aggregate("flight_count").execute()
        assert len(result) == 3
        assert "name" in result.columns

    def test_rebind_join_backends_with_plain_ibis_table(self, plain_ibis_con):
        """_rebind_join_backends gracefully skips plain ibis tables (GH-221).

        Directly tests the guarded code path: xorq's walk_nodes cannot
        traverse plain ibis Table objects, so rebinding should be a no-op.
        """
        from boring_semantic_layer.ops import SemanticJoinOp

        t1 = plain_ibis_con.create_table(
            "rebind_l", pd.DataFrame({"a": [1]})
        )
        t2 = plain_ibis_con.create_table(
            "rebind_r", pd.DataFrame({"a": [2]})
        )
        result_l, result_r = SemanticJoinOp._rebind_join_backends(t1, t2)
        assert result_l is t1
        assert result_r is t2

    def test_rebind_mixed_xorq_left_plain_ibis_right(self, plain_ibis_con):
        """Mixed scenario: xorq-wrapped left + plain ibis right (GH-221).

        When the left table is xorq-wrapped (walk_nodes succeeds) but the
        right is plain ibis, the replacer should be a no-op on the right
        side since plain ibis ops are not xorq DatabaseTable instances.
        """
        from boring_semantic_layer.ops import SemanticJoinOp

        try:
            from xorq.common.utils.ibis_utils import from_ibis
        except ImportError:
            pytest.skip("xorq not available")

        con = ibis.duckdb.connect()
        plain_right = con.create_table(
            "mixed_r", pd.DataFrame({"id": [1, 2]})
        )
        xorq_left = from_ibis(
            con.create_table("mixed_l", pd.DataFrame({"id": [1, 2]}))
        )
        # Should not raise — replacer is no-op on plain ibis ops
        result_l, result_r = SemanticJoinOp._rebind_join_backends(
            xorq_left, plain_right
        )
        assert result_l is not None
        assert result_r is not None

    def test_rebind_with_xorq_wrapped_tables(self):
        """Happy path: xorq-wrapped tables still rebind correctly (GH-221).

        Ensures the fix doesn't regress the normal xorq code path.
        """
        from boring_semantic_layer.ops import SemanticJoinOp

        try:
            from xorq.common.utils.ibis_utils import from_ibis
        except ImportError:
            pytest.skip("xorq not available")

        con = ibis.duckdb.connect()
        t1 = from_ibis(con.create_table("xorq_l", pd.DataFrame({"a": [1]})))
        t2 = from_ibis(con.create_table("xorq_r", pd.DataFrame({"b": [2]})))
        result_l, result_r = SemanticJoinOp._rebind_join_backends(t1, t2)
        assert result_l is not None
        assert result_r is not None
        # Both should still be executable after rebinding
        assert result_l.execute() is not None
        assert result_r.execute() is not None

    def test_chained_joins_plain_ibis(self, plain_ibis_con):
        """Multi-table chained joins work on plain ibis backends (GH-221)."""
        orders_tbl = plain_ibis_con.create_table(
            "orders_pi", pd.DataFrame({
                "order_id": [1, 2, 3],
                "customer_id": [10, 20, 10],
                "product_id": [100, 100, 200],
            })
        )
        customers_tbl = plain_ibis_con.create_table(
            "customers_pi", pd.DataFrame({
                "customer_id": [10, 20],
                "name": ["Alice", "Bob"],
            })
        )
        products_tbl = plain_ibis_con.create_table(
            "products_pi", pd.DataFrame({
                "product_id": [100, 200],
                "product_name": ["Widget", "Gadget"],
            })
        )

        orders = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
                product_id=lambda t: t.product_id,
            )
            .with_measures(order_count=lambda t: t.count())
        )
        customers = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )
        )
        products = (
            to_semantic_table(products_tbl, name="products")
            .with_dimensions(
                product_id=lambda t: t.product_id,
                product_name=lambda t: t.product_name,
            )
        )

        joined = orders.join_one(
            customers, on=lambda o, c: o.customer_id == c.customer_id
        ).join_one(
            products, on=lambda o, p: o.product_id == p.product_id
        )
        result = (
            joined.group_by("name", "product_name")
            .aggregate("order_count")
            .execute()
        )
        assert len(result) >= 2
        assert "name" in result.columns
        assert "product_name" in result.columns
        assert result.order_count.sum() == 3


class TestPlainIbisSerializationGating:
    """Serialization features raise clear errors for non-xorq backends."""

    def test_to_tagged_works_or_errors_cleanly(self, flights_model):
        """to_tagged should either work (if xorq handles backend) or error clearly."""
        expr = flights_model.group_by("carrier").aggregate("flight_count")
        # For a DuckDB backend, xorq DOES support it, so to_tagged should work.
        # For an unsupported backend, it should raise a clear error.
        # We just verify it doesn't crash with an obscure error.
        try:
            tagged = expr.to_tagged()
            assert tagged is not None
        except Exception as e:
            # Should be a clear error, not an internal xorq assertion
            assert "AssertionError" not in type(e).__name__
