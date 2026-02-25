"""
Tests for the query interface with filters and time dimensions.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def flights_data(con):
    """Sample flights data."""
    df = pd.DataFrame(
        {
            "carrier": ["AA", "UA", "DL", "AA", "UA", "DL"] * 5,
            "origin": ["JFK", "SFO", "LAX", "ORD", "DEN", "ATL"] * 5,
            "distance": [100, 200, 300, 150, 250, 350] * 5,
            "passengers": [50, 75, 100, 60, 80, 110] * 5,
        },
    )
    return con.create_table("flights", df)


@pytest.fixture(scope="module")
def sales_data(con):
    """Sample sales data with timestamps."""
    df = pd.DataFrame(
        {
            "order_date": pd.date_range("2024-01-01", periods=100, freq="D"),
            "amount": [100 + i * 10 for i in range(100)],
            "quantity": [1 + i % 5 for i in range(100)],
        },
    )
    return con.create_table("sales", df)


class TestBasicQuery:
    """Test basic query functionality."""

    def test_simple_query(self, flights_data):
        """Test basic query with dimensions and measures."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
        ).execute()

        assert len(result) == 3
        assert "carrier" in result.columns
        assert "total_passengers" in result.columns

    def test_query_without_dimensions(self, flights_data):
        """Test query with only measures (grand total)."""
        st = to_semantic_table(flights_data, "flights").with_measures(
            total_passengers=lambda t: t.passengers.sum(),
        )

        result = st.query(measures=["total_passengers"]).execute()

        assert len(result) == 1
        assert "total_passengers" in result.columns

    def test_query_with_model_prefixed_fields_on_standalone_model(self, flights_data):
        """Test query() resolves model-prefixed fields for standalone models."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["flights.carrier"],
            measures=["flights.total_passengers"],
            order_by=[("flights.total_passengers", "desc")],
        ).execute()

        assert len(result) == 3
        assert "carrier" in result.columns
        assert "total_passengers" in result.columns

    def test_query_does_not_strip_non_matching_prefix(self, flights_data):
        """Test query() keeps unknown prefixes and surfaces a field error."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(Exception, match="wrong.carrier"):
            st.query(
                dimensions=["wrong.carrier"],
                measures=["flights.total_passengers"],
            ).execute()

    def test_query_with_order_by(self, flights_data):
        """Test query with ordering."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            order_by=[("total_passengers", "desc")],
        ).execute()

        assert result["total_passengers"].iloc[0] >= result["total_passengers"].iloc[1]

    def test_query_with_limit(self, flights_data):
        """Test query with limit."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            limit=2,
        ).execute()

        assert len(result) == 2

    def test_aggregate_without_group_by(self, flights_data):
        """Test calling aggregate() directly without group_by() first."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(
                total_passengers=lambda t: t.passengers.sum(),
                avg_distance=lambda t: t.distance.mean(),
            )
        )

        # Should be able to call aggregate() directly
        result = st.aggregate("total_passengers", "avg_distance").execute()

        assert len(result) == 1
        assert "total_passengers" in result.columns
        assert "avg_distance" in result.columns
        # Check the actual values (5 repetitions of each passenger count)
        assert result["total_passengers"].iloc[0] == 5 * (50 + 75 + 100 + 60 + 80 + 110)
        # Average distance should be the mean of all distance values
        assert result["avg_distance"].iloc[0] == pytest.approx(225.0)

    def test_aggregate_with_single_measure(self, flights_data):
        """Test aggregate() with a single measure name."""
        st = to_semantic_table(flights_data, "flights").with_measures(
            flight_count=lambda t: t.count(),
        )

        result = st.aggregate("flight_count").execute()

        assert len(result) == 1
        assert "flight_count" in result.columns
        assert result["flight_count"].iloc[0] == 30  # 6 carriers * 5 repetitions


class TestMultiLayerDerivedFields:
    """Regression tests for multi-layer derived dimensions/measures in query()."""

    def test_query_multilayer_dimension_without_measures(self):
        df = pd.DataFrame(
            {
                "year": [2020, 2020, 2021, 2021],
                "distance": [100, 120, 200, 220],
            }
        )
        tbl = ibis.memtable(df)

        st = to_semantic_table(tbl, "flights").with_dimensions(
            year=lambda t: t.year,
            year_plus_one=lambda t: t.year + 1,
            year_plus_two=lambda t: t.year_plus_one + 1,
        )

        sql = st.query(dimensions=["year_plus_two"]).sql()

        assert "year_plus_two" in sql
        assert "SELECT *" not in sql

    def test_query_multilayer_dimension_with_measure(self):
        df = pd.DataFrame(
            {
                "year": [2020, 2020, 2021, 2021],
                "distance": [100, 120, 200, 220],
            }
        )
        tbl = ibis.memtable(df)

        st = to_semantic_table(tbl, "flights").with_dimensions(
            year=lambda t: t.year,
            year_plus_one=lambda t: t.year + 1,
            year_plus_two=lambda t: t.year_plus_one + 1,
        )
        st = st.with_measures(total_flights=lambda t: t.count())

        sql = st.query(
            dimensions=["year_plus_two"],
            measures=["total_flights"],
        ).sql()

        assert "year_plus_two" in sql
        assert "total_flights" in sql

    def test_query_multilayer_calculated_measure(self):
        df = pd.DataFrame(
            {
                "distance": [100, 200, 300, 150, 250, 350] * 5,
            }
        )
        tbl = ibis.memtable(df)

        st = to_semantic_table(tbl, "flights").with_measures(
            total_flights=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_distance_per_flight=lambda t: t.total_distance / t.total_flights + 1,
            avg_dist_plus_one=lambda t: t.avg_distance_per_flight + 1,
        )

        sql = st.query(measures=["avg_dist_plus_one"]).sql()

        assert "avg_dist_plus_one" in sql
        assert "total_distance" in sql
        assert "total_flights" in sql

    def test_query_multilayer_calculated_measure_method_chain(self):
        """Method-style calc chains (e.g. .add(1)) should resolve transitively."""
        df = pd.DataFrame(
            {
                "distance": [100, 200, 300, 150, 250, 350] * 5,
            }
        )
        tbl = ibis.memtable(df)

        st = to_semantic_table(tbl, "flights").with_measures(
            total_flights=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_distance_per_flight=lambda t: t.total_distance / t.total_flights,
            avg_dist_plus_one=lambda t: t.avg_distance_per_flight.add(1),
            avg_dist_plus_two=lambda t: t.avg_dist_plus_one.add(1),
        )

        sql_one = st.query(measures=["avg_dist_plus_one"]).sql()
        sql_two = st.query(measures=["avg_dist_plus_two"]).sql()

        assert "avg_dist_plus_one" in sql_one
        assert "avg_dist_plus_two" in sql_two
        assert "total_distance" in sql_two
        assert "total_flights" in sql_two


class TestFilters:
    """Test filter functionality."""

    def test_lambda_filter(self, flights_data):
        """Test query with lambda filter."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[lambda t: t.distance > 200],
        ).execute()

        assert len(result) > 0

    def test_json_filter_simple(self, flights_data):
        """Test query with JSON dict filter."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[{"field": "distance", "operator": ">", "value": 200}],
        ).execute()

        assert len(result) > 0

    def test_json_filter_in_operator(self, flights_data):
        """Test JSON filter with 'in' operator."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[{"field": "carrier", "operator": "in", "values": ["AA", "UA"]}],
        ).execute()

        assert len(result) == 2
        assert all(c in ["AA", "UA"] for c in result["carrier"])

    def test_json_filter_compound_and(self, flights_data):
        """Test JSON filter with compound AND."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "distance", "operator": ">", "value": 150},
                        {"field": "passengers", "operator": ">=", "value": 75},
                    ],
                },
            ],
        ).execute()

        assert len(result) > 0

    def test_multiple_filters(self, flights_data):
        """Test query with multiple filters."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                lambda t: t.distance > 100,
                {"field": "passengers", "operator": ">=", "value": 60},
            ],
        ).execute()

        assert len(result) > 0

    def test_json_filter_or_operator(self, flights_data):
        """Test JSON filter with OR operator."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": "carrier", "operator": "=", "value": "AA"},
                        {"field": "carrier", "operator": "=", "value": "UA"},
                    ],
                },
            ],
        ).execute()

        assert len(result) == 2
        assert all(c in ["AA", "UA"] for c in result["carrier"])

    def test_json_filter_nested_compound(self, flights_data):
        """Test nested compound filters (OR containing AND)."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                {
                    "operator": "OR",
                    "conditions": [
                        {
                            "operator": "AND",
                            "conditions": [
                                {"field": "carrier", "operator": "=", "value": "AA"},
                                {"field": "distance", "operator": ">", "value": 100},
                            ],
                        },
                        {"field": "carrier", "operator": "=", "value": "DL"},
                    ],
                },
            ],
        ).execute()

        assert len(result) > 0


class TestFiltersWithJoins:
    """Test filters on joined dimensions."""

    @pytest.fixture(scope="class")
    def joined_model(self, con):
        """Create models with joins for testing."""
        # Create customers table
        customers_df = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "country": ["US", "UK", "US"],
                "tier": ["gold", "silver", "gold"],
            }
        )
        customers_tbl = con.create_table("customers_for_filter", customers_df, overwrite=True)

        # Create orders table
        orders_df = pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104, 105],
                "customer_id": [1, 2, 1, 3, 2],
                "amount": [100, 200, 150, 300, 250],
            }
        )
        orders_tbl = con.create_table("orders_for_filter", orders_df, overwrite=True)

        # Create semantic tables
        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
                country=lambda t: t.country,
                tier=lambda t: t.tier,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(total_amount=lambda t: t.amount.sum(), order_count=lambda t: t.count())
        )

        # Join orders with customers
        return orders_st.join_one(customers_st, lambda o, c: o.customer_id == c.customer_id)

    def test_filter_on_joined_dimension(self, joined_model):
        """Test filtering on a dimension from joined table."""
        result = (
            joined_model.query(
                dimensions=["customers.name"],
                measures=["orders.total_amount"],
                filters=[{"field": "customers.country", "operator": "=", "value": "US"}],
            )
            .execute()
            .reset_index(drop=True)
        )

        # Should only include Alice and Charlie (US customers)
        assert len(result) == 2
        assert all(name in ["Alice", "Charlie"] for name in result["customers.name"])

    def test_filter_on_multiple_joined_dimensions(self, joined_model):
        """Test filtering on multiple dimensions from joined table."""
        result = (
            joined_model.query(
                dimensions=["customers.name"],
                measures=["orders.total_amount"],
                filters=[
                    {"field": "customers.country", "operator": "=", "value": "US"},
                    {"field": "customers.tier", "operator": "=", "value": "gold"},
                ],
            )
            .execute()
            .reset_index(drop=True)
        )

        # Should only include Alice and Charlie (US gold customers)
        assert len(result) == 2

    def test_filter_lambda_on_joined_table(self, joined_model):
        """Test lambda filter accessing joined table columns."""
        result = (
            joined_model.filter(lambda t: t.country == "US")
            .group_by("customers.name")
            .aggregate("orders.total_amount")
            .execute()
            .reset_index(drop=True)
        )

        # Should only include US customers
        assert len(result) == 2

    def test_filter_lambda_with_chained_access(self, joined_model):
        """Test lambda filter using chained access like t.customers.country."""
        # This tests the t.table_name.column_name pattern for joined models
        result = (
            joined_model.filter(lambda t: t.customers.country == "US")
            .group_by("customers.name")
            .aggregate("orders.total_amount")
            .execute()
            .reset_index(drop=True)
        )

        # Should only include US customers (Alice and Charlie)
        assert len(result) == 2
        assert all(name in ["Alice", "Charlie"] for name in result["customers.name"])

    def test_filter_lambda_with_chained_access_isin(self, joined_model):
        """Test lambda filter using chained access with isin()."""
        result = (
            joined_model.filter(lambda t: t.customers.tier.isin(["gold"]))
            .group_by("customers.name")
            .aggregate("orders.total_amount")
            .execute()
            .reset_index(drop=True)
        )

        # Should only include gold tier customers (Alice and Charlie)
        assert len(result) == 2
        assert all(name in ["Alice", "Charlie"] for name in result["customers.name"])


class TestJoinedModelChainedAccess:
    """Test chained attribute access for joined models (t.table.column)."""

    @pytest.fixture(scope="class")
    def con(self):
        return ibis.duckdb.connect(":memory:")

    @pytest.fixture(scope="class")
    def triple_joined_model(self, con):
        """Create a model with two levels of joins: orders -> customers -> regions."""
        # Create regions table
        regions_df = pd.DataFrame(
            {
                "region_id": [1, 2],
                "region_name": ["North", "South"],
                "country": ["US", "US"],
            }
        )
        regions_tbl = con.create_table("regions_deep", regions_df, overwrite=True)

        # Create customers table with region_id
        customers_df = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "region_id": [1, 2, 1],
            }
        )
        customers_tbl = con.create_table("customers_deep", customers_df, overwrite=True)

        # Create orders table
        orders_df = pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104],
                "customer_id": [1, 2, 1, 3],
                "amount": [100, 200, 150, 300],
            }
        )
        orders_tbl = con.create_table("orders_deep", orders_df, overwrite=True)

        # Create semantic tables
        regions_st = (
            to_semantic_table(regions_tbl, name="regions")
            .with_dimensions(
                region_id=lambda t: t.region_id,
                region_name=lambda t: t.region_name,
                country=lambda t: t.country,
            )
            .with_measures(region_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
                region_id=lambda t: t.region_id,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(total_amount=lambda t: t.amount.sum(), order_count=lambda t: t.count())
        )

        # Join: orders -> customers -> regions
        customers_with_regions = customers_st.join_one(
            regions_st, lambda c, r: c.region_id == r.region_id
        )
        return orders_st.join_one(
            customers_with_regions, lambda o, c: o.customer_id == c.customer_id
        )

    def test_joined_dimensions_available(self, triple_joined_model):
        """Test that joined dimensions are available with table prefixes."""
        dims = triple_joined_model.dimensions
        # Should have dimensions from all three tables
        assert "orders.order_id" in dims
        assert "customers.name" in dims
        assert "regions.region_name" in dims

    def test_filter_with_chained_access(self, triple_joined_model):
        """Test lambda filter with t.regions.region_name chained access.

        Joins flatten dimensions to {table_name}.{column}, so t.regions.region_name
        resolves to the "regions.region_name" dimension.
        """
        result = (
            triple_joined_model.filter(lambda t: t.regions.region_name == "North")
            .group_by("customers.name")
            .aggregate("orders.total_amount")
            .execute()
            .reset_index(drop=True)
        )

        # Should only include customers from North region (Alice and Charlie)
        assert len(result) == 2
        assert all(name in ["Alice", "Charlie"] for name in result["customers.name"])

    def test_filter_with_chained_access_isin(self, triple_joined_model):
        """Test lambda filter with chained access using isin()."""
        result = (
            triple_joined_model.filter(lambda t: t.regions.country.isin(["US"]))
            .group_by("regions.region_name")
            .aggregate("orders.order_count")
            .execute()
            .reset_index(drop=True)
        )

        # All orders are from US, so should have results for both regions
        assert len(result) == 2

    def test_filter_with_multiple_tables_in_same_query(self, triple_joined_model):
        """Test filtering on dimensions from multiple joined tables."""
        result = (
            triple_joined_model.filter(
                lambda t: ibis.and_(
                    t.regions.region_name == "North", t.customers.name.isin(["Alice", "Charlie"])
                )
            )
            .group_by("customers.name")
            .aggregate("orders.total_amount")
            .execute()
            .reset_index(drop=True)
        )

        # Should only include Alice and Charlie from North region
        assert len(result) == 2
        assert all(name in ["Alice", "Charlie"] for name in result["customers.name"])


class TestTimeDimensions:
    """Test time dimension functionality."""

    def test_time_dimension_metadata(self, sales_data):
        """Test that time dimensions can be defined with metadata."""
        st = to_semantic_table(sales_data, "sales").with_dimensions(
            order_date={
                "expr": lambda t: t.order_date,
                "description": "Date of order",
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )

        dims_dict = st.get_dimensions()
        assert dims_dict["order_date"].is_time_dimension is True
        assert dims_dict["order_date"].smallest_time_grain == "day"

    def test_time_grain_month(self, sales_data):
        """Test querying with monthly time grain."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        assert len(result) <= 4
        assert "order_date" in result.columns
        assert "total_amount" in result.columns

    def test_time_range_filter(self, sales_data):
        """Test querying with time range filter."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_range={"start": "2024-01-01", "end": "2024-01-31"},
        ).execute()

        assert len(result) <= 31

    def test_time_grain_validation(self, sales_data):
        """Test that requesting finer grain than allowed raises error."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "month",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        with pytest.raises(ValueError, match="finer than the smallest allowed grain"):
            st.query(
                dimensions=["order_date"],
                measures=["total_amount"],
                time_grain="TIME_GRAIN_DAY",
            ).execute()

    def test_time_range_without_time_dimension_fails(self, flights_data):
        """Test that time_range without a time dimension raises a clear error."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(
            ValueError,
            match="time_range filter requires a time dimension in the query dimensions",
        ):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                time_range={"start": "2024-01-01", "end": "2024-12-31"},
            )

    def test_time_grain_with_deferred_expression(self, sales_data):
        """Test time_grain works with Deferred expressions (regression test).

        This tests that using ibis._.column (Deferred) instead of lambda t: t.column
        works correctly with time_grain. Previously, capturing dim_obj.expr directly
        and calling orig(t) caused infinite recursion because Deferred.__call__
        doesn't resolve properly. The fix uses dim(t) which calls Dimension.__call__
        to properly resolve Deferred expressions.
        """
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": ibis._.order_date,  # Deferred expression, not lambda
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        # Should have 4 months max (Jan-Apr 2024 for 100 days from Jan 1)
        assert len(result) <= 4
        assert "order_date" in result.columns
        assert "total_amount" in result.columns

    def test_time_range_with_deferred_expression(self, sales_data):
        """Test time_range works with Deferred expressions (regression test).

        This tests that using ibis._.column (Deferred) instead of lambda t: t.column
        works correctly with time_range filters. Ensures Deferred expressions are
        compatible with timestamp literal comparisons for time range filtering.
        """
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": ibis._.order_date,  # Deferred expression, not lambda
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_range={"start": "2024-01-01", "end": "2024-01-31"},
        ).execute()

        # Should have at most 31 days in January
        assert len(result) <= 31
        assert "order_date" in result.columns
        assert "total_amount" in result.columns

    def test_time_grain_and_time_range_combined_with_deferred(self, sales_data):
        """Test both time_grain and time_range together with Deferred expressions.

        This is a comprehensive regression test ensuring time_grain and time_range
        work correctly when dimensions use Deferred expressions (ibis._) syntax.
        """
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": ibis._.order_date,  # Deferred expression
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
            time_range={"start": "2024-01-01", "end": "2024-03-31"},
        ).execute()

        # Should have at most 3 months (Jan, Feb, Mar)
        assert len(result) <= 3
        assert "order_date" in result.columns
        assert "total_amount" in result.columns


class TestFilterErrorHandling:
    """Test error handling in filter validation."""

    def test_invalid_operator(self, flights_data):
        """Test that invalid operator raises ValueError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(ValueError, match="Unsupported operator"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"field": "carrier", "operator": "INVALID", "value": "AA"}],
            ).execute()

    def test_missing_field_in_filter(self, flights_data):
        """Test that missing 'field' key raises KeyError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(KeyError, match="Missing required keys"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"operator": "=", "value": "AA"}],
            ).execute()

    def test_missing_operator_in_filter(self, flights_data):
        """Test that missing 'operator' key raises KeyError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(KeyError, match="Missing required keys"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"field": "carrier", "value": "AA"}],
            ).execute()

    def test_in_operator_without_values(self, flights_data):
        """Test that 'in' operator without 'values' field raises ValueError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(ValueError, match="requires 'values' field"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"field": "carrier", "operator": "in", "value": "AA"}],
            ).execute()

    def test_comparison_operator_without_value(self, flights_data):
        """Test that comparison operator without 'value' field raises ValueError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(ValueError, match="requires 'value' field"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"field": "distance", "operator": ">"}],
            ).execute()

    def test_is_null_with_value_field(self, flights_data):
        """Test that 'is null' operator with 'value' field raises ValueError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(ValueError, match="should not have 'value' or 'values' fields"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"field": "carrier", "operator": "is null", "value": "AA"}],
            ).execute()

    def test_empty_conditions_in_compound_filter(self, flights_data):
        """Test that compound filter with empty conditions raises ValueError."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        with pytest.raises(ValueError, match="must have non-empty conditions list"):
            st.query(
                dimensions=["carrier"],
                measures=["total_passengers"],
                filters=[{"operator": "AND", "conditions": []}],
            ).execute()


class TestFilterEdgeCases:
    """Test edge cases with special values in filters."""

    def test_empty_string_equality(self, con):
        """Test filtering with empty string value."""
        df = pd.DataFrame({"name": ["Alice", "", "Bob"], "value": [1, 2, 3]})
        tbl = con.create_table("test_empty_string", df, overwrite=True)
        st = (
            to_semantic_table(tbl, "test")
            .with_dimensions(name=lambda t: t.name)
            .with_measures(total=lambda t: t.value.sum())
        )

        result = st.query(
            dimensions=["name"],
            measures=["total"],
            filters=[{"field": "name", "operator": "=", "value": ""}],
        ).execute()

        assert len(result) == 1
        assert result["name"].iloc[0] == ""
        assert result["total"].iloc[0] == 2

    def test_empty_list_for_in_operator(self, flights_data):
        """Test 'in' operator with empty list returns no rows."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[{"field": "carrier", "operator": "in", "values": []}],
        ).execute()

        assert len(result) == 0

    def test_special_characters_in_value(self, con):
        """Test that special SQL characters are treated literally in equality."""
        df = pd.DataFrame({"name": ["test%", "test_", "test"], "value": [1, 2, 3]})
        tbl = con.create_table("test_special_chars", df, overwrite=True)
        st = (
            to_semantic_table(tbl, "test")
            .with_dimensions(name=lambda t: t.name)
            .with_measures(total=lambda t: t.value.sum())
        )

        # % should be treated literally, not as wildcard
        result = st.query(
            dimensions=["name"],
            measures=["total"],
            filters=[{"field": "name", "operator": "=", "value": "test%"}],
        ).execute()

        assert len(result) == 1
        assert result["name"].iloc[0] == "test%"


class TestUntestedOperators:
    """Test operators that were previously not tested."""

    @pytest.fixture(scope="class")
    def test_model(self, con):
        """Create a test model with various data types."""
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", None, "Charlie"],
                "value": [10, 20, 30, 40],
                "status": ["active", "inactive", "active", "active"],
            }
        )
        tbl = con.create_table("test_untested_ops", df, overwrite=True)
        return (
            to_semantic_table(tbl, "test")
            .with_dimensions(name=lambda t: t.name, status=lambda t: t.status)
            .with_measures(sum_value=lambda t: t.value.sum(), count=lambda t: t.count())
        )

    def test_not_equal_operator(self, test_model):
        """Test '!=' operator."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "status", "operator": "!=", "value": "active"}],
            )
            .execute()
            .reset_index(drop=True)
        )

        assert len(result) == 1
        assert result["name"].iloc[0] == "Bob"

    def test_less_than_operator(self, test_model):
        """Test '<' operator."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "value", "operator": "<", "value": 25}],
            )
            .execute()
            .reset_index(drop=True)
            .sort_values("name")
            .reset_index(drop=True)
        )

        assert len(result) == 2
        assert set(result["name"]) == {"Alice", "Bob"}

    def test_less_than_or_equal_operator(self, test_model):
        """Test '<=' operator."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "value", "operator": "<=", "value": 20}],
            )
            .execute()
            .reset_index(drop=True)
            .sort_values("name")
            .reset_index(drop=True)
        )

        assert len(result) == 2
        assert set(result["name"]) == {"Alice", "Bob"}

    def test_not_in_operator(self, test_model):
        """Test 'not in' operator."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "not in", "values": ["Alice", "Bob"]}],
            )
            .execute()
            .reset_index(drop=True)
        )

        assert len(result) == 1
        assert result["name"].iloc[0] == "Charlie"

    def test_like_operator(self, test_model):
        """Test 'like' operator (case-sensitive)."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "like", "value": "%li%"}],
            )
            .execute()
            .reset_index(drop=True)
            .sort_values("name")
            .reset_index(drop=True)
        )

        # Should match Alice and Charlie (case sensitive)
        assert len(result) == 2
        assert set(result["name"]) == {"Alice", "Charlie"}

    def test_not_like_operator(self, test_model):
        """Test 'not like' operator."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "not like", "value": "%li%"}],
            )
            .execute()
            .reset_index(drop=True)
        )

        assert len(result) == 1
        assert result["name"].iloc[0] == "Bob"

    def test_is_null_operator(self, test_model):
        """Test 'is null' operator."""
        result = (
            test_model.query(
                dimensions=["status"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "is null"}],
            )
            .execute()
            .reset_index(drop=True)
        )

        assert len(result) == 1
        assert result["sum_value"].iloc[0] == 30

    def test_is_not_null_operator(self, test_model):
        """Test 'is not null' operator."""
        result = (
            test_model.query(
                dimensions=["name"],
                measures=["sum_value"],
                filters=[{"field": "name", "operator": "is not null"}],
            )
            .execute()
            .reset_index(drop=True)
            .sort_values("name")
            .reset_index(drop=True)
        )

        assert len(result) == 3
        assert set(result["name"]) == {"Alice", "Bob", "Charlie"}


class TestCompoundFilterPatterns:
    """Test complex compound filter patterns."""

    def test_and_containing_or(self, flights_data):
        """Test AND containing OR conditions."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier, origin=lambda t: t.origin)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                {
                    "operator": "AND",
                    "conditions": [
                        {
                            "operator": "OR",
                            "conditions": [
                                {"field": "carrier", "operator": "=", "value": "AA"},
                                {"field": "carrier", "operator": "=", "value": "UA"},
                            ],
                        },
                        {"field": "distance", "operator": ">", "value": 150},
                    ],
                },
            ],
        ).execute()

        assert len(result) > 0
        assert all(c in ["AA", "UA"] for c in result["carrier"])

    def test_deeply_nested_filters(self, flights_data):
        """Test deeply nested compound filters (3 levels)."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                {
                    "operator": "OR",
                    "conditions": [
                        {
                            "operator": "AND",
                            "conditions": [
                                {"field": "carrier", "operator": "=", "value": "AA"},
                                {
                                    "operator": "OR",
                                    "conditions": [
                                        {"field": "distance", "operator": ">", "value": 200},
                                        {"field": "passengers", "operator": ">", "value": 70},
                                    ],
                                },
                            ],
                        },
                        {"field": "carrier", "operator": "=", "value": "DL"},
                    ],
                },
            ],
        ).execute()

        assert len(result) > 0


class TestFilterInteractions:
    """Test filter interactions with other operations."""

    def test_multiple_consecutive_filters(self, flights_data):
        """Test that multiple consecutive filters are ANDed correctly."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = (
            st.filter(lambda t: t.distance > 150)
            .filter(lambda t: t.passengers >= 75)
            .group_by("carrier")
            .aggregate("total_passengers")
            .execute()
        )

        assert len(result) > 0

    def test_filter_after_mutate(self, flights_data):
        """Test filtering on computed columns after mutate."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(
                total_passengers=lambda t: t.passengers.sum(),
                avg_distance=lambda t: t.distance.mean(),
            )
        )

        result = (
            st.group_by("carrier")
            .aggregate("total_passengers", "avg_distance")
            .mutate(passengers_per_mile=lambda t: t.total_passengers / t.avg_distance)
            .filter(lambda t: t.passengers_per_mile > 0.3)
            .execute()
        )

        assert len(result) > 0
        assert "passengers_per_mile" in result.columns


class TestDerivedTypeInference:
    """Test .type() on derived dimensions and measures (issue #175)."""

    def test_type_on_derived_dimensions(self, flights_data):
        st = to_semantic_table(flights_data, "flights").with_dimensions(
            dist_plus_one=lambda t: t.distance + 1,
            dist_plus_two=lambda t: t.dist_plus_one + 1,
        )
        assert str(st.distance.type()) == "int64"
        assert str(st.dist_plus_one.type()) == "int64"
        assert str(st.dist_plus_two.type()) == "int64"

    def test_type_on_base_columns_after_derived_dims(self, flights_data):
        st = to_semantic_table(flights_data, "flights").with_dimensions(
            dist_plus_one=lambda t: t.distance + 1,
        )
        # Base columns should still resolve after adding derived dims
        assert str(st.carrier.type()) == "string"
        assert str(st.distance.type()) == "int64"

    def test_type_on_base_measures(self, flights_data):
        st = to_semantic_table(flights_data, "flights").with_measures(
            total_passengers=lambda t: t.passengers.sum(),
            max_distance=lambda t: t.distance.max(),
        )
        assert str(st.total_passengers.type()) == "int64"
        assert str(st.max_distance.type()) == "int64"

    def test_type_on_derived_measures(self, flights_data):
        st = (
            to_semantic_table(flights_data, "flights")
            .with_measures(
                total_passengers=lambda t: t.passengers.sum(),
                total_distance=lambda t: t.distance.sum(),
            )
            .with_measures(
                passengers_per_distance=lambda t: t.total_passengers / t.total_distance,
            )
        )
        assert str(st.total_passengers.type()) == "int64"
        assert str(st.passengers_per_distance.type()) == "float64"


class TestMeasureFilters:
    """Test filtering by measures in query() (issue #177)."""

    def test_filter_measure_greater_than(self, flights_data):
        """Measure filter should apply as post-aggregation (HAVING)."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )
        result = st.query(
            dimensions=["carrier"],
            measures=["total_distance"],
            filters=[{"field": "total_distance", "operator": ">", "value": 1500}],
        ).execute()
        assert all(result["total_distance"] > 1500)

    def test_filter_measure_is_not_null(self, flights_data):
        """'is not null' on a measure should not error."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )
        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[{"field": "total_passengers", "operator": "is not null"}],
        ).execute()
        assert len(result) > 0

    def test_mixed_dimension_and_measure_filters(self, flights_data):
        """Dimension filters apply pre-agg, measure filters apply post-agg."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )
        # Filter: carrier != 'DL' AND total_distance > 1000
        result = st.query(
            dimensions=["carrier"],
            measures=["total_distance"],
            filters=[
                {"field": "carrier", "operator": "!=", "value": "DL"},
                {"field": "total_distance", "operator": ">", "value": 1000},
            ],
        ).execute()
        assert "DL" not in result["carrier"].values
        assert all(result["total_distance"] > 1000)

    def test_model_prefixed_measure_filter(self, flights_data):
        """Model-prefixed measure names should be detected as measure filters."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )
        result = st.query(
            dimensions=["flights.carrier"],
            measures=["flights.total_distance"],
            filters=[{"field": "flights.total_distance", "operator": ">", "value": 0}],
        ).execute()
        assert len(result) > 0
        assert all(result["total_distance"] > 0)

    def test_compound_measure_filter(self, flights_data):
        """Compound AND filter on measures should work post-aggregation."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )
        result = st.query(
            dimensions=["carrier"],
            measures=["total_distance"],
            filters=[{
                "operator": "AND",
                "conditions": [
                    {"field": "total_distance", "operator": ">", "value": 1000},
                    {"field": "total_distance", "operator": "<", "value": 5000},
                ],
            }],
        ).execute()
        assert all(result["total_distance"] > 1000)
        assert all(result["total_distance"] < 5000)

    def test_dimension_filter_still_pre_aggregation(self, flights_data):
        """Dimension filters should still be applied before aggregation."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )
        # Filter only AA rows, then aggregate — should only count AA distances
        all_result = st.query(
            dimensions=["carrier"], measures=["total_distance"]
        ).execute()
        filtered_result = st.query(
            dimensions=["carrier"],
            measures=["total_distance"],
            filters=[{"field": "carrier", "operator": "=", "value": "AA"}],
        ).execute()
        assert len(filtered_result) == 1
        assert filtered_result["carrier"].iloc[0] == "AA"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
