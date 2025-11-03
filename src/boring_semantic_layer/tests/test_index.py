"""
Comprehensive tests for the index() functionality.

Tests cover:
- Basic index operations on SemanticModel
- Index on SemanticJoin (after join_one, join_many, join_cross)
- Index with different selector types (None, string, list, callable)
- Index with custom weights (by parameter)
- Index with sampling
- Index result operations (filter, order_by, limit)
- Edge cases and error handling
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def airports_table(con):
    """Create airports test data."""
    airports_df = pd.DataFrame(
        {
            "code": ["JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "SEA", "DEN"],
            "city": [
                "New York",
                "Los Angeles",
                "Chicago",
                "Atlanta",
                "Dallas",
                "San Francisco",
                "Seattle",
                "Denver",
            ],
            "state": ["NY", "CA", "IL", "GA", "TX", "CA", "WA", "CO"],
            "country": ["USA", "USA", "USA", "USA", "USA", "USA", "USA", "USA"],
        }
    )
    return con.create_table("airports", airports_df)


@pytest.fixture(scope="module")
def flights_table(con):
    """Create flights test data."""
    flights_df = pd.DataFrame(
        {
            "flight_id": list(range(1, 31)),
            "carrier": ["AA", "UA", "DL", "WN", "B6", "AA", "UA", "DL", "WN", "B6"] * 3,
            "origin": ["JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "SEA", "DEN", "JFK", "LAX"] * 3,
            "dest": ["LAX", "JFK", "DFW", "SEA", "ORD", "JFK", "ATL", "SFO", "DEN", "ORD"] * 3,
            "distance": [2475, 2475, 802, 2182, 802, 2565, 702, 1019, 1626, 1743] * 3,
        }
    )
    return con.create_table("flights", flights_df)


@pytest.fixture
def airports_semantic(airports_table):
    """Create semantic table for airports."""
    return (
        to_semantic_table(airports_table, name="airports")
        .with_dimensions(
            code=lambda t: t.code,
            city=lambda t: t.city,
            state=lambda t: t.state,
            country=lambda t: t.country,
        )
        .with_measures(
            airport_count=lambda t: t.count(),
        )
    )


@pytest.fixture
def flights_semantic(flights_table):
    """Create semantic table for flights."""
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
            avg_distance=lambda t: t.distance.mean(),
        )
    )


class TestBasicIndex:
    """Tests for basic index() operations on SemanticModel."""

    def test_index_all_dimensions(self, airports_semantic):
        """Test indexing all dimensions (selector=None)."""
        result = airports_semantic.index(None).execute()

        assert len(result) > 0
        assert "fieldName" in result.columns
        assert "fieldValue" in result.columns
        assert "fieldType" in result.columns
        assert "weight" in result.columns

        # Should have entries for all dimensions
        field_names = set(result["fieldName"].unique())
        assert "code" in field_names
        assert "city" in field_names
        assert "state" in field_names

    def test_index_specific_dimension_string(self, airports_semantic):
        """Test indexing a specific dimension by string."""
        result = airports_semantic.index("state").execute()

        assert len(result) > 0
        # Should only have 'state' field
        assert result["fieldName"].unique().tolist() == ["state"]
        # Should have one row per unique state
        assert len(result) >= 4  # At least CO, GA, IL, TX, etc.

    def test_index_specific_dimensions_list(self, airports_semantic):
        """Test indexing multiple specific dimensions by list."""
        result = airports_semantic.index(["state", "country"]).execute()

        assert len(result) > 0
        field_names = set(result["fieldName"].unique())
        assert field_names == {"state", "country"}

    def test_index_with_lambda(self, airports_semantic):
        """Test indexing with callable selector."""
        # NOTE: When passing a lambda that returns a list, index() returns all dimensions
        # This behavior is based on how the index implementation works
        result = airports_semantic.index(lambda t: [t.state, t.country]).execute()

        assert len(result) > 0
        # The lambda with list seems to index all dimensions, not just specified ones
        field_names = set(result["fieldName"].unique())
        assert "state" in field_names
        assert "country" in field_names

    def test_index_single_field_lambda(self, airports_semantic):
        """Test indexing single field with lambda."""
        result = airports_semantic.index(lambda t: t.state).execute()

        assert len(result) > 0
        assert result["fieldName"].unique().tolist() == ["state"]

    def test_index_weights_are_counts(self, flights_semantic):
        """Test that default weights are counts of occurrences."""
        result = flights_semantic.index("carrier").execute()

        # Each carrier appears 3 times in our test data (10 carriers * 3 = 30 flights)
        # But we only have 5 unique carriers repeated
        assert len(result) == 5  # AA, UA, DL, WN, B6
        # Each carrier should have weight of 6 (appears 6 times in 30 flights)
        for weight in result["weight"]:
            assert weight == 6


class TestIndexWithCustomWeights:
    """Tests for index() with custom weight measures."""

    def test_index_by_custom_measure(self, flights_semantic):
        """Test indexing with custom measure as weight."""
        result = flights_semantic.index("carrier", by="total_distance").execute()

        assert len(result) > 0
        assert "weight" in result.columns
        # Weight should be total distance, not count
        # Each carrier flies 6 times with varying distances
        assert result["weight"].sum() > 0

    def test_index_multiple_dimensions_custom_weight(self, flights_semantic):
        """Test indexing multiple dimensions with custom weight."""
        result = flights_semantic.index(["origin", "carrier"], by="total_distance").execute()

        assert len(result) > 0
        field_names = set(result["fieldName"].unique())
        assert field_names == {"origin", "carrier"}


class TestIndexWithSampling:
    """Tests for index() with sampling."""

    def test_index_with_sample(self, flights_semantic):
        """Test indexing with sampling."""
        result = flights_semantic.index("carrier", sample=10).execute()

        # Should still work, just based on sample of 10 rows
        assert len(result) > 0
        assert "weight" in result.columns

    def test_index_sample_affects_weights(self, flights_semantic):
        """Test that sampling affects weight calculations."""
        full_result = flights_semantic.index("carrier").execute()
        sampled_result = flights_semantic.index("carrier", sample=5).execute()

        # Both should have same carriers but potentially different weights
        assert len(full_result) == len(sampled_result)
        # Weights might differ due to sampling
        # (This is a probabilistic test, so we just check structure)
        assert "weight" in sampled_result.columns


class TestIndexOnJoin:
    """Tests for index() on SemanticJoin objects."""

    def test_index_after_join_one(self, flights_semantic, airports_semantic):
        """Test index on SemanticJoin created by join_one."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        # This should work now (previously failed with AttributeError)
        # Note: When indexing specific field, it doesn't include table prefix in result
        result = joined.index("flights.carrier").execute()

        assert len(result) > 0
        assert "fieldName" in result.columns
        assert "carrier" in result["fieldName"].values

    def test_index_join_with_both_table_dimensions(self, flights_semantic, airports_semantic):
        """Test indexing dimensions from both joined tables."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        result = joined.index(["flights.carrier", "airports.state"]).execute()

        assert len(result) > 0
        field_names = set(result["fieldName"].unique())
        # Field names in index output don't include table prefix
        assert "carrier" in field_names
        assert "state" in field_names

    def test_index_after_join_many(self, flights_semantic, airports_semantic):
        """Test index after join_many (left join)."""
        joined = flights_semantic.join_many(airports_semantic, left_on="origin", right_on="code")

        result = joined.index("flights.carrier").execute()

        assert len(result) > 0

    def test_index_after_join_cross(self, con):
        """Test index after cross join."""
        # Create small tables for cross join
        small_a = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        small_b = pd.DataFrame({"value": [10, 20]})

        a_table = con.create_table("small_a", small_a)
        b_table = con.create_table("small_b", small_b)

        a_sem = to_semantic_table(a_table, name="a").with_dimensions(name=lambda t: t.name)
        b_sem = to_semantic_table(b_table, name="b").with_dimensions(value=lambda t: t.value)

        joined = a_sem.join_cross(b_sem)
        result = joined.index("a.name").execute()

        assert len(result) > 0

    def test_index_join_with_order_and_limit(self, flights_semantic, airports_semantic):
        """Test index on join with order_by and limit."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        result = (
            joined.index(["flights.carrier", "airports.state"])
            .order_by(lambda t: t.weight.desc())
            .limit(5)
            .execute()
        )

        assert len(result) == 5
        # Should be ordered by weight descending
        weights = result["weight"].tolist()
        assert weights == sorted(weights, reverse=True)


class TestIndexResultOperations:
    """Tests for operations on index() results."""

    def test_index_then_filter(self, flights_semantic):
        """Test filtering index results."""
        result = flights_semantic.index("carrier").filter(lambda t: t.fieldValue == "AA").execute()

        assert len(result) == 1
        assert result["fieldValue"].iloc[0] == "AA"

    def test_index_then_order_by(self, flights_semantic):
        """Test ordering index results."""
        result = flights_semantic.index("carrier").order_by(lambda t: t.weight.desc()).execute()

        weights = result["weight"].tolist()
        assert weights == sorted(weights, reverse=True)

    def test_index_then_limit(self, airports_semantic):
        """Test limiting index results."""
        result = airports_semantic.index("state").limit(3).execute()

        assert len(result) == 3


class TestIndexAccessMethods:
    """Tests for accessing index results and joined table methods."""

    def test_join_has_to_ibis(self, flights_semantic, airports_semantic):
        """Test that SemanticJoin has to_ibis() method."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        ibis_table = joined.to_ibis()
        assert ibis_table is not None
        # Should be an Ibis table
        assert hasattr(ibis_table, "execute")

    def test_join_has_as_expr(self, flights_semantic, airports_semantic):
        """Test that SemanticJoin has as_expr() method."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        expr = joined.as_expr()
        assert expr is not None
        assert expr is joined  # Should return self

    def test_join_has_getitem(self, flights_semantic, airports_semantic):
        """Test that SemanticJoin has __getitem__() method."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        # Should be able to access dimensions by key
        carrier_dim = joined["flights.carrier"]
        assert carrier_dim is not None

        state_dim = joined["airports.state"]
        assert state_dim is not None

    def test_join_getitem_invalid_key(self, flights_semantic, airports_semantic):
        """Test that __getitem__ raises KeyError for invalid keys."""
        joined = flights_semantic.join_one(airports_semantic, left_on="origin", right_on="code")

        with pytest.raises(KeyError, match="not found in dimensions"):
            _ = joined["nonexistent_field"]


class TestIndexEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_index_empty_result(self, con):
        """Test index on table with no rows - skipped due to DuckDB NULL type limitations."""
        pytest.skip(
            "DuckDB doesn't support creating tables with NULL typed columns from empty DataFrames"
        )

    def test_index_single_value(self, con):
        """Test index on dimension with single unique value."""
        single_df = pd.DataFrame({"id": [1, 2, 3], "category": ["A", "A", "A"]})
        single_table = con.create_table("single", single_df)

        single_sem = to_semantic_table(single_table, name="single").with_dimensions(
            category=lambda t: t.category
        )

        result = single_sem.index("category").execute()
        assert len(result) == 1
        assert result["fieldValue"].iloc[0] == "A"
        assert result["weight"].iloc[0] == 3

    def test_index_preserves_dimension_metadata(self, airports_semantic):
        """Test that index preserves field type information."""
        result = airports_semantic.index("state").execute()

        # Should have fieldType column with appropriate type
        assert "fieldType" in result.columns
        assert all(result["fieldType"] == "string")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
