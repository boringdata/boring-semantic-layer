"""
Test .unnest() method for working with nested/array data.

The unnest() method allows expanding array columns into separate rows,
which is essential for working with nested data structures like Google Analytics
sessions with nested hits.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


def test_basic_unnest():
    """Test basic unnesting of an array column."""
    con = ibis.duckdb.connect(":memory:")

    # Create a table with an array column
    data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "values": [[10, 20], [30], [40, 50, 60]],
        }
    )
    tbl = con.create_table("test_data", data)

    # Create semantic table and unnest the array
    semantic = to_semantic_table(tbl, name="test")
    unnested = semantic.unnest("values")

    # Execute and check results
    result = unnested.execute()

    # Should have 6 rows (2 + 1 + 3)
    assert len(result) == 6

    # Check that values were properly unnested
    assert result["values"].tolist() == [10, 20, 30, 40, 50, 60]

    # Each row should still have its id
    assert sorted(result["id"].tolist()) == [1, 1, 2, 3, 3, 3]


def test_unnest_with_measures():
    """Test unnesting followed by aggregation with measures."""
    con = ibis.duckdb.connect(":memory:")

    # Create nested data similar to GA sessions with hits
    sessions = pd.DataFrame(
        {
            "session_id": [1, 2, 3],
            "user_id": ["user_a", "user_b", "user_a"],
            "hit_values": [[10, 20, 30], [40, 50], [60]],
        }
    )
    tbl = con.create_table("sessions", sessions)

    # Create semantic model and unnest
    unnested = to_semantic_table(tbl, name="ga_sessions").unnest("hit_values")

    # Execute to verify unnesting worked
    result = unnested.execute()

    assert len(result) == 6  # 3 + 2 + 1 hits
    assert result["hit_values"].sum() == 210  # 10+20+30+40+50+60
    assert result["session_id"].nunique() == 3
    assert result["user_id"].nunique() == 2


def test_unnest_with_groupby():
    """Test grouping by dimensions after unnesting."""
    con = ibis.duckdb.connect(":memory:")

    # Create data with nested structure
    data = pd.DataFrame(
        {
            "category": ["A", "B", "A"],
            "items": [[1, 2], [3, 4, 5], [6]],
        }
    )
    tbl = con.create_table("data", data)

    # Unnest
    unnested = to_semantic_table(tbl, name="data").unnest("items")

    # Execute and verify grouping works
    result = unnested.execute()

    assert len(result) == 6  # 2 + 3 + 1

    # Verify we can aggregate the unnested data
    category_a_sum = result[result.category == "A"]["items"].sum()
    category_b_sum = result[result.category == "B"]["items"].sum()

    assert category_a_sum == 9  # 1+2+6
    assert category_b_sum == 12  # 3+4+5


def test_unnest_with_filter():
    """Test filtering after unnesting."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "id": [1, 2],
            "values": [[10, 20, 30], [5, 15]],
        }
    )
    tbl = con.create_table("data", data)

    # Unnest and filter for values > 15
    semantic = to_semantic_table(tbl, name="data").unnest("values")

    result = semantic.filter(lambda t: t.values > 15).execute()

    assert len(result) == 2  # 20 and 30
    assert sorted(result["values"].tolist()) == [20, 30]


def test_unnest_preserves_other_columns():
    """Test that unnesting preserves other columns correctly."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "session_id": [1, 2],
            "user_name": ["Alice", "Bob"],
            "codes": [["A", "B"], ["C"]],
        }
    )
    tbl = con.create_table("data", data)

    unnested = to_semantic_table(tbl, name="data").unnest("codes")
    result = unnested.execute()

    # Should have 3 rows (2 + 1)
    assert len(result) == 3

    # Check that session_id and user_name are preserved
    assert result.loc[result.session_id == 1, "user_name"].iloc[0] == "Alice"
    assert result.loc[result.session_id == 2, "user_name"].iloc[0] == "Bob"


def test_unnest_error_on_missing_column():
    """Test that unnesting non-existent column raises an error."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame({"id": [1, 2], "values": [[1, 2], [3, 4]]})
    tbl = con.create_table("data", data)

    semantic = to_semantic_table(tbl, name="data")

    # Try to unnest a column that doesn't exist
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        semantic.unnest("nonexistent").execute()


def test_unnest_chaining():
    """Test that unnest can be chained with other operations."""
    con = ibis.duckdb.connect(":memory:")

    data = pd.DataFrame(
        {
            "category": ["A", "B"],
            "items": [[1, 2, 3], [4, 5]],
        }
    )
    tbl = con.create_table("data", data)

    # Chain: unnest -> filter -> mutate
    result = (
        to_semantic_table(tbl, name="data")
        .unnest("items")
        .filter(lambda t: t.items > 2)
        .mutate(doubled=lambda t: t.items * 2)
        .execute()
    )

    # items > 2: [3, 4, 5]
    assert len(result) == 3
    assert result["items"].sum() == 12  # 3 + 4 + 5
    assert result["doubled"].sum() == 24  # 6 + 8 + 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
