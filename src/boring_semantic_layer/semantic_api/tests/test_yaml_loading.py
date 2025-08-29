"""Tests for YAML loading functionality in semantic API."""

import pytest
import ibis
import tempfile
import os
from boring_semantic_layer.semantic_api.yaml_loader import (
    from_yaml,
    from_yaml_with_metadata,
)
from boring_semantic_layer.semantic_api.api import SemanticTableExpr


@pytest.fixture
def duckdb_conn():
    """Create a DuckDB connection for testing."""
    return ibis.duckdb.connect()


@pytest.fixture
def sample_tables(duckdb_conn):
    """Create sample tables for testing."""
    # Create carriers table
    carriers_data = {
        "code": ["AA", "UA", "DL", "SW"],
        "name": [
            "American Airlines",
            "United Airlines",
            "Delta Airlines",
            "Southwest Airlines",
        ],
        "nickname": ["American", "United", "Delta", "Southwest"],
    }
    carriers_tbl = duckdb_conn.create_table("carriers", carriers_data)

    # Create flights table
    flights_data = {
        "carrier": ["AA", "UA", "DL", "AA", "SW", "UA"],
        "origin": ["JFK", "LAX", "ATL", "JFK", "DAL", "ORD"],
        "destination": ["LAX", "JFK", "ORD", "ATL", "HOU", "LAX"],
        "dep_delay": [10, -5, 20, 0, 15, 30],
        "distance": [2475, 2475, 606, 760, 239, 1744],
        "tail_num": ["N123", "N456", "N789", "N123", "N987", "N654"],
        "arr_time": [
            "2024-01-01 10:00:00",
            "2024-01-01 11:00:00",
            "2024-01-01 12:00:00",
            "2024-01-01 13:00:00",
            "2024-01-01 14:00:00",
            "2024-01-01 15:00:00",
        ],
        "dep_time": [
            "2024-01-01 07:00:00",
            "2024-01-01 08:00:00",
            "2024-01-01 09:00:00",
            "2024-01-01 10:00:00",
            "2024-01-01 11:00:00",
            "2024-01-01 12:00:00",
        ],
    }
    # Convert time strings to timestamp
    flights_tbl = duckdb_conn.create_table("flights", flights_data)
    flights_tbl = flights_tbl.mutate(
        arr_time=flights_tbl.arr_time.cast("timestamp"),
        dep_time=flights_tbl.dep_time.cast("timestamp"),
    )

    return {"carriers_tbl": carriers_tbl, "flights_tbl": flights_tbl}


def test_load_simple_model(sample_tables):
    """Test loading a simple model without joins."""
    yaml_content = """
carriers:
  table: carriers_tbl
  primary_key: code

  dimensions:
    code: _.code
    name: _.name
    nickname: _.nickname

  measures:
    carrier_count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        # Load model from YAML
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["carriers"]

        # Verify it's a semantic table
        assert isinstance(model, SemanticTableExpr)

        # Test metadata using from_yaml_with_metadata
        models_with_meta, metadata = from_yaml_with_metadata(
            yaml_path, tables=sample_tables
        )
        assert metadata["carriers"]["primary_key"] == "code"

        # Test query execution using semantic API style
        result = (
            model.group_by("code")
            .aggregate(carrier_count=lambda t: t.count())
            .execute()
        )

        assert len(result) == 4  # 4 carriers
        assert result["carrier_count"].sum() == 4
    finally:
        os.unlink(yaml_path)


def test_load_model_with_time_dimension(sample_tables):
    """Test loading a model with time dimensions."""
    yaml_content = """
flights:
  table: flights_tbl
  time_dimension: arr_time
  smallest_time_grain: TIME_GRAIN_HOUR

  dimensions:
    origin: _.origin
    destination: _.destination
    carrier: _.carrier
    arr_time: _.arr_time

    # Computed dimensions
    year: _.arr_time.year()
    month: _.arr_time.month()

  measures:
    flight_count: _.count()
    avg_dep_delay: _.dep_delay.mean()
    total_distance: _.distance.sum()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Verify metadata
        _, metadata = from_yaml_with_metadata(yaml_path, tables=sample_tables)
        assert metadata["flights"]["time_dimension"] == "arr_time"
        assert metadata["flights"]["smallest_time_grain"] == "TIME_GRAIN_HOUR"

        # Test computed dimensions
        result = (
            model.group_by("year", "month")
            .aggregate(flight_count=lambda t: t.count())
            .execute()
        )

        assert len(result) == 1  # All flights in 2024-01
        assert result.iloc[0]["year"] == 2024
        assert result.iloc[0]["month"] == 1
    finally:
        os.unlink(yaml_path)


def test_load_model_with_joins(sample_tables):
    """Test loading models with join relationships."""
    yaml_content = """
carriers:
  table: carriers_tbl
  primary_key: code

  dimensions:
    code: _.code
    name: _.name

  measures:
    carrier_count: _.count()

flights:
  table: flights_tbl

  dimensions:
    origin: _.origin
    destination: _.destination
    carrier: _.carrier

  measures:
    flight_count: _.count()
    avg_distance: _.distance.mean()

  joins:
    carriers:
      model: carriers
      type: one
      with: _.carrier
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        # Load all models at once
        models = from_yaml(yaml_path, tables=sample_tables)

        assert isinstance(models, dict)
        assert "carriers" in models
        assert "flights" in models

        # Check metadata for join info
        _, metadata = from_yaml_with_metadata(yaml_path, tables=sample_tables)
        assert "joins" in metadata["flights"]
        assert "carriers" in metadata["flights"]["joins"]

        # Note: Testing actual joins would require more sophisticated
        # join condition parsing in the yaml_loader
        # For now, we verify the structure is loaded
    finally:
        os.unlink(yaml_path)


def test_error_handling():
    """Test error handling for invalid YAML configurations."""
    # Test missing table reference
    yaml_content = """
test:
  table: nonexistent_table

  dimensions:
    test: _.test
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(KeyError, match="Table 'nonexistent_table' not found"):
            from_yaml(yaml_path, tables={})
    finally:
        os.unlink(yaml_path)

    # Test missing table field
    yaml_content = """
test:
  dimensions:
    test: _.test
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(ValueError, match="Model 'test' must specify 'table' field"):
            from_yaml(yaml_path, tables={})
    finally:
        os.unlink(yaml_path)


def test_complex_expressions(sample_tables):
    """Test loading models with complex expressions."""
    yaml_content = """
flights:
  table: flights_tbl

  dimensions:
    origin: _.origin
    destination: _.destination
    route: _.origin + '-' + _.destination
    is_delayed: _.dep_delay > 0

  measures:
    flight_count: _.count()
    on_time_rate: (_.dep_delay <= 0).mean()
    total_delay: _.dep_delay.sum()
    delay_per_mile: _.dep_delay.sum() / _.distance.sum()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Test computed dimension
        result = (
            model.group_by("route")
            .aggregate(flight_count=lambda t: t.count())
            .execute()
        )

        routes = result["route"].tolist()
        assert "JFK-LAX" in routes
        assert "LAX-JFK" in routes

        # Test complex measure - use constant dimension for single-row aggregation
        result = (
            model.with_dimensions(
                constant=lambda t: ibis.literal(1)
            )  # Add constant dimension
            .group_by("constant")
            .aggregate(
                on_time_rate=lambda t: (t.dep_delay <= 0).mean(),
                delay_per_mile=lambda t: t.dep_delay.sum() / t.distance.sum(),
            )
            .execute()
        )

        assert 0 <= result.iloc[0]["on_time_rate"] <= 1
        assert result.iloc[0]["delay_per_mile"] is not None
    finally:
        os.unlink(yaml_path)


def test_load_multiple_models_from_one_file(sample_tables):
    """Test loading multiple models from a single YAML file."""
    yaml_content = """
carriers:
  table: carriers_tbl
  primary_key: code

  dimensions:
    code: _.code
    name: _.name

  measures:
    carrier_count: _.count()

flights:
  table: flights_tbl

  dimensions:
    origin: _.origin
    carrier: _.carrier

  measures:
    flight_count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        # Load all models at once
        models = from_yaml(yaml_path, tables=sample_tables)
        assert isinstance(models, dict)
        assert len(models) == 2
        assert "carriers" in models
        assert "flights" in models

        # Each should be a semantic table
        assert isinstance(models["carriers"], SemanticTableExpr)
        assert isinstance(models["flights"], SemanticTableExpr)
    finally:
        os.unlink(yaml_path)


def test_yaml_file_not_found():
    """Test handling of non-existent YAML file."""
    with pytest.raises(FileNotFoundError):
        from_yaml("nonexistent.yml", tables={})


def test_invalid_join_type(sample_tables):
    """Test error handling for invalid join type."""
    yaml_content = """
test:
  table: flights_tbl

  dimensions:
    test: _.test

  joins:
    other:
      model: carriers
      type: invalid_type
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        # Create a simple carriers model first
        carriers_yaml = """
carriers:
  table: carriers_tbl
  dimensions:
    code: _.code
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as cf:
            cf.write(carriers_yaml)
            carriers_path = cf.name

        # Load both YAML files together to handle inter-model references
        combined_yaml = carriers_yaml + "\n" + yaml_content
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yml", delete=False
        ) as combined_f:
            combined_f.write(combined_yaml)
            combined_path = combined_f.name

        try:
            with pytest.raises(ValueError, match="Invalid join type 'invalid_type'"):
                from_yaml(combined_path, tables=sample_tables)
        finally:
            os.unlink(combined_path)

        os.unlink(carriers_path)
    finally:
        os.unlink(yaml_path)


def test_load_model_with_descriptions(sample_tables):
    """Test loading models with dimension/measure descriptions and model description."""
    yaml_content = """
carriers:
    table: carriers_tbl
    primary_key: code
    description: "Carriers table description"

    dimensions:
        # Old format - no descriptions
        code: _.code

        # New format
        name:
            expr: _.name
            description: "Full airline name"

        nickname:
            expr: _.nickname
            description: "Short airline name"

        code_upper:
            expr: _.code.upper()
            description: "Upper case airline code"

    measures:
        # Old format
        carrier_count: _.count()

        # New format
        total_carriers:
            expr: _.count()
            description: "Total number of carriers"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        # Load model from YAML
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["carriers"]

        # Verify metadata includes descriptions
        _, metadata = from_yaml_with_metadata(yaml_path, tables=sample_tables)
        assert metadata["carriers"]["description"] == "Carriers table description"

        # Check dimension descriptions
        assert metadata["carriers"]["dimension_descriptions"]["code"] == ""
        assert (
            metadata["carriers"]["dimension_descriptions"]["name"]
            == "Full airline name"
        )
        assert (
            metadata["carriers"]["dimension_descriptions"]["nickname"]
            == "Short airline name"
        )
        assert (
            metadata["carriers"]["dimension_descriptions"]["code_upper"]
            == "Upper case airline code"
        )

        # Check measure descriptions
        assert metadata["carriers"]["measure_descriptions"]["carrier_count"] == ""
        assert (
            metadata["carriers"]["measure_descriptions"]["total_carriers"]
            == "Total number of carriers"
        )

        # Test that queries still work with both old and new style
        result = (
            model.group_by("code", "name", "code_upper")
            .aggregate(
                carrier_count=lambda t: t.count(), total_carriers=lambda t: t.count()
            )
            .execute()
        )

        assert len(result) == 4
        assert result["carrier_count"].sum() == 4
        assert result["total_carriers"].sum() == 4

        # Verify computed dimension works
        assert all(code.isupper() for code in result["code_upper"])
    finally:
        os.unlink(yaml_path)


def test_yaml_description_error_handling(sample_tables):
    """Test error handling for invalid description format."""
    yaml_content = """
test:
    table: carriers_tbl

    dimensions:
        invalid_dim:
            description: "Missing expr field"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(
            ValueError,
            match="Expression 'invalid_dim' must specify 'expr' field when using dict format",
        ):
            from_yaml(yaml_path, tables=sample_tables)
    finally:
        os.unlink(yaml_path)
