"""Tests for YAML loading functionality."""

import pytest
import ibis
import tempfile
import os
from boring_semantic_layer import SemanticModel, Join


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
        models = SemanticModel.from_yaml(yaml_path, tables=sample_tables)
        model = models["carriers"]

        # Verify basic properties
        assert model.name == "carriers"
        assert model.primary_key == "code"

        # Verify dimensions
        assert "code" in model.dimensions
        assert "name" in model.dimensions
        assert "nickname" in model.dimensions

        # Verify measures
        assert "carrier_count" in model.measures

        # Test query execution
        result = model.query(dimensions=["code"], measures=["carrier_count"]).execute()

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
        models = SemanticModel.from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Verify time dimension settings
        assert model.time_dimension == "arr_time"
        assert model.smallest_time_grain == "TIME_GRAIN_HOUR"

        # Test computed dimensions
        result = model.query(
            dimensions=["year", "month"], measures=["flight_count"]
        ).execute()

        assert len(result) == 1  # All flights in 2024-01
        assert result.iloc[0]["year"] == 2024
        assert result.iloc[0]["month"] == 1
    finally:
        os.unlink(yaml_path)


def test_load_model_with_joins(sample_tables):
    """Test loading models with join relationships."""
    # First create carriers model
    carriers_yaml = """
carriers:
  table: carriers_tbl
  primary_key: code

  dimensions:
    code: _.code
    name: _.name

  measures:
    carrier_count: _.count()
"""

    flights_yaml = """
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
      model: carriers_sm
      type: one
      with: _.carrier
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(carriers_yaml)
        carriers_path = f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(flights_yaml)
        flights_path = f.name

    try:
        # Load carriers model first
        carriers_models = SemanticModel.from_yaml(carriers_path, tables=sample_tables)
        carriers_sm = carriers_models["carriers"]

        # Load flights model with carriers in tables
        tables = {**sample_tables, "carriers_sm": carriers_sm}
        flights_models = SemanticModel.from_yaml(flights_path, tables=tables)
        flights_sm = flights_models["flights"]

        # Verify join exists
        assert "carriers" in flights_sm.joins
        assert isinstance(flights_sm.joins["carriers"], Join)

        # Test query with joined dimension
        result = flights_sm.query(
            dimensions=["carrier", "carriers.name"], measures=["flight_count"]
        ).execute()

        assert len(result) == 4  # 4 unique carriers
        assert "carriers_name" in result.columns
    finally:
        os.unlink(carriers_path)
        os.unlink(flights_path)


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
            SemanticModel.from_yaml(yaml_path, tables={})
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
            SemanticModel.from_yaml(yaml_path, tables={})
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
        models = SemanticModel.from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Test computed dimension
        result = model.query(dimensions=["route"], measures=["flight_count"]).execute()

        routes = result["route"].tolist()
        assert "JFK-LAX" in routes
        assert "LAX-JFK" in routes

        # Test complex measure
        result = model.query(measures=["on_time_rate", "delay_per_mile"]).execute()

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

  joins:
    carriers:
      model: carriers  # Reference to carriers model in same file
      type: one
      with: _.carrier
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        # Load all models at once
        models = SemanticModel.from_yaml(yaml_path, tables=sample_tables)
        assert isinstance(models, dict)
        assert "carriers" in models
        assert "flights" in models

        carriers_sm = models["carriers"]
        flights_sm = models["flights"]

        assert carriers_sm.name == "carriers"
        assert flights_sm.name == "flights"
        assert "carriers" in flights_sm.joins

        # Test that we can access individual models from the result
        assert carriers_sm.name == "carriers"
        assert flights_sm.name == "flights"
    finally:
        os.unlink(yaml_path)


def test_yaml_file_not_found():
    """Test handling of non-existent YAML file."""
    with pytest.raises(FileNotFoundError):
        SemanticModel.from_yaml("nonexistent.yml", tables={})


def test_invalid_join_type(sample_tables):
    """Test error handling for invalid join type."""
    yaml_content = """
test:
  table: flights_tbl

  dimensions:
    test: _.test

  joins:
    other:
      model: other_model
      type: invalid_type
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        other_model = SemanticModel(
            table=sample_tables["flights_tbl"],
            dimensions={"test": lambda t: t.origin},
            measures={"count": lambda t: t.count()},
            primary_key="test",
        )

        tables = {**sample_tables, "other_model": other_model}

        with pytest.raises(ValueError, match="Invalid join type 'invalid_type'"):
            SemanticModel.from_yaml(yaml_path, tables=tables)
    finally:
        os.unlink(yaml_path)


def test_load_model_With_descriptions(sample_tables):
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
        models = SemanticModel.from_yaml(yaml_path, tables=sample_tables)
        model = models["carriers"]

        # Verify model description
        assert model.description == "Carriers table description"

        # Test json_definition to verify descriptions are properly loaded
        json_def = model.json_definition

        # Verify model description in JSON
        assert json_def["description"] == "Carriers table description"

        # Verify dimension descriptions
        assert json_def["dimensions"]["code"]["description"] == ""
        assert json_def["dimensions"]["name"]["description"] == "Full airline name"
        assert json_def["dimensions"]["nickname"]["description"] == "Short airline name"
        assert (
            json_def["dimensions"]["code_upper"]["description"]
            == "Upper case airline code"
        )

        # Verify measure descriptions
        assert json_def["measures"]["carrier_count"]["description"] == ""
        assert (
            json_def["measures"]["total_carriers"]["description"]
            == "Total number of carriers"
        )

        # Test that queries still work with both old and new style
        result = model.query(
            dimensions=["code", "name", "code_upper"],
            measures=["carrier_count", "total_carriers"],
        ).execute()

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
            SemanticModel.from_yaml(yaml_path, tables=sample_tables)
    finally:
        os.unlink(yaml_path)
