"""Tests for YAML loading functionality with semantic API."""

import pytest
import ibis
import tempfile
import os
from boring_semantic_layer import from_yaml, SemanticModel, SemanticTable


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
  description: "Airline carriers"

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

        # Verify it's a SemanticTable
        assert isinstance(model, SemanticTable)
        assert model.name == "carriers"

        # Verify dimensions
        assert "code" in model.dimensions
        assert "name" in model.dimensions
        assert "nickname" in model.dimensions

        # Verify measures
        assert "carrier_count" in model.measures

        # Test a query
        result = model.group_by("name").aggregate("carrier_count").execute()
        assert len(result) == 4
        assert "carrier_count" in result.columns

    finally:
        os.unlink(yaml_path)


def test_load_model_with_descriptions(sample_tables):
    """Test loading a model with descriptions in extended format."""
    yaml_content = """
carriers:
  table: carriers_tbl

  dimensions:
    code:
      expr: _.code
      description: "Airline code"
    name:
      expr: _.name
      description: "Full airline name"

  measures:
    carrier_count:
      expr: _.count()
      description: "Number of carriers"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["carriers"]

        # Verify dimensions with descriptions
        assert model.dimensions["code"].description == "Airline code"
        assert model.dimensions["name"].description == "Full airline name"

        # Verify measures with descriptions (use _base_measures to get Measure objects)
        assert model._base_measures["carrier_count"].description == "Number of carriers"

    finally:
        os.unlink(yaml_path)


def test_load_model_with_time_dimension(sample_tables):
    """Test loading a model with time dimension metadata."""
    yaml_content = """
flights:
  table: flights_tbl

  dimensions:
    origin: _.origin
    arr_time:
      expr: _.arr_time
      description: "Arrival time"
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_DAY"

  measures:
    flight_count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Verify time dimension metadata
        arr_time_dim = model.dimensions["arr_time"]
        assert arr_time_dim.is_time_dimension is True
        assert arr_time_dim.smallest_time_grain == "TIME_GRAIN_DAY"

    finally:
        os.unlink(yaml_path)


def test_load_model_with_join_one(sample_tables):
    """Test loading a model with a one-to-one join."""
    yaml_content = """
carriers:
  table: carriers_tbl
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
    avg_distance: _.distance.mean()
  joins:
    carriers:
      model: carriers
      type: one
      left_on: carrier
      right_on: code
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        flights = models["flights"]

        # Test query with joined dimension (use dot notation)
        result = (
            flights.group_by("flights.origin", "carriers.name")
            .aggregate("flight_count")
            .execute()
        )

        # Verify the join worked
        assert "flights.origin" in result.columns
        assert "carriers.name" in result.columns
        assert "flight_count" in result.columns
        assert len(result) > 0

    finally:
        os.unlink(yaml_path)


def test_load_multiple_models(sample_tables):
    """Test loading multiple models from the same YAML file."""
    yaml_content = """
carriers:
  table: carriers_tbl
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
  measures:
    flight_count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)

        # Verify both models were loaded
        assert "carriers" in models
        assert "flights" in models

        # Test both models work
        carriers_result = models["carriers"].group_by("name").aggregate("carrier_count").execute()
        assert len(carriers_result) == 4

        flights_result = models["flights"].group_by("origin").aggregate("flight_count").execute()
        assert len(flights_result) > 0

    finally:
        os.unlink(yaml_path)


def test_error_on_missing_table(sample_tables):
    """Test that an error is raised when referencing a non-existent table."""
    yaml_content = """
missing:
  table: nonexistent_table
  dimensions:
    col: _.col
  measures:
    count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(KeyError, match="Table 'nonexistent_table' not found"):
            from_yaml(yaml_path, tables=sample_tables)
    finally:
        os.unlink(yaml_path)


def test_error_on_missing_join_model(sample_tables):
    """Test that an error is raised when joining to a non-existent model."""
    yaml_content = """
flights:
  table: flights_tbl
  dimensions:
    origin: _.origin
  measures:
    flight_count: _.count()
  joins:
    missing:
      model: nonexistent_model
      type: one
      left_on: carrier
      right_on: code
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(KeyError, match="Model 'nonexistent_model'.*not found"):
            from_yaml(yaml_path, tables=sample_tables)
    finally:
        os.unlink(yaml_path)


def test_mixed_simple_and_extended_format(sample_tables):
    """Test mixing simple and extended dimension/measure formats."""
    yaml_content = """
flights:
  table: flights_tbl
  dimensions:
    origin: _.origin
    destination:
      expr: _.destination
      description: "Destination airport"
  measures:
    flight_count: _.count()
    avg_distance:
      expr: _.distance.mean()
      description: "Average flight distance"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Simple format dimension has no description
        assert model.dimensions["origin"].description is None

        # Extended format dimension has description
        assert model.dimensions["destination"].description == "Destination airport"

        # Simple format measure has no description (use _base_measures to get Measure objects)
        assert model._base_measures["flight_count"].description is None

        # Extended format measure has description
        assert model._base_measures["avg_distance"].description == "Average flight distance"

    finally:
        os.unlink(yaml_path)


def test_computed_dimensions(sample_tables):
    """Test loading models with computed/derived dimensions."""
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
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Test computed dimension in query
        result = model.group_by("route").aggregate("flight_count").execute()

        routes = result["route"].tolist()
        assert "JFK-LAX" in routes
        assert "LAX-JFK" in routes
        assert len(result) == 6  # 6 unique routes

    finally:
        os.unlink(yaml_path)


def test_complex_measure_expressions(sample_tables):
    """Test loading models with complex measure expressions."""
    yaml_content = """
flights:
  table: flights_tbl
  dimensions:
    carrier: _.carrier
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

        # Test complex measures
        result = model.group_by("carrier").aggregate("on_time_rate", "delay_per_mile").execute()

        assert "on_time_rate" in result.columns
        assert "delay_per_mile" in result.columns
        assert len(result) == 4  # 4 carriers

        # Test without grouping
        result = model.group_by().aggregate("on_time_rate", "total_delay").execute()
        assert 0 <= result.iloc[0]["on_time_rate"] <= 1
        assert result.iloc[0]["total_delay"] is not None

    finally:
        os.unlink(yaml_path)


def test_file_not_found():
    """Test handling of non-existent YAML file."""
    with pytest.raises(FileNotFoundError):
        from_yaml("nonexistent.yml", tables={})


def test_invalid_dimension_format(sample_tables):
    """Test error handling for invalid dimension format."""
    yaml_content = """
test:
  table: flights_tbl
  dimensions:
    invalid_dim:
      description: "Missing expr field"
  measures:
    count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(
            ValueError,
            match="Dimension 'invalid_dim' must specify 'expr' field when using dict format",
        ):
            from_yaml(yaml_path, tables=sample_tables)
    finally:
        os.unlink(yaml_path)


def test_invalid_measure_format(sample_tables):
    """Test error handling for invalid measure format."""
    yaml_content = """
test:
  table: flights_tbl
  dimensions:
    origin: _.origin
  measures:
    invalid_measure:
      description: "Missing expr field"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        with pytest.raises(
            ValueError,
            match="Measure 'invalid_measure' must specify 'expr' field when using dict format",
        ):
            from_yaml(yaml_path, tables=sample_tables)
    finally:
        os.unlink(yaml_path)
