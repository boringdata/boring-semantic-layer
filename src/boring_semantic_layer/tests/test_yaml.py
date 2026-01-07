"""Tests for YAML loading functionality with semantic API."""

import os
import tempfile

import ibis
import pytest

from boring_semantic_layer import SemanticTable, from_config, from_yaml


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
        assert model.get_dimensions()["code"].description == "Airline code"
        assert model.get_dimensions()["name"].description == "Full airline name"

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
        arr_time_dim = model.get_dimensions()["arr_time"]
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
            flights.group_by("flights.origin", "carriers.name").aggregate("flight_count").execute()
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
        assert model.get_dimensions()["origin"].description is None

        # Extended format dimension has description
        assert model.get_dimensions()["destination"].description == "Destination airport"

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
    """Test loading models with complex measure expressions.

    Note: With ColumnProxy enabled, complex BinOp expressions need base measures defined first.
    """
    yaml_content = """
flights:
  table: flights_tbl
  dimensions:
    carrier: _.carrier
  measures:
    flight_count: _.count()
    on_time_rate: (_.dep_delay <= 0).mean()
    total_delay: _.dep_delay.sum()
    total_distance: _.distance.sum()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Test complex measures - we can add calc measures after loading
        flights_with_calc = model.with_measures(
            delay_per_mile=lambda t: t.total_delay / t.total_distance,
        )

        # Test complex measures
        result = (
            flights_with_calc.group_by("carrier")
            .aggregate("on_time_rate", "delay_per_mile")
            .execute()
        )

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


def test_duplicate_table_names_from_profiles():
    """Test error when different profiles provide tables with the same name."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create parquet files first
        import ibis

        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)

        flights1 = ibis.memtable({"id": [1, 2]})
        flights1_path = os.path.join(data_dir, "flights1.parquet")
        flights1.to_parquet(flights1_path)

        flights2 = ibis.memtable({"id": [3, 4]})
        flights2_path = os.path.join(data_dir, "flights2.parquet")
        flights2.to_parquet(flights2_path)

        # Create two profile files with overlapping table names
        profile1_path = os.path.join(tmpdir, "profile1.yml")
        with open(profile1_path, "w") as f:
            f.write(f"""
db1:
  type: duckdb
  database: ":memory:"
  tables:
    flights: "{flights1_path}"
""")

        profile2_path = os.path.join(tmpdir, "profile2.yml")
        with open(profile2_path, "w") as f:
            f.write(f"""
db2:
  type: duckdb
  database: ":memory:"
  tables:
    flights: "{flights2_path}"
""")

        # Create YAML with two models using different profiles
        yaml_content = f"""
flights1:
  profile:
    name: db1
    file: {profile1_path}
  table: flights
  dimensions:
    id: _.id

flights2:
  profile:
    name: db2
    file: {profile2_path}
  table: flights
  dimensions:
    id: _.id
"""

        yaml_path = os.path.join(tmpdir, "model.yml")
        with open(yaml_path, "w") as f:
            f.write(yaml_content)

        # Should raise error about duplicate table name
        with pytest.raises(ValueError, match="Table name conflict.*flights"):
            from_yaml(yaml_path)


def test_duplicate_table_names_from_manual_and_profile():
    """Test error when manually provided tables conflict with profile tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create parquet file
        import ibis

        data_dir = os.path.join(tmpdir, "data")
        os.makedirs(data_dir)
        test_data = ibis.memtable({"id": [1, 2]})
        parquet_path = os.path.join(data_dir, "test.parquet")
        test_data.to_parquet(parquet_path)

        # Create a profile with a table
        profile_path = os.path.join(tmpdir, "profile.yml")
        with open(profile_path, "w") as f:
            f.write(f"""
test_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_table: "{parquet_path}"
""")

        # Create YAML
        yaml_content = f"""
model1:
  profile:
    name: test_db
    file: {profile_path}
  table: my_table
  dimensions:
    id: _.id
"""

        yaml_path = os.path.join(tmpdir, "model.yml")
        with open(yaml_path, "w") as f:
            f.write(yaml_content)

        # Create manual table with same name
        con = ibis.duckdb.connect()
        manual_table = con.create_table("my_table", {"x": [10, 20]})

        # Should raise error about duplicate table name
        with pytest.raises(ValueError, match="Table name conflict"):
            from_yaml(yaml_path, tables={"my_table": manual_table})


def test_load_model_with_entity_dimensions(sample_tables):
    """Test loading a model with entity dimension metadata."""
    yaml_content = """
flights:
  table: flights_tbl

  dimensions:
    origin:
      expr: _.origin
      description: "Origin airport"
      is_entity: true
    tail_num:
      expr: _.tail_num
      description: "Aircraft tail number"
      is_entity: true
    destination: _.destination

  measures:
    flight_count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Verify entity dimension metadata
        origin_dim = model.get_dimensions()["origin"]
        assert origin_dim.is_entity is True
        assert origin_dim.description == "Origin airport"

        tail_num_dim = model.get_dimensions()["tail_num"]
        assert tail_num_dim.is_entity is True
        assert tail_num_dim.description == "Aircraft tail number"

        # Regular dimension should not be an entity
        dest_dim = model.get_dimensions()["destination"]
        assert dest_dim.is_entity is False

        # Verify json_definition includes entity dimensions
        json_def = model.json_definition
        assert "entity_dimensions" in json_def
        assert "origin" in json_def["entity_dimensions"]
        assert "tail_num" in json_def["entity_dimensions"]
        assert json_def["dimensions"]["origin"]["is_entity"] is True
        assert json_def["dimensions"]["tail_num"]["is_entity"] is True

    finally:
        os.unlink(yaml_path)


def test_load_model_with_event_timestamp(sample_tables):
    """Test loading a model with event timestamp dimension metadata."""
    yaml_content = """
flights:
  table: flights_tbl

  dimensions:
    origin: _.origin
    arr_time:
      expr: _.arr_time
      description: "Arrival timestamp"
      is_event_timestamp: true
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_MINUTE"
    dep_time:
      expr: _.dep_time
      description: "Departure time (regular time dim)"
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_MINUTE"

  measures:
    flight_count: _.count()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Verify event timestamp dimension metadata
        arr_time_dim = model.get_dimensions()["arr_time"]
        assert arr_time_dim.is_event_timestamp is True
        assert arr_time_dim.is_time_dimension is True
        assert arr_time_dim.smallest_time_grain == "TIME_GRAIN_MINUTE"
        assert arr_time_dim.description == "Arrival timestamp"

        # Regular time dimension should not be event timestamp
        dep_time_dim = model.get_dimensions()["dep_time"]
        assert dep_time_dim.is_event_timestamp is False
        assert dep_time_dim.is_time_dimension is True
        assert dep_time_dim.smallest_time_grain == "TIME_GRAIN_MINUTE"

        # Verify json_definition includes event timestamp
        json_def = model.json_definition
        assert "event_timestamp" in json_def
        assert "arr_time" in json_def["event_timestamp"]
        assert json_def["dimensions"]["arr_time"]["is_event_timestamp"] is True
        # dep_time should NOT be in event_timestamp
        assert "dep_time" not in json_def["event_timestamp"]

    finally:
        os.unlink(yaml_path)


def test_load_model_with_filter(sample_tables):
    """Test loading a model with a filter applied."""
    yaml_content = """
flights:
  table: flights_tbl
  filter: _.distance > 500
  dimensions:
    origin: _.origin
    destination: _.destination
  measures:
    flight_count: _.count()
    avg_distance: _.distance.mean()
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Query the filtered model
        result = model.group_by().aggregate("flight_count", "avg_distance").execute()

        # Original data has 6 flights, but only 3 have distance > 500
        # JFK-LAX: 2475, LAX-JFK: 2475, ATL-ORD: 606, JFK-ATL: 760, DAL-HOU: 239, ORD-LAX: 1744
        # Filter keeps: JFK-LAX, LAX-JFK, ATL-ORD, JFK-ATL, ORD-LAX = 5 flights
        assert result.iloc[0]["flight_count"] == 5

    finally:
        os.unlink(yaml_path)


def test_load_model_with_complex_filter(sample_tables):
    """Test loading a model with a complex filter expression."""
    yaml_content = """
flights:
  table: flights_tbl
  filter: _.origin.isin(['JFK', 'LAX'])
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
        models = from_yaml(yaml_path, tables=sample_tables)
        model = models["flights"]

        # Query by origin
        result = model.group_by("origin").aggregate("flight_count").execute()

        # Filter keeps flights from JFK or LAX origins
        # JFK origins: JFK-LAX, JFK-ATL = 2
        # LAX origins: LAX-JFK = 1
        # Total: 3 flights
        total = result["flight_count"].sum()
        assert total == 3

    finally:
        os.unlink(yaml_path)


# ============================================================================
# from_config tests
# ============================================================================


def test_from_config_simple_model(sample_tables):
    """Test loading a simple model from a config dict."""
    config = {
        "carriers": {
            "table": "carriers_tbl",
            "description": "Airline carriers",
            "dimensions": {
                "code": "_.code",
                "name": "_.name",
            },
            "measures": {
                "carrier_count": "_.count()",
            },
        }
    }

    models = from_config(config, tables=sample_tables)
    model = models["carriers"]

    # Verify it's a SemanticTable
    assert isinstance(model, SemanticTable)
    assert model.name == "carriers"

    # Verify dimensions and measures
    assert "code" in model.dimensions
    assert "name" in model.dimensions
    assert "carrier_count" in model.measures

    # Test a query
    result = model.group_by("name").aggregate("carrier_count").execute()
    assert len(result) == 4


def test_from_config_with_descriptions(sample_tables):
    """Test loading a model with descriptions from config dict."""
    config = {
        "carriers": {
            "table": "carriers_tbl",
            "dimensions": {
                "code": {"expr": "_.code", "description": "Airline code"},
                "name": {"expr": "_.name", "description": "Full airline name"},
            },
            "measures": {
                "carrier_count": {"expr": "_.count()", "description": "Number of carriers"},
            },
        }
    }

    models = from_config(config, tables=sample_tables)
    model = models["carriers"]

    # Verify descriptions
    assert model.get_dimensions()["code"].description == "Airline code"
    assert model.get_dimensions()["name"].description == "Full airline name"
    assert model._base_measures["carrier_count"].description == "Number of carriers"


def test_from_config_with_filter(sample_tables):
    """Test loading a model with a filter from config dict."""
    config = {
        "flights": {
            "table": "flights_tbl",
            "filter": "_.distance > 500",
            "dimensions": {
                "origin": "_.origin",
            },
            "measures": {
                "flight_count": "_.count()",
            },
        }
    }

    models = from_config(config, tables=sample_tables)
    model = models["flights"]

    result = model.group_by().aggregate("flight_count").execute()
    # 5 flights have distance > 500
    assert result.iloc[0]["flight_count"] == 5


def test_from_config_with_joins(sample_tables):
    """Test loading models with joins from config dict."""
    config = {
        "carriers": {
            "table": "carriers_tbl",
            "dimensions": {"code": "_.code", "name": "_.name"},
            "measures": {"carrier_count": "_.count()"},
        },
        "flights": {
            "table": "flights_tbl",
            "dimensions": {"origin": "_.origin", "carrier": "_.carrier"},
            "measures": {"flight_count": "_.count()"},
            "joins": {
                "carriers": {
                    "model": "carriers",
                    "type": "one",
                    "left_on": "carrier",
                    "right_on": "code",
                }
            },
        },
    }

    models = from_config(config, tables=sample_tables)
    flights = models["flights"]

    # Test query with joined dimension
    result = flights.group_by("flights.origin", "carriers.name").aggregate("flight_count").execute()

    assert "flights.origin" in result.columns
    assert "carriers.name" in result.columns
    assert len(result) > 0


def test_from_config_multiple_models(sample_tables):
    """Test loading multiple models from config dict."""
    config = {
        "carriers": {
            "table": "carriers_tbl",
            "dimensions": {"code": "_.code"},
            "measures": {"carrier_count": "_.count()"},
        },
        "flights": {
            "table": "flights_tbl",
            "dimensions": {"origin": "_.origin"},
            "measures": {"flight_count": "_.count()"},
        },
    }

    models = from_config(config, tables=sample_tables)

    assert "carriers" in models
    assert "flights" in models
    assert isinstance(models["carriers"], SemanticTable)
    assert isinstance(models["flights"], SemanticTable)


def test_from_config_error_missing_table(sample_tables):
    """Test error when config references non-existent table."""
    config = {
        "missing": {
            "table": "nonexistent_table",
            "dimensions": {"col": "_.col"},
            "measures": {"count": "_.count()"},
        }
    }

    with pytest.raises(KeyError, match="Table 'nonexistent_table' not found"):
        from_config(config, tables=sample_tables)


def test_load_model_with_database_kwarg():
    """Test loading a model with database kwarg for multi-part table identifiers."""
    import ibis

    # Create a DuckDB connection with a schema
    con = ibis.duckdb.connect()

    # Create a schema and table in that schema
    con.raw_sql("CREATE SCHEMA test_schema")
    con.raw_sql("CREATE TABLE test_schema.my_table (id INTEGER, value VARCHAR)")
    con.raw_sql("INSERT INTO test_schema.my_table VALUES (1, 'a'), (2, 'b')")

    # Test via from_config with a table that has the connection
    config = {
        "test_model": {
            "table": "my_table",
            "database": "test_schema",
            "dimensions": {"id": "_.id", "value": "_.value"},
            "measures": {"count": "_.count()"},
        }
    }

    # Create a table reference that has the connection
    base_table = con.table("my_table", database="test_schema")
    models = from_config(config, tables={"my_table": base_table})
    model = models["test_model"]

    # Verify the model works
    result = model.group_by().aggregate("count").execute()
    assert result.iloc[0]["count"] == 2


def test_load_model_with_database_list_kwarg():
    """Test that database kwarg list gets converted to tuple for ibis."""
    import ibis

    con = ibis.duckdb.connect()

    # Create schema and table
    con.raw_sql("CREATE SCHEMA analytics")
    con.raw_sql("CREATE TABLE analytics.events (event_id INTEGER, event_name VARCHAR)")
    con.raw_sql("INSERT INTO analytics.events VALUES (1, 'click'), (2, 'view')")

    config = {
        "events": {
            "table": "events",
            "database": "analytics",
            "dimensions": {"event_id": "_.event_id", "event_name": "_.event_name"},
            "measures": {"event_count": "_.count()"},
        }
    }

    # Create base table with connection
    base_table = con.table("events", database="analytics")
    models = from_config(config, tables={"events": base_table})
    model = models["events"]

    # Verify the model works
    result = model.group_by("event_name").aggregate("event_count").execute()
    assert len(result) == 2


def test_database_list_to_tuple_conversion():
    """Test that database list is converted to tuple in _load_table_for_yaml_model."""
    from unittest.mock import MagicMock, patch

    from boring_semantic_layer.yaml import _load_table_for_yaml_model

    # Mock the get_connection to return a mock connection
    mock_connection = MagicMock()
    mock_table = MagicMock()
    mock_connection.table.return_value = mock_table

    with patch("boring_semantic_layer.yaml.get_connection", return_value=mock_connection):
        model_config = {
            "profile": {"type": "duckdb"},
            "database": ["catalog", "schema"],  # List form
        }

        result = _load_table_for_yaml_model(model_config, {}, "my_table")

        # Verify table was called with tuple (converted from list)
        mock_connection.table.assert_called_once_with("my_table", database=("catalog", "schema"))
        assert "my_table" in result


def test_from_config_matches_from_yaml(sample_tables):
    """Test that from_config produces same result as from_yaml."""
    yaml_content = """
carriers:
  table: carriers_tbl
  dimensions:
    code: _.code
    name: _.name
  measures:
    carrier_count: _.count()
"""

    config = {
        "carriers": {
            "table": "carriers_tbl",
            "dimensions": {
                "code": "_.code",
                "name": "_.name",
            },
            "measures": {
                "carrier_count": "_.count()",
            },
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        yaml_path = f.name

    try:
        yaml_models = from_yaml(yaml_path, tables=sample_tables)
        config_models = from_config(config, tables=sample_tables)

        # Both should produce same query results
        yaml_result = yaml_models["carriers"].group_by("name").aggregate("carrier_count").execute()
        config_result = (
            config_models["carriers"].group_by("name").aggregate("carrier_count").execute()
        )

        assert len(yaml_result) == len(config_result)
        assert set(yaml_result.columns) == set(config_result.columns)

    finally:
        os.unlink(yaml_path)
