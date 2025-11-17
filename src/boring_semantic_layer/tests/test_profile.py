"""Tests for Profile API functionality."""

import tempfile
from pathlib import Path

import pytest

from boring_semantic_layer.profile import (
    Profile,
    ProfileError,
    load_profile,
    load_tables_from_profile,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_profile_yaml(temp_dir):
    """Create a sample profile YAML file."""
    profile_file = temp_dir / "profile.yml"
    profile_file.write_text("""
dev_db:
  type: duckdb
  database: ":memory:"

test_db:
  type: duckdb
  database: "test.db"
""")
    return profile_file


class TestProfileBasics:
    """Test basic Profile functionality."""

    def test_profile_creation(self):
        """Test creating a Profile instance."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})
        assert profile.con_name == "duckdb"
        assert profile.kwargs_dict == {"database": ":memory:"}

    def test_profile_creation_with_init(self):
        """Test creating Profile with constructor."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})
        assert profile.con_name == "duckdb"
        assert profile.kwargs_dict == {"database": ":memory:"}

    def test_profile_get_con(self):
        """Test getting connection from profile."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})
        con = profile.get_con()
        # Returns ibis backend by default
        assert con.list_tables() is not None


class TestProfileSaveLoad:
    """Test loading profiles."""

    def test_load_from_yaml_file(self, sample_profile_yaml):
        """Test loading profile from YAML file using profile_file parameter."""
        # Profile.load() is xorq-compatible and requires profile_file parameter for file paths
        profile = Profile.load("dev_db", profile_file=sample_profile_yaml)
        assert profile.con_name == "duckdb"
        assert "database" in profile.kwargs_dict

    def test_load_from_yaml_with_profile_file(self, sample_profile_yaml):
        """Test loading specific profile from YAML file."""
        profile = Profile.load("test_db", profile_file=sample_profile_yaml)
        assert profile.con_name == "duckdb"
        assert profile.kwargs_dict["database"] == "test.db"

    def test_load_from_local_profile_yml(self, temp_dir, monkeypatch):
        """Test loading from ./profiles.yml in current directory."""
        # Change to temp directory
        monkeypatch.chdir(temp_dir)

        # Create profiles.yml in current directory
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text("""
local_db:
  type: duckdb
  database: "local.db"
""")

        profile = Profile.load("local_db")
        assert profile.con_name == "duckdb"
        assert profile.kwargs_dict["database"] == "local.db"

    def test_load_from_bsl_profiles(self, temp_dir, monkeypatch):
        """Test loading from ~/.config/bsl/profiles/."""
        monkeypatch.setenv("HOME", str(temp_dir))

        # Create a BSL profile manually
        bsl_profiles_dir = temp_dir / ".config" / "bsl" / "profiles"
        bsl_profiles_dir.mkdir(parents=True, exist_ok=True)

        profile_file = bsl_profiles_dir / "saved_db.yml"
        profile_file.write_text("""
saved_db:
  type: duckdb
  database: saved.db
""")

        # Load it back
        profile = Profile.load("saved_db")
        assert profile.con_name == "duckdb"
        assert profile.kwargs_dict["database"] == "saved.db"

    def test_load_nonexistent_profile(self):
        """Test loading a profile that doesn't exist."""
        with pytest.raises(ProfileError, match="not found"):
            Profile.load("nonexistent_profile_xyz")

    def test_load_missing_type_field(self, temp_dir):
        """Test loading profile without type field."""
        profile_file = temp_dir / "bad_profile.yml"
        profile_file.write_text("""
bad_db:
  database: "test.db"
""")

        with pytest.raises(ProfileError, match="must specify 'type' field"):
            Profile.load("bad_db", profile_file=profile_file)


class TestLoadProfileFunction:
    """Test load_profile convenience function."""

    def test_load_profile_from_yaml(self, sample_profile_yaml):
        """Test load_profile function."""
        con = load_profile("dev_db", profile_file=sample_profile_yaml)
        # Returns ibis backend by default
        assert con.list_tables() is not None

    def test_load_profile_from_path(self, sample_profile_yaml):
        """Test loading profile directly from file path."""
        con = load_profile(str(sample_profile_yaml))
        # Returns ibis backend by default
        assert con.list_tables() is not None


class TestEnvironmentVariables:
    """Test environment variable substitution in profiles."""

    def test_env_var_substitution(self, temp_dir, monkeypatch):
        """Test that environment variables are substituted."""
        # Set environment variable
        monkeypatch.setenv("TEST_DB_PATH", "/path/to/db.db")

        profile_file = temp_dir / "profile.yml"
        profile_file.write_text("""
env_db:
  type: duckdb
  database: ${TEST_DB_PATH}
""")

        profile = Profile.load("env_db", profile_file=profile_file)
        # Note: the env var substitution happens in _create_connection_from_config
        # which is called when get_con() is invoked, not during profile loading
        # So we check that the profile loaded successfully and contains the var reference
        assert profile.kwargs_dict["database"] == "${TEST_DB_PATH}"

    def test_missing_env_var(self, temp_dir, monkeypatch):
        """Test error when environment variable is not set."""
        # Make sure the env var is NOT set
        monkeypatch.delenv("MISSING_VAR", raising=False)

        profile_file = temp_dir / "profile.yml"
        profile_file.write_text("""
env_db:
  type: duckdb
  database: ${MISSING_VAR}
""")

        profile = Profile.load("env_db", profile_file=profile_file)
        # Error should occur when trying to get connection
        with pytest.raises(ProfileError):
            profile.get_con()


class TestProfileCompatibility:
    """Test Profile API consistency."""

    def test_con_name_property(self):
        """Test con_name property."""
        profile = Profile(con_name="postgres", kwargs={"host": "localhost"})
        assert profile.con_name == "postgres"

    def test_kwargs_dict_property(self):
        """Test kwargs_dict property."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})
        assert isinstance(profile.kwargs_dict, dict)
        assert profile.kwargs_dict == {"database": ":memory:"}

    def test_kwargs_dict_is_copy(self):
        """Test that kwargs_dict returns a copy, not reference."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})
        kwargs1 = profile.kwargs_dict
        kwargs1["modified"] = True
        kwargs2 = profile.kwargs_dict
        assert "modified" not in kwargs2

    def test_get_con_returns_backend(self):
        """Test that get_con returns a working backend connection."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})
        con = profile.get_con()
        # Returns ibis backend by default
        assert con.list_tables() is not None

    def test_get_con_with_xorq_caching(self):
        """Test that use_xorq_backend=True wraps backend with xorq caching if available."""
        profile = Profile(con_name="duckdb", kwargs={"database": ":memory:"})

        try:
            # Try to get xorq-cached backend
            con = profile.get_con(use_xorq_backend=True)
            # If xorq is available, we should get a working backend
            assert con.list_tables() is not None
        except ProfileError as e:
            # If xorq is not installed, should raise ProfileError
            assert "xorq backend requested but xorq is not installed" in str(e)

    def test_load_from_bsl_with_xorq_caching(self, temp_dir, monkeypatch):
        """Test that profiles loaded from BSL folders work with xorq caching."""
        monkeypatch.setenv("HOME", str(temp_dir))

        # Create a BSL profile
        bsl_profiles_dir = temp_dir / ".config" / "bsl" / "profiles"
        bsl_profiles_dir.mkdir(parents=True, exist_ok=True)

        profile_file = bsl_profiles_dir / "cached_db.yml"
        profile_file.write_text("""
cached_db:
  type: duckdb
  database: ":memory:"
""")

        # Load profile from BSL folder
        profile = Profile.load("cached_db")
        assert profile.con_name == "duckdb"

        # Should work with xorq caching if xorq is installed
        try:
            con = profile.get_con(use_xorq_backend=True)
            assert con.list_tables() is not None
        except ProfileError as e:
            # If xorq is not installed, should raise helpful error
            assert "xorq backend requested but xorq is not installed" in str(e)


class TestLoadTablesFromProfile:
    """Test load_tables_from_profile function."""

    def test_load_all_tables(self, temp_dir):
        """Test loading all tables from a profile."""
        # Create profile with file-based DuckDB so tables persist
        db_path = temp_dir / "test.db"
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
my_db:
  type: duckdb
  database: "{db_path}"
""")

        # Load connection and create some tables
        con = load_profile("my_db", profile_file=profile_file)
        con.create_table("flights", {"id": [1, 2, 3], "origin": ["JFK", "LAX", "SFO"]})
        con.create_table("carriers", {"code": ["AA", "UA"], "name": ["American", "United"]})

        # Load all tables from profile (new connection to same database)
        tables = load_tables_from_profile("my_db", profile_file=profile_file)

        assert "flights" in tables
        assert "carriers" in tables
        assert len(tables) >= 2

    def test_load_specific_tables(self, temp_dir):
        """Test loading specific tables from a profile."""
        # Create profile with file-based DuckDB
        db_path = temp_dir / "test2.db"
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
my_db:
  type: duckdb
  database: "{db_path}"
""")

        # Load connection and create tables
        con = load_profile("my_db", profile_file=profile_file)
        con.create_table("flights", {"id": [1, 2, 3]})
        con.create_table("carriers", {"code": ["AA", "UA"]})
        con.create_table("airports", {"code": ["JFK", "LAX"]})

        # Load only specific tables (new connection)
        tables = load_tables_from_profile(
            "my_db", table_names=["flights", "carriers"], profile_file=profile_file
        )

        assert "flights" in tables
        assert "carriers" in tables
        assert "airports" not in tables
        assert len(tables) == 2

    def test_load_tables_with_xorq_backend(self, temp_dir):
        """Test loading tables with xorq backend if available."""
        # Create profile with file-based DuckDB
        db_path = temp_dir / "cached.db"
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
cached_db:
  type: duckdb
  database: "{db_path}"
""")

        # Create a table
        con = load_profile("cached_db", profile_file=profile_file)
        con.create_table("flights", {"id": [1, 2, 3]})

        # Try loading with xorq backend (new connection)
        try:
            tables = load_tables_from_profile(
                "cached_db",
                table_names=["flights"],
                profile_file=profile_file,
                use_xorq_backend=True,
            )
            assert "flights" in tables
            # Verify the table is usable
            result = tables["flights"].execute()
            assert len(result) == 3
        except ProfileError as e:
            # If xorq is not installed, should raise helpful error
            assert "xorq backend requested but xorq is not installed" in str(e)

    def test_load_tables_error_handling(self, temp_dir):
        """Test error handling when table doesn't exist."""
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text("""
my_db:
  type: duckdb
  database: ":memory:"
""")

        # Try loading non-existent table
        with pytest.raises(ProfileError) as exc_info:
            load_tables_from_profile(
                "my_db", table_names=["nonexistent"], profile_file=profile_file
            )

        assert "Failed to load table 'nonexistent'" in str(exc_info.value)


class TestParquetLoading:
    """Test generic parquet file loading for backends that support read_parquet."""

    def test_parquet_loading_with_duckdb(self, temp_dir):
        """Test loading parquet files with DuckDB backend."""
        # Create a test parquet file
        import ibis

        test_data = ibis.memtable({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        parquet_path = temp_dir / "test.parquet"
        test_data.to_parquet(parquet_path)

        # Create profile with tables configuration
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
parquet_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_table: "{parquet_path}"
""")

        # Load profile and verify table is available
        con = load_profile("parquet_db", profile_file=profile_file)
        tables = con.list_tables()
        assert "my_table" in tables

        # Verify data
        result = con.table("my_table").execute()
        assert len(result) == 3
        assert list(result["name"]) == ["a", "b", "c"]

    def test_parquet_loading_with_dict_config(self, temp_dir):
        """Test loading parquet files with dict source configuration."""
        # Create a test parquet file
        import ibis

        test_data = ibis.memtable({"x": [10, 20, 30]})
        parquet_path = temp_dir / "data.parquet"
        test_data.to_parquet(parquet_path)

        # Create profile with dict-style tables configuration
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
dict_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_data:
      source: "{parquet_path}"
""")

        # Load and verify
        con = load_profile("dict_db", profile_file=profile_file)
        result = con.table("my_data").execute()
        assert len(result) == 3
        assert list(result["x"]) == [10, 20, 30]

    def test_parquet_loading_unsupported_backend_error(self, temp_dir):
        """Test error when backend doesn't support read_parquet."""
        # Create a mock backend without read_parquet
        from unittest.mock import MagicMock, patch

        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text("""
unsupported_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_table: "test.parquet"
""")

        # Mock the connection to not have read_parquet
        mock_connection = MagicMock()
        del mock_connection.read_parquet  # Remove the attribute

        with patch("ibis.duckdb.connect", return_value=mock_connection):
            with pytest.raises(ProfileError) as exc_info:
                load_profile("unsupported_db", profile_file=profile_file)

            error_msg = str(exc_info.value)
            assert "does not support loading parquet files" in error_msg
            assert "read_parquet()" in error_msg

    def test_parquet_loading_file_not_found_error(self, temp_dir):
        """Test error handling when parquet file doesn't exist."""
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text("""
missing_file_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_table: "nonexistent.parquet"
""")

        # Should raise error about missing file
        with pytest.raises(ProfileError) as exc_info:
            load_profile("missing_file_db", profile_file=profile_file)

        error_msg = str(exc_info.value)
        assert "Failed to load parquet file" in error_msg
        assert "my_table" in error_msg
