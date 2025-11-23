"""Tests for Profile API functionality."""

import tempfile
from pathlib import Path

import pytest

from boring_semantic_layer.profile import (
    ProfileError,
    get_connection,
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


class TestProfileSaveLoad:
    """Test loading profiles."""

    def test_load_from_yaml_file(self, sample_profile_yaml):
        """Test loading profile from YAML file using profile_file parameter."""
        con = get_connection("dev_db", profile_file=sample_profile_yaml)
        assert con.list_tables() is not None

    def test_load_from_yaml_with_profile_file(self, sample_profile_yaml):
        """Test loading specific profile from YAML file."""
        con = get_connection("test_db", profile_file=sample_profile_yaml)
        assert con.list_tables() is not None

    def test_load_from_local_profile_yml(self, temp_dir, monkeypatch):
        """Test loading from ./profiles.yml in current directory."""
        monkeypatch.chdir(temp_dir)

        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text("""
local_db:
  type: duckdb
  database: "local.db"
""")

        con = get_connection("local_db")
        assert con.list_tables() is not None

    def test_load_from_bsl_profiles(self, temp_dir, monkeypatch):
        """Test loading from ~/.config/bsl/profiles/."""
        monkeypatch.setenv("HOME", str(temp_dir))

        bsl_profiles_dir = temp_dir / ".config" / "bsl" / "profiles"
        bsl_profiles_dir.mkdir(parents=True, exist_ok=True)

        profile_file = bsl_profiles_dir / "saved_db.yml"
        profile_file.write_text("""
saved_db:
  type: duckdb
  database: saved.db
""")

        con = get_connection("saved_db")
        assert con.list_tables() is not None

    def test_load_nonexistent_profile(self):
        """Test loading a profile that doesn't exist."""
        with pytest.raises(ProfileError, match="not found"):
            get_connection("nonexistent_profile_xyz")

    def test_load_missing_type_field(self, temp_dir):
        """Test loading profile without type field."""
        profile_file = temp_dir / "bad_profile.yml"
        profile_file.write_text("""
bad_db:
  database: "test.db"
""")

        with pytest.raises(ProfileError, match="must specify 'type' field"):
            get_connection("bad_db", profile_file=profile_file)


class TestLoadProfileFunction:
    """Test get_connection() function."""

    def test_load_profile_from_yaml(self, sample_profile_yaml):
        """Test get_connection() function."""
        con = get_connection("dev_db", profile_file=sample_profile_yaml)
        assert con.list_tables() is not None

    def test_load_profile_from_path(self, sample_profile_yaml):
        """Test loading profile directly from file path."""
        con = get_connection(str(sample_profile_yaml))
        assert con.list_tables() is not None


class TestEnvironmentVariables:
    """Test environment variable substitution in profiles."""

    def test_env_var_substitution(self, temp_dir, monkeypatch):
        """Test that environment variables are substituted."""
        db_path = temp_dir / "test_env.db"
        monkeypatch.setenv("TEST_DB_PATH", str(db_path))

        profile_file = temp_dir / "profile.yml"
        profile_file.write_text("""
env_db:
  type: duckdb
  database: ${TEST_DB_PATH}
""")

        con = get_connection("env_db", profile_file=profile_file)
        assert con.list_tables() is not None

    def test_missing_env_var(self, temp_dir, monkeypatch):
        """Test error when environment variable is not set."""
        monkeypatch.delenv("MISSING_VAR", raising=False)

        profile_file = temp_dir / "profile.yml"
        profile_file.write_text("""
env_db:
  type: duckdb
  database: ${MISSING_VAR}
""")

        with pytest.raises((ProfileError, KeyError)):
            get_connection("env_db", profile_file=profile_file)

    def test_bsl_profile_file_env_var(self, temp_dir, monkeypatch):
        """Test BSL_PROFILE_FILE environment variable for profile file path."""
        profile_file = temp_dir / "my_profiles.yml"
        profile_file.write_text("""
test_profile:
  type: duckdb
  database: ":memory:"
""")

        monkeypatch.setenv("BSL_PROFILE_FILE", str(profile_file))

        # get_connection should use BSL_PROFILE_FILE when profile_file is not provided
        connection = get_connection(profile="test_profile")
        # Should not raise an error - the file path was read from env var
        assert connection is not None
        assert hasattr(connection, "list_tables")


class TestParquetLoading:
    """Test generic parquet file loading for backends that support read_parquet."""

    def test_parquet_loading_with_duckdb(self, temp_dir):
        """Test loading parquet files with DuckDB backend."""
        import ibis

        test_data = ibis.memtable({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        parquet_path = temp_dir / "test.parquet"
        test_data.to_parquet(parquet_path)

        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
parquet_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_table: "{parquet_path}"
""")

        con = get_connection("parquet_db", profile_file=profile_file)
        tables = con.list_tables()
        assert "my_table" in tables

        result = con.table("my_table").execute()
        assert len(result) == 3
        assert list(result["name"]) == ["a", "b", "c"]

    def test_parquet_loading_with_dict_config(self, temp_dir):
        """Test loading parquet files with dict source configuration."""
        import ibis

        test_data = ibis.memtable({"x": [10, 20, 30]})
        parquet_path = temp_dir / "data.parquet"
        test_data.to_parquet(parquet_path)

        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text(f"""
dict_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_data:
      source: "{parquet_path}"
""")

        con = get_connection("dict_db", profile_file=profile_file)
        result = con.table("my_data").execute()
        assert len(result) == 3
        assert list(result["x"]) == [10, 20, 30]

    def test_parquet_loading_unsupported_backend_error(self, temp_dir):
        """Test error when backend doesn't support read_parquet or file doesn't exist."""
        profile_file = temp_dir / "profiles.yml"
        profile_file.write_text("""
unsupported_db:
  type: duckdb
  database: ":memory:"
  tables:
    my_table: "nonexistent_test.parquet"
""")

        with pytest.raises(ProfileError) as exc_info:
            get_connection("unsupported_db", profile_file=profile_file)

        error_msg = str(exc_info.value)
        assert (
            "Failed to load parquet file" in error_msg
            or "does not support loading parquet files" in error_msg
        )

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

        with pytest.raises(ProfileError) as exc_info:
            get_connection("missing_file_db", profile_file=profile_file)

        error_msg = str(exc_info.value)
        assert "Failed to load parquet file" in error_msg
        assert "my_table" in error_msg
