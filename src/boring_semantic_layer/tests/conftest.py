"""
Pytest configuration and shared fixtures for api tests.

This module provides common test fixtures for:
- Database connections (Ibis/DuckDB and Malloy)
- Dataset loading from malloy-samples
- Connection management and cleanup
"""

import pytest
from pathlib import Path
import sys

# Add fixtures directory to path for imports
_fixtures_path = Path(__file__).parent / "fixtures"
if str(_fixtures_path) not in sys.path:
    sys.path.insert(0, str(_fixtures_path))

from datasets import DatasetManager, get_dataset
from connections import ConnectionManager, reset_connection_manager


@pytest.fixture(scope="session")
def dataset_manager():
    """
    Session-scoped dataset manager for downloading and caching test data.

    Returns:
        DatasetManager instance
    """
    return DatasetManager()


@pytest.fixture(scope="module")
def connection_manager():
    """
    Module-scoped connection manager for database connections.

    Provides both Ibis and Malloy connections. Automatically cleans up
    after each test module.

    Returns:
        ConnectionManager instance

    Yields:
        ConnectionManager that will be closed after module tests complete
    """
    manager = ConnectionManager(in_memory=True)
    yield manager
    manager.close()
    reset_connection_manager()


@pytest.fixture(scope="module")
def ibis_con(connection_manager):
    """
    Module-scoped Ibis DuckDB connection for BSL queries.

    Returns:
        Ibis connection object
    """
    return connection_manager.get_ibis_connection()


@pytest.fixture
def malloy_runtime(connection_manager):
    """
    Function-scoped Malloy runtime context manager.

    Usage in tests:
        def test_something(malloy_runtime):
            with malloy_runtime as runtime:
                result = await runtime.load_file("query.malloy").run()

    Returns:
        Context manager for malloy.Runtime
    """
    return connection_manager.get_malloy_runtime()


# Dataset fixtures - commonly used datasets
@pytest.fixture(scope="session")
def flights_dataset(dataset_manager):
    """Path to flights.parquet from malloy-samples."""
    return dataset_manager.get("flights")


@pytest.fixture(scope="session")
def carriers_dataset(dataset_manager):
    """Path to carriers.parquet from malloy-samples."""
    return dataset_manager.get("carriers")


@pytest.fixture(scope="session")
def airports_dataset(dataset_manager):
    """Path to airports.parquet from malloy-samples."""
    return dataset_manager.get("airports")


@pytest.fixture(scope="session")
def order_items_dataset(dataset_manager):
    """Path to order_items.parquet from malloy-samples."""
    return dataset_manager.get("order_items")


@pytest.fixture(scope="session")
def users_dataset(dataset_manager):
    """Path to users.parquet from malloy-samples."""
    return dataset_manager.get("users")


@pytest.fixture(scope="session")
def products_dataset(dataset_manager):
    """Path to products.parquet from malloy-samples."""
    return dataset_manager.get("products")


@pytest.fixture(scope="session")
def inventory_items_dataset(dataset_manager):
    """Path to inventory_items.parquet from malloy-samples."""
    return dataset_manager.get("inventory_items")


@pytest.fixture
def load_dataset(connection_manager):
    """Fixture that returns a function to load datasets into Ibis."""

    def _load(dataset_name: str):
        path = get_dataset(dataset_name)
        return connection_manager.load_parquet_ibis(path)

    return _load
