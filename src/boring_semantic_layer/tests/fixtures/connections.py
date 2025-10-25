"""
Database connection management for Malloy correctness testing.

Provides shared connection fixtures for both BSL (Ibis) and Malloy (malloy-py).
"""

from pathlib import Path
from typing import Optional
import ibis
from contextlib import contextmanager

try:
    import malloy
    from malloy.data.duckdb import DuckDbConnection as MalloyDuckDbConnection

    MALLOY_AVAILABLE = True
except ImportError:
    MALLOY_AVAILABLE = False
    MalloyDuckDbConnection = None


class ConnectionManager:
    """
    Manages database connections for both BSL and Malloy testing.

    Ensures connections are properly configured for test isolation
    and provides convenience methods for loading datasets.
    """

    def __init__(self, in_memory: bool = True):
        """
        Initialize connection manager.

        Args:
            in_memory: If True, use in-memory DuckDB connections
        """
        self.in_memory = in_memory
        self._ibis_connection = None
        self._malloy_runtime = None

    def get_ibis_connection(self) -> ibis.BaseBackend:
        """
        Get or create an Ibis DuckDB connection for BSL queries.

        Returns:
            Ibis connection object
        """
        if self._ibis_connection is None:
            if self.in_memory:
                self._ibis_connection = ibis.duckdb.connect()
            else:
                self._ibis_connection = ibis.duckdb.connect(":memory:")

        return self._ibis_connection

    @contextmanager
    def get_malloy_runtime(self):
        """
        Get a Malloy runtime context manager for executing Malloy queries.

        Yields:
            malloy.Runtime object configured with DuckDB connection

        Example:
            >>> cm = ConnectionManager()
            >>> with cm.get_malloy_runtime() as runtime:
            ...     result = await runtime.load_file("query.malloy").run()
        """
        if not MALLOY_AVAILABLE:
            raise ImportError(
                "malloy package is not available. Install with: pip install malloy"
            )

        with malloy.Runtime() as runtime:
            # Add DuckDB connection to Malloy runtime
            # Note: Malloy uses current directory as home_dir by default
            runtime.add_connection(MalloyDuckDbConnection(home_dir="."))
            yield runtime

    def load_parquet_ibis(
        self, dataset_path: Path, table_name: Optional[str] = None
    ) -> ibis.expr.types.relations.Table:
        """
        Load a parquet file into Ibis for BSL queries.

        Args:
            dataset_path: Path to parquet file
            table_name: Optional name for the table

        Returns:
            Ibis table expression
        """
        con = self.get_ibis_connection()
        return con.read_parquet(str(dataset_path))

    def load_parquet_url_ibis(
        self, url: str, table_name: Optional[str] = None
    ) -> ibis.expr.types.relations.Table:
        """
        Load a parquet file from URL into Ibis for BSL queries.

        Args:
            url: URL to parquet file
            table_name: Optional name for the table

        Returns:
            Ibis table expression
        """
        con = self.get_ibis_connection()
        return con.read_parquet(url)

    def close(self):
        """Close all open connections."""
        if self._ibis_connection is not None:
            # DuckDB connections in Ibis are automatically managed
            self._ibis_connection = None

        if self._malloy_runtime is not None:
            # Malloy runtime is context-managed, no explicit close needed
            self._malloy_runtime = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Global connection manager for tests (singleton pattern)
_default_connection_manager = None


def get_connection_manager() -> ConnectionManager:
    """
    Get the default connection manager for tests.

    Returns:
        Shared ConnectionManager instance
    """
    global _default_connection_manager
    if _default_connection_manager is None:
        _default_connection_manager = ConnectionManager(in_memory=True)
    return _default_connection_manager


def reset_connection_manager():
    """
    Reset the global connection manager.

    Useful for test isolation between test modules.
    """
    global _default_connection_manager
    if _default_connection_manager is not None:
        _default_connection_manager.close()
        _default_connection_manager = None
