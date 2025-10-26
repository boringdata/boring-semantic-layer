"""
Dataset management for Malloy correctness testing.

Provides utilities to download, cache, and load datasets from malloy-samples.
All datasets are cached locally to avoid repeated downloads.
"""

import urllib.error
import urllib.request
from functools import cache
from pathlib import Path

MALLOY_SAMPLES_BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

# Available datasets in malloy-samples
AVAILABLE_DATASETS = {
    "aircraft": "aircraft.parquet",
    "aircraft_models": "aircraft_models.parquet",
    "airports": "airports.parquet",
    "carriers": "carriers.parquet",
    "flights": "flights.parquet",
    "ga_sample": "ga_sample.parquet",
    "inventory_items": "inventory_items.parquet",
    "order_items": "order_items.parquet",
    "products": "products.parquet",
    "users": "users.parquet",
}


def get_data_dir() -> Path:
    """Get the local data directory for cached datasets."""
    data_dir = Path(__file__).parent / "sample_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def download_dataset(dataset_name: str, force_redownload: bool = False) -> Path:
    """
    Download a dataset from malloy-samples if not already cached.

    Args:
        dataset_name: Name of the dataset (e.g., 'flights', 'order_items')
        force_redownload: If True, download even if file exists

    Returns:
        Path to the downloaded parquet file

    Raises:
        ValueError: If dataset_name is not in AVAILABLE_DATASETS
        urllib.error.URLError: If download fails
    """
    if dataset_name not in AVAILABLE_DATASETS:
        available = ", ".join(AVAILABLE_DATASETS.keys())
        raise ValueError(f"Unknown dataset '{dataset_name}'. Available: {available}")

    filename = AVAILABLE_DATASETS[dataset_name]
    data_dir = get_data_dir()
    local_path = data_dir / filename

    # Return cached file if it exists and we're not forcing redownload
    if local_path.exists() and not force_redownload:
        return local_path

    # Download from malloy-samples GitHub
    url = f"{MALLOY_SAMPLES_BASE_URL}/{filename}"

    try:
        print(f"Downloading {dataset_name} from {url}...")
        urllib.request.urlretrieve(url, local_path)
        print(f"✓ Downloaded {dataset_name} to {local_path}")
        return local_path
    except urllib.error.URLError as e:
        raise urllib.error.URLError(
            f"Failed to download {dataset_name} from {url}: {e}",
        ) from e


@cache
def get_dataset_path(dataset_name: str) -> Path:
    """
    Get the path to a dataset, downloading it if necessary.
    Results are cached to avoid repeated downloads.

    Args:
        dataset_name: Name of the dataset (e.g., 'flights', 'order_items')

    Returns:
        Path to the parquet file
    """
    return download_dataset(dataset_name)


def download_all_datasets(force_redownload: bool = False) -> dict[str, Path]:
    """
    Download all available datasets from malloy-samples.

    Args:
        force_redownload: If True, download even if files exist

    Returns:
        Dictionary mapping dataset names to local file paths
    """
    results = {}
    for dataset_name in AVAILABLE_DATASETS:
        try:
            path = download_dataset(dataset_name, force_redownload)
            results[dataset_name] = path
        except Exception as e:
            print(f"✗ Failed to download {dataset_name}: {e}")
            results[dataset_name] = None

    return results


def get_dataset_url(dataset_name: str) -> str:
    """
    Get the remote URL for a dataset.

    This is useful for direct DuckDB loading via HTTP.

    Args:
        dataset_name: Name of the dataset

    Returns:
        Full URL to the dataset file
    """
    if dataset_name not in AVAILABLE_DATASETS:
        available = ", ".join(AVAILABLE_DATASETS.keys())
        raise ValueError(f"Unknown dataset '{dataset_name}'. Available: {available}")

    filename = AVAILABLE_DATASETS[dataset_name]
    return f"{MALLOY_SAMPLES_BASE_URL}/{filename}"


class DatasetManager:
    """
    Manages dataset downloads and caching for tests.

    Example:
        >>> dm = DatasetManager()
        >>> flights_path = dm.get("flights")
        >>> orders_path = dm.get("order_items")
    """

    def __init__(self, cache_dir: Path | None = None):
        """
        Initialize the dataset manager.

        Args:
            cache_dir: Directory for caching datasets.
                      Defaults to fixtures/sample_data
        """
        self.cache_dir = cache_dir or get_data_dir()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, dataset_name: str) -> Path:
        """Get a dataset, downloading if necessary."""
        return get_dataset_path(dataset_name)

    def get_url(self, dataset_name: str) -> str:
        """Get the remote URL for a dataset."""
        return get_dataset_url(dataset_name)

    def download_all(self, force: bool = False) -> dict[str, Path]:
        """Download all available datasets."""
        return download_all_datasets(force)

    def list_available(self) -> list[str]:
        """List all available datasets."""
        return list(AVAILABLE_DATASETS.keys())

    def list_cached(self) -> list[str]:
        """List datasets that are already cached locally."""
        cached = []
        for name, filename in AVAILABLE_DATASETS.items():
            if (self.cache_dir / filename).exists():
                cached.append(name)
        return cached

    def clear_cache(self) -> None:
        """Delete all cached datasets."""
        for filename in AVAILABLE_DATASETS.values():
            path = self.cache_dir / filename
            if path.exists():
                path.unlink()
                print(f"Deleted {path}")


# Convenience function for tests
def get_dataset(name: str) -> Path:
    """
    Quick access to a dataset by name.

    Args:
        name: Dataset name (e.g., 'flights', 'order_items')

    Returns:
        Path to the parquet file
    """
    return get_dataset_path(name)
