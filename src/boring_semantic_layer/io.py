"""YAML/URL loading helpers for model configuration files."""

from __future__ import annotations

from pathlib import Path

import yaml


def _is_url(path: str | Path | None) -> bool:
    """Check if a path is a URL."""
    if path is None:
        return False
    from urllib.parse import urlparse

    parsed = urlparse(str(path))
    return parsed.scheme in ("http", "https")


def _fetch_url_content(url: str) -> str:
    """Fetch content from a URL.

    Args:
        url: The URL to fetch

    Returns:
        The content as a string

    Raises:
        ValueError: If the fetch fails
    """
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise ValueError(f"HTTP Error {e.code}: {e.reason} for URL: {url}") from e
    except urllib.error.URLError as e:
        raise ValueError(f"URL Error: {e.reason} for URL: {url}") from e
    except Exception as e:
        raise ValueError(f"Failed to fetch URL {url}: {e}") from e


def read_yaml_file(yaml_path: str | Path) -> dict:
    """Read and parse YAML file into dict. Supports local files and URLs.

    Args:
        yaml_path: Path to local file or URL (http:// or https://)

    Returns:
        Parsed YAML content as dict
    """
    try:
        if _is_url(yaml_path):
            content_str = _fetch_url_content(str(yaml_path))
            content = yaml.safe_load(content_str)
        else:
            yaml_path = Path(yaml_path)
            if not yaml_path.exists():
                raise FileNotFoundError(f"YAML file not found: {yaml_path}")

            with open(yaml_path) as f:
                content = yaml.safe_load(f)

        if not isinstance(content, dict):
            raise ValueError(f"YAML file must contain a dict, got: {type(content)}")

        return content
    except (FileNotFoundError, ValueError):
        raise
    except Exception as e:
        raise ValueError(f"Failed to parse YAML file {yaml_path}: {e}") from e
