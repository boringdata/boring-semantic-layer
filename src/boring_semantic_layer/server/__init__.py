"""HTTP server entrypoints for boring-semantic-layer."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

from .api import create_app

__all__ = ["create_app", "main"]


def main(
    config: str | None = None,
    host: str = "0.0.0.0",
    port: int = 8000,
    reload: bool = False,
    cors_origins: Sequence[str] | None = None,
) -> None:
    """Start the BSL HTTP API server with uvicorn."""
    try:
        import uvicorn
    except ImportError as exc:
        raise ImportError(
            "uvicorn is required to run the BSL server. "
            "Install it with: pip install 'boring-semantic-layer[server]'"
        ) from exc

    if config:
        os.environ["BSL_CONFIG_PATH"] = str(Path(config).resolve())
    if cors_origins:
        os.environ["BSL_CORS_ORIGINS"] = ",".join(cors_origins)

    uvicorn.run(
        "boring_semantic_layer.server.api:app",
        host=host,
        port=port,
        reload=reload,
    )
