"""FastAPI HTTP server for boring-semantic-layer.

Start with:
    bsl serve --config ./semantic_config.py

or programmatically:
    from boring_semantic_layer.server import main
    main(config="./semantic_config.py", port=8000)
"""
from __future__ import annotations

import os
import pathlib


def main(
    config: str | None = None,
    host: str = "0.0.0.0",
    port: int = 8000,
    reload: bool = False,
) -> None:
    """Start the BSL FastAPI server with uvicorn."""
    try:
        import uvicorn
    except ImportError:
        raise ImportError(
            "uvicorn is required to run the BSL server. "
            "Install it with: pip install 'boring-semantic-layer[server]'"
        )

    if config:
        os.environ["BSL_CONFIG_PATH"] = str(pathlib.Path(config).resolve())

    uvicorn.run(
        "boring_semantic_layer.server.api:app",
        host=host,
        port=port,
        reload=reload,
    )
