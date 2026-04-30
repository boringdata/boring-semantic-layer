"""Load semantic models for the HTTP server from a Python config file."""

from __future__ import annotations

import importlib.util
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from uuid import uuid4


def resolve_config_path(config_path: str | Path | None = None) -> Path:
    """Resolve the semantic config path from args, env, or the cwd default."""
    if config_path is None:
        env_path = os.environ.get("BSL_CONFIG_PATH")
        config_path = Path(env_path) if env_path else Path("semantic_config.py")

    resolved = Path(config_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(
            f"Config file not found at {resolved}. "
            "Pass --config <path> to bsl serve or set BSL_CONFIG_PATH."
        )
    return resolved


def load_models(config_path: str | Path | None = None) -> Mapping[str, object]:
    """Load and return the top-level MODELS mapping from a config file."""
    resolved = resolve_config_path(config_path)
    module_name = f"bsl_semantic_config_{uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not import semantic config from {resolved}")

    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(resolved.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        if sys.path and sys.path[0] == str(resolved.parent):
            sys.path.pop(0)

    models = getattr(module, "MODELS", None)
    if not isinstance(models, Mapping):
        raise TypeError(f"{resolved} must define a top-level mapping named MODELS")
    return models
