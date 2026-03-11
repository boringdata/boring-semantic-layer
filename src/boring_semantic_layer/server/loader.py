"""Dynamically loads a semantic_config.py and returns its MODELS dict.

Config path resolution order:
  1. Explicit path passed to load_models()
  2. BSL_CONFIG_PATH environment variable
  3. semantic_config.py in the current working directory
"""
from __future__ import annotations

import importlib.util
import os
import pathlib


def load_models(config_path: str | pathlib.Path | None = None):
    if config_path is None:
        env_path = os.environ.get("BSL_CONFIG_PATH")
        config_path = pathlib.Path(env_path) if env_path else pathlib.Path("semantic_config.py")

    config_path = pathlib.Path(config_path).resolve()

    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found at {config_path}. "
            "Pass --config <path> to bsl serve or set the BSL_CONFIG_PATH environment variable."
        )

    spec = importlib.util.spec_from_file_location("semantic_config", config_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if not hasattr(mod, "MODELS"):
        raise AttributeError(
            f"{config_path} must define a top-level dict named MODELS."
        )

    return mod.MODELS
