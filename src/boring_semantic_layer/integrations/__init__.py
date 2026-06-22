"""Optional integrations for dashboard and BI runtimes."""

from .perspective import (
    PerspectiveArtifact,
    PerspectiveColumn,
    infer_perspective_schema,
    perspective_viewer_config,
    write_perspective_artifact,
)

__all__ = [
    "PerspectiveArtifact",
    "PerspectiveColumn",
    "infer_perspective_schema",
    "perspective_viewer_config",
    "write_perspective_artifact",
]
