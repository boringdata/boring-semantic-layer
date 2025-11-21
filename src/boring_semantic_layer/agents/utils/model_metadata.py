"""Utilities for extracting metadata from semantic models."""

from typing import Any


def get_model_description(model: Any) -> str | None:
    """
    Get description for a semantic model.

    For base models, returns the description directly.
    For joined models, constructs a description from underlying tables.

    Args:
        model: SemanticTable instance

    Returns:
        Description string or None if no description available
    """
    description = getattr(model, "description", None)

    if description:
        return description

    # For joined models, construct description from base tables
    if hasattr(model, "op") and type(model.op()).__name__ == "SemanticJoinOp":
        from ...ops import _find_all_root_models

        roots = _find_all_root_models(model.op())
        base_descriptions = []

        for root in roots:
            root_name = getattr(root, "name", None) or "unnamed"
            root_desc = getattr(root, "description", None)
            if root_desc:
                base_descriptions.append(f"{root_name} ({root_desc})")
            else:
                base_descriptions.append(root_name)

        if base_descriptions:
            return "Joined model combining: " + ", ".join(base_descriptions)

    return None
