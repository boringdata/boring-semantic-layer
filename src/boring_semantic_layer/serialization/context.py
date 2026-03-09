"""BSL serialization context — carries version and configuration."""

from __future__ import annotations

from typing import Any

from attrs import frozen

from .freeze import list_to_tuple, thaw, thaw_shallow


@frozen
class BSLSerializationContext:
    """Configuration context threaded through serialization/deserialization."""

    version: str = "2.0"

    def deserialize_expr(self, struct_data: Any, label: str) -> Any:
        """Deserialize a structured expression.

        Args:
            struct_data: Structured expression data (tuple/list).
            label: Human-readable context for error messages.

        Returns:
            Deserialized callable/deferred.

        Raises:
            ValueError: If deserialization fails.
        """
        from .helpers import deserialize_structured

        return deserialize_structured(struct_data, label)

    def deserialize_join_predicate(self, struct_data: Any) -> Any:
        """Deserialize a join predicate from structured data.

        Raises:
            ValueError: If deserialization fails.
        """
        from ..utils import structured_to_join_predicate

        if isinstance(struct_data, tuple | list):
            data = list_to_tuple(struct_data) if isinstance(struct_data, list) else struct_data
            predicate = structured_to_join_predicate(data).value_or(None)
            if predicate is None:
                raise ValueError("SemanticJoinOp: failed to deserialize on_struct")
            return predicate
        raise ValueError("SemanticJoinOp: no on_struct data")

    def parse_field(self, metadata: dict, field: str) -> dict | list:
        """Extract a field from tag metadata, thawing frozen tuples.

        xorq's FrozenOrderedDict stores dicts as tuples-of-pairs and lists
        as tuples. This reverses that transformation.
        """
        value = metadata.get(field)
        if not value:
            return {} if field != "order_keys" else []
        return thaw(value)

    def parse_structured_dict(self, raw: Any) -> dict:
        """Convert a FrozenOrderedDict-encoded tuple-of-pairs to a dict (one level).

        Unlike ``parse_field``, this does NOT recurse into values.
        """
        return thaw_shallow(raw)
