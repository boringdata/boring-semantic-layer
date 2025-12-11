"""Token counting utilities for BSL agents."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any


@lru_cache(maxsize=4)
def _get_encoder(model: str = "gpt-4"):
    """Get tiktoken encoder for a model (cached)."""
    try:
        import tiktoken

        # Map common model names to tiktoken encoding
        if "gpt-4" in model or "gpt-3.5" in model:
            return tiktoken.encoding_for_model("gpt-4")
        elif "claude" in model:
            # Claude uses similar tokenization to GPT-4 (cl100k_base)
            return tiktoken.get_encoding("cl100k_base")
        else:
            # Default to cl100k_base (GPT-4 encoding)
            return tiktoken.get_encoding("cl100k_base")
    except ImportError:
        return None


def count_tokens(text: str, model: str = "gpt-4") -> int:
    """Count tokens in a string using tiktoken.

    Args:
        text: The text to count tokens for
        model: Model name to use for tokenization

    Returns:
        Token count, or 0 if tiktoken is not available
    """
    encoder = _get_encoder(model)
    if encoder is None:
        return 0
    return len(encoder.encode(text))


def count_message_tokens(messages: list[dict[str, Any]], model: str = "gpt-4") -> int:
    """Count tokens in a list of chat messages.

    Accounts for message structure overhead (role, content formatting).

    Args:
        messages: List of message dicts with 'role' and 'content' keys
        model: Model name to use for tokenization

    Returns:
        Estimated token count
    """
    encoder = _get_encoder(model)
    if encoder is None:
        return 0

    # Token overhead per message (approximately 4 tokens for role/formatting)
    tokens_per_message = 4
    total = 0

    for msg in messages:
        total += tokens_per_message
        content = msg.get("content", "")
        if isinstance(content, str):
            total += len(encoder.encode(content))
        elif isinstance(content, list):
            # Handle Claude's mixed content format
            for block in content:
                if isinstance(block, dict) and "text" in block:
                    total += len(encoder.encode(block["text"]))

        # Count role
        role = msg.get("role", "")
        if role:
            total += len(encoder.encode(role))

    # Add 3 tokens for assistant priming
    total += 3
    return total


def count_tools_tokens(tools: list[dict[str, Any]], model: str = "gpt-4") -> int:
    """Count tokens in tool definitions.

    Args:
        tools: List of tool definition dicts (OpenAI format)
        model: Model name to use for tokenization

    Returns:
        Estimated token count for tools
    """
    encoder = _get_encoder(model)
    if encoder is None:
        return 0

    # Serialize tools to JSON and count tokens
    # This is an approximation - actual tokenization may vary
    tools_json = json.dumps(tools)
    return len(encoder.encode(tools_json))


def estimate_input_tokens(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    system_prompt: str | None = None,
    model: str = "gpt-4",
) -> int:
    """Estimate total input tokens for an LLM request.

    Args:
        messages: List of chat messages
        tools: Optional list of tool definitions
        system_prompt: Optional system prompt (if not in messages)
        model: Model name for tokenization

    Returns:
        Estimated total input tokens
    """
    total = count_message_tokens(messages, model)

    if tools:
        total += count_tools_tokens(tools, model)

    if system_prompt:
        total += count_tokens(system_prompt, model)

    return total
