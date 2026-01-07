"""Agent backend adapters.

Available backends:
- LangGraphBackend: LangGraph agent with selective middleware
"""

__all__ = ["LangGraphBackend"]


def __getattr__(name):
    """Lazy import of backends to avoid requiring all dependencies."""
    if name == "LangGraphBackend":
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        return LangGraphBackend
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
