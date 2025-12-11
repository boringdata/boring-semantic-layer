"""Agent backend adapters.

Available backends:
- LangGraphBackend: LangGraph agent with selective middleware
"""

from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

__all__ = ["LangGraphBackend"]
