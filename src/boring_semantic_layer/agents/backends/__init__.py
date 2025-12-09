"""Agent backend adapters (LangChain, LangGraph, DeepAgent, MCP, Slack).

Available backends:
- LangChainAgent: Simple tool-calling loop with any LLM
- LangGraphReActAgent: Full ReAct agent with LangGraph
- DeepAgentBackend: Planning agent with DeepAgents (default)
"""

from boring_semantic_layer.agents.backends.langchain import LangChainAgent
from boring_semantic_layer.agents.backends.langgraph import LangGraphReActAgent

__all__ = [
    "LangChainAgent",
    "LangGraphReActAgent",
    "DeepAgentBackend",
]


def __getattr__(name: str):
    """Lazy import for optional dependencies."""
    if name == "DeepAgentBackend":
        from boring_semantic_layer.agents.backends.deepagent import DeepAgentBackend

        return DeepAgentBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
