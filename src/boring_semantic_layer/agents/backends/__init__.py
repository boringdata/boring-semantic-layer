"""Agent backend adapters (LangChain, LangGraph, MCP, Slack).

Available backends:
- LangChainAgent: Simple tool-calling loop with any LLM
- LangGraphReActAgent: Full ReAct agent with LangGraph
"""

from boring_semantic_layer.agents.backends.langchain import LangChainAgent
from boring_semantic_layer.agents.backends.langgraph_react import LangGraphReActAgent

__all__ = [
    "LangChainAgent",
    "LangGraphReActAgent",
]
