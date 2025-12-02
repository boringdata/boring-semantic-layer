"""Agent backend adapters (LangChain, LangGraph, OpenAI Assistants, MCP, Slack).

Available backends:
- LangChainAgent: Simple tool-calling loop with any LLM
- LangGraphReActAgent: Full ReAct agent with LangGraph
- OpenAIAssistantAgent: OpenAI Assistants API with native tool handling
"""

from boring_semantic_layer.agents.backends.langchain import LangChainAgent
from boring_semantic_layer.agents.backends.langgraph_react import LangGraphReActAgent
from boring_semantic_layer.agents.backends.openai_assistant import OpenAIAssistantAgent

__all__ = [
    "LangChainAgent",
    "LangGraphReActAgent",
    "OpenAIAssistantAgent",
]
