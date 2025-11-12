# BSL Integrations

This directory contains integrations for various platforms and frameworks.

## Available Integrations

### LangChain Agent
- **File**: [langchain_agent.py](langchain_agent.py)
- **Purpose**: LangChain-based agent for querying semantic models
- **Usage**: See main documentation

### Slack
- **File**: [slack.py](slack.py)
- **Purpose**: Slack bot integration for BSL queries
- **Usage**: See main documentation

### AiChat
- **Directory**: [aichat/](aichat/)
- **Purpose**: Interactive chat interface using aichat
- **Usage**: `from boring_semantic_layer.integrations.aichat import start_aichat_session`

## Agent Guide

**For all LLM agents and integrations**, see the single comprehensive guide:

📖 **[BSL_AGENT_GUIDE.md](BSL_AGENT_GUIDE.md)**

This is the **only** agent documentation file. It contains:
- Complete BSL query syntax
- Time dimension transformations (IMPORTANT)
- Common patterns and examples
- Error handling
- Integration-specific instructions

All previous scattered documentation has been removed.

## Quick Start for Agents

```python
# 1. Always use .with_dimensions() for time transformations
flights_with_year = flights.with_dimensions(
    arr_year=lambda t: t.arr_time.year()
)

# 2. Then group by dimension name (string)
result = flights_with_year.group_by("arr_year").aggregate("flight_count")
```

**Key Rule**: `group_by()` only accepts dimension names as strings (`tuple[str, ...]`).

See [BSL_AGENT_GUIDE.md](BSL_AGENT_GUIDE.md) for complete documentation.
