# Query Agent

The Query Agent is the easiest way to ask natural-language questions of your semantic tables.

It converts a user's prompt into a valid BSL query and returns the resulting table or chart together with a concise summary.

Depending on your workflow, you can expose the Query Agent in several ways:

- [**BSL as an MCP integration**](/agents/query/mcp) — give an LLM access to your tables via an MCP interface.

- [**BSL as an LLM tool**](/agents/query/llm-tool) — expose BSL as a callable tool for the model to use.

- [**BSL as a skill for Claude Code / Codex / Cursor**](/agents/query/skill) — add querying capabilities to your local coding assistant.

Choose the surface that best matches your needs — each option offers the same core querying power but integrates differently depending on where and how you run your LLM.

| Option | Pros | Cons |
| --- | --- | --- |
| [BSL as an MCP integration](/agents/query/mcp) | Zero custom UI; Claude Desktop immediately understands your models | Requires running an MCP server alongside your project |
| [BSL as an LLM tool](/agents/query/llm-tool) | Works in CLI, Slack, or any LangChain stack with full control | Needs LangChain wiring plus hosting infra/tokens |
| [BSL as a skill](/agents/query/skill) | Fastest to share—paste the skill into any local coding agent | No automatic tool execution unless paired with MCP/CLI |
