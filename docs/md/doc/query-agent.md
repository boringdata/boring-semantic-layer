# Query Agent

The Query Agent lets you ask natural-language questions about your semantic tables.

It converts a user prompt into a valid BSL query and returns the resulting table or chart along with a concise summary.

You can expose the Query Agent in three ways, depending on your workflow:

### [MCP Server](/agents/mcp)

Expose your tables to any LLM via the Model Context Protocol.

- **Pros:** All major LLM providers support MCP out-of-the-box
- **Cons:** Requires running an MCP server alongside your project

### [LLM Tool](/agents/tool)

Let the model execute BSL queries directly as a callable tool.

- **Pros:** No additional infrastructure—the LLM executes queries inline
- **Cons:** Requires a sandboxing solution for production use

### [AI Skills (CLI)](/agents/skill)

Add BSL querying to your local coding assistant (Claude Code, Cursor, Codex).

- **Pros:** Fastest setup—run `bsl skill install` and you're ready
- **Cons:** Runs locally only

---

Want to try it out? The [Demo Chat](/agents/chat) provides a built-in CLI interface to explore your semantic models using natural language.
