# Query Agent: BSL as a Claude Code Skill

The Query Agent ships with a dedicated Claude skill so you can paste the instructions into Claude Desktop (or any Claude Code surface) and keep self-hosted agents honest.

## Where to find the skill

The canonical file lives at [`src/boring_semantic_layer/agents/claude-code/bsl-query-expert/SKILL.md`](../../src/boring_semantic_layer/agents/claude-code/bsl-query-expert/SKILL.md). It documents:

- The exact method order (`model -> with_dimensions -> filter -> group_by -> aggregate`).
- Time-dimension recipes with `.truncate()` and string casting.
- Windowing with `.mutate().over(...)` for rolling averages, cumulative sums, and rankings.
- Tool usage rules (always call `query_model` instead of returning pseudo-code).

## Loading it in Claude Desktop

1. Open Claude Desktop and click **Skills -> New Skill**.
2. Copy the entire `SKILL.md` contents into the editor.
3. Name it something memorable like "BSL Query Expert" and save.
4. Add optional tags ("data", "analytics") so you can search for it quickly.

Now, whenever you spin up a Claude session with your MCP server or plain text context, attach this skill. Claude will follow the instructions verbatim and call your Query Agent via MCP or CLI commands without hallucinating SQL.

## Reusing the skill elsewhere

- **Claude Projects** -> point to the same `SKILL.md` file to give project-specific contexts the semantic-layer instructions.
- **Prompt libraries** -> paste the skill into your LangChain prompt templates so non-Claude LLMs mimic the same guard rails.
- **Docs** -> this page is a handy reminder for new teammates about where the skill lives and why it matters.
