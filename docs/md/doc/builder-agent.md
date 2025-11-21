# Builder Agent

The Builder Agent is focused on authoring and evolving semantic tables: defining dimensions, measures, joins, calculated measures, YAML config, and validation patterns. It uses a different Claude skill than the Query Agent because it needs to reason about modeling APIs rather than issuing queries.

## Claude Code Skill

- File: [`src/boring_semantic_layer/agents/claude-code/bsl-model-building/SKILL.md`](../../src/boring_semantic_layer/agents/claude-code/bsl-model-building/SKILL.md)
- Use it when you want Claude Desktop to help write new semantic tables, add time dimensions, or compose models.
- The skill includes:
  - Python DSL examples showing `SemanticTable(...)`, `.with_dimensions`, `.with_measures`, `.with_calculated_measures`, and `.join()` patterns.
  - YAML equivalents so you can copy the same logic into declarative configs.
  - Gotchas such as "measures must aggregate" and "join keys must be defined dimensions".

**Workflow:** Load the skill in Claude Desktop, paste the schema or YAML snippet you are editing, and ask "Generate a semantic table for flights with avg delay and join to airports". Claude will respond with both Python and YAML patterns that mirror the documentation.

## Codex Skill

Running inside the Codex CLI (the environment this assistant uses) already gives you repo access. Pair that with the Builder skill to automate scaffolding:

1. Open `docs/content/semantic-table.md` or the relevant source file in your editor for context.
2. Ask Codex to "apply the builder skill" when drafting new semantic tables. It will reference `bsl-model-building/SKILL.md` to keep the API usage correct.
3. Use the CLI's `apply_patch` output directly to drop in the generated models or YAML definitions.

This approach keeps all modeling work version-controlled while still benefiting from the same guard rails the Claude skill enforces.

## Cursor (or other AI IDEs)

If you prefer Cursor, VS Code Copilot Chat, or another AI-assisted IDE:

1. Store the builder skill text in a snippet (Cursor: *Settings -> Custom Instructions*).
2. Add quick prompts like "Use the BSL builder skill" so the IDE pastes the instructions before generating code.
3. Point the IDE at your actual data context (DuckDB schema, YAML file) so it can thread the builder guard rails through your request.

Regardless of the host, the Builder Agent should always cite the same modeling patterns. That keeps upstream MCP/Query agents consistent because every semantic table passes through the same validation philosophy.
