# Query Agent: BSL Skills for AI Coding Assistants

AI coding assistants can help you build and maintain your semantic model, but they don't know about BSL out of the box.

The easiest way to provide this context is to use the `bsl` CLI, which copies pre-built prompts into your agent's config folder.

## Installation

### Claude Code

```bash
bsl skill install claude-code
```

Or manually copy the prompt from [`skills/claude-code/`](https://github.com/boringdata/boring-semantic-layer/tree/main/docs/md/skills/claude-code/) to `.claude/skills/bsl-query-expert/SKILL.md`

### Claude Desktop

1. Open Claude Desktop and click **Skills -> New Skill**.
2. Run `bsl skill show claude-code` and copy the output into the editor.
3. Name it something memorable like "BSL Query Expert" and save.
4. Add optional tags ("data", "analytics") so you can search for it quickly.

### Cursor

```bash
bsl skill install cursor
```

Or manually copy the prompt from [`skills/cursor/`](https://github.com/boringdata/boring-semantic-layer/tree/main/docs/md/skills/cursor) to `.cursorrules` in your project root

### Codex (OpenAI)

```bash
bsl skill install codex
```

Or manually copy the prompt from [`skills/codex/`](https://github.com/boringdata/boring-semantic-layer/tree/main/docs/md/skills/codex) to your Codex system instructions

## CLI Reference

```bash
# List available skills
bsl skill list

# Preview a skill before installing
bsl skill show claude-code

# Install a skill to your project
bsl skill install claude-code
bsl skill install cursor
bsl skill install codex

# Overwrite existing file
bsl skill install cursor --force
```
