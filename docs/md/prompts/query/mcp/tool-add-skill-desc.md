Create a new skill on disk. Writes `<skills_dir>/<name>/SKILL.md` with YAML frontmatter (`name`, `description`) and the markdown body you provide as `steps`. The new skill is immediately registered as MCP resources (`skill://<name>/SKILL.md` and any per-file URIs) and shows up in subsequent calls to `get_domain_context`.

Use this when you've learned a non-obvious analytical convention, domain rule, or runbook that future agents should be able to retrieve. Skills should be:

- **Triggered, not described.** The `description` is the trigger phrase agents see when deciding whether to invoke the skill — phrase it so the matching scenarios are clear.
- **Runnable.** The `steps` body should be operational instructions, not a description of what the skill is.
- **Domain-specific.** Generic programming advice doesn't belong in a skill — that's training data. Save the things that come from this dataset / this domain / this team.

`name` must be kebab-case. If `model_name` is provided, the skill is scoped to that model; otherwise it goes into the parent skills_dir. Refuses to overwrite an existing skill — pick a different name to update an existing one. Use `reference` for longer supporting docs that don't belong in the main SKILL.md.
