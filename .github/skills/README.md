# Agent Skills

Modular skill definitions that extend agent capabilities within this project.
Each skill encapsulates domain-specific patterns, best practices, and reusable
guidance that the agent loads on demand — keeping the base prompt lean while
enabling deep expertise when needed.

## Folder Structure

```
agent_skills/
  <skill_name>/
    SKILL.md        # Required: purpose, scope, guidance, and exclusions
    examples/       # Optional: worked code samples the agent can reference
    scripts/        # Optional: runnable utilities the skill can invoke
```

## How to Add a New Skill

1. Create a subdirectory named after the skill (use `snake_case`).
2. Add a `SKILL.md` with the following sections:
   - **Purpose** — what the skill does.
   - **When to Load** — trigger conditions (be specific).
   - **Do NOT Load When** — explicit exclusions to avoid false positives.
   - **Key Guidance** — patterns, steps, code templates.
3. Add an `examples/` folder for representative code snippets (optional but recommended).
4. Add a `scripts/` folder for standalone utilities the skill references (optional).

## Available Skills

| Skill | Description |
| --- | --- |
| [python_doc_expert](./python_doc_expert/SKILL.md) | Writing, reviewing, and generating Python documentation — docstrings, type annotations, and doc-site tooling. |
| [readme_expert](./readme_expert/SKILL.md) | Contextual README generator. Evaluates directory context to produce a root-level or sub-module `README.md`, with a built-in pre-flight check against redundant documentation. |
