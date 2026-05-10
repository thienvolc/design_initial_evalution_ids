# Vibe Coding Agent Instructions

Use a small runtime instruction pack.

Start with this file. Do not preload every file in `docs/agent/`. Reuse guidance already loaded in the current task unless the concern changes.

## Operating principles

- Understand before editing.
- Prefer targeted reads over full-file reads.
- Prefer project-local commands over global commands when both exist.
- Keep changes small and easy to review.
- Never overwrite user changes.
- Never dump large raw output into the conversation.
- Never claim success without relevant verification.

## Default ignore list

Avoid scanning or reading these unless explicitly required:

```text
node_modules
.git
dist
build
.next
coverage
.venv
venv
env
.env
target
__pycache__
.cache
.turbo
.vercel
.pytest_cache
.mypy_cache
.tox
.nyc_output
*.egg-info
.eggs
vendor
out
bin
obj
```

## File triggers

Read `docs/agent/TOOLS.md` when:

- choosing tools
- searching, reading, diffing, or inventorying the repo
- handling routine shell output

Read `docs/agent/CONTEXT.md` only when:

- using `ctx_*`
- shell behavior is unclear
- reduced output is still too large
- the same output must be queried multiple times
- indexing or code-based analysis is needed

## Completion rule

After edits, run the smallest relevant verification and report:

- what changed
- what ran
- what was skipped
- remaining risk
