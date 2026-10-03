# Retail Data Platform — Agent Rules

Portfolio-grade, end-to-end retail data platform. The design is fixed in
`docs/superpowers/specs/retail-data-platform-design.md`; read the sections relevant to your task
before writing code. Anything not in the spec needs a spec change first — do not invent scope.

## Conventions

- Python 3.12, `uv` workspace (one lockfile at root, one package per top-level directory) in a
  project-local `.venv/` created by `uv sync` and pinned by `.python-version`. Never use the system
  Python (3.14) or a global site-packages; `uv run <cmd>` for everything; never `pip install` ad hoc.
- `ruff` (lint + format) and `mypy --strict` must pass. SQL in dbt is `sqlfluff`-clean.
- Tests with `pytest`; TDD: failing test first, then the minimum code to pass. Pure functions for all
  transformation/cleaning logic so they are testable without infrastructure.
- Config via environment variables with a documented default in `.env.example`; secrets never
  committed. Docker images pinned by tag. Containers talk to Ollama at `host.docker.internal:11434`.
- Host OS is Windows (Docker Desktop + WSL2). Scripts must be POSIX-shell compatible (run inside
  containers or Git Bash); avoid Windows-only paths in code.
- Keep diffs minimal: no speculative abstractions, no extra dependencies without an ADR note, no
  comments unless the *why* is non-obvious. Every non-obvious decision gets a short ADR in `docs/adr/`.
- Commit messages: conventional commits (`feat(ingestion): …`, `fix(dbt): …`).

## Multi-agent workflow

The lead agent plans each phase (`docs/superpowers/plans/`), tags every task with an owner profile,
dispatches independent tasks in parallel, reviews, and integrates. Specialist profiles live in
`.devin/agents/` and own disjoint directories:

| Profile | Owns |
|---|---|
| `platform-engineer` | `docker-compose.yml`, `Makefile`, `pyproject.toml` (root), `.github/`, `terraform/`, `observability/`, `.env.example` |
| `data-engineer` | `ingestion/`, `lakehouse/`, `orchestration/` |
| `analytics-engineer` | `analytics/`, `bi/` |
| `ml-engineer` | `ml/`, `ds/` |
| `ai-engineer` | `agents/`, `genai/` |
| `reviewer` | read-only |

Models: implementers run on `sonnet` (pinned in each profile), `reviewer` on `opus`; the final
whole-branch review also uses `reviewer`. Profiles load only at session start — after editing
`.devin/agents/`, restart the session. The lead dispatches tasks with disjoint files in parallel and
overlaps each task's review with the next implementation; it never runs implementers serially through
one worker.

Rules for every specialist:
1. Touch only your owned paths. If a task needs a change elsewhere (e.g. a new Compose service or a
   root dependency), stop and report the exact change needed instead of making it.
2. Work from the task text you were given plus the spec. Do not re-plan or expand scope.
3. Finish with the verification commands named in the task, run them, and report: files changed,
   test/lint output (verbatim tail), anything you could not do, and open questions. No success claims
   without the output to back them.
4. Commit your task's work on the current task branch with conventional-commit messages. Never
   push, merge, or open PRs — the lead reviews and integrates.
