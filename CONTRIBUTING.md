# Contributing to clAWS

Thank you for your interest in contributing. clAWS is an Apache 2.0 open-source
reference architecture — contributions that improve correctness, safety, or clarity
are welcome.

## Before you start

- For significant changes, open a GitHub issue first to discuss the approach.
- Bug fixes and documentation improvements can go straight to a PR.
- All contributions must be compatible with Apache 2.0 licensing.

## Setup

```bash
git clone https://github.com/scttfrdmn/claws.git
cd claws
uv sync --extra dev
```

## Making changes

### Branch naming

```
fix/<short-description>       # bug fix
feat/<short-description>      # new feature or capability
docs/<short-description>      # documentation only
refactor/<short-description>  # internal restructuring, no behavior change
```

### Code conventions

- Python 3.12+, type hints on all functions
- Line length 100, enforced by ruff
- Handler pattern: `handler(event: dict, context: Any) -> dict`
- New AWS service clients follow the lazy singleton pattern in `tools/shared.py`
- Deferred imports inside functions use `# noqa: PLC0415`

### Tests

Every change needs a test. Run the full suite before pushing:

```bash
uv run pytest tools/ -v
```

- Unit tests (no AWS): add to the relevant `tools/<tool>/tests/` directory
- Integration tests: use the `substrate` fixture for S3/DynamoDB/Glue/Athena
- OpenSearch is currently mocked via `MagicMock` (substrate support pending:
  [scttfrdmn/substrate#253](https://github.com/scttfrdmn/substrate/issues/253))
- Bedrock is always mocked in the substrate suite; use `tools/tests/live/` for
  real-model validation

The suite must stay green and the test count in `CLAUDE.md` must be updated if
you add tests.

### Linting

```bash
uv run ruff check tools/     # must pass with zero errors
uv run ruff format tools/    # auto-format
uv run mypy tools/           # type check
```

## Commit messages

Follow the existing style — imperative present tense, reference issue numbers:

```
Add OpenSearch aggregation flattening for nested bucket responses

Fixes #19. Flattens nested bucket aggregations into a flat row list
so excavate results have a consistent shape regardless of query type.
```

## Pull requests

- Target the `main` branch
- Fill in the PR description: what changed, why, and how to test it
- One logical change per PR — split unrelated fixes into separate PRs
- All CI checks must pass (ruff, mypy, pytest)

## What we're not looking for

- Speculative abstractions or helpers for hypothetical future use cases
- Backwards-compatibility shims for removed behavior
- Documentation of obvious code — comments should explain *why*, not *what*
- Feature flags, environment-variable-driven behavior changes, or optional
  imports for core logic

## Security issues

Please do **not** open a public GitHub issue for security vulnerabilities.
Report them privately via GitHub's security advisory feature on this repository.

## Code of conduct

Be direct and constructive. Critique ideas, not people.
