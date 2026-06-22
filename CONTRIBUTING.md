# Contributing to fastsqs

Issues and pull requests are welcome. For anything non-trivial, open an issue
first so the approach can be agreed before you write code.

By participating, you agree to abide by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

Requires Python 3.10 or newer.

```bash
git clone https://github.com/fastsqs/fastsqs
cd fastsqs
pip install -e . -r requirements-dev.txt

make test          # run the unit suite
```

The unit suite runs without AWS or Docker; the integration tests auto-skip when
Docker is unavailable. To exercise the handler in the real AWS Lambda runtime:

```bash
make start-local        # build and run the Lambda image (Docker RIE)
make invoke-standard     # POST a sample SQS batch at the container
make stop-local
```

## Working on the docs

The documentation site is built with MkDocs Material.

```bash
pip install -e ".[docs]"
mkdocs serve             # preview at http://127.0.0.1:8000
mkdocs build --strict    # the build CI runs
```

## Pull requests

- Keep changes focused; one concern per pull request.
- Add or update tests for behavior changes; keep the unit suite green.
- Update `CHANGELOG.md` under an `Unreleased` section for user-visible changes.
- Match the surrounding code style.
