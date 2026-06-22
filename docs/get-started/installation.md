# Installation

Install FastSQS from PyPI and confirm the import works.

```bash
pip install fastsqs
```

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()
print(app)
```

## Requirements

FastSQS targets Python 3.10 and later.

| Requirement | Version |
|---|---|
| Python | >=3.10 |
| pydantic | >=2.0.0 |
| fast-depends | >=3.0.0,<4.0.0 |

`pip install fastsqs` pulls in `pydantic` and `fast-depends` automatically.

!!! note
    FastSQS runs inside an SQS-triggered Lambda. The AWS Lambda Python runtimes cover 3.10 through 3.13, the same range FastSQS supports.

## Type checking

FastSQS ships a `py.typed` marker (PEP 561), so mypy and editors read its inline type hints with no extra stubs.

## Verify the install

Confirm the import and the installed version on the command line.

```bash
python -c "import fastsqs; from importlib.metadata import version; print(version('fastsqs'))"
```

If `import fastsqs` raises `ModuleNotFoundError`, the package landed in a different environment than the interpreter you ran. Activate the virtualenv that received the install, then retry.

## Install from source

Clone the repository and install in editable mode with the development dependencies.

```bash
git clone https://github.com/fastsqs/fastsqs
cd fastsqs
pip install -e . -r requirements-dev.txt
```

The editable install adds the test suite and local-invoke tooling. See [Contributing](../about/contributing.md) for the development workflow.

## License

FastSQS is released under the MIT license.

## Next steps

- Build and run your first handler in the [Quickstart](quickstart.md).
- Read [Why FastSQS](why.md) to decide whether it fits your queue.
- Browse runnable samples in [`examples/`](https://github.com/fastsqs/fastsqs/tree/main/examples).
