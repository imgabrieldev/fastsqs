# Contributing

This page shows you how to set up FastSQS for local development, run the test suite, and exercise a real Lambda image before you open a pull request.

```bash
git clone https://github.com/fastsqs/fastsqs
cd fastsqs
pip install -e . -r requirements-dev.txt
make test
```

Issues and pull requests are welcome. Open an issue at [github.com/fastsqs/fastsqs/issues](https://github.com/fastsqs/fastsqs/issues) to discuss anything non-trivial before you write code. An agreed approach saves a round of review.

## Set up the dev environment

Install FastSQS in editable mode alongside the development requirements. Editable mode (`-e`) maps the installed package to your working tree, so edits take effect without reinstalling.

```bash
pip install -e . -r requirements-dev.txt
```

This pulls the runtime dependencies (`pydantic>=2`, `fast-depends>=3,<4`) plus `pytest` for the test suite. FastSQS targets Python 3.10 and later.

## Run the tests

Run the unit suite with the `test` target.

```bash
make test
```

The target runs `pytest`. Add a test for any behavior you change, and confirm the full suite passes before you push.

To run the integration tests, pass the opt-in flag.

```bash
make test-integration
```

## Run the Lambda image locally

The `local/` directory builds the handler into a Lambda container with the [Runtime Interface Emulator](https://docs.aws.amazon.com/lambda/latest/dg/images-test.html), so you can invoke it over HTTP. Build and start the container.

```bash
make start-local
```

POST a sample SQS batch at the running container.

```bash
make invoke-standard
```

`make invoke-standard` sends a standard-queue batch; `make invoke-fifo` sends a FIFO batch; `make invoke-invalid` sends a malformed body. Each target curls a fixture from `tests/events/` at the running emulator and prints the response. Stream the container logs with `make logs`, and tear it down with `make stop-local`.

!!! tip
    For routing and validation work, the in-process [test client](../guide/testing.md) drives `app.handler` directly with synthetic events and needs no Docker. Reserve the local image for end-to-end checks against the real Lambda runtime.

## Open a pull request

1. Open an issue first for anything beyond a small fix, and agree on the approach.
2. Branch from `main`.
3. Add or update tests covering your change, and run `make test`.
4. Keep the change focused on one concern.
5. Open the pull request and reference the issue it resolves.

## Where things live

Use the [examples](../examples.md) as runnable references when you add a feature, and mirror an existing example's layout for any new one. The public API surface you build against is documented in the [API reference](../reference/index.md). For the behavioral guarantees behind a change, see the [concepts](../concepts/lifecycle.md) pages.
