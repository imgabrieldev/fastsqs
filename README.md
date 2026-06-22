# FastSQS

**A FastAPI-style router for AWS SQS on Lambda**: pydantic routing, dependency
injection, a middleware system, and native partial batch failure.

[![PyPI version](https://img.shields.io/pypi/v/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![Status](https://img.shields.io/pypi/status/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![Python](https://img.shields.io/pypi/pyversions/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![License: MIT](https://img.shields.io/pypi/l/fastsqs.svg)](https://github.com/fastsqs/fastsqs/blob/main/LICENSE)

[Documentation](https://fastsqs.github.io) · [Changelog](https://github.com/fastsqs/fastsqs/blob/main/CHANGELOG.md) · [Source](https://github.com/fastsqs/fastsqs) · [Issues](https://github.com/fastsqs/fastsqs/issues)

---

FastSQS turns an SQS-triggered Lambda into a typed, declarative app. You write
handlers for pydantic event models; FastSQS parses each record, routes it,
validates it, runs your middleware, and returns the `batchItemFailures` SQS
expects, so failed messages are redelivered and dead-lettered by the queue's
own redrive policy, not by bespoke in-app code.

## Features

- **FastAPI-style routing**: `@app.route(OrderCreated)` dispatches by a payload discriminator (default key `"type"`).
- **Pydantic validation**: handlers receive a validated `SQSEvent` model; bad messages become clean batch failures.
- **Dependency injection**: declare `Depends(...)` params (powered by `fast-depends`); no `@inject` needed.
- **Typed `Context`**: `ctx.message_id`, `ctx.queue_type`, and more as typed attributes; arbitrary scratch in `ctx.state`.
- **Middleware**: `before`/`after` hooks with balanced unwind (resources acquired in `before` are always released).
- **Partial batch failure**: native `ReportBatchItemFailures` for standard and FIFO queues.
- **FIFO-aware**: queue type is inferred from the event-source ARN; per-group ordering with a configurable failure mode.
- **EventBridge Pipes ready**: `app.handler` accepts both the Lambda `{"Records": [...]}` envelope and a bare list of records.
- **Shape detection**: `is_sqs_event(event)` lets one Lambda multiplex SQS and non-SQS (e.g. API Gateway) events.
- **In-process test client**: drive your app with synthetic events, no AWS required.
- **Typed**: ships `py.typed`; full editor and mypy support.

## Install

```bash
pip install fastsqs
```

Requires Python 3.10+. Depends on `pydantic>=2` and `fast-depends>=3,<4`.

## Quick start

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()  # queue type auto-detected from the event-source ARN


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    print("processing", msg.order_id, msg.amount)
    # raising marks this record as failed -> SQS redelivers it


# Lambda entry point (set as the function handler):
def handler(event, context):
    return app.handler(event, context)
```

A message routes by its discriminator value (`"type"` by default), matched to
the event model's name in snake_case, so `{"type": "order_created", ...}` routes
to `OrderCreated`. Field names accept both snake_case and their camelCase
aliases via Pydantic alias generation.

## Why fastsqs

Reach for fastsqs when a queue carries **many message types** and you would
otherwise branch by hand in one large handler. It gives you correct
`ReportBatchItemFailures` together with the FastAPI model on top: route by type,
validate with pydantic, inject dependencies, and read a typed `Context`. For a
single trivial handler with no validation, a plain `boto3` loop is still fine.

## Documentation

Full documentation, including guides, concepts, and the API reference:
**[fastsqs.github.io](https://fastsqs.github.io)**

- [Quickstart](https://fastsqs.github.io/get-started/quickstart/)
- [Guide](https://fastsqs.github.io/guide/routing-by-type/) — routing, dependency injection, middleware, FIFO, testing
- [Concepts](https://fastsqs.github.io/concepts/routing/) — how routing, the batch lifecycle, and partial batch failure work
- [API Reference](https://fastsqs.github.io/reference/)

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for
dev setup, and the [Code of Conduct](CODE_OF_CONDUCT.md). Open an issue to
discuss anything non-trivial first.

## License

MIT. See [LICENSE](https://github.com/fastsqs/fastsqs/blob/main/LICENSE).
