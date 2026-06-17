# FastSQS

**A FastAPI-style router for AWS SQS on Lambda** — pydantic routing, dependency
injection, a middleware system, and native partial batch failure.

[![PyPI version](https://img.shields.io/pypi/v/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)

---

FastSQS turns an SQS-triggered Lambda into a typed, declarative app. You write
handlers for pydantic event models; FastSQS parses each record, routes it,
validates it, runs your middleware, and returns the `batchItemFailures` SQS
expects — so failed messages are redelivered and dead-lettered by the queue's
own redrive policy, not by bespoke in-app code.

## Features

- 🚀 **FastAPI-style routing** — `@app.route(OrderCreated)` dispatches by a payload discriminator (default key `"type"`).
- 🔒 **Pydantic validation** — handlers receive a validated `SQSEvent` model; bad messages become clean batch failures.
- 💉 **Dependency injection** — declare `Depends(...)` params (powered by `fast-depends`); no `@inject` needed.
- 🧩 **Typed `Context`** — `ctx.message_id`, `ctx.queue_type`, … as typed attributes; arbitrary scratch in `ctx.state`.
- 🪝 **Middleware** — `before`/`after` hooks with balanced unwind (resources acquired in `before` are always released).
- 🦾 **Partial batch failure** — native `ReportBatchItemFailures` for standard and FIFO queues.
- 🔀 **FIFO-aware** — queue type is inferred from the event-source ARN; per-group ordering with configurable failure mode.
- 🧪 **In-process test client** — drive your app with synthetic events, no AWS required.
- 🐍 **Typed** — ships `py.typed`; full editor/mypy support.

## Install

```bash
pip install fastsqs
```

Requires Python 3.10+. Depends on `pydantic>=2` and `fast-depends>=3`.

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

A message is routed by its discriminator value (`"type"` by default), matched to
the event model's name in snake_case — `{"type": "order_created", "order_id": "...", "amount": 1}`
routes to `OrderCreated`.

## Typed context

Annotate a handler (or middleware) param `ctx: Context` for typed access to the
framework-owned fields. Put your own scratch data in `ctx.state`:

```python
from fastsqs import FastSQS, SQSEvent, Context

app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, ctx: Context):
    ctx.message_id        # str
    ctx.queue_type        # QueueType enum (.value for the string)
    ctx.fifo_info         # FifoInfo | None (.message_group_id, ...)
    ctx.state.tenant = "acme"   # arbitrary scratch — never collides with a framework field
```

## Dependency injection

Declare `Depends(...)` params and FastSQS wires them per invocation (no decorator):

```python
from fastsqs import FastSQS, SQSEvent, Depends

def get_db():
    return Database(...)

app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, db=Depends(get_db)):
    await db.save(msg.order_id)
```

Sub-dependencies (a `Depends` that itself takes `Depends`) resolve automatically.

## Middleware

Subclass `Middleware` and override `before` / `after`. `after` always runs for
every middleware whose `before` completed (balanced unwind), and receives the
`error` (or `None`):

```python
from fastsqs import FastSQS, Middleware, TimingMiddleware, LoggingMiddleware

class Audit(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.t0 = ...
    async def after(self, payload, record, context, ctx, error):
        if error is not None:
            ...  # observe the failure

app = FastSQS()
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())
app.add_middleware(Audit())
```

Observability, idempotency and PII masking are application concerns — compose
them as your own middleware (or use `aws-lambda-powertools` alongside FastSQS).

## FIFO & partial batch failure

- **Queue type** is `QueueType.AUTO` by default: FastSQS infers FIFO from a
  `.fifo` event-source ARN. Force it with `FastSQS(queue_type=QueueType.FIFO)`.
- **`fifo_failure_mode`** (FIFO only): `"isolate_groups"` (default) blocks only
  the failed `messageGroupId`'s tail; `"halt_batch"` halts the whole batch at the
  first failure (AWS Powertools' default).
- **`partial_batch_failure`** (default `True`) reports per-record failures. Set
  it `False` to fail the entire batch (raising `BatchFailedError`) so SQS
  redelivers every message.

FastSQS only *reports* failures — redelivery and dead-lettering are the queue's
job (visibility timeout + `maxReceiveCount` + redrive policy).

`max_concurrent_messages` (default 10) bounds concurrency on standard queues;
FIFO records are processed in order per group.

## Testing

```python
from fastsqs.testing import SQSTestClient, RecordSpec

client = SQSTestClient(app)

# one message
result = client.send({"type": "order_created", "order_id": "1", "amount": 5})
assert result == {"batchItemFailures": []}

# a FIFO batch with two message groups (a .fifo ARN is set so AUTO infers FIFO)
client.send_batch([
    RecordSpec({"type": "order_created", "order_id": "1", "amount": 1}, group_id="g1"),
    RecordSpec({"type": "order_created", "order_id": "2", "amount": 2}, group_id="g2"),
])

# a raw (malformed) body to exercise the InvalidMessageError path
client.send("{not json", message_id="bad")
```

## Exceptions

All errors derive from `FastSQSError`: `RouteNotFoundError`,
`InvalidMessageError`, and `BatchFailedError` (whose `.failures` holds the failed
item ids).

## License

MIT — see [LICENSE](LICENSE).
