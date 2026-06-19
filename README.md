# FastSQS

**A FastAPI-style router for AWS SQS on Lambda** — pydantic routing, dependency
injection, a middleware system, and native partial batch failure.

[![PyPI version](https://img.shields.io/pypi/v/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![Status](https://img.shields.io/pypi/status/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![Python](https://img.shields.io/pypi/pyversions/fastsqs.svg)](https://pypi.org/project/fastsqs/)
[![License: MIT](https://img.shields.io/pypi/l/fastsqs.svg)](LICENSE)

[Documentation](https://github.com/imgabrieldev/fastsqs#readme) · [Changelog](https://github.com/imgabrieldev/fastsqs/blob/main/CHANGELOG.md) · [Source](https://github.com/imgabrieldev/fastsqs) · [Issues](https://github.com/imgabrieldev/fastsqs/issues)

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
- 🔀 **FIFO-aware** — queue type is inferred from the event-source ARN; per-group ordering with a configurable failure mode.
- 🔌 **EventBridge Pipes ready** — `app.handler` accepts both the Lambda `{"Records": [...]}` envelope and a bare list of records (the Pipes target shape).
- 🧭 **Shape detection** — `is_sqs_event(event)` lets one Lambda multiplex SQS and non-SQS (e.g. API Gateway) events.
- 🧪 **In-process test client** — drive your app with synthetic events, no AWS required.
- 🐍 **Typed** — ships `py.typed`; full editor/mypy support.

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

A message is routed by its discriminator value (`"type"` by default), matched to
the event model's name in snake_case — `{"type": "order_created", "order_id": "...", "amount": 1}`
routes to `OrderCreated`. Field names accept both snake_case and their camelCase
aliases (`order_id` or `orderId`) via Pydantic alias generation.

`app.handler` also accepts a bare `list` of records — the shape an EventBridge
Pipes SQS-source target delivers — so the same function works behind both an
event source mapping and a Pipe (see below).

## EventBridge Pipes & multiplexed handlers

`app.handler` accepts both Lambda event shapes for an SQS source: the event
source mapping envelope `{"Records": [...]}` and a **bare list** of records (the
shape an EventBridge Pipes SQS-source target delivers). The same handler routes
both unchanged.

To run a single Lambda for both SQS and non-SQS (e.g. API Gateway) traffic,
dispatch by shape with `is_sqs_event`:

```python
from fastsqs import is_sqs_event

def handler(event, context):
    if is_sqs_event(event):               # a bare list OR {"Records": [...]}
        return app.handler(event, context)
    return http_handler(event, context)   # e.g. an API Gateway proxy event
```

## Routers, key-value routing & default handler

Split routes across modules with `SQSRouter`, then attach them with
`app.include_router(...)`. A router supports pydantic routing **and** key-value
routing (`@router.route("value")`), an optional `model=` for validation on
key-value routes, and nesting via `subrouter(...)`:

```python
from fastsqs import FastSQS, SQSRouter, SQSEvent

orders = SQSRouter()


@orders.route(OrderCreated)                             # pydantic routing
async def on_created(msg: OrderCreated):
    ...


@orders.route("order_cancelled", model=OrderCancelled)  # key-value + validation
async def on_cancelled(msg: OrderCancelled):
    ...


@orders.route("ping")                                   # key-value, no model -> raw SQSEvent
async def on_ping(msg: SQSEvent):
    ...


app = FastSQS()
app.include_router(orders)                              # tried after the app's own routes
```

Nest with `orders.subrouter("v2", child_router)`. Register a catch-all for
unmatched messages with `@app.default()` (or `@router.default()`) — without one,
an unmatched message raises `RouteNotFoundError` and becomes a batch failure:

```python
@app.default()
async def fallback(msg, ctx):
    ...
```

`flexible_matching=True` (on `FastSQS` or `SQSRouter`, default `False`) also
matches the ClassName plus camelCase / kebab-case variants of the discriminator
value. A single discriminator value may use only one routing style — registering
it as both a pydantic and a key-value route raises `ValueError` at import.

## Typed context

Annotate a handler (or middleware) param `ctx: Context` for typed access to the
framework-owned fields. Put your own scratch data in `ctx.state` (a `State`
namespace):

```python
from fastsqs import FastSQS, SQSEvent, Context

app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, ctx: Context):
    ctx.message_id            # str
    ctx.queue_type            # QueueType enum (.value for the string)
    ctx.fifo_info             # FifoInfo | None (.message_group_id, ...)
    ctx.state.tenant = "acme"  # attribute access — never collides with a framework field
    ctx.state["tenant"]        # item access works too
    ctx.state.get("missing")   # use .get() for optional reads (bare .missing raises AttributeError)
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

`LoggingMiddleware` takes a custom `logger=` plus `include_payload` /
`include_record` / `include_context` / `verbose` toggles; `TimingMiddleware`
writes `duration_ms` into `ctx.state` (key configurable via `store_key_ms`).

Observability, idempotency and PII masking are application concerns — compose
them as your own middleware.

## FIFO & partial batch failure

- **Queue type** — `QueueType.AUTO` (default; infers FIFO from a `.fifo`
  event-source ARN), or force `QueueType.STANDARD` / `QueueType.FIFO`.
- **`fifo_failure_mode`** (FIFO only): `"isolate_groups"` (default) blocks only
  the failed `MessageGroupId`'s tail; `"halt_batch"` halts the whole batch at the
  first failure.
- **`partial_batch_failure`** (default `True`) reports per-record failures. Set
  it `False` to fail the entire batch (raising `BatchFailedError`) so SQS
  redelivers every message.

FastSQS only *reports* failures — redelivery and dead-lettering are the queue's
job (visibility timeout + `maxReceiveCount` + redrive policy). The event source
mapping must enable `FunctionResponseTypes: ["ReportBatchItemFailures"]`, or SQS
ignores the partial response and redelivers the whole batch.

> **FIFO footgun:** SQS exposes system attributes (`MessageGroupId`,
> `MessageDeduplicationId`) in **PascalCase** under `record["attributes"]`,
> unlike the camelCase record-level keys. Keep raw test events faithful, or FIFO
> grouping silently collapses into one group (`SQSTestClient` already emits PascalCase).

`max_concurrent_messages` (default 10) bounds concurrency on standard queues;
FIFO records are processed in order per group. `debug` (default `False`) enables
verbose per-record logging through a registered `LoggingMiddleware`.

## Why fastsqs

FastSQS gives you correct `ReportBatchItemFailures` **and** the FastAPI model on
top of it: the message body *routes to a handler by type*, with pydantic
validation, dependency injection, and a typed `Context`. Reach for it when a
queue carries **many message types** and you'd otherwise branch by hand in one
big handler; for a single trivial handler with no validation, a plain `boto3`
loop is still fine — FastSQS earns its place the moment routing, validation, or
DI enter the picture.

| You have… | by hand | FastSQS |
|---|---|---|
| Many message types on one queue, routed by payload | branch in one handler | declarative `@app.route(Model)` |
| Pydantic validation per type | bring your own | built in |
| Dependency injection / typed `Context` | — | built in |
| FIFO per-group isolation by default | hand-rolled | `isolate_groups` |
| Partial batch failure | hand-rolled response shape | native |

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

# a raw (malformed) body becomes a reported failure, not an exception
result = client.send("{not json", message_id="bad")
assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
```

For hand-built events, `fastsqs.testing` also exports `make_record(...)` and
`make_event(records)`.

## Examples

Runnable end-to-end samples (handler + Dockerfile + tests) live in
[`examples/`](https://github.com/imgabrieldev/fastsqs/tree/main/examples):

- [simple_standard_example](https://github.com/imgabrieldev/fastsqs/tree/main/examples/simple_standard_example) — minimal standard-queue app
- [simple_fifo_example](https://github.com/imgabrieldev/fastsqs/tree/main/examples/simple_fifo_example) — FIFO with per-group ordering
- [nested_example](https://github.com/imgabrieldev/fastsqs/tree/main/examples/nested_example) — routers & subrouters
- [custom_middleware_example](https://github.com/imgabrieldev/fastsqs/tree/main/examples/custom_middleware_example) — writing middleware
- [comprehensive_example](https://github.com/imgabrieldev/fastsqs/tree/main/examples/comprehensive_example) — routing + DI + middleware together

See the [roadmap](https://github.com/imgabrieldev/fastsqs/blob/main/docs/ROADMAP.md) for what's next.

## Exceptions

All errors derive from `FastSQSError`:

- `RouteNotFoundError` — a message matched no route and no default handler is registered.
- `InvalidMessageError` — a non-JSON body, a non-object body, or a pydantic validation failure.
- `BatchFailedError` — raised when `partial_batch_failure=False` and any record fails; `.failures` holds the failed item ids.

## Contributing

Issues and PRs are welcome — open an issue at
[github.com/imgabrieldev/fastsqs/issues](https://github.com/imgabrieldev/fastsqs/issues)
to discuss anything non-trivial first. Dev setup:

```bash
pip install -e . -r requirements-dev.txt
make test              # unit suite
make start-local       # build the Lambda image (Docker RIE) for local invokes
make invoke-standard   # POST a sample SQS batch at the running container
```

## License

MIT — see [LICENSE](LICENSE).
