# Examples

This page points you at the runnable end-to-end samples in the repository and shows the smallest version of each pattern inline. Each sample ships a handler, dependencies, and a `README.md`; some add a Dockerfile and tests.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    print("processing", msg.order_id)


def handler(event, context):
    return app.handler(event, context)
```

All samples live under [`examples/`](https://github.com/fastsqs/fastsqs/tree/main/examples). Clone the repo, enter a sample directory, install its `requirements.txt`, and run the handler directly to drive it with synthetic events.

## The samples

| Sample | What it shows |
|---|---|
| [simple_standard_example](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_standard_example) | A standard-queue app with two pydantic routes. |
| [simple_fifo_example](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_fifo_example) | A FIFO app reading `ctx.fifo_info` for per-group ordering. |
| [nested_example](https://github.com/fastsqs/fastsqs/tree/main/examples/nested_example) | Key-value routing across routers and subrouters, with a default handler and a Dockerfile. |
| [custom_middleware_example](https://github.com/fastsqs/fastsqs/tree/main/examples/custom_middleware_example) | Custom `Middleware` subclasses for logging, error handling, and metrics. |
| [comprehensive_example](https://github.com/fastsqs/fastsqs/tree/main/examples/comprehensive_example) | Routing, built-in middleware, and bounded concurrency together. |
| [ordering_with_standard_queues](https://github.com/fastsqs/fastsqs/tree/main/examples/ordering_with_standard_queues) | Per-entity ordering on a standard queue using application locks. |

## Standard queue

Route by payload type on a standard queue. The discriminator defaults to `"type"`, so `{"type": "order_created", ...}` routes to `OrderCreated`.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()


class OrderCreated(SQSEvent):
    order_id: str


class OrderUpdated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated):
    print("order created:", msg.order_id)


@app.route(OrderUpdated)
async def handle_order_updated(msg: OrderUpdated):
    print("order updated:", msg.order_id)


def handler(event, context):
    return app.handler(event, context)
```

See [Routing by type](guide/routing-by-type.md) for the discriminator and snake_case matching rules. Source: [simple_standard_example](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_standard_example).

## FIFO queue

Force `QueueType.FIFO` and read the per-message FIFO attributes from the typed `Context`. `ctx.fifo_info` is `None` on standard queues, so guard the access.

```python
from fastsqs import FastSQS, SQSEvent, Context, QueueType

app = FastSQS(queue_type=QueueType.FIFO)


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated, ctx: Context):
    fifo = ctx.fifo_info
    group = fifo.message_group_id if fifo else None
    print("order", msg.order_id, "in group", group, "msg", ctx.message_id)


def handler(event, context):
    return app.handler(event, context)
```

!!! warning
    SQS exposes FIFO system attributes (`MessageGroupId`, `MessageDeduplicationId`) in PascalCase under `record["attributes"]`, unlike the camelCase record-level keys. Keep raw test events faithful, or FIFO grouping collapses into one group. `SQSTestClient` already emits PascalCase.

See [FIFO failure modes](guide/fifo-failure-modes.md) and [FIFO ordering](concepts/fifo-ordering.md). Source: [simple_fifo_example](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_fifo_example).

## Routers and subrouters

Split routes across routers, set a per-router discriminator, and nest with `subrouter(...)`. Key-value routes take an optional `model=` for validation. Register a `default()` for unmatched messages.

```python
from fastsqs import FastSQS, SQSRouter, SQSEvent


class CreateUser(SQSEvent):
    name: str


class WriteToRds(SQSEvent):
    table: str


router = SQSRouter(discriminator="action")
create_router = SQSRouter(discriminator="entity")
db_router = SQSRouter(discriminator="db")

router.subrouter("create", create_router)
router.subrouter("write", db_router)


@create_router.route("user", model=CreateUser)
async def handle_create_user(msg: CreateUser):
    print("create user", msg.name)


@db_router.route("rds", model=WriteToRds)
async def handle_write_to_rds(msg: WriteToRds):
    print("write rds", msg.table)


@router.default()
async def handle_unknown(payload: dict):
    print("unknown action", payload)


app = FastSQS()
app.include_router(router)


def handler(event, context):
    return app.handler(event, context)
```

So `{"action": "create", "entity": "user", "name": "Ada"}` reaches `handle_create_user`. See [Routing by key](guide/routing-by-key.md) and [Routers and defaults](guide/routers-and-defaults.md). Source: [nested_example](https://github.com/fastsqs/fastsqs/tree/main/examples/nested_example).

## Custom middleware

Subclass `Middleware` and override `before` / `after`. `after` runs for every middleware whose `before` completed and receives the `error` (or `None`). Use `ctx.state` for scratch data that flows from `before` to `after`.

```python
import time
from fastsqs import FastSQS, Middleware


class Metrics(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.t0 = time.time()

    async def after(self, payload, record, context, ctx, error):
        elapsed = time.time() - ctx.state.t0
        if error is not None:
            print("failed after", elapsed, "error:", error)
        else:
            print("ok in", elapsed)


app = FastSQS()
app.add_middleware(Metrics())
```

See [Middleware](guide/middleware.md) for the before/after contract and balanced unwind. Source: [custom_middleware_example](https://github.com/fastsqs/fastsqs/tree/main/examples/custom_middleware_example).

## Routing, middleware, and concurrency together

Combine pydantic routes, built-in middleware, and bounded concurrency. `max_concurrent_messages` (default 10) caps how many standard-queue records run at once. Raising in a handler marks that record as a partial batch failure.

```python
import asyncio
from fastsqs import FastSQS, SQSEvent, LoggingMiddleware, TimingMiddleware


class OrderProcessing(SQSEvent):
    order_id: str


app = FastSQS(max_concurrent_messages=5)
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())


@app.route(OrderProcessing)
async def process_order(msg: OrderProcessing):
    await asyncio.sleep(0.5)
    if msg.order_id.endswith("error"):
        raise ValueError(f"failed order {msg.order_id}")
    return {"order_id": msg.order_id, "status": "processed"}


def handler(event, context):
    return app.handler(event, context)
```

See [Built-in middleware](guide/builtin-middleware.md) and [Partial batch failure](guide/partial-batch-failure.md). Source: [comprehensive_example](https://github.com/fastsqs/fastsqs/tree/main/examples/comprehensive_example).

## Per-entity ordering on a standard queue

A standard queue gives no ordering guarantee. To serialize work per entity while still processing different entities in parallel, take an application lock keyed by the entity id.

```python
import asyncio
from fastsqs import FastSQS, SQSEvent

app = FastSQS(max_concurrent_messages=10)

_locks: dict[str, asyncio.Lock] = {}


def lock_for(key: str) -> asyncio.Lock:
    return _locks.setdefault(key, asyncio.Lock())


class OrderEvent(SQSEvent):
    order_id: str
    event_type: str


@app.route(OrderEvent)
async def handle_order_event(msg: OrderEvent):
    async with lock_for(f"order_{msg.order_id}"):
        print("order", msg.order_id, "event", msg.event_type)
        await asyncio.sleep(0.1)
        return {"order_id": msg.order_id, "status": "processed"}


def handler(event, context):
    return app.handler(event, context)
```

!!! note
    A process-local lock serializes records within one Lambda invocation. It does not order records across concurrent invocations. For cross-invocation ordering, use a FIFO queue with a `MessageGroupId`.

See [FIFO ordering](concepts/fifo-ordering.md) for the queue-level alternative. Source: [ordering_with_standard_queues](https://github.com/fastsqs/fastsqs/tree/main/examples/ordering_with_standard_queues).

## Drive an example without AWS

Each sample's handler runs locally. To assert outcomes, use the in-process test client: it builds synthetic events and returns the same `batchItemFailures` shape your Lambda returns.

```python
from fastsqs.testing import SQSTestClient

client = SQSTestClient(app)

result = client.send({"type": "order_created", "order_id": "1"})
assert result == {"batchItemFailures": []}
```

See [Testing](guide/testing.md) for `SQSTestClient`, `RecordSpec`, `make_record`, and `make_event`.
