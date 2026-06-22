# The batch lifecycle

This page traces what FastSQS does between the Lambda invocation and the response it returns to SQS.

```python
from fastsqs import FastSQS, SQSEvent, Context, Middleware


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


class Audit(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.t0 = "start"

    async def after(self, payload, record, context, ctx, error):
        print("order finished", ctx.message_id, "error:", error)


app = FastSQS()
app.add_middleware(Audit())


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated, ctx: Context):
    print("processing", msg.order_id, msg.amount)
    # raising here marks this one record as failed -> SQS redelivers it


def handler(event, context):
    return app.handler(event, context)
```

One invocation carries a batch of records. FastSQS walks the batch through five stages: event normalization, per-record routing, dependency resolution, middleware unwind, and failure collection. Each record fails or succeeds on its own; the batch as a whole reports which records to redeliver.

## Event normalization

`app.handler` accepts two event shapes. The Lambda event source mapping delivers `{"Records": [...]}`. An EventBridge Pipes SQS-source target delivers a bare `list` of records. FastSQS reads the records out of either shape:

```python
records = event if isinstance(event, list) else (event.get("Records") or [])
```

An empty or missing batch returns `{"batchItemFailures": []}` immediately. With records present, FastSQS resolves the queue type once for the batch. `QueueType.AUTO` infers FIFO from a `.fifo` event-source ARN; you can force `QueueType.STANDARD` or `QueueType.FIFO` instead. The queue type decides how the batch is scheduled: standard records run concurrently, FIFO records run in per-group order. See [Queue type detection](queue-type-detection.md).

## Per-record routing

FastSQS handles each record in isolation. It parses the JSON body, builds a [`Context`](context-and-state.md) for the record, and dispatches to a handler.

The body must be a JSON object. A non-dict record, a non-JSON body, or a body that decodes to something other than an object raises `InvalidMessageError`. Parsing happens per record, so one malformed message never aborts its siblings.

Routing tries the app's own routes first, then each included router in registration order. The first router that claims the record wins, and FastSQS keeps its handler result on the context. If nothing matches, FastSQS raises `RouteNotFoundError` — unless you registered a [default handler](../guide/routers-and-defaults.md) to catch unmatched messages. Both `RouteNotFoundError` and `InvalidMessageError` are caught later as record failures, not crashes. See [Routing](routing.md).

!!! warning
    SQS exposes FIFO system attributes (`MessageGroupId`, `MessageDeduplicationId`) in PascalCase under `record["attributes"]`, unlike the camelCase record-level keys. FastSQS reads them in PascalCase to populate `ctx.fifo_info`. Keep raw test events faithful, or FIFO grouping collapses into one group.

## Dependency resolution

A matched handler runs with its declared dependencies resolved per invocation. Declare them with `Depends(...)` and FastSQS wires the values before calling the handler — no decorator:

```python
from fastsqs import FastSQS, Depends


def get_db():
    return Database(...)


app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, db=Depends(get_db)):
    await db.save(msg.order_id)
```

Resolution is powered by `fast-depends`. Sub-dependencies (a `Depends` that itself takes `Depends`) resolve automatically. FastSQS also injects the framework parameters a handler asks for by annotation, such as `ctx: Context`. See [Dependency injection](../guide/dependency-injection.md).

## Middleware unwind

Routing and dependency resolution run inside the middleware stack. For each record, FastSQS calls every middleware's `before` hook in registration order, runs the route, then calls `after` in reverse order:

```text
before A -> before B -> [route + handler] -> after B -> after A
```

The unwind is balanced. Only middlewares whose `before` completed are unwound, and `after` runs for each of them even when a later `before`, the route, or the handler raises. Resources acquired in `before` — a concurrency slot, a monitor task — are always released. The `after` hook receives the `error` (or `None`), so it observes the same failure the record will report:

```python
class Audit(Middleware):
    async def after(self, payload, record, context, ctx, error):
        if error is not None:
            ...  # the record failed; observe it here
```

!!! note
    A `before` hook that raises aborts the record: the handler does not run, and the middlewares that already entered still unwind through `after`. After-hooks are isolated — one raising never aborts the others nor masks the original error.

See [Middleware](../guide/middleware.md) for the full hook contract.

## Failure collection

A record that completes its handler without raising succeeds. A record whose body is invalid, whose route is missing, whose middleware aborts, or whose handler raises is collected as a failure. FastSQS records the source `messageId` of each failed record and returns them:

```python
{"batchItemFailures": [{"itemIdentifier": "the-failed-message-id"}]}
```

This is the `ReportBatchItemFailures` shape SQS expects. FastSQS only reports failures; redelivery and dead-lettering are the queue's job, driven by the visibility timeout, `maxReceiveCount`, and redrive policy.

!!! warning
    The event source mapping must enable `FunctionResponseTypes: ["ReportBatchItemFailures"]`, or SQS ignores the partial response and redelivers the whole batch.

How failures are collected depends on the queue type:

- **Standard queues** process records concurrently, bounded by `max_concurrent_messages` (default 10). Every record runs; the failures are whichever records raised. See [Partial batch failure](partial-batch-failure.md).
- **FIFO queues** preserve order. Under the default `"isolate_groups"` mode, a failed record blocks the rest of its `MessageGroupId`: that record and every later record in the group are reported so SQS redelivers the tail in order. Under `"halt_batch"`, the first failure halts the whole batch. See [FIFO ordering](fifo-ordering.md) and [FIFO failure modes](../guide/fifo-failure-modes.md).

By default `partial_batch_failure` is `True` and FastSQS returns the per-record `batchItemFailures`. Set it `False` to fail the entire batch instead: any failure raises `BatchFailedError`, so SQS redelivers every message.

!!! warning
    With `partial_batch_failure=False`, a single failing record fails the whole batch, including records that already succeeded. SQS redelivers all of them, so your handlers must tolerate redelivery.
