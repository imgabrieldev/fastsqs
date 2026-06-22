# Why fastsqs

This page explains what fastsqs gives you over a hand-written SQS Lambda, and when a plain `boto3` loop is the better choice.

```python
from fastsqs import FastSQS, SQSEvent, Context, Depends

app = FastSQS()  # queue type inferred from the event-source ARN


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


class OrderCancelled(SQSEvent):
    order_id: str


def get_db():
    return Database(...)


@app.route(OrderCreated)
async def on_created(msg: OrderCreated, ctx: Context, db=Depends(get_db)):
    await db.save(msg.order_id)


@app.route(OrderCancelled)
async def on_cancelled(msg: OrderCancelled):
    ...


def handler(event, context):
    return app.handler(event, context)
```

One queue carries two message types. Each routes to its own handler, arrives validated, and gets a database connection injected. A raised exception marks only that record as failed. The handler returns the `batchItemFailures` shape SQS expects.

## What an SQS Lambda actually has to do

An SQS-triggered Lambda receives a batch of records under `{"Records": [...]}`. To process it correctly you parse each `body`, decide which logic handles it, validate the payload, run your work, and report which records failed so the rest commit.

That last step is the one most hand-written handlers get wrong. SQS redelivers and dead-letters messages based on the `ReportBatchItemFailures` response, not on in-app retry code. If your handler returns nothing and one record in a batch of ten raises, the whole batch is redelivered, including the nine that already succeeded. Getting the response shape right is the difference between at-least-once delivery that works and a queue that reprocesses good messages.

## What fastsqs adds

fastsqs turns the batch loop into a typed, declarative app. You write handlers for pydantic event models. fastsqs parses each record, routes it to the matching handler, validates it, runs your middleware, and returns the correct `batchItemFailures` response.

| You have… | by hand | fastsqs |
|---|---|---|
| Many message types on one queue, routed by payload | branch in one handler | declarative `@app.route(Model)` |
| Pydantic validation per type | bring your own | built in |
| Dependency injection / typed `Context` | wire it by hand | built in |
| FIFO per-group isolation by default | hand-rolled | `isolate_groups` |
| Partial batch failure | hand-rolled response shape | native |

The value compounds when the queue carries several message types. Without a router you branch on the payload inside one growing handler. With fastsqs each type is a model and a function. See [Routing by type](../guide/routing-by-type.md) and [Routers and defaults](../guide/routers-and-defaults.md).

Validation comes from the model. A handler annotated with `OrderCreated` receives a validated instance, so a malformed body becomes a clean batch failure instead of a `KeyError` deep inside your logic. See [Partial batch failure](../guide/partial-batch-failure.md).

Cross-cutting concerns stay out of the handler. `Depends(...)` wires per-invocation dependencies (powered by `fast-depends`), and a typed `Context` exposes `ctx.message_id`, `ctx.queue_type`, and `ctx.fifo_info` as attributes, with `ctx.state` for your own scratch. See [Dependency injection](../guide/dependency-injection.md) and [Context and State](../concepts/context-and-state.md).

!!! warning
    Partial batch failure only works when the event source mapping enables `ReportBatchItemFailures`. Without it, SQS ignores the response and redelivers the whole batch on any error.

## FIFO without hand-rolled ordering

A FIFO queue must not skip ahead within a message group: if one message in a group fails, the messages behind it have to wait. fastsqs infers the queue type from the event-source ARN and, by default, isolates failures to their group. A failed group reports the rest of its messages as failures so ordering holds, while other groups commit. See [FIFO ordering](../concepts/fifo-ordering.md) and [FIFO failure modes](../guide/fifo-failure-modes.md).

## When not to use it

A single trivial handler with no validation is fine with a plain `boto3` loop. If one queue carries one message type, the payload is already trusted, and you have no dependencies or middleware, fastsqs earns nothing for the dependency it adds.

```python
def handler(event, context):
    for record in event["Records"]:
        process(record["body"])
    # no partial failure reporting: any raise redelivers the whole batch
```

This is acceptable only when every record is independent and reprocessing a succeeded message is harmless. fastsqs earns its place the moment routing, validation, dependency injection, or correct partial batch failure enter the picture.

## Next steps

- [Installation](installation.md) to add fastsqs to a project.
- [Quickstart](quickstart.md) to build a working handler.
- [Examples](../examples.md) for runnable end-to-end samples.
