# Route by payload type

Dispatch each message to a handler chosen from the payload's discriminator value, and receive it as a validated model.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


class OrderUpdated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_created(msg: OrderCreated):
    print("created", msg.order_id, msg.amount)


@app.route(OrderUpdated)
async def handle_updated(msg: OrderUpdated):
    print("updated", msg.order_id)


def handler(event, context):
    return app.handler(event, context)
```

This message routes to `handle_created`:

```json
{"type": "order_created", "order_id": "order-123", "amount": 5}
```

## How a message reaches a handler

`@app.route(Model)` registers a route keyed on the model's message type. FastSQS reads the discriminator value from the body, matches it to a registered model, validates the body against that model, and calls the handler with the parsed instance.

The discriminator key is `"type"` by default. The match value is the model class name in snake_case. `OrderCreated.get_message_type()` returns `"order_created"`, so a body with `"type": "order_created"` routes to the handler registered for `OrderCreated`.

## Define an event model

Subclass `SQSEvent` and declare typed fields. The class name is the message type; FastSQS validates the fields per message.

```python
from fastsqs import SQSEvent


class PaymentCaptured(SQSEvent):
    payment_id: str
    amount_cents: int
```

`SQSEvent` accepts both snake_case field names and their camelCase aliases. The following two bodies both parse into `PaymentCaptured`:

```json
{"type": "payment_captured", "payment_id": "p_1", "amount_cents": 1200}
{"type": "payment_captured", "paymentId": "p_1", "amountCents": 1200}
```

A body whose fields fail validation, is not valid JSON, or is not a JSON object becomes an `InvalidMessageError` for that record. FastSQS reports that record as a batch failure; the rest of the batch continues. See [Partial batch failure](partial-batch-failure.md).

## Receive the typed context

Annotate a second parameter `ctx: Context` to read framework-owned fields such as `ctx.message_id` and `ctx.queue_type`, and to stash your own data in `ctx.state`.

```python
from fastsqs import FastSQS, SQSEvent, Context

app = FastSQS()


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@app.route(OrderCreated)
async def handle_created(msg: OrderCreated, ctx: Context):
    print(msg.order_id, "on", ctx.queue_type.value)
```

`ctx` is optional. Declare it only when the handler needs it. See [Context and state](../concepts/context-and-state.md).

## Change the discriminator key

The discriminator key is `"type"`. Pass `discriminator=` to read the message type from a different key.

```python
app = FastSQS(discriminator="event")
```

With this app, the route value is still the snake_case class name, but FastSQS reads it from `"event"`:

```json
{"event": "order_created", "order_id": "order-123", "amount": 5}
```

## Match name variants

By default a message matches only the exact snake_case class name. Set `flexible_matching=True` to also match the class name and its camelCase and kebab-case forms.

```python
app = FastSQS(flexible_matching=True)
```

For `OrderCreated`, this matches `"order_created"`, `"OrderCreated"`, `"orderCreated"`, and `"order-created"`.

## Fail a record on purpose

Raise from a handler to mark its record as failed. FastSQS reports that record in `batchItemFailures`, and SQS redelivers it under the queue's redrive policy.

```python
@app.route(OrderCreated)
async def handle_created(msg: OrderCreated):
    if msg.amount <= 0:
        raise ValueError("amount must be positive")
    await charge(msg.order_id, msg.amount)
```

!!! warning
    The event source mapping must set `FunctionResponseTypes: ["ReportBatchItemFailures"]`. Without it, SQS ignores the partial response and redelivers the whole batch.

## Handle unmatched messages

A message whose discriminator value matches no route raises `RouteNotFoundError` and becomes a batch failure. Register a catch-all with `@app.default()` to handle unmatched messages instead.

```python
@app.default()
async def fallback(msg, ctx):
    print("unrouted", ctx.message_id)
```

See [Routers and default handlers](routers-and-defaults.md).

## Related

- [Route by a key value](routing-by-key.md) — route on a string value with an optional model.
- [Routing](../concepts/routing.md) — how matching and validation work.
- [Dependency injection](dependency-injection.md) — declare `Depends(...)` params on a handler.
- [simple_standard_example](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_standard_example) — a runnable standard-queue app.
- AWS: [Reporting batch item failures](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html).
