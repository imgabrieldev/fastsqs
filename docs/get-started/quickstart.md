# Quickstart

Build a typed SQS app end to end: one event model, one route, and the Lambda entry point.

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

Install the package first if you have not already (see [Installation](installation.md)):

```bash
pip install fastsqs
```

## What each piece does

`FastSQS()` creates the app. With the default `QueueType.AUTO`, it infers the
queue type from each record's event-source ARN, so the same code serves standard
and FIFO queues.

`SQSEvent` is the base for your event models. Subclass it and declare the fields
you expect on the message body. FastSQS validates each record against the matched
model with pydantic before calling your handler.

`@app.route(OrderCreated)` registers `handle_order` for messages that route to
`OrderCreated`. The handler receives the validated model.

`def handler(event, context)` is the function AWS invokes. It delegates to
`app.handler`, which parses the batch, routes and validates each record, runs your
middleware, and returns the response SQS expects.

## How a message routes to your model

A message routes by its discriminator value (key `"type"` by default), matched to
the event model's name in snake_case. The class `OrderCreated` matches the value
`"order_created"`. So this body reaches `handle_order`:

```json
{ "type": "order_created", "order_id": "abc", "amount": 1 }
```

Field names accept both snake_case and their camelCase aliases. The body above
and `{ "type": "order_created", "orderId": "abc", "amount": 1 }` both validate.

!!! note
    Routing matches the model name converted to snake_case, not the raw class
    name. Name your models in PascalCase and send the snake_case value.

## Run it without AWS

Drive the app in-process with the test client. No AWS, no Lambda, no live queue:

```python
from fastsqs.testing import SQSTestClient

client = SQSTestClient(app)

result = client.send({"type": "order_created", "order_id": "abc", "amount": 1})
assert result == {"batchItemFailures": []}
```

An empty `batchItemFailures` list means every record succeeded. A record that
raises, or a body that fails validation, appears in that list as a reported
failure rather than crashing the batch. See [Testing](../guide/testing.md) for
batches, FIFO groups, and malformed inputs.

## Deploy the entry point

Set the Lambda function handler to the module path of your `handler` function
(for example `app.handler` if the file is `app.py`). Attach the function to your
queue with an event source mapping.

!!! warning
    The event source mapping must enable
    `FunctionResponseTypes: ["ReportBatchItemFailures"]`. Without it, SQS ignores
    the partial response and redelivers the whole batch on any failure. See the
    [AWS docs on partial batch responses](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html#services-sqs-batchfailurereporting).

FastSQS only reports failures. Redelivery and dead-lettering remain the queue's
job, driven by the visibility timeout, `maxReceiveCount`, and redrive policy. See
[Partial batch failure](../concepts/partial-batch-failure.md) for the full model.

## Next steps

- [Route by payload type](../guide/routing-by-type.md) when one queue carries many message types.
- [Route by key value](../guide/routing-by-key.md) for string-keyed dispatch with optional validation.
- [Inject dependencies](../guide/dependency-injection.md) with `Depends(...)`.
- [Add middleware](../guide/middleware.md) for cross-cutting `before`/`after` hooks.
- Browse runnable [examples](../examples.md) covering standard, FIFO, routers, and middleware.
