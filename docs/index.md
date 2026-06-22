# FastSQS

FastSQS turns an SQS-triggered Lambda into a typed, declarative app. You write
handlers for pydantic event models; FastSQS parses each record, routes it by
type, validates it, runs your middleware, and returns the `batchItemFailures`
shape SQS expects, so failed messages are redelivered and dead-lettered by the
queue's own redrive policy.

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


# Lambda entry point:
def handler(event, context):
    return app.handler(event, context)
```

## Where to go next

- [Why fastsqs](get-started/why.md) — when to reach for it, and when a plain `boto3` loop is enough.
- [Installation](get-started/installation.md) and [Quickstart](get-started/quickstart.md) — install and run a consumer end to end.
- [Guide](guide/routing-by-type.md) — task-focused how-tos: routing, dependency injection, middleware, FIFO, testing.
- [Concepts](concepts/routing.md) — how routing, the batch lifecycle, and partial batch failure work.
- [API Reference](reference/index.md) — the full public surface.
