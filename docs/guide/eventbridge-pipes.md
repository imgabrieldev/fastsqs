# Consume EventBridge Pipes events

Run one FastSQS app behind both a Lambda SQS event source mapping and an EventBridge Pipes SQS-source target, without changing your routes.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    print("processing", msg.order_id, msg.amount)


# Lambda entry point (set as the function handler):
def handler(event, context):
    return app.handler(event, context)
```

`app.handler` accepts two event shapes for an SQS source. The event source mapping delivers the envelope `{"Records": [...]}`. A Pipes SQS-source target delivers a bare `list` of records. The handler routes both unchanged, so the function above works behind either trigger with no branching.

## What a Pipes target receives

EventBridge Pipes passes the records to its target as a bare list, not wrapped in a `Records` key. The per-record fields match the SQS event source mapping format: each element carries `messageId`, `body`, `eventSourceARN`, and the rest. Routing, the discriminator, and queue-type detection from `eventSourceARN` behave the same as under an event source mapping.

!!! note
    Return the result of `app.handler` from your function. When `partial_batch_failure` is enabled (the default), the handler returns a `batchItemFailures` response so SQS and Pipes redeliver only the failed records. See [Report partial batch failures](partial-batch-failure.md).

## Report batch item failures from a Pipe

A Pipes target reports partial failure with the same `ReportBatchItemFailures` response shape as an event source mapping. Keep `partial_batch_failure` on so the handler emits it.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS(partial_batch_failure=True)  # default


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    if msg.amount <= 0:
        raise ValueError("amount must be positive")  # this record is reported failed


def handler(event, context):
    return app.handler(event, context)  # returns {"batchItemFailures": [...]}
```

!!! warning
    The Pipe (or the event source mapping) must enable `ReportBatchItemFailures` for the returned `batchItemFailures` list to take effect. Without it, the source treats any failure as a whole-batch failure and redelivers every record.

## Malformed enrichment elements

A Pipe enrichment step can inject a non-dict element into the bare list, such as a JSON string, number, or `null`. The handler reports that element as its own batch-item failure and processes its siblings normally. One malformed item does not fail the batch.

When a record carries a present-but-empty or `None` `messageId`, the handler coalesces the identifier to the `"UNKNOWN"` sentinel in the `batchItemFailures` entry. The handler never emits an empty-string or `null` `itemIdentifier`, because SQS and EventBridge read an empty or null identifier as a whole-batch failure.

## Multiplex SQS and non-SQS events

To serve both SQS traffic and non-SQS traffic (for example an API Gateway proxy event) from one Lambda, dispatch by shape with `is_sqs_event`. It returns `True` for both a bare list and a `{"Records": [...]}` envelope.

```python
from fastsqs import is_sqs_event


def handler(event, context):
    if is_sqs_event(event):               # a bare list OR {"Records": [...]}
        return app.handler(event, context)
    return http_handler(event, context)   # e.g. an API Gateway proxy event
```

For more on shape-based dispatch, see [Multiplex SQS and non-SQS events](multiplexing.md).

## Related

- [Report partial batch failures](partial-batch-failure.md)
- [Multiplex SQS and non-SQS events](multiplexing.md)
- [is_sqs_event reference](../reference/utils.md)
- [EventBridge Pipes documentation](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-pipes.html)
