# Multiplex SQS and API Gateway

This page shows how to run one Lambda function that handles both SQS batches and non-SQS events such as API Gateway proxy requests.

```python
from fastsqs import FastSQS, SQSEvent, is_sqs_event

app = FastSQS()


class OrderPlaced(SQSEvent):
    order_id: str


@app.route(OrderPlaced)
async def handle_order(event: OrderPlaced):
    print("processing", event.order_id)


def http_handler(event, context):
    return {"statusCode": 200, "body": "ok"}


def handler(event, context):
    if is_sqs_event(event):
        return app.handler(event, context)
    return http_handler(event, context)
```

## Why one function handles two shapes

A Lambda function can sit behind more than one trigger. The same `handler` may receive an SQS batch from an event source mapping and an HTTP request from API Gateway. The two payloads differ in shape, so dispatch on shape.

`is_sqs_event(event)` returns `True` for the two shapes FastSQS processes:

- a `dict` carrying a `"Records"` key — the Lambda SQS event source mapping envelope, `{"Records": [...]}`.
- a bare `list` of records — the shape an EventBridge Pipes SQS-source target delivers. See [Process EventBridge Pipes batches](eventbridge-pipes.md).

Anything else returns `False`. An API Gateway proxy event is a `dict` without a `"Records"` key, so it falls through to your own handler.

## Route by shape

Branch at the top of the Lambda entry point. Send SQS shapes to `app.handler`; send everything else to the HTTP path.

```python
from fastsqs import is_sqs_event


def handler(event, context):
    if is_sqs_event(event):
        return app.handler(event, context)
    return http_handler(event, context)
```

`app.handler` returns the partial batch failure response for SQS traffic. The HTTP branch returns whatever API Gateway expects. The two responses never mix, because each branch owns one trigger.

!!! note
    `is_sqs_event` inspects the payload shape only. It does not parse records or read the `eventSourceARN`. FastSQS infers the queue type later, during `app.handler`. See [Detect the queue type](../concepts/queue-type-detection.md).

## Match an HTTP request first

When the non-SQS branch handles several event kinds, test for the SQS shape first, then narrow the HTTP cases by their own keys.

```python
from fastsqs import is_sqs_event


def handler(event, context):
    if is_sqs_event(event):
        return app.handler(event, context)
    if isinstance(event, dict) and "httpMethod" in event:
        return http_handler(event, context)
    raise ValueError("unrecognized event shape")
```

Raising on an unrecognized shape surfaces a misconfigured trigger early rather than passing an unexpected payload to the wrong branch.

## Next steps

- [Process EventBridge Pipes batches](eventbridge-pipes.md) — the bare-list shape `is_sqs_event` also accepts.
- [Report partial batch failures](partial-batch-failure.md) — what `app.handler` returns for the SQS branch.
- [is_sqs_event reference](../reference/utils.md) — the full signature and accepted shapes.
