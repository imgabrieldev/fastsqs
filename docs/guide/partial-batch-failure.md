# Enable partial batch failure

This page shows you how to report only the records that failed in a batch, so SQS redelivers those records and leaves the ones that succeeded alone.

Partial batch failure is on by default. A handler that raises on one record produces a `batchItemFailures` response listing only that record.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()  # partial_batch_failure=True by default


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated):
    if msg.order_id == "bad":
        raise RuntimeError("downstream rejected the order")
    # process the order


def handler(event, context):
    return app.handler(event, context)
```

Send a two-record batch where one record fails, and `app.handler` returns the failed id alone:

```python
{"batchItemFailures": [{"itemIdentifier": "msg-of-the-bad-record"}]}
```

The successful record is not in the list, so SQS does not redeliver it.

## Enable ReportBatchItemFailures on the event source mapping

FastSQS only builds the response. The Lambda event source mapping has to opt into reading it.

!!! warning
    The event source mapping must set `FunctionResponseTypes: ["ReportBatchItemFailures"]`. Without it, SQS ignores the partial response and redelivers the entire batch, including records that already succeeded.

Set it when you create or update the mapping. With the AWS CLI:

```bash
aws lambda create-event-source-mapping \
  --function-name my-fn \
  --event-source-arn arn:aws:sqs:us-east-1:123456789012:orders \
  --function-response-types ReportBatchItemFailures
```

FastSQS reports failures; redelivery and dead-lettering remain the queue's job. Tune redelivery with the queue's visibility timeout, `maxReceiveCount`, and a redrive policy to a dead-letter queue.

## Fail the whole batch instead

Set `partial_batch_failure=False` to fail the entire batch when any record fails. FastSQS then raises `BatchFailedError` instead of returning a `batchItemFailures` response, and SQS redelivers every message in the batch.

```python
from fastsqs import FastSQS, BatchFailedError, SQSEvent

app = FastSQS(partial_batch_failure=False)


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated):
    ...


def handler(event, context):
    try:
        return app.handler(event, context)
    except BatchFailedError as exc:
        # exc.failures holds the failed item ids
        raise
```

The raised `BatchFailedError` carries the failed item ids on `exc.failures`.

!!! note
    With `partial_batch_failure=False`, records that already succeeded are redelivered along with the failures. Make handlers idempotent, or keep the default so only failed records come back.

## FIFO queues

Partial batch failure applies to FIFO queues as well. The queue type is inferred from the event-source ARN, and a failed record blocks the rest of its message group. Choose how a failure propagates with [fifo_failure_mode](fifo-failure-modes.md).

## Test the failure response

Use `SQSTestClient` to assert the exact `batchItemFailures` shape without a real queue. A malformed body becomes a reported failure, not an exception.

```python
from fastsqs.testing import SQSTestClient

client = SQSTestClient(app)

result = client.send("{not json", message_id="bad")
assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
```

See [Test handlers](testing.md) for batches and FIFO message groups.

## See also

- [Partial batch failure (concept)](../concepts/partial-batch-failure.md)
- [FIFO failure modes](fifo-failure-modes.md)
- [FastSQS reference](../reference/app.md)
- [Exceptions reference](../reference/exceptions.md)
- [AWS: reporting batch item failures](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html)
