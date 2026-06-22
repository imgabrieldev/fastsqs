# Partial batch failure and redelivery

This page explains how FastSQS turns per-record handler failures into a partial batch response, and how the queue uses that response to redeliver or dead-letter only the records that failed.

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS()  # partial_batch_failure=True by default


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated):
    charge(msg.order_id)  # if this raises, only this record is reported as failed


def handler(event, context):
    return app.handler(event, context)
```

When one record in a batch raises and the others succeed, the call to `app.handler` returns:

```python
{"batchItemFailures": [{"itemIdentifier": "msg-002"}]}
```

That payload is the AWS `ReportBatchItemFailures` shape. It names the records that failed. SQS reads it and redelivers only those records.

## What FastSQS does and what the queue does

FastSQS only reports failures. Redelivery and dead-lettering are the queue's job: visibility timeout, `maxReceiveCount`, and the redrive policy. The split matters.

FastSQS runs each record's handler. A handler that raises marks its record failed; a handler that returns marks its record succeeded. At the end of the batch FastSQS collects the failed `messageId` values and returns them under `batchItemFailures`. It does not delete, retry, or move any message itself.

SQS owns the rest. A record named in `batchItemFailures` becomes visible again after its visibility timeout expires. SQS then redelivers it in a later batch. Each redelivery increments the record's receive count. When that count exceeds `maxReceiveCount`, the redrive policy moves the record to the dead-letter queue. Records not named in the response are treated as processed and deleted.

!!! warning
    The event source mapping must enable `ReportBatchItemFailures` (`FunctionResponseTypes: ["ReportBatchItemFailures"]`). Without it, SQS ignores the partial response and redelivers the whole batch, including the records that already succeeded.

## Why the report shape matters

A Lambda handler that returns normally tells SQS the entire batch succeeded, so SQS deletes every record. If one record failed inside that batch, its work is lost.

The historical workaround was to raise from the Lambda handler so SQS redelivers the batch. That redelivers the records that already succeeded too, so handlers had to be idempotent to survive duplicate processing.

`ReportBatchItemFailures` removes the trade-off. The handler returns, names only the failed records, and SQS redelivers only those. The default `partial_batch_failure=True` produces this report for both standard and FIFO queues.

## Failing the whole batch on purpose

Set `partial_batch_failure=False` to opt out of per-record reporting.

```python
from fastsqs import FastSQS, BatchFailedError

app = FastSQS(partial_batch_failure=False)
```

In this mode any failed record raises `BatchFailedError` from `app.handler`. The Lambda invocation fails, so SQS redelivers every record in the batch, including those whose handlers returned. The failed item identifiers are on `BatchFailedError.failures`.

Earlier behavior in this mode silently reported no failures, which dropped the failed records. The exception replaces that data-loss path with an explicit whole-batch redelivery.

!!! warning
    With `partial_batch_failure=False`, records that already succeeded are reprocessed on redelivery. Make those handlers idempotent, or you will double-process them.

Choose the mode by the cost of a duplicate. Prefer the default per-record reporting when reprocessing a successful record is expensive or non-idempotent. Reach for whole-batch failure when the batch is a unit that must advance or roll back together.

## FIFO queues

FIFO queues add ordering on top of the same report. A failed record blocks later records in its message group, because delivering past it would break order. The `fifo_failure_mode` setting decides how much it blocks: `"isolate_groups"` (default) holds only the failed group's tail, while `"halt_batch"` stops the whole batch at the first failure.

See [FIFO ordering and failure modes](fifo-ordering.md) for how the report interacts with ordering, and the [partial batch failure guide](../guide/partial-batch-failure.md) for FIFO configuration.

## Related pages

- [Partial batch failure guide](../guide/partial-batch-failure.md) for configuration and worked examples.
- [Lifecycle](lifecycle.md) for where failure reporting sits in per-record processing.
- [Queue type detection](queue-type-detection.md) for how FastSQS infers standard vs FIFO.
- [AWS: reporting batch item failures](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html) for the event source mapping and redrive policy.
