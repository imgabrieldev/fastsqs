# FIFO ordering and failure modes

This page explains how FastSQS preserves order on FIFO queues and what the two failure modes do when a record in a batch fails.

```python
from fastsqs import FastSQS, SQSEvent, Context

# Queue type is AUTO by default; a ".fifo" event-source ARN makes this FIFO.
app = FastSQS(fifo_failure_mode="isolate_groups")


class OrderEvent(SQSEvent):
    order_id: str


@app.route(OrderEvent)
async def handle(msg: OrderEvent, ctx: Context):
    # ctx.fifo_info.message_group_id tells you which group this record belongs to.
    print(ctx.fifo_info.message_group_id, msg.order_id)


def handler(event, context):
    return app.handler(event, context)
```

## What FIFO guarantees

A FIFO queue delivers messages that share a `MessageGroupId` in the order they were sent. Messages in different groups carry no ordering relationship to each other. SQS chooses the group as your ordering boundary so that unrelated work still flows in parallel.

FastSQS reads each record's group from the SQS system attributes and reflects it on the typed [Context](context-and-state.md) as `ctx.fifo_info.message_group_id`. The `fifo_info` field is a `FifoInfo` with `message_group_id` and `message_deduplication_id`, parsed from the record. It is `None` on standard queues.

FastSQS infers the queue type from each batch's `eventSourceARN`. A `.fifo` suffix means FIFO. See [Queue type detection](queue-type-detection.md) for how `QueueType.AUTO` decides, and how to force `QueueType.FIFO` or `QueueType.STANDARD`.

!!! warning
    SQS exposes FIFO system attributes (`MessageGroupId`, `MessageDeduplicationId`) in **PascalCase** under `record["attributes"]`, not in the camelCase used for record-level keys. If a hand-built test event uses the wrong case, every record collapses into one group and ordering tests pass for the wrong reason. `SQSTestClient` already emits PascalCase, so prefer it for FIFO fixtures.

## How FastSQS processes a FIFO batch

On a FIFO batch, FastSQS groups the records by `MessageGroupId`. Each group runs as its own sequential task: records inside a group process in arrival order, one after another. Different groups run concurrently, because FIFO imposes no order across groups.

This is the structural difference from a standard queue. On a standard queue FastSQS processes records concurrently up to `max_concurrent_messages` (default 10), with no per-record ordering. On a FIFO queue concurrency is bounded by the number of message groups in the batch, and order within each group is preserved.

When a record fails, FastSQS stops that group and does not skip ahead. Skipping a failed record and processing its successors would deliver later messages before an earlier one succeeds, which breaks the per-group guarantee. So the failed record and every record after it in the same group are reported as failures and left for SQS to redeliver in order.

## Choosing a failure mode

`fifo_failure_mode` controls how far a failure spreads. It applies to FIFO queues only.

`"isolate_groups"` (the default) contains a failure to its own group. The failed record and the rest of its group's tail are reported; other groups keep running and their successes stand. This keeps throughput high when groups are independent, which is the usual case.

```python
app = FastSQS(fifo_failure_mode="isolate_groups")
# Group "acct-1" fails on its second record -> "acct-1" tail is reported.
# Group "acct-2" is untouched and completes.
```

`"halt_batch"` treats the whole batch as one ordered stream. FastSQS processes records in arrival order and stops at the first failure; that record and every record after it across all groups are reported. This matches AWS Powertools' default behaviour. Choose it when records in the same batch have a cross-group dependency that a per-group view would miss.

```python
app = FastSQS(fifo_failure_mode="halt_batch")
# The first failing record halts the batch; it and all later records are reported.
```

The trade-off is blast radius against cross-group ordering. `isolate_groups` minimises the records sent back for redelivery but assumes groups are independent. `halt_batch` is stricter and redelivers more, including records that may sit in unrelated groups.

!!! note
    Both modes report failures through `ReportBatchItemFailures`. The event source mapping must enable `FunctionResponseTypes: ["ReportBatchItemFailures"]`, or SQS ignores the partial response and redelivers the whole batch. See [Partial batch failure](partial-batch-failure.md) for the reporting contract, and [FIFO failure modes](../guide/fifo-failure-modes.md) for a task-oriented walkthrough.

## Ordering without FIFO

FIFO queues cap throughput. If you need per-entity order but not a FIFO queue, run a standard queue and enforce order in your application. Process events for one entity sequentially behind a per-entity lock, and let different entities run in parallel. Treat deduplication the same way: FastSQS ships no idempotency middleware, so use an idempotency-key check (a DynamoDB conditional put, a Redis `SETNX`) in your handler or as your own [middleware](../guide/middleware.md).

The runnable [`ordering_with_standard_queues`](https://github.com/fastsqs/fastsqs/tree/main/examples/ordering_with_standard_queues) example shows per-entity sequencing, timestamp ordering, and priority ordering on a standard queue. The [`simple_fifo_example`](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_fifo_example) shows per-group ordering on a FIFO queue.

For the AWS rules behind groups and deduplication, see the [Amazon SQS FIFO queue documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html).
