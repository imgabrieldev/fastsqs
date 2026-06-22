# Choose a FIFO failure mode

Pick how a FIFO queue's batch reacts when one record fails: isolate the failed message group, or halt the whole batch.

```python
from fastsqs import FastSQS, SQSEvent, QueueType

app = FastSQS(
    queue_type=QueueType.FIFO,
    fifo_failure_mode="isolate_groups",  # the default; "halt_batch" is the alternative
)


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle(msg: OrderCreated):
    ...  # raising marks this record failed and blocks the rest of its group


def handler(event, context):
    return app.handler(event, context)
```

`fifo_failure_mode` applies only to FIFO queues. On a standard queue it has no effect, because standard queues carry no message groups and impose no per-group order.

## What FIFO ordering requires

SQS delivers a FIFO queue's messages in order per `MessageGroupId`, and will not advance a group past a message until that message leaves the queue. So when a record in a group fails, every later record in the same group must also be redelivered, or you break the ordering guarantee. Both modes honor this; they differ in how much of the batch a single failure stalls.

FastSQS only reports failures. Redelivery and dead-lettering are the queue's job, driven by the visibility timeout, `maxReceiveCount`, and the redrive policy. The event source mapping must enable `ReportBatchItemFailures`, or SQS ignores the partial response. See [Enable partial batch failure](partial-batch-failure.md).

!!! warning
    SQS exposes FIFO system attributes (`MessageGroupId`, `MessageDeduplicationId`) in PascalCase under `record["attributes"]`, unlike the camelCase record-level keys. Keep raw test events faithful, or grouping collapses into one group and the failure modes look identical. `SQSTestClient` already emits PascalCase. See [Testing](testing.md).

## isolate_groups (default)

Each message group runs independently. A failure blocks only the tail of that one group; other groups keep processing and can fully succeed.

```python
app = FastSQS(queue_type=QueueType.FIFO, fifo_failure_mode="isolate_groups")
```

Consider a batch with two groups:

- `customer-001`: `A1`, `A2`, `A3`
- `customer-002`: `B1`, `B2`

If `A2` fails, FastSQS reports `A2` and `A3` (the tail of `customer-001`) so SQS redelivers them in order. `A1` succeeds. The entire `customer-002` group runs and succeeds, unaffected. The reported `batchItemFailures` is `[A2, A3]`.

Choose this mode when message groups are independent units of work (one group per order, account, or tenant). A poison message in one customer's group does not stall every other customer.

## halt_batch

FastSQS processes the batch in arrival order and stops at the first failure. It reports that record and every record after it in the batch as failures, across all groups.

```python
app = FastSQS(queue_type=QueueType.FIFO, fifo_failure_mode="halt_batch")
```

Using the same batch, if `A2` fails, FastSQS reports `A2` and everything after it in arrival order, regardless of group. `A1` succeeds; `A3`, `B1`, and `B2` are all redelivered. This matches the default behavior of AWS Lambda Powertools' FIFO batch processor.

!!! warning
    `halt_batch` re-reports records that already succeeded in earlier batches only if they were redelivered, but within one batch it re-reports records that never ran. Those records are redelivered and reprocessed, so handlers must be idempotent. Application-level idempotency (an idempotency-key check in your handler) is your responsibility; FastSQS ships no idempotency middleware.

Choose this mode when the whole batch is one ordered stream and a single failure should stop forward progress for everything behind it.

## Comparing the two modes

| | `isolate_groups` (default) | `halt_batch` |
|---|---|---|
| Scope of a failure | the failed group's tail only | the whole batch from the failure onward |
| Other groups | run independently to completion | redelivered if they arrive after the failure |
| Reported failures for a failed `A2` | `A2`, `A3` | `A2`, `A3`, `B1`, `B2` |
| Reprocessing pressure | bounded to one group | spans the batch |
| Fits | independent per-group work | one strictly ordered stream |

Both modes redeliver the failed record's tail in order, so ordering holds either way. The difference is blast radius.

## Reading FIFO attributes in a handler

A FIFO record carries its group and deduplication id. Read them through the typed `Context`.

```python
from fastsqs import FastSQS, SQSEvent, Context, QueueType

app = FastSQS(queue_type=QueueType.FIFO)


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle(msg: OrderCreated, ctx: Context):
    if ctx.queue_type is QueueType.FIFO and ctx.fifo_info is not None:
        group = ctx.fifo_info.message_group_id
        dedup = ctx.fifo_info.message_deduplication_id
        print(f"order {msg.order_id} in group {group} (dedup {dedup})")
```

`ctx.fifo_info` is a `FifoInfo` with `message_group_id` and `message_deduplication_id`, or `None` on a standard queue. See [Context and State](../concepts/context-and-state.md).

## When a standard queue fits better

If your groups are fully independent and you do not need a queue-level total order, a standard queue with `max_concurrent_messages` and your own per-entity locking gives higher throughput at lower cost. The [ordering_with_standard_queues](https://github.com/fastsqs/fastsqs/tree/main/examples/ordering_with_standard_queues) example holds a per-entity `asyncio.Lock` so events for one order serialize while different orders run in parallel. See [FIFO ordering](../concepts/fifo-ordering.md) for the trade-off in full.

## Related

- [FIFO ordering](../concepts/fifo-ordering.md) — why per-group ordering forces tail redelivery
- [Enable partial batch failure](partial-batch-failure.md) — the `ReportBatchItemFailures` wiring both modes depend on
- [Detect the queue type](../concepts/queue-type-detection.md) — how `QueueType.AUTO` infers FIFO from the event-source ARN
- [simple_fifo_example](https://github.com/fastsqs/fastsqs/tree/main/examples/simple_fifo_example) — a runnable FIFO app
- [AWS SQS FIFO queue documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)
