# Comprehensive FastSQS Example

This example demonstrates the core FastSQS features.

## Features Demonstrated

### 1. Concurrency
Configured on the app: `FastSQS(max_concurrent_messages=5)`. Records in a batch
are processed concurrently up to that limit (asyncio).

### 2. Logging & Timing
- `LoggingMiddleware` — structured JSON logging.
- `TimingMiddleware` — per-message duration.

### 3. Failure handling = SQS
fastsqs reports failed records as `batchItemFailures`; the queue's **redrive
policy** (`maxReceiveCount` → dead-letter target) makes SQS redeliver and then
move poison messages to the DLQ automatically. There is no in-app DLQ
middleware — dead-lettering is infrastructure (configure it on the queue, e.g.
in Terraform).

> Retries are **not** performed in-process. SQS redelivers via the visibility
> timeout + `maxReceiveCount`, with its own native dead-letter queue.

## Middleware Stack

1. **LoggingMiddleware** — structured logging
2. **TimingMiddleware** — per-message timing

(Need error classification, metrics, idempotency, masking? Write a small
`Middleware` subclass — see `examples/custom_middleware_example`.)

## Usage

### Local Testing
```bash
python lambda_function.py
```

### AWS Lambda Deployment
1. Package the code with dependencies
2. Set IAM permissions for SQS
3. Configure the SQS trigger; set a redrive policy → DLQ on the source queue

## Message Routing

The example routes three event models by their `type` discriminator:

- **order_processing** — standard order processing
- **high_volume_message** — high-throughput processing
- **critical_message** — critical messages

## Production Considerations

1. **Dead-letter**: set a redrive policy (`maxReceiveCount` → DLQ) on the queue
2. **Monitoring**: integrate logging with CloudWatch or your system
3. **Concurrency**: tune `max_concurrent_messages` for your workload
4. **Timeouts**: set the queue visibility timeout based on processing time
