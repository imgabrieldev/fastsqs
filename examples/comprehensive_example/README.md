# Comprehensive FastSQS Example

This example demonstrates the advanced middleware features of FastSQS.

## Features Demonstrated

### 1. Error Handling & DLQ Management
- Circuit breaker pattern for failing handlers
- Dead Letter Queue handler for terminal failures
- Error classification (permanent vs temporary)

> Retries are **not** performed in-process. SQS already redelivers failed
> messages via the visibility timeout + `maxReceiveCount`, with its own native
> dead-letter queue — so the middleware fails fast and lets SQS redeliver.

### 2. Visibility Timeout Management
- Automatic monitoring of processing time vs visibility timeout
- Configurable warning thresholds
- Optional timeout-extension callback for long-running processes

### 3. Parallelization
- Concurrent message processing with semaphore-based limiting
- Thread pool for CPU-intensive tasks
- Optional batch processing with configurable batch sizes

## Middleware Stack

The example configures a comprehensive middleware stack:

1. **LoggingMiddleware** - Structured logging
2. **TimingMsMiddleware** - Performance timing
3. **ErrorHandlingMiddleware** - Circuit breaker + dead-letter routing
4. **DeadLetterQueueMiddleware** - DLQ management
5. **VisibilityTimeoutMonitor** - Timeout monitoring
6. **ProcessingTimeMiddleware** - Processing metrics
7. **ParallelizationMiddleware** - Concurrency control

## Usage

### Local Testing
```bash
python lambda_function.py
```

### AWS Lambda Deployment
1. Package the code with dependencies
2. Set appropriate IAM permissions for SQS (and your DLQ)
3. Configure the SQS trigger with the desired queue

## Configuration Options

### Error Handling Configuration
```python
circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)
error_middleware = ErrorHandlingMiddleware(
    circuit_breaker=circuit_breaker,
    dead_letter_handler=my_dlq_handler,   # optional, sync or async
)
```

### Parallelization Configuration
```python
config = ParallelizationConfig(
    max_concurrent_messages=10,
    use_thread_pool=True,
    thread_pool_size=5,
    batch_processing=True,
    batch_size=10,
    batch_timeout=5.0
)
```

## Message Routing

The example routes three event models by their `type` discriminator:

- **order_processing**: Standard order processing with the full middleware stack
- **high_volume_message**: High-throughput processing with parallelization
- **critical_message**: Critical messages with strict timeout monitoring

## Production Considerations

1. **Monitoring**: Integrate logging/metrics with CloudWatch or your system
2. **Error Handling**: Configure an SQS DLQ + alerting; tune the circuit breaker
3. **Concurrency**: Tune parallelization based on your workload
4. **Timeouts**: Configure visibility timeouts based on processing requirements
