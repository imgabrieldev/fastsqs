# FastSQS Preset Example

This example demonstrates how to use FastSQS middleware presets for quick setup.

## Available Presets

### Production Preset
```python
app.use_preset("production",
    max_concurrent=10,            # Optional: default is 10
    visibility_timeout=30.0,      # Optional: default is 30.0
    circuit_breaker_threshold=5   # Optional: default is 5
)
```

Includes:
- LoggingMiddleware
- TimingMsMiddleware
- ErrorHandlingMiddleware (circuit breaker + dead-letter routing)
- VisibilityTimeoutMonitor
- ParallelizationMiddleware (thread pool)

### Development Preset
```python
app.use_preset("development", max_concurrent=5)  # Optional: default is 5
```

Includes:
- LoggingMiddleware (verbose, includes record)
- TimingMsMiddleware
- ErrorHandlingMiddleware
- VisibilityTimeoutMonitor (relaxed thresholds)
- ParallelizationMiddleware (no thread pool)

### Minimal Preset
```python
app.use_preset("minimal")
```

Includes:
- LoggingMiddleware
- TimingMsMiddleware

## Usage

Instead of manually configuring each middleware:
```python
# Before (verbose)
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMsMiddleware())
app.add_middleware(ErrorHandlingMiddleware(...))
# ... more middleware
```

Use a preset:
```python
# After (simple)
app.use_preset("production", max_concurrent=15)
```

> Retries are not done in-process: SQS redelivers failed messages via the
> visibility timeout + `maxReceiveCount`, with its own dead-letter queue.
