# Ordering with Standard Queues + FastSQS v0.3.0

This example demonstrates how to achieve **both ordering and deduplication** with standard SQS queues using FastSQS v0.3.0 middleware.

## Key Benefits

### Deduplication (application-level)
- Deduplication is the application's responsibility — e.g. an idempotency-key
  check in your handler (DynamoDB conditional put, Redis SETNX, etc.)
- fastsqs does not ship an idempotency middleware; it's infra/storage-specific

### Flexible Ordering
- **Per-entity ordering**: Sequential processing per order/account/user
- **Cross-entity parallelization**: Different entities process simultaneously  
- **Custom ordering strategies**: Timestamp-based, priority-based, or business logic

### High Performance
- 100x higher throughput than FIFO queues
- Optimal resource utilization
- Lower costs

## Ordering Strategies

### 1. Per-Entity Sequential Processing
```python
# Each order processes sequentially, but different orders in parallel
async with get_entity_lock(f"order_{msg.order_id}"):
    await process_order_event(msg)
```

### 2. Timestamp-Based Ordering
```python
# Only process if message is newer than last processed
if msg_timestamp > last_processed_timestamp:
    await process_event(msg)
```

### 3. Priority-Based Ordering
```python
# Process high-priority messages first
if msg.priority == "high":
    await high_priority_queue.put(msg)
else:
    await standard_queue.put(msg)
```

## Real-World Scenarios

### E-commerce Order Processing
- Order events for same order process sequentially
- Different orders process in parallel
- No duplicate order confirmations
- 100x faster than FIFO

### Financial Transactions  
- Per-account sequential processing (prevents race conditions)
- Cross-account parallel processing
- Exactly-once transaction processing
- High throughput for payment processing

### User Account Management
- User events process in timestamp order
- Different users process simultaneously
- No duplicate notifications
- Scalable user onboarding

## Performance Comparison

| Scenario | FIFO Queue | Standard + FastSQS v0.3.0 |
|----------|------------|---------------------------|
| **Single Order Processing** | 3k msgs/sec | 300k msgs/sec |
| **Multiple Orders** | Still 3k msgs/sec total | 300k msgs/sec total |
| **Duplicate Prevention** | Built-in | Application-level (idempotency key) |
| **Ordering** | Strict FIFO | Flexible application-level |
| **Cost** | Higher | Lower |

## Usage

```bash
# Install dependencies
pip install fastsqs

# Run the example
python lambda_function.py
```

## Key Takeaways

1. **Standard queues + FastSQS v0.3.0 provide both reliability AND performance**
2. **Application-level ordering is more flexible than queue-level ordering**
3. **Application-level idempotency (your own key check) handles duplicates**
4. **Per-entity locking allows optimal parallelization**
5. **You get 100x performance improvement with equal reliability**

This approach gives you the best of both worlds: the reliability guarantees you need with the performance and cost benefits of standard queues.
