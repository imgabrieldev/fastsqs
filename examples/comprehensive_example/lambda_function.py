import json
import asyncio
from typing import Dict, Any, List
from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import (
    ErrorHandlingMiddleware, RetryConfig, DeadLetterQueueMiddleware,
    VisibilityTimeoutMonitor, ProcessingTimeMiddleware,
    ParallelizationMiddleware, ParallelizationConfig,
    TimingMsMiddleware, LoggingMiddleware
)

# Define event models
class OrderProcessing(SQSEvent):
    order_id: str
    customer_id: str = None
    amount: float = None

class HighVolumeMessage(SQSEvent):
    message_id: str
    data: str = None

class CriticalMessage(SQSEvent):
    critical_id: str
    priority: str = "high"

app = FastSQS()

# Configure comprehensive middleware stack
retry_config = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=60.0,
    exponential_backoff=True,
    retry_exceptions=[ValueError, ConnectionError]
)
error_middleware = ErrorHandlingMiddleware(retry_config)
dlq_middleware = DeadLetterQueueMiddleware(max_processing_time=300.0)

visibility_monitor = VisibilityTimeoutMonitor(
    warning_threshold=0.8,
    default_visibility_timeout=30.0
)
processing_middleware = ProcessingTimeMiddleware()

parallelization_config = ParallelizationConfig(
    max_concurrent_messages=5,
    use_thread_pool=True,
    thread_pool_size=3,
    batch_processing=True,
    batch_size=10,
    batch_timeout=5.0
)
parallel_middleware = ParallelizationMiddleware(parallelization_config)

# Add middleware in order
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMsMiddleware())
app.add_middleware(error_middleware)
app.add_middleware(dlq_middleware)
app.add_middleware(visibility_monitor)
app.add_middleware(processing_middleware)
app.add_middleware(parallel_middleware)

@app.route(OrderProcessing)
async def process_order(msg: OrderProcessing) -> Dict[str, Any]:
    """Process order with comprehensive error handling and monitoring"""
    order_id = msg.order_id
    print(f"Processing order {order_id}")
    
    # Simulate some processing time
    await asyncio.sleep(0.5)
    
    # Simulate occasional errors for testing retry logic
    if order_id and order_id.endswith("error"):
        raise ValueError(f"Simulated error for order {order_id}")
    
    return {
        "order_id": order_id,
        "status": "processed",
        "processed_at": "2024-01-01T00:00:00Z"
    }

@app.route(HighVolumeMessage)
async def handle_high_volume(msg: HighVolumeMessage) -> Dict[str, Any]:
    """Handle high-volume queue with parallelization"""
    message_id = msg.message_id
    
    # Simulate CPU-intensive work
    await asyncio.sleep(0.1)
    
    return {"message_id": message_id, "status": "processed"}

@app.route(CriticalMessage)
async def handle_critical_messages(msg: CriticalMessage) -> Dict[str, Any]:
    """Handle critical messages with strict monitoring"""
    critical_id = msg.critical_id
    
    # Simulate processing that might need visibility timeout extension
    await asyncio.sleep(2)  # Shorter for testing
    
    return {
        "critical_id": critical_id,
        "status": "critical_processed",
        "priority": "high"
    }

def lambda_handler(event, context):
    """AWS Lambda handler with comprehensive middleware"""
    try:
        # Process the SQS event
        result = app.handler(event, context)
        
        # Get middleware statistics
        stats = {
            "processing_stats": processing_middleware.get_stats() if hasattr(processing_middleware, 'get_stats') else {},
            "parallelization_stats": parallel_middleware.get_stats() if hasattr(parallel_middleware, 'get_stats') else {}
        }
        
        print(f"Processing completed. Stats: {json.dumps(stats, indent=2)}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully processed messages",
                "stats": stats
            })
        }
        
    except Exception as e:
        print(f"Error processing event: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }

# Example usage for local testing
if __name__ == "__main__":
    # Sample SQS event for testing
    test_event = {
        "Records": [
            {
                "messageId": "test-message-1",
                "receiptHandle": "test-receipt-1",
                "body": json.dumps({
                    "type": "order_processing",
                    "order_id": "order-123",
                    "customer_id": "cust-456",
                    "amount": 99.99
                }),
                "attributes": {
                    "SentTimestamp": "1640995200000",
                    "ApproximateReceiveCount": "1",
                    "ApproximateFirstReceiveTimestamp": "1640995200000"
                },
                "messageAttributes": {},
                "md5OfBody": "test-md5",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:OrderProcessing",
                "awsRegion": "us-east-1"
            },
            {
                "messageId": "test-message-2",
                "receiptHandle": "test-receipt-2",
                "body": json.dumps({
                    "type": "high_volume_message",
                    "message_id": "msg-789",
                    "data": "high-volume-data"
                }),
                "attributes": {
                    "SentTimestamp": "1640995200000",
                    "ApproximateReceiveCount": "1",
                    "ApproximateFirstReceiveTimestamp": "1640995200000"
                },
                "messageAttributes": {},
                "md5OfBody": "test-md5-2",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:HighVolumeMessage",
                "awsRegion": "us-east-1"
            }
        ]
    }
    
    # Test the handler
    result = lambda_handler(test_event, {})
    print(f"Test result: {json.dumps(result, indent=2)}")
