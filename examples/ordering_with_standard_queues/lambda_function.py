"""
Example: Achieving Order and Deduplication with Standard Queues + FastSQS v0.3.0

This example demonstrates how FastSQS v0.3.0 provides both ordering and deduplication
with standard queues, giving you the best of both worlds.
"""

import asyncio
import json
from typing import Dict, Any
from datetime import datetime
from fastsqs import FastSQS, SQSEvent

# Concurrency is configured on the app itself.
app = FastSQS(max_concurrent_messages=10)

# In-memory locks for per-entity ordering
entity_locks = {}

async def get_entity_lock(entity_id: str) -> asyncio.Lock:
    """Get or create a lock for a specific entity (order, account, user, etc.)"""
    if entity_id not in entity_locks:
        entity_locks[entity_id] = asyncio.Lock()
    return entity_locks[entity_id]

# Event models
class OrderEvent(SQSEvent):
    order_id: str
    event_type: str  # created, paid, shipped, delivered
    timestamp: str
    data: Dict[str, Any] = {}

class PaymentEvent(SQSEvent):
    account_id: str
    transaction_id: str
    amount: float
    event_type: str  # pending, processing, completed, failed
    timestamp: str

class UserEvent(SQSEvent):
    user_id: str
    event_type: str  # signup, verify, activate, suspend
    timestamp: str
    data: Dict[str, Any] = {}

@app.route(OrderEvent)
async def handle_order_event(msg: OrderEvent) -> Dict[str, Any]:
    """
    Handle order events with per-order ordering but cross-order parallelization
    
    This achieves:
    - No duplicates (application-level idempotency)
    - Per-order sequential processing (application lock)
    - Cross-order parallel processing (different orders process simultaneously)
    - High throughput (not limited by FIFO queue constraints)
    """
    
    # Get lock for this specific order (allows other orders to process in parallel)
    lock = await get_entity_lock(f"order_{msg.order_id}")
    
    async with lock:
        print(f"Processing order {msg.order_id} event: {msg.event_type}")
        
        # Simulate order state validation and processing
        if msg.event_type == "created":
            await process_order_creation(msg)
        elif msg.event_type == "paid":
            await process_payment_confirmation(msg)
        elif msg.event_type == "shipped":
            await process_shipment(msg)
        elif msg.event_type == "delivered":
            await process_delivery(msg)
        
        return {
            "order_id": msg.order_id,
            "event_type": msg.event_type,
            "status": "processed",
            "processed_at": datetime.now().isoformat()
        }

@app.route(PaymentEvent)
async def handle_payment_event(msg: PaymentEvent) -> Dict[str, Any]:
    """
    Handle payment events with per-account ordering
    
    Critical for financial accuracy:
    - No duplicate transactions (idempotency)
    - Per-account sequential processing (prevents race conditions)
    - Cross-account parallel processing (higher throughput)
    """
    
    # Per-account locking for financial consistency
    lock = await get_entity_lock(f"account_{msg.account_id}")
    
    async with lock:
        print(f"Processing payment for account {msg.account_id}: {msg.event_type}")
        
        # Critical: ensure balance updates happen in order per account
        current_balance = await get_account_balance(msg.account_id)
        
        if msg.event_type == "pending":
            await reserve_funds(msg.account_id, msg.amount)
        elif msg.event_type == "completed":
            await deduct_funds(msg.account_id, msg.amount)
        elif msg.event_type == "failed":
            await release_reserved_funds(msg.account_id, msg.amount)
        
        return {
            "account_id": msg.account_id,
            "transaction_id": msg.transaction_id,
            "status": "processed"
        }

@app.route(UserEvent)
async def handle_user_event(msg: UserEvent) -> Dict[str, Any]:
    """
    Handle user events with timestamp-based ordering
    
    Demonstrates flexible ordering strategies:
    - No duplicates (idempotency)
    - Timestamp-based processing order
    - Parallel processing for different users
    """
    
    lock = await get_entity_lock(f"user_{msg.user_id}")
    
    async with lock:
        # Get last processed timestamp for this user
        last_timestamp = await get_last_processed_timestamp(msg.user_id)
        msg_timestamp = datetime.fromisoformat(msg.timestamp)
        
        # Only process if this message is newer (handles out-of-order delivery)
        if not last_timestamp or msg_timestamp > last_timestamp:
            print(f"Processing user {msg.user_id} event: {msg.event_type}")
            
            await process_user_event(msg)
            await update_last_processed_timestamp(msg.user_id, msg_timestamp)
            
            return {"status": "processed", "user_id": msg.user_id}
        else:
            print(f"Skipping old event for user {msg.user_id}")
            return {"status": "skipped_old_event", "user_id": msg.user_id}

# Simulation functions
async def process_order_creation(msg: OrderEvent):
    await asyncio.sleep(0.1)  # Simulate processing time

async def process_payment_confirmation(msg: OrderEvent):
    await asyncio.sleep(0.1)

async def process_shipment(msg: OrderEvent):
    await asyncio.sleep(0.1)

async def process_delivery(msg: OrderEvent):
    await asyncio.sleep(0.1)

async def get_account_balance(account_id: str) -> float:
    # Simulate database lookup
    return 1000.0

async def reserve_funds(account_id: str, amount: float):
    await asyncio.sleep(0.05)  # Simulate database update

async def deduct_funds(account_id: str, amount: float):
    await asyncio.sleep(0.05)

async def release_reserved_funds(account_id: str, amount: float):
    await asyncio.sleep(0.05)

async def get_last_processed_timestamp(user_id: str):
    # Simulate getting last processed timestamp from database
    return None

async def update_last_processed_timestamp(user_id: str, timestamp: datetime):
    await asyncio.sleep(0.01)  # Simulate database update

async def process_user_event(msg: UserEvent):
    await asyncio.sleep(0.1)

def lambda_handler(event, context):
    """AWS Lambda handler"""
    return app.handler(event, context)

# Example test data showing concurrent processing
if __name__ == "__main__":
    test_event = {
        "Records": [
            # Multiple orders can process in parallel
            {
                "messageId": "msg-1",
                "body": json.dumps({
                    "type": "order_event",
                    "order_id": "order-123",
                    "event_type": "created",
                    "timestamp": "2024-01-01T10:00:00Z"
                }),
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:OrderEvent"
            },
            {
                "messageId": "msg-2", 
                "body": json.dumps({
                    "type": "order_event",
                    "order_id": "order-456",  # Different order - processes in parallel
                    "event_type": "created",
                    "timestamp": "2024-01-01T10:00:01Z"
                }),
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:OrderEvent"
            },
            {
                "messageId": "msg-3",
                "body": json.dumps({
                    "type": "order_event", 
                    "order_id": "order-123",  # Same order - waits for msg-1
                    "event_type": "paid",
                    "timestamp": "2024-01-01T10:01:00Z"
                }),
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:OrderEvent"
            },
            # Payment events for different accounts process in parallel
            {
                "messageId": "msg-4",
                "body": json.dumps({
                    "type": "payment_event",
                    "account_id": "acc-789",
                    "transaction_id": "txn-001",
                    "amount": 50.0,
                    "event_type": "pending",
                    "timestamp": "2024-01-01T10:00:00Z"
                }),
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:PaymentEvent"
            }
        ]
    }
    
    result = lambda_handler(test_event, {})
    print(f"Result: {json.dumps(result, indent=2)}")
