import json
from typing import Dict, Any
from fastsqs import FastSQS, SQSEvent

class OrderProcessing(SQSEvent):
    order_id: str
    customer_id: str = None
    amount: float = None

class UserRegistration(SQSEvent):
    user_id: str
    email: str
    timestamp: float = None

app = FastSQS()

app.use_preset("production", 
    max_concurrent=15,
    retry_attempts=3,
    visibility_timeout=45.0
)

@app.route(OrderProcessing)
async def process_order(msg: OrderProcessing) -> Dict[str, Any]:
    print(f"Processing order {msg.order_id}")
    return {"order_id": msg.order_id, "status": "processed"}

@app.route(UserRegistration)
async def register_user(msg: UserRegistration) -> Dict[str, Any]:
    print(f"Registering user {msg.user_id}")
    return {"user_id": msg.user_id, "status": "registered"}

def lambda_handler(event, context):
    return app.handler(event, context)

if __name__ == "__main__":
    test_event = {
        "Records": [
            {
                "messageId": "test-1",
                "body": json.dumps({
                    "type": "order_processing",
                    "order_id": "order-123",
                    "customer_id": "cust-456",
                    "amount": 99.99
                }),
                "attributes": {"ApproximateReceiveCount": "1"}
            }
        ]
    }
    
    result = lambda_handler(test_event, {})
    print(f"Result: {result}")
