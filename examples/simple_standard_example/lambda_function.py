from fastsqs import FastSQS, SQSEvent
import asyncio


class OrderCreated(SQSEvent):
    order_id: str

class OrderUpdated(SQSEvent):
    order_id: str


print("=== Standard Queue Example ===")
standard_app = FastSQS(
    debug=True,
)


@standard_app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated):
    print("Processing order in Standard queue (parallel processing)")
    print(f"Order created: {msg.order_id}")

@standard_app.route(OrderUpdated)
async def handle_order_updated(msg: OrderUpdated):
    print("Processing order update in Standard queue (parallel processing)")
    print(f"Order updated: {msg.order_id}")

def lambda_handler(event, context):
    return standard_app.handler(event, context)


if __name__ == "__main__":
    event = {
        "Records": [
            {
                "messageId": "msg-001",
                "body": '{"type": "order_created", "order_id": "order-123"}',
                "attributes": {},
            },
            {
                "messageId": "msg-002",
                "body": '{"type": "order_updated", "order_id": "order-123"}',
                "attributes": {},
            },
            {
                "messageId": "msg-003",
                "body": '{"type": "order_created", "order_id": "order-456"}',
                "attributes": {},
            },
        ]
    }

    result = lambda_handler(event, None)
    print(f"Processing complete. Result: {result}")