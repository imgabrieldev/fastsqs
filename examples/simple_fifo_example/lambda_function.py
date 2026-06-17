from fastsqs import FastSQS, SQSEvent, Context, QueueType


class OrderCreated(SQSEvent):
    order_id: str

class OrderUpdated(SQSEvent):
    order_id: str


fifo_app = FastSQS(
    queue_type=QueueType.FIFO,
    debug=True,
)


@fifo_app.route(OrderCreated)
async def handle_order_created(msg: OrderCreated, ctx: Context):
    if ctx.queue_type.value == "fifo":
        fifo_info = ctx.fifo_info
        message_group = fifo_info.message_group_id if fifo_info else None
        dedup_id = fifo_info.message_deduplication_id if fifo_info else None
        print(f"Processing order in FIFO queue, group: {message_group}")
        print(f"Deduplication ID: {dedup_id}")
    else:
        print("Processing order in Standard queue")

    print(f"Order created: {msg.order_id}")
    print(f"Message ID: {ctx.message_id}")


@fifo_app.route(OrderUpdated)
async def handle_order_updated(msg: OrderUpdated, ctx: Context):
    if ctx.queue_type.value == "fifo":
        fifo_info = ctx.fifo_info
        message_group = fifo_info.message_group_id if fifo_info else None
        dedup_id = fifo_info.message_deduplication_id if fifo_info else None
        print(f"Processing order update in FIFO queue, group: {message_group}")
        print(f"Deduplication ID: {dedup_id}")
    else:
        print("Processing order update in Standard queue")

    print(f"Order updated: {msg.order_id}")
    print(f"Message ID: {ctx.message_id}")


def lambda_handler(event, context):
    return fifo_app.handler(event, context)


if __name__ == "__main__":
    event = {
        "Records": [
            {
                "messageId": "msg-fifo-001",
                "body": '{"type": "order_created", "order_id": "order-123"}',
                "attributes": {
                    "messageGroupId": "customer-001",
                    "messageDeduplicationId": "dedup-001",
                },
            },
            {
                "messageId": "msg-fifo-002",
                "body": '{"type": "order_updated", "order_id": "order-123"}',
                "attributes": {
                    "messageGroupId": "customer-001",
                    "messageDeduplicationId": "dedup-002",
                },
            },
            {
                "messageId": "msg-fifo-003",
                "body": '{"type": "order_created", "order_id": "order-456"}',
                "attributes": {
                    "messageGroupId": "customer-002",
                    "messageDeduplicationId": "dedup-003",
                },
            },
        ]
    }
    
    result = lambda_handler(event, None)
    print(f"Processing complete. Result: {result}")
