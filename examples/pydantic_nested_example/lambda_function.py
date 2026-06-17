from fastsqs import FastSQS, SQSRouter, SQSEvent, LoggingMiddleware, TimingMiddleware
import json


# Base Events for different domains
class UserEvent(SQSEvent):
    """Base class for user-related events"""

    pass


class OrderEvent(SQSEvent):
    """Base class for order-related events"""

    pass


class SystemEvent(SQSEvent):
    """Base class for system-related events"""

    pass


# User Management Events
class CreateUser(UserEvent):
    name: str
    email: str = None
    department: str = "general"


class UpdateUser(UserEvent):
    user_id: str
    name: str = None
    email: str = None


class DeleteUser(UserEvent):
    user_id: str
    reason: str = "user_request"


# Order Management Events
class CreateOrder(OrderEvent):
    order_id: str
    customer_id: str
    amount: float


class UpdateOrder(OrderEvent):
    order_id: str
    status: str
    tracking_number: str = None


class ProcessPayment(OrderEvent):
    order_id: str
    payment_method: str
    amount: float


# System Events
class DatabaseBackup(SystemEvent):
    database_name: str
    backup_type: str = "full"


class SendNotification(SystemEvent):
    recipient: str
    message: str
    channel: str = "email"


class GenerateReport(SystemEvent):
    report_type: str
    date_range: str
    format: str = "pdf"


# Main FastSQS App
app = FastSQS(
    debug=True,
)

# Create domain-specific routers that use pydantic routing
user_router = SQSRouter()
order_router = SQSRouter()
system_router = SQSRouter()

# Add middleware to main app
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())

# Include the nested routers
app.include_router(user_router)
app.include_router(order_router)
app.include_router(system_router)


# User Management Handlers (in user_router)
@user_router.route(CreateUser)
async def handle_create_user(msg: CreateUser):
    print(f"[USER ROUTER] Creating user: {msg.name} ({msg.email}) in {msg.department}")
    return {"status": "success", "action": "create_user", "name": msg.name}


@user_router.route(UpdateUser)
async def handle_update_user(msg: UpdateUser):
    print(
        f"[USER ROUTER] Updating user {msg.user_id}: name={msg.name}, email={msg.email}"
    )
    return {"status": "success", "action": "update_user", "user_id": msg.user_id}


@user_router.route(DeleteUser)
async def handle_delete_user(msg: DeleteUser):
    print(f"[USER ROUTER] Deleting user {msg.user_id}, reason: {msg.reason}")
    return {"status": "success", "action": "delete_user", "user_id": msg.user_id}


# Order Management Handlers (in order_router)
@order_router.route(CreateOrder)
async def handle_create_order(msg: CreateOrder):
    print(
        f"[ORDER ROUTER] Creating order {msg.order_id} for customer {msg.customer_id}: ${msg.amount}"
    )
    return {"status": "success", "action": "create_order", "order_id": msg.order_id}


@order_router.route(UpdateOrder)
async def handle_update_order(msg: UpdateOrder):
    print(
        f"[ORDER ROUTER] Updating order {msg.order_id}: status={msg.status}, tracking={msg.tracking_number}"
    )
    return {"status": "success", "action": "update_order", "order_id": msg.order_id}


@order_router.route(ProcessPayment)
async def handle_process_payment(msg: ProcessPayment):
    print(
        f"[ORDER ROUTER] Processing payment for order {msg.order_id}: ${msg.amount} via {msg.payment_method}"
    )
    return {"status": "success", "action": "process_payment", "order_id": msg.order_id}


# System Handlers (in system_router)
@system_router.route(DatabaseBackup)
async def handle_database_backup(msg: DatabaseBackup):
    print(f"[SYSTEM ROUTER] Creating {msg.backup_type} backup of '{msg.database_name}'")
    return {
        "status": "success",
        "action": "database_backup",
        "database": msg.database_name,
    }


@system_router.route(SendNotification)
async def handle_send_notification(msg: SendNotification):
    print(f"[SYSTEM ROUTER] Sending {msg.channel} to {msg.recipient}: '{msg.message}'")
    return {
        "status": "success",
        "action": "send_notification",
        "recipient": msg.recipient,
    }


@system_router.route(GenerateReport)
async def handle_generate_report(msg: GenerateReport):
    print(
        f"[SYSTEM ROUTER] Generating {msg.report_type} report for {msg.date_range} ({msg.format})"
    )
    return {
        "status": "success",
        "action": "generate_report",
        "report_type": msg.report_type,
    }


# Main app can also have its own pydantic routes
class HealthCheck(SQSEvent):
    service: str
    timestamp: str = None


@app.route(HealthCheck)
async def handle_health_check(msg: HealthCheck):
    print(f"[MAIN APP] Health check for service: {msg.service}")
    return {"status": "success", "action": "health_check", "service": msg.service}


def lambda_handler(event, context):
    return app.handler(event, context)


def test_pydantic_nested_routing():
    print("=== Testing Pydantic Nested Routing ===")

    test_cases = [
        # User Management (handled by user_router)
        {
            "type": "create_user",
            "name": "Alice Johnson",
            "email": "alice@company.com",
            "department": "engineering",
        },
        {
            "type": "update_user",
            "user_id": "user-123",
            "name": "Alice Smith",
            "email": "alice.smith@company.com",
        },
        {"type": "delete_user", "user_id": "user-456", "reason": "account_closure"},
        # Order Management (handled by order_router)
        {
            "type": "create_order",
            "order_id": "ORD-001",
            "customer_id": "CUST-789",
            "amount": 299.99,
        },
        {
            "type": "update_order",
            "order_id": "ORD-001",
            "status": "shipped",
            "tracking_number": "TRK-12345",
        },
        {
            "type": "process_payment",
            "order_id": "ORD-002",
            "payment_method": "credit_card",
            "amount": 150.00,
        },
        # System Operations (handled by system_router)
        {
            "type": "database_backup",
            "database_name": "production",
            "backup_type": "incremental",
        },
        {
            "type": "send_notification",
            "recipient": "admin@company.com",
            "message": "System maintenance scheduled",
            "channel": "email",
        },
        {
            "type": "generate_report",
            "report_type": "sales",
            "date_range": "2024-01",
            "format": "excel",
        },
        # Main App Route
        {
            "type": "health_check",
            "service": "user-service",
            "timestamp": "2024-01-15T10:00:00Z",
        },
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- Test Case {i}: {test_case['type']} ---")
        event = {
            "Records": [
                {
                    "messageId": f"msg-{i}",
                    "body": json.dumps(test_case),
                    "attributes": {},
                }
            ]
        }

        result = lambda_handler(event, None)
        print(f"Result: {result}")


if __name__ == "__main__":
    test_pydantic_nested_routing()
