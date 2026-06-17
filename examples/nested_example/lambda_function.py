from fastsqs import FastSQS, SQSRouter, SQSEvent, LoggingMiddleware, TimingMiddleware
import json


class CreateUser(SQSEvent):
    name: str
    email: str = None


class CreateOrder(SQSEvent):
    order_id: str


class UpdateUser(SQSEvent):
    user_id: str
    name: str


class DeleteUser(SQSEvent):
    user_id: str


class SearchUsers(SQSEvent):
    query: str


class WriteToRds(SQSEvent):
    table: str
    data: dict = None


class WriteToCache(SQSEvent):
    key: str
    value: str = None


app = FastSQS(
    debug=True,
)

router = SQSRouter(discriminator="action")

create_router = SQSRouter(discriminator="entity")
db_router = SQSRouter(discriminator="db")

router.subrouter("create", create_router)
router.subrouter("write", db_router)

app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())

app.include_router(router)


@create_router.route("user", model=CreateUser)
async def handle_create_user(msg: CreateUser):
    print(f"[CREATE USER] name={msg.name} email={msg.email}")
    return {"status": "success", "action": "create_user", "name": msg.name}


@create_router.route("order", model=CreateOrder)
async def handle_create_order(msg: CreateOrder):
    print(f"[CREATE ORDER] order_id={msg.order_id}")
    return {"status": "success", "action": "create_order", "order_id": msg.order_id}


@router.route("update", model=UpdateUser)
async def handle_update_user(msg: UpdateUser):
    print(f"[UPDATE USER] user_id={msg.user_id} name={msg.name}")
    return {"status": "success", "action": "update", "user_id": msg.user_id}


@router.route("delete", model=DeleteUser)
async def handle_delete_user(msg: DeleteUser):
    print(f"[DELETE USER] user_id={msg.user_id}")
    return {"status": "success", "action": "delete", "user_id": msg.user_id}


@router.route("search", model=SearchUsers)
async def handle_search_users(msg: SearchUsers):
    print(f"[SEARCH USERS] query={msg.query}")
    return {"status": "success", "action": "search", "query": msg.query}


@db_router.route("rds", model=WriteToRds)
async def handle_write_to_rds(msg: WriteToRds):
    print(f"[WRITE TO RDS] table={msg.table} data={msg.data}")
    return {"status": "success", "action": "write_rds", "table": msg.table}


@db_router.route("cache", model=WriteToCache)
async def handle_write_to_cache(msg: WriteToCache):
    print(f"[WRITE TO CACHE] key={msg.key} value={msg.value}")
    return {"status": "success", "action": "write_cache", "key": msg.key}


@router.default()
async def handle_unknown_action(payload: dict):
    print(f"[FALLBACK] Unknown action with data={payload.get('data')}")
    return {"status": "error", "message": "Unknown action"}


def lambda_handler(event, context):
    return app.handler(event, context)


def test_nested_routing():
    print("=== Testing Nested Routing ===")

    test_cases = [
        {"action": "create", "entity": "user", "name": "John Doe"},
        {"action": "create", "entity": "order", "orderId": "12345"},
        {"action": "update", "userId": "user123", "name": "Jane Doe"},
        {"action": "delete", "userId": "user123"},
        {"action": "search", "query": "John"},
        {"action": "write", "db": "rds", "table": "users"},
        {"action": "write", "db": "cache", "key": "user:123"},
        {"action": "unknown", "data": "test"},
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- Test Case {i}: {test_case['action']} ---")
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
    test_nested_routing()
