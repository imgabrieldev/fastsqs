
from fastsqs import FastSQS, SQSEvent
import json
from middleware.custom_logging import CustomLoggingMiddleware
from middleware.error_handling import ErrorHandlingMiddleware
from middleware.metrics import MetricsMiddleware


class UserLogin(SQSEvent):
    user_id: str
    
    @classmethod
    def get_message_type(cls) -> str:
        return "login"

class UserLogout(SQSEvent):
    user_id: str
    
    @classmethod
    def get_message_type(cls) -> str:
        return "logout"

class InvalidMessage(SQSEvent):
    data: str
    
    @classmethod
    def get_message_type(cls) -> str:
        return "invalid"


app = FastSQS(
    debug=True,
    discriminator="action",
)

app.add_middleware(CustomLoggingMiddleware())
app.add_middleware(ErrorHandlingMiddleware())
app.add_middleware(MetricsMiddleware())


@app.route(UserLogin)
async def handle_user_login(msg: UserLogin):
    print(f"User logged in: {msg.user_id}")
    return {"status": "success", "userId": msg.user_id}


@app.route(UserLogout)
async def handle_user_logout(msg: UserLogout):
    print(f"User logged out: {msg.user_id}")
    return {"status": "success", "userId": msg.user_id}


@app.route(InvalidMessage)
async def handle_invalid_message(msg: InvalidMessage):
    print(f"Processing invalid message: {msg.data}")
    raise ValueError("This is a test error")

def lambda_handler(event, context):
    return app.handler(event, context)


def test_custom_middleware():
    print("=== Testing Custom Middleware ===")

    test_events = [
        {
            "Records": [
                {
                    "messageId": "msg-001",
                    "body": json.dumps({"action": "login", "user_id": "user123"}),
                    "attributes": {},
                }
            ]
        },
        {
            "Records": [
                {
                    "messageId": "msg-002",
                    "body": json.dumps({"action": "logout", "user_id": "user123"}),
                    "attributes": {},
                }
            ]
        },
        {
            "Records": [
                {
                    "messageId": "msg-003",
                    "body": json.dumps({"action": "invalid"}),
                    "attributes": {},
                }
            ]
        },
    ]

    for i, event in enumerate(test_events, 1):
        print(f"\n--- Test Event {i} ---")
        try:
            result = lambda_handler(event, None)
            print(f"Result: {result}")
        except Exception as e:
            print(f"Exception: {e}")


if __name__ == "__main__":
    test_custom_middleware()