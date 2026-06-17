from fastsqs import FastSQS, SQSEvent, LoggingMiddleware, TimingMiddleware
import json
import asyncio


class UserLogin(SQSEvent):
    user_id: str
    password: str
    timestamp: str
    
    @classmethod
    def get_message_type(cls) -> str:
        return "login"

class UserLogout(SQSEvent):
    user_id: str
    session_id: str
    
    @classmethod
    def get_message_type(cls) -> str:
        return "logout"

class ProfileUpdate(SQSEvent):
    user_id: str
    ssn: str
    email: str
    
    @classmethod
    def get_message_type(cls) -> str:
        return "profile_update"


app = FastSQS(
    debug=True,
    discriminator="action",
)

app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())


@app.route(UserLogin)
async def handle_user_login(msg: UserLogin):
    print(f"User logged in: {msg.user_id}")
    await asyncio.sleep(0.1)
    return {"status": "success", "userId": msg.user_id}


@app.route(UserLogout)
async def handle_user_logout(msg: UserLogout):
    print(f"User logged out: {msg.user_id}")
    await asyncio.sleep(0.05)
    return {"status": "success", "userId": msg.user_id}


@app.route(ProfileUpdate)
async def handle_profile_update(msg: ProfileUpdate):
    print(f"Profile updated for user: {msg.user_id}")
    await asyncio.sleep(0.2)
    return {"status": "success", "userId": msg.user_id}

def lambda_handler(event, context):
    return app.handler(event, context)


def test_middleware():
    print("=== Testing Middleware ===")

    test_events = [
        {
            "Records": [
                {
                    "messageId": "msg-001",
                    "body": json.dumps(
                        {
                            "action": "login",
                            "user_id": "user123",
                            "password": "secret123",
                            "timestamp": "2024-01-01T10:00:00Z",
                        }
                    ),
                    "attributes": {},
                }
            ]
        },
        {
            "Records": [
                {
                    "messageId": "msg-002",
                    "body": json.dumps(
                        {
                            "action": "profile_update",
                            "user_id": "user123",
                            "ssn": "123-45-6789",
                            "email": "user@example.com",
                        }
                    ),
                    "attributes": {},
                }
            ]
        },
        {
            "Records": [
                {
                    "messageId": "msg-003",
                    "body": json.dumps(
                        {
                            "action": "logout",
                            "user_id": "user123",
                            "session_id": "sess-456",
                        }
                    ),
                    "attributes": {},
                }
            ]
        },
    ]

    for i, event in enumerate(test_events, 1):
        print(f"\n--- Test Event {i} ---")
        result = lambda_handler(event, None)
        print(f"Result: {result}")


if __name__ == "__main__":
    test_middleware()