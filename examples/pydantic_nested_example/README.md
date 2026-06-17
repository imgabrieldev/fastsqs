# Pydantic Nested Routing Example

This example demonstrates **true nested routing** using multiple `SQSRouter` instances, each handling different pydantic event models. Unlike the main app's single router approach, this shows how to distribute routing logic across specialized routers while maintaining full type safety.

## Features

- **Multiple SQSRouter instances** - separate routers for different domains
- **Pure pydantic model routing** - each router uses `route()` with pydantic models
- **Type safety** - all message types are validated with pydantic models  
- **Domain separation** - clear boundaries between business domains
- **Middleware support** - logging and timing middleware across all routers
- **Mixed routing** - main app can have its own routes alongside nested routers

## Architecture

```
FastSQS App
├── user_router (SQSRouter)
│   ├── CreateUser event
│   ├── UpdateUser event
│   └── DeleteUser event
├── order_router (SQSRouter)
│   ├── CreateOrder event
│   ├── UpdateOrder event
│   └── ProcessPayment event
├── system_router (SQSRouter)
│   ├── DatabaseBackup event
│   ├── SendNotification event
│   └── GenerateReport event
└── Main App Routes
    └── HealthCheck event
```

## Key Differences from String-Based Routing

### Traditional String-Based Routing
```python
router = SQSRouter(discriminator="action")  # Routes based on "action" key

@router.route("create_user")  # String-based route
async def handle_create_user(msg):
    pass
```

### Pydantic Nested Routing (This Example)
```python
app = FastSQS()
user_router = SQSRouter(discriminator="type")
app.include_router(user_router)

@user_router.route(CreateUser)  # Nested pydantic model route
async def handle_create_user(msg: CreateUser):
    pass

@app.route(HealthCheck)  # Main app can also have routes
async def handle_health_check(msg: HealthCheck):
    pass
```

## Benefits

1. **Type Safety**: All messages are validated against pydantic models
2. **Domain Separation**: Clear boundaries between different business domains
3. **Scalability**: Easy to add new routers for new domains
4. **IDE Support**: Full autocomplete and type checking
5. **Documentation**: Self-documenting with pydantic model fields
6. **Validation**: Automatic validation of message structure and types
7. **Flexibility**: Mix main app routes with nested router routes

## Usage

```bash
# Run the example
python lambda_function.py
```

## Message Format

All messages follow the same pattern:
```json
{
  "type": "create_user",
  "name": "Alice Johnson", 
  "email": "alice@company.com",
  "department": "engineering"
}
```

The `type` field determines which pydantic model and handler to use.

## Expected Output

The example processes 10 different message types across 3 specialized routers plus the main app, demonstrating true nested routing where each router handles its own domain-specific pydantic events. You'll see different log prefixes indicating which router handled each message:

- `[USER ROUTER]` - Messages handled by user_router
- `[ORDER ROUTER]` - Messages handled by order_router  
- `[SYSTEM ROUTER]` - Messages handled by system_router
- `[MAIN APP]` - Messages handled by the main app's routes
