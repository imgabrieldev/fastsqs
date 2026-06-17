from fastsqs import Middleware

class ErrorHandlingMiddleware(Middleware):
    async def before(self, payload, record, context, ctx):
        if not payload.get("action"):
            raise ValueError("Missing required field: action")
        ctx.state["error_count"] = ctx.state.get("error_count", 0)

    async def after(self, payload, record, context, ctx, error):
        if error is not None:
            error_count = ctx.state.get("error_count", 0) + 1
            ctx.state["error_count"] = error_count
            print(f"[ERROR] Message {record.get('messageId')} failed")
            print(f"[ERROR] Error: {type(error).__name__}: {error}")
            print(f"[ERROR] Total errors: {error_count}")
        else:
            print(f"[SUCCESS] Message {record.get('messageId')} completed")
