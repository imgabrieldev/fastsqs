from fastsqs import Middleware
import time

class CustomLoggingMiddleware(Middleware):
    async def before(self, payload, record, context, ctx):
        print(f"[BEFORE] Processing message {record.get('messageId')}")
        print(f"[BEFORE] Action: {payload.get('action')}")
        print(f"[BEFORE] Timestamp: {time.time()}")
        ctx.state["start_time"] = time.time()
        ctx.state["message_count"] = ctx.state.get("message_count", 0) + 1

    async def after(self, payload, record, context, ctx, error):
        end_time = time.time()
        start_time = ctx.state.get("start_time", end_time)
        duration = end_time - start_time
        print(f"[AFTER] Message {record.get('messageId')} processed")
        print(f"[AFTER] Duration: {duration:.3f}s")
        print(f"[AFTER] Error: {error}")
        message_count = ctx.state.get("message_count", 0)
        print(f"[AFTER] Total messages processed: {message_count}")
