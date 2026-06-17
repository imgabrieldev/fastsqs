"""Timing middleware for measuring message processing duration."""

import time
from .base import Middleware


class TimingMsMiddleware(Middleware):
    """Middleware that measures and logs message processing duration.
    
    Records start time before processing and calculates duration after,
    storing the result in the processing context.
    """
    
    def __init__(self, store_key_start: str = "start_ns", store_key_ms: str = "duration_ms"):
        """Initialize timing middleware.
        
        Args:
            store_key_start: Context key for storing start time
            store_key_ms: Context key for storing duration in milliseconds
        """
        super().__init__()
        self.store_key_start = store_key_start
        self.store_key_ms = store_key_ms

    async def before(self, payload, record, context, ctx):
        """Record processing start time.
        
        Args:
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Processing context
        """
        msg_id = record.get("messageId", "UNKNOWN")
        ctx[self.store_key_start] = time.perf_counter_ns()
        self._log("debug", "Processing started", msg_id=msg_id)

    async def after(self, payload, record, context, ctx, error):
        """Calculate and log processing duration.
        
        Args:
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Processing context
            error: Exception if processing failed
        """
        msg_id = record.get("messageId", "UNKNOWN")
        start = ctx.get(self.store_key_start)
        if start is not None:
            dur_ns = time.perf_counter_ns() - start
            duration_ms = round(dur_ns / 1_000_000, 3)
            ctx[self.store_key_ms] = duration_ms
            
            status = "FAILED" if error else "SUCCESS"
            self._log("info", "Processing completed", 
                     msg_id=msg_id, status=status, duration_ms=duration_ms)