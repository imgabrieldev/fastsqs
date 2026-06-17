"""Timing middleware for measuring message processing duration."""

import logging
import time

from .base import Middleware

_logger = logging.getLogger("fastsqs")


class TimingMiddleware(Middleware):
    """Middleware that measures message processing duration.

    Records a start time before processing and stores the duration (ms) in
    ``ctx.state`` after, so downstream middleware/handlers can read it.
    """

    def __init__(
        self, store_key_start: str = "start_ns", store_key_ms: str = "duration_ms"
    ):
        """Initialize timing middleware.

        Args:
            store_key_start: ``ctx.state`` key for the start time
            store_key_ms: ``ctx.state`` key for the duration in milliseconds
        """
        self.store_key_start = store_key_start
        self.store_key_ms = store_key_ms

    async def before(self, payload, record, context, ctx):
        """Record processing start time in ``ctx.state``."""
        ctx.state[self.store_key_start] = time.perf_counter_ns()

    async def after(self, payload, record, context, ctx, error):
        """Compute and store processing duration in ``ctx.state``."""
        msg_id = record.get("messageId", "UNKNOWN")
        # .get (not attribute) — before may not have run during an unwind.
        start = ctx.state.get(self.store_key_start)
        if start is not None:
            duration_ms = round((time.perf_counter_ns() - start) / 1_000_000, 3)
            ctx.state[self.store_key_ms] = duration_ms
            status = "FAILED" if error else "SUCCESS"
            _logger.info(
                "Processing completed msg_id=%s status=%s duration_ms=%s",
                msg_id, status, duration_ms,
            )
