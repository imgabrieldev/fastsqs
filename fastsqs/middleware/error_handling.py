"""Error handling middleware: error classification and dead-letter routing.

SQS handles retries (visibility timeout + maxReceiveCount + native DLQ), so this
middleware does not retry in-process. On a terminal error it classifies the
error and routes it to a dead-letter handler if one is configured, then lets the
message fail so SQS can redeliver.
"""

from __future__ import annotations

import time
from typing import Any, Awaitable, Optional, Callable, Union
from .base import Middleware
from ..utils import maybe_await


class ErrorHandlingMiddleware(Middleware):
    """Classify errors and route terminal failures to a dead-letter handler."""

    def __init__(
        self,
        dead_letter_handler: Optional[Callable[..., Union[None, Awaitable[None]]]] = None,
        error_classifier: Optional[Callable[[Exception], str]] = None,
    ):
        """Initialize error handling middleware.

        Args:
            dead_letter_handler: Optional callable invoked on a terminal failure
                (sync or async): ``(payload, record, error)``.
            error_classifier: Optional ``(exc) -> "permanent" | "temporary"``.
        """
        super().__init__()
        self.dead_letter_handler = dead_letter_handler
        self.error_classifier = error_classifier or self._default_error_classifier

    def _default_error_classifier(self, exception: Exception) -> str:
        permanent = {
            "ValidationError", "InvalidMessage", "TypeError",
            "ValueError", "KeyError", "AttributeError",
        }
        return "permanent" if type(exception).__name__ in permanent else "temporary"

    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        ctx["error_history"] = []

    async def after(
        self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]
    ) -> None:
        if error is None:
            return
        msg_id = record.get("messageId", "UNKNOWN")
        error_type = self.error_classifier(error)
        ctx.setdefault("error_history", []).append(
            {"error": str(error), "error_type": error_type, "timestamp": time.time()}
        )
        self._log(
            "error", "Record failed", msg_id=msg_id, error_type=error_type, error=str(error)
        )
        if self.dead_letter_handler:
            try:
                await maybe_await(self.dead_letter_handler(payload, record, error))
            except Exception as dlq_error:
                self._log(
                    "error", "Dead letter handler failed", msg_id=msg_id, dlq_error=str(dlq_error)
                )


class DeadLetterQueueMiddleware(Middleware):
    """Route failed (or timed-out) messages to a dead-letter handler.

    Builds a structured DLQ record and hands it to ``dlq_handler`` (sync or
    async). Optionally flags messages that exceed ``max_processing_time``.
    """

    def __init__(
        self,
        dlq_handler: Optional[Callable[..., Union[None, Awaitable[None]]]] = None,
        max_processing_time: Optional[float] = None,
        include_context: bool = True,
    ):
        """Initialize dead letter queue middleware.

        Args:
            dlq_handler: Handler for dead-letter messages (defaults to a logger).
            max_processing_time: If set, flag messages exceeding this many seconds.
            include_context: Whether to include processing context in DLQ records.
        """
        super().__init__()
        self.dlq_handler = dlq_handler or self._default_dlq_handler
        self.max_processing_time = max_processing_time
        self.include_context = include_context

    async def _default_dlq_handler(
        self, payload: dict, record: dict, error: Exception, ctx: dict
    ) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        dlq_record = {
            "timestamp": int(time.time()),
            "message_id": msg_id,
            "original_payload": payload,
            "error": str(error),
            "error_type": type(error).__name__,
            "processing_attempts": int(
                record.get("attributes", {}).get("ApproximateReceiveCount", "1")
            ),
        }
        if self.include_context:
            dlq_record["context"] = {
                "error_history": ctx.get("error_history", []),
                "processing_time": ctx.get("duration_ms"),
                "queue_type": ctx.get("queueType"),
            }
        self._log("info", "Message sent to dead letter queue", msg_id=msg_id, dlq_record=dlq_record)

    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        if self.max_processing_time:
            ctx["dlq_start_time"] = time.time()

    async def after(
        self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]
    ) -> None:
        msg_id = record.get("messageId", "UNKNOWN")

        if self.max_processing_time:
            processing_time = time.time() - ctx.get("dlq_start_time", 0)
            if processing_time > self.max_processing_time:
                self._log(
                    "error", "Processing timeout exceeded", msg_id=msg_id,
                    processing_time=processing_time, max_time=self.max_processing_time,
                )
                timeout_error = ProcessingTimeoutError(
                    f"Processing exceeded {self.max_processing_time}s"
                )
                await maybe_await(self.dlq_handler(payload, record, timeout_error, ctx))
                return

        if error:
            self._log(
                "info", "Sending to DLQ due to error", msg_id=msg_id,
                error_type=type(error).__name__,
            )
            await maybe_await(self.dlq_handler(payload, record, error, ctx))


class ProcessingTimeoutError(Exception):
    """Exception raised when message processing exceeds its timeout limit."""
    pass
