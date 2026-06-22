"""Logging middleware for structured message processing logs."""

import json
import time
import traceback
from typing import Any, Callable, Optional

from .base import Middleware


class LoggingMiddleware(Middleware):
    """Middleware that provides structured logging for message processing.

    Logs detailed information about message processing including payloads,
    timing, errors, and processing context with field masking support.
    Defaults to JSON-line logging on stdout (CloudWatch-friendly), no
    external dependencies.
    """

    def __init__(
        self,
        logger: Optional[Callable[[dict], None]] = None,
        level: str = "INFO",
        include_payload: bool = True,
        include_record: bool = False,
        include_context: bool = False,
        verbose: bool = True,
    ):
        """Initialize logging middleware.

        Args:
            logger: Optional custom logger function (defaults to JSON print to stdout)
            level: Default log level
            include_payload: Whether to include message payload in logs
            include_record: Whether to include SQS record in logs
            include_context: Whether to include Lambda context in logs
            verbose: Enable verbose logging with additional context
        """
        self.level = level
        self.include_payload = include_payload
        self.include_record = include_record
        self.include_context = include_context
        self.verbose = verbose

        def _default_logger(obj: dict) -> None:
            print(json.dumps(obj, ensure_ascii=False))

        self.logger: Callable[[dict], None] = logger or _default_logger

    def log(self, level: str, message: str, **data: Any) -> None:
        """Log a message with structured data.

        Args:
            level: Log level
            message: Log message
            **data: Additional structured data
        """
        entry = {
            "ts": time.time(),
            "lvl": level.upper(),
            "message": message,
            **data
        }
        self.logger(entry)

    async def before(self, payload, record, context, ctx):
        """Log message processing start with context information."""
        entry = {
            "ts": time.time(),
            "lvl": self.level,
            "stage": "before_processing",
            "msg_id": record.get("messageId"),
            "middleware": "LoggingMiddleware",
        }

        entry["message_info"] = {
            "source": record.get("eventSource"),
            "source_arn": record.get("eventSourceARN"),
            "aws_region": record.get("awsRegion"),
            "approximate_receive_count": record.get("attributes", {}).get("ApproximateReceiveCount"),
            "sent_timestamp": record.get("attributes", {}).get("SentTimestamp"),
        }

        entry["processing_info"] = {
            "route_path": ctx.route_path,
            "message_type": payload.get("type") if isinstance(payload, dict) else None,
            "queue_type": ctx.queue_type.value,
            "context_aws_request_id": getattr(context, 'aws_request_id', None),
            "context_function_name": getattr(context, 'function_name', None),
            "context_memory_limit": getattr(context, 'memory_limit_in_mb', None),
        }

        if self.include_payload:
            entry["payload"] = payload
        if self.include_record:
            entry["record"] = record
        if self.include_context:
            entry["context_repr"] = repr(context)

        if self.verbose:
            entry["state_keys"] = list(ctx.state)

        self.logger(entry)

    async def after(self, payload, record, context, ctx, error):
        """Log message processing completion with results and errors."""
        entry = {
            "ts": time.time(),
            "lvl": "ERROR" if error else self.level,
            "stage": "after_processing",
            "msg_id": record.get("messageId"),
            "middleware": "LoggingMiddleware",
        }

        handler_result = ctx.handler_result
        entry["processing_results"] = {
            "duration_ms": ctx.state.get("duration_ms"),
            "route_path": ctx.route_path,
            "message_type": ctx.message_type,
            "handler_result_type": type(handler_result).__name__ if handler_result is not None else None,
        }

        if error:
            entry["error_details"] = {
                "error_type": type(error).__name__,
                "error_message": str(error),
                "error_repr": repr(error),
                "traceback": traceback.format_exc(),
            }

        if self.verbose:
            entry["state_keys"] = list(ctx.state)

        if self.include_payload:
            entry["payload"] = payload
        if self.include_record:
            entry["record"] = record
        if self.include_context:
            entry["context_repr"] = repr(context)

        self.logger(entry)
