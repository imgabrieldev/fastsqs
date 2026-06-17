"""Logging middleware for structured message processing logs."""

import json
import time
import traceback
from typing import Callable, List, Optional

from .base import Middleware
from ..utils import shallow_mask


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
        mask_fields: Optional[List[str]] = None,
        verbose: bool = True,
    ):
        """Initialize logging middleware.

        Args:
            logger: Optional custom logger function (defaults to JSON print to stdout)
            level: Default log level
            include_payload: Whether to include message payload in logs
            include_record: Whether to include SQS record in logs
            include_context: Whether to include Lambda context in logs
            mask_fields: List of fields to mask in payloads
            verbose: Enable verbose logging with additional context
        """
        self.level = level
        self.include_payload = include_payload
        self.include_record = include_record
        self.include_context = include_context
        self.mask_fields = mask_fields or []
        self.verbose = verbose

        def _default_logger(obj: dict) -> None:
            print(json.dumps(obj, ensure_ascii=False))

        self.logger: Callable[[dict], None] = logger or _default_logger

    def log(self, level: str, message: str, **data) -> None:
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
            "route_path": ctx.get("route_path", []),
            "message_type": payload.get("type") if isinstance(payload, dict) else None,
            "queue_type": ctx.get("queueType"),
            "context_aws_request_id": getattr(context, 'aws_request_id', None),
            "context_function_name": getattr(context, 'function_name', None),
            "context_memory_limit": getattr(context, 'memory_limit_in_mb', None),
        }

        if self.include_payload:
            entry["payload"] = shallow_mask(payload, self.mask_fields)
        if self.include_record:
            entry["record"] = record
        if self.include_context:
            entry["context_repr"] = repr(context)

        if self.verbose:
            entry["ctx_keys"] = list(ctx.keys())

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

        entry["processing_results"] = {
            "duration_ms": ctx.get("duration_ms"),
            "route_path": ctx.get("route_path", []),
            "message_type": ctx.get("message_type"),
            "handler_result_type": type(ctx.get("handler_result")).__name__ if ctx.get("handler_result") is not None else None,
        }

        if error:
            entry["error_details"] = {
                "error_type": type(error).__name__,
                "error_message": str(error),
                "error_repr": repr(error),
                "traceback": traceback.format_exc(),
                "error_history": ctx.get("error_history", []),
            }

        if self.verbose:
            entry["context_summary"] = {
                "all_ctx_keys": list(ctx.keys()),
                "visibility_timeout": ctx.get("visibility_timeout"),
                "visibility_warned": ctx.get("visibility_warned"),
                "concurrency_stats": ctx.get("concurrency_stats"),
            }

        if self.include_payload:
            entry["payload"] = shallow_mask(payload, self.mask_fields)
        if self.include_record:
            entry["record"] = record
        if self.include_context:
            entry["context_repr"] = repr(context)

        self.logger(entry)
