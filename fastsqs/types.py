"""Type definitions for FastSQS."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, List, TypedDict, Union, TypeVar
from enum import Enum
from pydantic import BaseModel


class QueueType(Enum):
    """Enumeration for SQS queue types."""
    STANDARD = "standard"
    FIFO = "fifo"


Handler = Callable[..., Union[None, Awaitable[None], Any]]
"""Type alias for message handler functions."""

RouteValue = Union[str, int]
"""Type alias for route values."""

T = TypeVar('T', bound=BaseModel)
"""Type variable bound to Pydantic BaseModel."""


ProcessingContext = TypedDict(
    "ProcessingContext",
    {
        "messageId": str,
        "record": dict,
        "context": Any,
        "route_path": List[str],
        "queueType": str,
        "fifoInfo": dict,
        "message_type": str,
        "handler_result": Any,
        "retry_attempt": int,
        "error_history": List[Any],
        "should_retry": bool,
        "retry_delay": float,
        "dlq_start_time": float,
        "concurrency_stats": dict,
        "concurrency_wait_time": float,
        "visibility_timeout": float,
        "visibility_warning_time": float,
        "visibility_start_time": float,
        "visibility_warned": bool,
        "visibility_timeout_usage": float,
        "visibility_monitor_task": Any,
        "duration_ms": float,
        "processing_start_time": float,
        "processing_start_time_ns": int,
        "processing_duration_seconds": float,
        "processing_duration_ms": float,
        "processing_metrics": dict,
        "metrics_start_time": float,
        "_parallelization_middleware": Any,
    },
    total=False,
)
"""Per-message processing context shared across middleware + handlers.
All keys optional (total=False) — documents the contract, not enforced."""
