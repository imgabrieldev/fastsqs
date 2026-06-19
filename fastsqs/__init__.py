"""FastSQS - A FastAPI-style AWS SQS message handling framework.

A FastAPI-inspired interface for handling AWS SQS messages on Lambda: pydantic
routing, a middleware system, dependency injection, and native partial batch
failure (standard + FIFO).
"""

from fast_depends import Depends

from .types import QueueType, Context, State, FifoInfo
from .exceptions import (
    FastSQSError,
    RouteNotFoundError,
    InvalidMessageError,
    BatchFailedError,
)
from .app import FastSQS
from .routing import SQSRouter
from .utils import is_sqs_event
from .middleware import (
    Middleware,
    TimingMiddleware,
    LoggingMiddleware,
)
from .events import SQSEvent

__all__ = [
    "FastSQS",
    "SQSRouter",
    "SQSEvent",
    "is_sqs_event",
    "Context",
    "State",
    "FifoInfo",
    "QueueType",
    "Depends",
    "Middleware",
    "TimingMiddleware",
    "LoggingMiddleware",
    "FastSQSError",
    "RouteNotFoundError",
    "InvalidMessageError",
    "BatchFailedError",
]
