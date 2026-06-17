"""FastSQS - A FastAPI-style AWS SQS message handling framework.

This package provides a modern, FastAPI-inspired interface for handling
AWS SQS messages with support for routing, middleware, validation, and more.
"""

from .types import QueueType, Handler, RouteValue
from .exceptions import RouteNotFound, InvalidMessage, BatchFailedError
from .app import FastSQS
from .routing import SQSRouter, RouteEntry
from .middleware import (
    Middleware,
    TimingMsMiddleware,
    LoggingMiddleware,
    ErrorHandlingMiddleware,
    DeadLetterQueueMiddleware,
)
from .events import SQSEvent
from .presets import MiddlewarePreset

__all__ = [
    "QueueType",
    "Handler",
    "RouteValue",
    "RouteNotFound",
    "InvalidMessage",
    "BatchFailedError",
    "FastSQS",
    "SQSRouter",
    "RouteEntry",
    "Middleware",
    "TimingMsMiddleware",
    "LoggingMiddleware",
    "SQSEvent",
    "ErrorHandlingMiddleware",
    "DeadLetterQueueMiddleware",
    "MiddlewarePreset",
]
