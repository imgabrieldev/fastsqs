"""Predefined middleware presets for common use cases."""

from __future__ import annotations

from typing import List

from .middleware import (
    Middleware,
    ErrorHandlingMiddleware,
    DeadLetterQueueMiddleware,
    LoggingMiddleware,
    TimingMsMiddleware,
)


class MiddlewarePreset:
    """Factory for predefined middleware stacks.

    Concurrency is configured on ``FastSQS`` itself (``max_concurrent_messages``);
    presets only assemble cross-cutting middleware.
    """

    @staticmethod
    def production() -> List[Middleware]:
        """Structured logging + timing + error classification + dead-letter routing."""
        return [
            LoggingMiddleware(verbose=True, include_context=True, include_record=False),
            TimingMsMiddleware(),
            ErrorHandlingMiddleware(),
            DeadLetterQueueMiddleware(),
        ]

    @staticmethod
    def development() -> List[Middleware]:
        """Verbose logging (with record) + timing + error classification."""
        return [
            LoggingMiddleware(verbose=True, include_context=True, include_record=True),
            TimingMsMiddleware(),
            ErrorHandlingMiddleware(),
        ]

    @staticmethod
    def minimal() -> List[Middleware]:
        """Logging + timing only."""
        return [
            LoggingMiddleware(),
            TimingMsMiddleware(),
        ]
