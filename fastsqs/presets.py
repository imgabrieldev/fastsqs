"""Predefined middleware presets for common use cases."""

from __future__ import annotations

from typing import List

from .middleware import (
    Middleware,
    ErrorHandlingMiddleware, CircuitBreaker,
    VisibilityTimeoutMonitor, ParallelizationMiddleware, ParallelizationConfig,
    LoggingMiddleware, TimingMsMiddleware
)


class MiddlewarePreset:
    """Factory class for creating predefined middleware configurations."""

    @staticmethod
    def production(
        max_concurrent: int = 10,
        visibility_timeout: float = 30.0,
        circuit_breaker_threshold: int = 5
    ) -> List[Middleware]:
        """Create production-ready middleware configuration."""
        middlewares: List[Middleware] = []

        middlewares.append(LoggingMiddleware(
            verbose=True,
            include_context=True,
            include_record=False
        ))
        middlewares.append(TimingMsMiddleware())

        circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=60.0
        )
        middlewares.append(ErrorHandlingMiddleware(
            circuit_breaker=circuit_breaker
        ))

        middlewares.append(VisibilityTimeoutMonitor(
            default_visibility_timeout=visibility_timeout,
            warning_threshold=0.8
        ))

        parallel_config = ParallelizationConfig(
            max_concurrent_messages=max_concurrent,
            use_thread_pool=True,
            thread_pool_size=min(32, max_concurrent)
        )
        middlewares.append(ParallelizationMiddleware(config=parallel_config))

        return middlewares

    @staticmethod
    def development(max_concurrent: int = 5) -> List[Middleware]:
        """Create development-friendly middleware configuration."""
        middlewares: List[Middleware] = []

        middlewares.append(LoggingMiddleware(
            verbose=True,
            include_context=True,
            include_record=True
        ))
        middlewares.append(TimingMsMiddleware())

        middlewares.append(ErrorHandlingMiddleware())

        middlewares.append(VisibilityTimeoutMonitor(
            default_visibility_timeout=30.0,
            warning_threshold=0.9
        ))

        parallel_config = ParallelizationConfig(
            max_concurrent_messages=max_concurrent,
            use_thread_pool=False
        )
        middlewares.append(ParallelizationMiddleware(config=parallel_config))

        return middlewares

    @staticmethod
    def minimal() -> List[Middleware]:
        """Create minimal middleware configuration."""
        return [
            LoggingMiddleware(),
            TimingMsMiddleware(),
        ]
