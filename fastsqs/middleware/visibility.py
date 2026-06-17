"""Visibility timeout monitoring and processing time tracking middleware."""

from __future__ import annotations

import time
import asyncio
from typing import Any, Awaitable, Optional, Callable, Union
from .base import Middleware
from ..utils import maybe_await


class VisibilityTimeoutMonitor(Middleware):
    """Middleware that monitors SQS visibility timeout during message processing.
    
    Tracks processing time and warns when approaching visibility timeout,
    with optional callbacks for timeout extension and warnings.
    """
    
    def __init__(
        self,
        default_visibility_timeout: float = 30.0,
        warning_threshold: float = 0.8,
        extend_timeout_callback: Optional[Callable[..., Union[None, Awaitable[None]]]] = None,
        timeout_warning_callback: Optional[Callable[..., Union[None, Awaitable[None]]]] = None
    ):
        """Initialize visibility timeout monitor.
        
        Args:
            default_visibility_timeout: Default SQS visibility timeout in seconds
            warning_threshold: Fraction of timeout at which to warn (0.8 = 80%)
            extend_timeout_callback: Callback to extend visibility timeout
            timeout_warning_callback: Callback for timeout warnings
        """
        self.default_visibility_timeout = default_visibility_timeout
        self.warning_threshold = warning_threshold
        self.extend_timeout_callback = extend_timeout_callback
        self.timeout_warning_callback = timeout_warning_callback or self._default_warning_callback
    
    def _default_warning_callback(self, payload: dict, record: dict, elapsed: float, timeout: float) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        remaining = timeout - elapsed
        self._log("warning", "Message approaching timeout", 
                 msg_id=msg_id, elapsed=elapsed, remaining=remaining, 
                 usage_percent=(elapsed/timeout)*100)
    
    def _extract_visibility_timeout(self, record: dict) -> float:
        attributes = record.get("attributes", {})
        
        if "VisibilityTimeout" in attributes:
            return float(attributes["VisibilityTimeout"])
        
        receive_count = int(attributes.get("ApproximateReceiveCount", "1"))
        if receive_count > 1:
            return self.default_visibility_timeout * 1.5
        
        return self.default_visibility_timeout
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        visibility_timeout = self._extract_visibility_timeout(record)
        warning_time = visibility_timeout * self.warning_threshold
        
        self._log("info", "Starting visibility monitoring", 
                 msg_id=msg_id, timeout=visibility_timeout, warning_at=warning_time)
        
        ctx["visibility_timeout"] = visibility_timeout
        ctx["visibility_warning_time"] = warning_time
        ctx["visibility_start_time"] = time.time()
        ctx["visibility_warned"] = False
        
        ctx["visibility_monitor_task"] = asyncio.create_task(
            self._monitor_visibility_timeout(payload, record, ctx)
        )
    
    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        monitor_task = ctx.get("visibility_monitor_task")
        
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                self._log("debug", "Monitor task cancelled", msg_id=msg_id)
        
        start_time = ctx.get("visibility_start_time")
        if start_time:
            total_time = time.time() - start_time
            visibility_timeout = ctx.get("visibility_timeout", self.default_visibility_timeout)
            usage_percent = (total_time / visibility_timeout) * 100
            
            ctx["visibility_timeout_usage"] = total_time / visibility_timeout
            
            self._log("info", "Processing completed", 
                     msg_id=msg_id, total_time=total_time, 
                     timeout=visibility_timeout, usage_percent=usage_percent)
            
            if total_time > visibility_timeout:
                self._log("error", "Message exceeded visibility timeout!", 
                         msg_id=msg_id, processing_time=total_time, timeout=visibility_timeout)
    
    async def _monitor_visibility_timeout(self, payload: dict, record: dict, ctx: dict) -> None:
        try:
            start_time = ctx["visibility_start_time"]
            warning_time = ctx["visibility_warning_time"]
            visibility_timeout = ctx["visibility_timeout"]
            
            while True:
                await asyncio.sleep(1.0)  # Check every second
                
                elapsed = time.time() - start_time
                
                if not ctx["visibility_warned"] and elapsed >= warning_time:
                    ctx["visibility_warned"] = True
                    await maybe_await(self.timeout_warning_callback(payload, record, elapsed, visibility_timeout))
                    
                    if self.extend_timeout_callback:
                        try:
                            await maybe_await(self.extend_timeout_callback(payload, record, elapsed))
                        except Exception as e:
                            self._log("warning", "Failed to extend timeout", error=str(e))
                
                if elapsed > visibility_timeout:
                    break
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log("error", "Monitor error", error=str(e))


class ProcessingTimeMiddleware(Middleware):
    """Middleware for detailed processing time metrics and slow processing detection.
    
    Collects comprehensive timing metrics and warns about slow processing,
    with optional metrics callback for external monitoring systems.
    """
    
    def __init__(
        self,
        slow_processing_threshold: float = 10.0,
        store_detailed_metrics: bool = True,
        metrics_callback: Optional[Callable[..., Union[None, Awaitable[None]]]] = None
    ):
        """Initialize processing time middleware.
        
        Args:
            slow_processing_threshold: Threshold in seconds for slow processing warnings
            store_detailed_metrics: Whether to store detailed metrics in context
            metrics_callback: Optional callback for metrics export
        """
        self.slow_processing_threshold = slow_processing_threshold
        self.store_detailed_metrics = store_detailed_metrics
        self.metrics_callback = metrics_callback
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        ctx["processing_start_time"] = time.time()
        ctx["processing_start_time_ns"] = time.perf_counter_ns()
        
        if self.store_detailed_metrics:
            ctx["processing_metrics"] = {
                "start_timestamp": ctx["processing_start_time"],
                "message_id": record.get("messageId"),
                "message_size": len(record.get("body", "")),
                "queue_type": ctx.get("queueType"),
                "receive_count": int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
            }
    
    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        end_time = time.time()
        end_time_ns = time.perf_counter_ns()
        
        start_time = ctx.get("processing_start_time", end_time)
        start_time_ns = ctx.get("processing_start_time_ns", end_time_ns)
        
        duration_seconds = end_time - start_time
        duration_ms = (end_time_ns - start_time_ns) / 1_000_000
        
        ctx["processing_duration_seconds"] = duration_seconds
        ctx["processing_duration_ms"] = duration_ms
        
        if duration_seconds > self.slow_processing_threshold:
            msg_id = record.get("messageId", "UNKNOWN")
            self._log("warning", "Slow processing detected", 
                     msg_id=msg_id, duration=duration_seconds)
        
        if self.store_detailed_metrics:
            metrics = ctx.get("processing_metrics", {})
            metrics.update({
                "end_timestamp": end_time,
                "duration_seconds": duration_seconds,
                "duration_ms": duration_ms,
                "success": error is None,
                "error_type": type(error).__name__ if error else None,
                "visibility_timeout_usage": ctx.get("visibility_timeout_usage", 0.0)
            })
            
            if self.metrics_callback:
                try:
                    await maybe_await(self.metrics_callback(metrics))
                except Exception as e:
                    self._log("error", "Metrics callback error", error=str(e))
class QueueMetricsMiddleware(Middleware):
    """Middleware for aggregating and emitting queue-level metrics.
    
    Collects metrics across messages in time windows and emits
    aggregated statistics for monitoring and alerting.
    """
    
    def __init__(
        self,
        metrics_aggregation_window: float = 60.0,
        emit_metrics_callback: Optional[Callable[..., Union[None, Awaitable[None]]]] = None
    ):
        """Initialize queue metrics middleware.
        
        Args:
            metrics_aggregation_window: Time window in seconds for metrics aggregation
            emit_metrics_callback: Callback for emitting aggregated metrics
        """
        self.metrics_aggregation_window = metrics_aggregation_window
        self.emit_metrics_callback = emit_metrics_callback or self._default_emit_metrics
        
        self._metrics = {
            "messages_processed": 0,
            "messages_failed": 0,
            "total_processing_time": 0.0,
            "max_processing_time": 0.0,
            "min_processing_time": float("inf"),
            "visibility_timeout_violations": 0,
            "last_reset": time.time()
        }
    
    def _default_emit_metrics(self, metrics: dict) -> None:
        self._log("info", "Queue metrics", **metrics)
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        ctx["metrics_start_time"] = time.time()
    
    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        processing_time = ctx.get("processing_duration_seconds", 0.0)
        
        self._metrics["messages_processed"] += 1
        if error:
            self._metrics["messages_failed"] += 1
        
        self._metrics["total_processing_time"] += processing_time
        self._metrics["max_processing_time"] = max(self._metrics["max_processing_time"], processing_time)
        self._metrics["min_processing_time"] = min(self._metrics["min_processing_time"], processing_time)
        
        if ctx.get("visibility_timeout_usage", 0.0) > 1.0:
            self._metrics["visibility_timeout_violations"] += 1
        
        current_time = time.time()
        if current_time - self._metrics["last_reset"] >= self.metrics_aggregation_window:
            await self._emit_aggregated_metrics()
    
    async def _emit_aggregated_metrics(self) -> None:
        if self._metrics["messages_processed"] > 0:
            avg_processing_time = self._metrics["total_processing_time"] / self._metrics["messages_processed"]
            success_rate = (self._metrics["messages_processed"] - self._metrics["messages_failed"]) / self._metrics["messages_processed"]
            
            aggregated_metrics = {
                "timestamp": int(time.time()),
                "window_duration": self.metrics_aggregation_window,
                "messages_processed": self._metrics["messages_processed"],
                "messages_failed": self._metrics["messages_failed"],
                "success_rate": success_rate,
                "avg_processing_time": avg_processing_time,
                "max_processing_time": self._metrics["max_processing_time"],
                "min_processing_time": self._metrics["min_processing_time"] if self._metrics["min_processing_time"] != float("inf") else 0.0,
                "visibility_timeout_violations": self._metrics["visibility_timeout_violations"]
            }
            
            await maybe_await(self.emit_metrics_callback(aggregated_metrics))
        
        self._metrics = {
            "messages_processed": 0,
            "messages_failed": 0,
            "total_processing_time": 0.0,
            "max_processing_time": 0.0,
            "min_processing_time": float("inf"),
            "visibility_timeout_violations": 0,
            "last_reset": time.time()
        }
