"""Parallelization and concurrency control middleware."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
from .base import Middleware


class ConcurrencyLimiter:
    """Limits concurrent message processing using semaphores.
    
    Provides backpressure control and statistics for monitoring
    concurrent processing loads.
    """
    
    def __init__(self, max_concurrent: int = 10):
        """Initialize concurrency limiter.
        
        Args:
            max_concurrent: Maximum number of concurrent operations
        """
        self.max_concurrent = max_concurrent
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.active_count = 0
        self.waiting_count = 0

    def _get_semaphore(self) -> asyncio.Semaphore:
        # Bind the semaphore lazily to the running loop: asyncio primitives bind
        # to the loop at creation pre-3.10, and a semaphore cannot be reused
        # across loops (each Lambda invocation runs a fresh asyncio.run()).
        loop = asyncio.get_running_loop()
        if self._semaphore is None or self._loop is not loop:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)
            self._loop = loop
            self.active_count = 0
        return self._semaphore

    async def acquire(self) -> None:
        semaphore = self._get_semaphore()
        self.waiting_count += 1
        try:
            await semaphore.acquire()
            self.active_count += 1
        finally:
            self.waiting_count -= 1

    def release(self) -> None:
        if self._semaphore is None:
            return
        self.active_count = max(0, self.active_count - 1)
        self._semaphore.release()
    
    @property
    def stats(self) -> Dict[str, int]:
        return {
            "max_concurrent": self.max_concurrent,
            "active_count": self.active_count,
            "waiting_count": self.waiting_count,
            "available_slots": self.max_concurrent - self.active_count
        }


class ResourcePool:
    """Thread pool resource management for CPU-bound operations.
    
    Provides controlled access to thread pool executor with
    monitoring and graceful shutdown capabilities.
    """
    
    def __init__(self, max_workers: int = 5):
        """Initialize resource pool.
        
        Args:
            max_workers: Maximum number of worker threads
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = 0
    
    async def run_in_executor(self, func: Callable, *args) -> Any:
        self.active_tasks += 1
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.executor, func, *args)
        finally:
            self.active_tasks = max(0, self.active_tasks - 1)
    
    def shutdown(self, wait: bool = True) -> None:
        self.executor.shutdown(wait=wait)
    
    @property
    def stats(self) -> Dict[str, int]:
        return {
            "max_workers": self.max_workers,
            "active_tasks": self.active_tasks
        }


class ParallelizationConfig:
    """Configuration for parallelization middleware behavior.
    
    Defines concurrency limits, thread pool usage, and batch processing
    settings for optimal message processing performance.
    """
    
    def __init__(
        self,
        max_concurrent_messages: int = 10,
        max_concurrent_per_group: int = 1,
        use_thread_pool: bool = False,
        thread_pool_size: Optional[int] = None,
        batch_processing: bool = False,
        batch_size: int = 5,
        batch_timeout: float = 1.0
    ):
        """Initialize parallelization configuration.
        
        Args:
            max_concurrent_messages: Maximum concurrent message processing
            max_concurrent_per_group: Maximum concurrent per FIFO group
            use_thread_pool: Whether to use thread pool for CPU-bound tasks
            thread_pool_size: Size of thread pool (None for default)
            batch_processing: Enable batch processing mode
            batch_size: Number of messages per batch
            batch_timeout: Timeout for incomplete batches in seconds
        """
        self.max_concurrent_messages = max_concurrent_messages
        self.max_concurrent_per_group = max_concurrent_per_group
        self.use_thread_pool = use_thread_pool
        self.thread_pool_size = thread_pool_size or min(32, max_concurrent_messages)
        self.batch_processing = batch_processing
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout


class ParallelizationMiddleware(Middleware):
    """Middleware for managing concurrent and parallel message processing.
    
    Provides concurrency control, resource pooling, batch processing,
    and thread pool execution for optimal performance.
    """
    
    def __init__(
        self,
        config: Optional[ParallelizationConfig] = None,
        resource_pools: Optional[Dict[str, ResourcePool]] = None,
        concurrency_limiter: Optional[ConcurrencyLimiter] = None
    ):
        """Initialize parallelization middleware.
        
        Args:
            config: Parallelization configuration
            resource_pools: Named resource pools for different operations
            concurrency_limiter: Custom concurrency limiter instance
        """
        super().__init__()
        self.config = config or ParallelizationConfig()
        self.resource_pools = resource_pools or {}
        self.concurrency_limiter = concurrency_limiter or ConcurrencyLimiter(
            self.config.max_concurrent_messages
        )
        
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        if self.config.use_thread_pool:
            self.thread_pool = ThreadPoolExecutor(
                max_workers=self.config.thread_pool_size,
                thread_name_prefix="fastsqs-worker"
            )
        
        self.pending_batches: Dict[str, List[Dict]] = {}
        self.batch_timers: Dict[str, asyncio.Task] = {}
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        start_time = time.time()
        
        self._log("debug", "Acquiring concurrency slot", 
                 msg_id=msg_id, current_stats=self.concurrency_limiter.stats)
        
        await self.concurrency_limiter.acquire()
        wait_time = time.time() - start_time
        
        self._log("debug", "Concurrency slot acquired", 
                 msg_id=msg_id, wait_time=wait_time, new_stats=self.concurrency_limiter.stats)
        
        ctx["concurrency_wait_time"] = wait_time
        ctx["concurrency_stats"] = self.concurrency_limiter.stats

        ctx["_parallelization_middleware"] = self
    
    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        msg_id = record.get("messageId", "UNKNOWN")

        self.concurrency_limiter.release()
        self._log("debug", "Concurrency slot released",
                 msg_id=msg_id, new_stats=self.concurrency_limiter.stats)

    async def run_in_thread_pool(self, func: Callable, *args, **kwargs) -> Any:
        if not self.thread_pool:
            raise RuntimeError("Thread pool not enabled")
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.thread_pool, func, *args, **kwargs)
    
    async def process_batch(self, batch_key: str, items: List[Dict], 
                          handler: Callable[[List[Dict]], Any]) -> Any:
        try:
            if asyncio.iscoroutinefunction(handler):
                return await handler(items)
            else:
                return handler(items)
        except Exception as e:
            raise BatchProcessingError(f"Batch processing failed for key '{batch_key}': {e}")
    
    async def add_to_batch(self, batch_key: str, item: Dict, 
                          batch_handler: Callable[[List[Dict]], Any]) -> None:
        if batch_key not in self.pending_batches:
            self.pending_batches[batch_key] = []
        
        self.pending_batches[batch_key].append(item)
        
        if len(self.pending_batches[batch_key]) >= self.config.batch_size:
            await self._process_ready_batch(batch_key, batch_handler)
        elif batch_key not in self.batch_timers:
            self.batch_timers[batch_key] = asyncio.create_task(
                self._batch_timeout_handler(batch_key, batch_handler)
            )
    
    async def _process_ready_batch(self, batch_key: str, handler: Callable[[List[Dict]], Any]) -> None:
        if batch_key in self.pending_batches and self.pending_batches[batch_key]:
            items = self.pending_batches[batch_key]
            del self.pending_batches[batch_key]
            
            if batch_key in self.batch_timers:
                self.batch_timers[batch_key].cancel()
                del self.batch_timers[batch_key]
            
            await self.process_batch(batch_key, items, handler)
    
    async def _batch_timeout_handler(self, batch_key: str, handler: Callable[[List[Dict]], Any]) -> None:
        try:
            await asyncio.sleep(self.config.batch_timeout)
            await self._process_ready_batch(batch_key, handler)
        except asyncio.CancelledError:
            pass
    
    async def cleanup(self) -> None:
        for pool in self.resource_pools.values():
            pool.shutdown()

        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        for timer in self.batch_timers.values():
            timer.cancel()
        
        self.batch_timers.clear()
        self.pending_batches.clear()


class LoadBalancingMiddleware(Middleware):
    """Middleware for load balancing across multiple handlers.
    
    Distributes messages across registered handlers using various
    strategies with health monitoring and automatic failover.
    """
    
    def __init__(
        self,
        strategy: str = "round_robin",
        health_check_interval: float = 30.0,
        weights: Optional[Dict[str, float]] = None
    ):
        """Initialize load balancing middleware.
        
        Args:
            strategy: Load balancing strategy (round_robin, least_busy, weighted)
            health_check_interval: Interval for health checks in seconds
            weights: Handler weights for weighted strategy
        """
        super().__init__()
        self.strategy = strategy
        self.health_check_interval = health_check_interval
        self.weights = weights or {}
        
        self.handler_stats: Dict[str, Dict] = {}
        self.current_index = 0
        self.last_health_check = 0.0
    
    def register_handler(self, handler_id: str, handler: Callable, weight: float = 1.0) -> None:
        self.handler_stats[handler_id] = {
            "handler": handler,
            "weight": weight,
            "active_requests": 0,
            "total_requests": 0,
            "total_errors": 0,
            "avg_response_time": 0.0,
            "healthy": True,
            "last_request_time": 0.0
        }
    
    def select_handler(self) -> Optional[tuple[str, Callable]]:
        healthy_handlers = [
            (handler_id, stats) for handler_id, stats in self.handler_stats.items()
            if stats["healthy"]
        ]
        
        if not healthy_handlers:
            return None
        
        if self.strategy == "round_robin":
            self.current_index = (self.current_index + 1) % len(healthy_handlers)
            handler_id, stats = healthy_handlers[self.current_index]
            return handler_id, stats["handler"]
        
        elif self.strategy == "least_busy":
            handler_id, stats = min(healthy_handlers, key=lambda x: x[1]["active_requests"])
            return handler_id, stats["handler"]
        
        elif self.strategy == "weighted":
            import random
            total_weight = sum(stats["weight"] for _, stats in healthy_handlers)
            if total_weight == 0:
                return None
            
            rand_val = random.uniform(0, total_weight)
            current_weight = 0
            
            for handler_id, stats in healthy_handlers:
                current_weight += stats["weight"]
                if rand_val <= current_weight:
                    return handler_id, stats["handler"]
        
        return None
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        selected = self.select_handler()
        if selected:
            handler_id, handler = selected
            ctx["selected_handler_id"] = handler_id
            ctx["selected_handler"] = handler
            ctx["handler_start_time"] = time.time()
            
            self.handler_stats[handler_id]["active_requests"] += 1
            self.handler_stats[handler_id]["total_requests"] += 1
            self.handler_stats[handler_id]["last_request_time"] = time.time()
    
    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        handler_id = ctx.get("selected_handler_id")
        if handler_id and handler_id in self.handler_stats:
            start_time = ctx.get("handler_start_time", time.time())
            response_time = time.time() - start_time
            
            stats = self.handler_stats[handler_id]
            stats["active_requests"] = max(0, stats["active_requests"] - 1)
            
            prev_avg = stats["avg_response_time"]
            total_requests = stats["total_requests"]
            stats["avg_response_time"] = ((prev_avg * (total_requests - 1)) + response_time) / total_requests
            
            if error:
                stats["total_errors"] += 1
                
                error_rate = stats["total_errors"] / stats["total_requests"]
                if error_rate > 0.5 and stats["total_requests"] > 10:
                    stats["healthy"] = False


class ResourcePoolExhaustedError(Exception):
    """Exception raised when resource pool is exhausted."""
    pass


class ResourceCreationError(Exception):
    """Exception raised when resource creation fails."""
    pass


class BatchProcessingError(Exception):
    """Exception raised when batch processing fails."""
    pass
