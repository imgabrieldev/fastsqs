from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional

if TYPE_CHECKING:
    from ..types import Context

_logger = logging.getLogger("fastsqs")


class Middleware:
    """Base class for FastSQS middleware.

    Middleware can hook into message processing before and after handler
    execution. Subclasses override :meth:`before` and/or :meth:`after`.
    """

    async def before(
        self, payload: dict, record: dict, context: Any, ctx: "Context"
    ) -> None:
        """Hook called before handler execution.

        Args:
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Per-record processing Context

        Raising from ``before`` aborts processing for this record (the handler
        does not run); already-entered middlewares are still unwound via ``after``.
        """
        return None

    async def after(
        self,
        payload: dict,
        record: dict,
        context: Any,
        ctx: "Context",
        error: Optional[Exception],
    ) -> None:
        """Hook called after handler execution.

        Args:
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Per-record processing Context
            error: Exception if the handler (or a ``before`` hook) failed, else None
        """
        return None


def call_middleware_hook(mw: Middleware, hook: str, *args) -> Awaitable[None]:
    """Call a middleware hook method safely.

    Args:
        mw: Middleware instance
        hook: Hook method name ('before' or 'after')
        *args: Arguments to pass to hook

    Returns:
        Awaitable that resolves to None
    """
    fn = getattr(mw, hook, None)
    if fn is None:
        async def _noop():
            return None
        return _noop()
    res = fn(*args)
    if inspect.isawaitable(res):
        return res

    async def _wrap():
        return None

    return _wrap()


async def _run_middleware_stack(
    mws: List[Middleware],
    payload: dict,
    record: dict,
    context: Any,
    ctx: "Context",
    call_inner: Callable[[], Awaitable[Any]],
) -> Any:
    """Run the before -> inner -> after middleware stack with balanced cleanup.

    Only middlewares whose ``before`` completed are unwound (``after`` runs in
    reverse) — even if a later ``before`` or the inner call raises. This keeps
    enter/exit symmetric so resources acquired in ``before`` (e.g. a concurrency
    slot, a monitor task) are always released. After-hooks are isolated: one
    raising never aborts the others nor masks the original error, which is
    re-raised after cleanup.
    """
    entered: List[Middleware] = []
    err: Optional[Exception] = None
    try:
        for mw in mws:
            await call_middleware_hook(mw, "before", payload, record, context, ctx)
            entered.append(mw)
        return await call_inner()
    except Exception as e:
        err = e
        raise
    finally:
        for mw in reversed(entered):
            try:
                await call_middleware_hook(
                    mw, "after", payload, record, context, ctx, err
                )
            except Exception as hook_error:
                _logger.error("after middleware hook raised: %s", hook_error)
