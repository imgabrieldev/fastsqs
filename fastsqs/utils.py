"""Utility functions for FastSQS."""

from __future__ import annotations

import inspect
from typing import Any, Dict, List

from fast_depends import inject as _fd_inject

from .types import Handler


def uses_depends(fn: Handler) -> bool:
    """True if ``fn`` declares any fast-depends ``Depends(...)`` parameter."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (ValueError, TypeError):
        return False
    return any(type(p.default).__name__ == "Dependant" for p in params)


def maybe_inject(fn: Handler) -> Handler:
    """Passively wrap a handler with fast-depends ``inject()`` IFF it declares
    ``Depends(...)`` params — so users get DI without writing ``@inject``.
    Handlers with no dependencies are returned untouched (zero behaviour change).
    """
    if getattr(fn, "_fastsqs_injected", False):
        return fn
    if not uses_depends(fn):
        return fn
    wrapped = _fd_inject(fn)
    setattr(wrapped, "_fastsqs_injected", True)
    return wrapped


def group_records_by_message_group(
    records: List[dict]
) -> Dict[str, List[dict]]:
    """Group SQS records by message group ID for FIFO processing.

    Args:
        records: List of SQS record dictionaries

    Returns:
        Dictionary mapping message group IDs to lists of records
    """
    groups: Dict[str, List[dict]] = {}

    for record in records:
        attributes = record.get("attributes", {})
        # Real SQS events expose system attributes in PascalCase ("MessageGroupId");
        # the record-level keys (messageId, body, ...) are camelCase, but this
        # sub-map is not. Reading the wrong case silently collapses every record
        # into one group, breaking FIFO isolation.
        message_group_id = attributes.get("MessageGroupId", "default")

        if message_group_id not in groups:
            groups[message_group_id] = []
        groups[message_group_id].append(record)

    return groups


def select_kwargs(fn: Handler, **candidates) -> Dict[str, Any]:
    """Select keyword arguments that match function signature.

    Args:
        fn: Handler function to inspect
        **candidates: Candidate keyword arguments

    Returns:
        Dictionary of matching keyword arguments
    """
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return candidates
    accepted = {
        p.name for p in sig.parameters.values()
        if p.kind in (p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD)
    }
    return {k: v for k, v in candidates.items() if k in accepted}


async def invoke_handler(fn: Handler, **kwargs) -> Any:
    # An injected handler (fast-depends) binds the params it declares and
    # resolves its Depends(); it ignores the extra framework kwargs. A plain
    # handler gets the name-matched subset via select_kwargs.
    if getattr(fn, "_fastsqs_injected", False):
        kw = kwargs
    else:
        kw = select_kwargs(fn, **kwargs)

    result = fn(**kw)
    if inspect.isawaitable(result):
        result = await result

    return result


def is_sqs_event(event: Any) -> bool:
    """True if ``event`` is an SQS batch this app processes.

    Two shapes count: a bare ``list`` of records (an EventBridge Pipes target
    receives the batch as a list) or a ``dict`` carrying a ``"Records"`` key (the
    Lambda SQS event source mapping). Anything else — e.g. an API Gateway proxy
    event — returns ``False``, so a multiplexed Lambda can route by shape::

        if is_sqs_event(event):
            return app.handler(event, context)
        return http_handler(event, context)
    """
    if isinstance(event, list):
        return True
    return isinstance(event, dict) and "Records" in event


async def maybe_await(value: Any) -> Any:
    """Await ``value`` if it is awaitable, else return it as-is.

    Lets configurable callbacks be either sync or async without the caller
    knowing which.
    """
    if inspect.isawaitable(value):
        return await value
    return value
