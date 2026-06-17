"""Exception hierarchy for FastSQS.

All fastsqs exceptions derive from :class:`FastSQSError`, so callers can catch
any framework error with a single ``except FastSQSError``.
"""

from typing import List, Optional


class FastSQSError(Exception):
    """Base class for all FastSQS errors."""


class RouteNotFoundError(FastSQSError):
    """Raised when no route handler matches a message and no default handler is set."""


class InvalidMessageError(FastSQSError):
    """Raised when a message body has an invalid format or content."""


class BatchFailedError(FastSQSError):
    """Raised when ``partial_batch_failure`` is False and at least one record
    failed: the whole batch is failed (the Lambda invocation raises) so SQS
    redelivers every message, instead of silently reporting no failures.

    The failed item identifiers are available on :attr:`failures`.
    """

    def __init__(self, failures: List[str], message: Optional[str] = None) -> None:
        self.failures = failures
        super().__init__(
            message
            or f"{len(failures)} record(s) failed and partial_batch_failure is "
            "False; failing the whole batch"
        )
