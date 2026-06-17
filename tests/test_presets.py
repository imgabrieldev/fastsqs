from fastsqs import MiddlewarePreset


def _names(middlewares):
    return [type(m).__name__ for m in middlewares]


def test_minimal_preset():
    assert _names(MiddlewarePreset.minimal()) == ["LoggingMiddleware", "TimingMsMiddleware"]


def test_production_preset_has_no_idempotency():
    names = _names(MiddlewarePreset.production())
    assert "IdempotencyMiddleware" not in names
    assert names == [
        "LoggingMiddleware",
        "TimingMsMiddleware",
        "ErrorHandlingMiddleware",
        "VisibilityTimeoutMonitor",
        "ParallelizationMiddleware",
    ]


def test_development_preset_has_no_idempotency():
    names = _names(MiddlewarePreset.development())
    assert "IdempotencyMiddleware" not in names
    assert "LoggingMiddleware" in names
