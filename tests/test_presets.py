from fastsqs import MiddlewarePreset


def _names(middlewares):
    return [type(m).__name__ for m in middlewares]


def test_minimal_preset():
    assert _names(MiddlewarePreset.minimal()) == ["LoggingMiddleware", "TimingMsMiddleware"]


def test_production_preset_composition():
    assert _names(MiddlewarePreset.production()) == [
        "LoggingMiddleware",
        "TimingMsMiddleware",
        "ErrorHandlingMiddleware",
        "DeadLetterQueueMiddleware",
    ]


def test_development_preset_composition():
    assert _names(MiddlewarePreset.development()) == [
        "LoggingMiddleware",
        "TimingMsMiddleware",
        "ErrorHandlingMiddleware",
    ]
