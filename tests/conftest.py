"""Shared pytest config.

Integration tests (Docker / Lambda RIE) are marked `integration` and are
SKIPPED by default so the fast unit suite stays Docker-free. Enable them with
`--run-integration` (or `RUN_INTEGRATION=1`):

    pytest                       # fast unit suite only
    pytest --run-integration     # + Docker RIE integration tests
"""

import os

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run Docker-based integration tests (Lambda RIE)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: Docker-based integration test (Lambda RIE / SQS)"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration") or os.getenv("RUN_INTEGRATION"):
        return
    skip = pytest.mark.skip(reason="needs --run-integration (or RUN_INTEGRATION=1)")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
