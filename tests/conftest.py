"""Shared pytest config.

Two opt-in test tiers are skipped by default so the fast unit suite stays
dependency-free:

    pytest                       # fast unit suite only
    pytest --run-integration     # + Docker RIE integration tests
    pytest --run-aws             # + tests against real AWS (needs creds)
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
    parser.addoption(
        "--run-aws",
        action="store_true",
        default=False,
        help="run tests against REAL AWS (creates/deletes real resources; needs credentials)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: Docker-based integration test (Lambda RIE / SQS)"
    )
    config.addinivalue_line(
        "markers", "aws: hits real AWS (creates/deletes real resources)"
    )
    config.addinivalue_line(
        "markers", "slow: slow/timing-sensitive real-AWS test (still runs under --run-aws)"
    )


def pytest_collection_modifyitems(config, items):
    run_integration = config.getoption("--run-integration") or os.getenv("RUN_INTEGRATION")
    run_aws = config.getoption("--run-aws") or os.getenv("RUN_AWS")
    for item in items:
        if "integration" in item.keywords and not run_integration:
            item.add_marker(pytest.mark.skip(reason="needs --run-integration (or RUN_INTEGRATION=1)"))
        if "aws" in item.keywords and not run_aws:
            item.add_marker(pytest.mark.skip(reason="needs --run-aws (or RUN_AWS=1)"))
