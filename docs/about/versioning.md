# Versioning policy

This page tells you what a fastsqs version number means, so you can pin a range and know which upgrades are safe.

```toml
# pyproject.toml — pin the major you tested against
[project]
dependencies = [
    "fastsqs>=1.1,<2",
]
```

fastsqs follows [Semantic Versioning](https://semver.org/). A release is `MAJOR.MINOR.PATCH`. The number you read tells you how a change relates to the public API.

- **MAJOR** changes when a release removes or breaks the public API.
- **MINOR** changes when a release adds public API without breaking what exists.
- **PATCH** changes when a release fixes behavior without changing the public API.

The contract took effect at `1.0.0`. The changelog records it: "SemVer applies from here." The `0.x` line carried no compatibility guarantee, and `1.0.0` broke the `0.x` surface deliberately to land a clean v1.

## What "the public API" covers

The public API is the names exported from `fastsqs.__all__` and `fastsqs.testing`, plus their documented parameters and behavior. A breaking change to any of these moves the major version.

```python
from fastsqs import FastSQS, SQSRouter, Context, Depends, QueueType

app = FastSQS()  # constructor signature is part of the public API
```

The `1.0.0` changelog shows the kind of change that forces a major bump: renamed config (`enable_partial_batch_failure` to `partial_batch_failure`), reparented exceptions (`RouteNotFound` to `RouteNotFoundError`), a typed `Context` replacing the old `dict`, and the minimum Python version moving from 3.8 to 3.10. Each of those breaks code that depended on the old surface, so each requires a new major.

!!! note
    Names prefixed with an underscore are internal. `_run_middleware_stack` and similar private helpers are not part of the public API and can change in any release.

## Additive minor releases

A minor release adds capability and leaves existing code working. `1.1.0` added `is_sqs_event(event)` and taught `FastSQS.handler` to accept a bare list of records (the [EventBridge Pipes](../guide/eventbridge-pipes.md) target shape). Code written against `1.0.0` keeps running on `1.1.0` unchanged.

Patch releases fix behavior without touching the surface. `1.1.0` also corrected two partial-batch-failure bugs in the same release; a fix that arrives on its own ships as a patch.

!!! note
    `1.1.1` and `1.1.2` were docs-and-packaging releases with no code change. A version bump does not always mean new runtime behavior.

## Supported Python

fastsqs requires **Python >= 3.10**. The package is tested and classified for 3.10, 3.11, 3.12, and 3.13.

```toml
# pyproject.toml
[project]
requires-python = ">=3.10"
```

Dropping a Python version that the current major supports is a breaking change and waits for the next major release.

## The fast-depends compatibility contract

fastsqs builds dependency injection on [fast-depends](https://github.com/Lancetnik/FastDepends) and pins it to a single major:

```toml
# pyproject.toml
[project]
dependencies = [
    "pydantic>=2.0.0",
    "fast-depends>=3.0.0,<4.0.0",
]
```

The `<4.0.0` cap is deliberate. fast-depends powers `Depends(...)` injection (see [Dependency injection](../guide/dependency-injection.md)), and its `4.0` line could change behavior fastsqs relies on. Pinning to `>=3,<4` means a fastsqs install pulls a fast-depends release fastsqs has been built against. Relaxing that cap to admit a new fast-depends major is itself a public-API decision and ships in a fastsqs release of its own.

`pydantic>=2.0.0` is the other hard floor: routing and validation run on Pydantic 2.

## How to pin

Pin the major you tested against and allow minor and patch updates within it.

```toml
# pyproject.toml
[project]
dependencies = [
    "fastsqs>=1.1,<2",
]
```

This accepts additive features and fixes while holding the major fixed, so an upgrade never breaks your handlers without you choosing a new major. Read the [Changelog](changelog.md) before moving across a major boundary.
