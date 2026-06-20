# Changelog

## 1.1.2 - 2026-06-20

Docs & packaging only — no code change.

### Packaging
- Migrated project metadata to **PEP 621**: a full `[project]` table in
  `pyproject.toml`, and `setup.py` removed (`MANIFEST.in` no longer ships it).
- Trimmed `build-system.requires` to `["setuptools>=77"]` — dropped the runtime
  `pydantic` dependency (never used at build time) and the redundant `wheel`
  (already provided by `setuptools.build_meta`).
- License now declared as an SPDX expression (`license = "MIT"` +
  `license-files = ["LICENSE"]`, PEP 639), replacing the legacy
  `License :: OSI Approved :: MIT License` classifier. Renders the same on PyPI.

### Docs
- Removed decorative emojis from the README, the `ordering_with_standard_queues`
  example README, and its handler docstrings for a more conventional OSS tone.

## 1.1.1 - 2026-06-19

Docs & packaging only — no code change from 1.1.0.

### Docs
- Documented the v1.1.0 surface in the README: EventBridge Pipes / bare-list
  handler + `is_sqs_event`, routers / subrouters / key-value routing + default
  handler, a "why fastsqs" section, runnable examples, contributing, and accuracy
  fixes (e.g. the `fast-depends>=3,<4` pin and the PascalCase FIFO-attribute note).

### Packaging
- Added `project_urls` (Documentation, Changelog, Repository, Issues) so they
  render in the PyPI sidebar.

## 1.1.0 - 2026-06-19

### Added
- `is_sqs_event(event)`: returns `True` when `event` is an SQS batch — a bare
  list of records (the shape an EventBridge Pipes target receives) or a dict with
  a `Records` key (the Lambda SQS event source mapping). Lets a multiplexed Lambda
  route SQS vs non-SQS events (e.g. API Gateway) by shape.
- `FastSQS.handler` / `async_handler` now accept a bare list of records (the
  EventBridge Pipes target shape) in addition to `{"Records": [...]}`.

### Fixed
- A bare-list event containing a **non-dict element** (e.g. a malformed enrichment
  array item: a JSON string/number/null) no longer crashes the whole batch with an
  uncaught `AttributeError`. The offending element is reported as its own
  batch-item failure and its siblings are processed normally.
- `batchItemFailures` entries are never emitted with an **empty-string or `null`
  `itemIdentifier`** when a record carries a present-but-empty/`None` `messageId`.
  SQS/EventBridge read an empty or null identifier as a *whole-batch* failure; all
  failure paths now coalesce the identifier to the `"UNKNOWN"` sentinel
  consistently (matching the per-record path).

## 1.0.0 - 2026-06-19

First stable release. fastsqs is now a focused, typed **SQS-on-Lambda router**:
pydantic routing + dependency injection + a middleware system + native partial
batch failure (standard + FIFO). SemVer applies from here. This release breaks
the 0.x surface deliberately to land a clean v1.

### Added
- **Dependency injection** (via `fast-depends`): declare `Depends(...)` params on
  a handler and `@app.route(...)` wires them — no `@inject`. Sub-dependencies and
  type-checked injection included.
- **Typed `Context`**: framework fields are typed attributes (`ctx.message_id`,
  `ctx.queue_type`, `ctx.record`, `ctx.lambda_context`, `ctx.route_path`,
  `ctx.message_type`, `ctx.fifo_info`, `ctx.handler_result`); arbitrary
  middleware/handler scratch lives in a separate `ctx.state` namespace
  (`ctx.state.foo` / `ctx.state["foo"]`). New `State` and `FifoInfo` types.
- **`QueueType.AUTO`** (the new default): infers FIFO vs standard from each
  batch's `eventSourceARN` (a `.fifo` suffix means FIFO).
- **`fifo_failure_mode`** (`"isolate_groups"` | `"halt_batch"`): FIFO failure
  strategy, replacing the `skip_group_on_error` boolean.
- **`FastSQSError`** base exception; `BatchFailedError.failures` exposes the
  failed item ids; exception chaining (`raise … from e`) preserves causes.
- **`SQSTestClient`** widened: `RecordSpec` for per-record FIFO groups in a
  batch, raw `str`/`bytes` bodies (to reach the malformed-body path), and public
  `make_record` / `make_event` builders in `fastsqs.testing`.
- **`py.typed`** marker (PEP 561) — annotations now reach downstream type checkers.

### Changed (breaking)
- **Python >= 3.10** (was 3.8).
- **`Context` is a typed object, not a `dict` subclass.** `ctx["messageId"]` and
  the other string-key reads/writes are gone; use the typed attributes and
  `ctx.state` for scratch. ctx keys are snake_case throughout.
- **`queue_type` defaults to `AUTO`** (was `STANDARD`): a FIFO queue is no longer
  silently processed on the concurrent standard path (which broke ordering).
- **Single `discriminator` param** replaces `key` + `message_type_key` on both
  `FastSQS` and `SQSRouter` (default still `"type"`).
- **`flexible_matching` defaults to `False`** (was `True`); the UPPER/lower
  message-type variants are dropped, and a variant collision now raises
  `ValueError` instead of warning.
- **Field normalization** no longer uses a bespoke fuzzy normalizer; camelCase is
  accepted via Pydantic alias generation (`populate_by_name`). kebab-case keys are
  no longer auto-mapped.
- **Config renames**: `enable_partial_batch_failure` → `partial_batch_failure`;
  `skip_group_on_error: bool` → `fifo_failure_mode: Literal[...]`.
- **Exceptions renamed and reparented** under `FastSQSError`:
  `RouteNotFound` → `RouteNotFoundError`, `InvalidMessage` → `InvalidMessageError`.
- `partial_batch_failure=False` RAISES `BatchFailedError` on any failure (whole
  batch redelivered) instead of silently reporting no failures (data loss).
- `LoggingMiddleware` (verbose) logs `ctx.state` keys only — no longer leaks
  framework/scratch internals into CloudWatch.
- `TimingMsMiddleware` → `TimingMiddleware`.
- The `FastSQS` constructor is keyword-only. It also accepts `debug` (default
  `False`) for verbose per-record debug logging via `LoggingMiddleware`.
- Registering the SAME discriminator value as BOTH a pydantic route and a
  key-value route now raises `ValueError` at decoration time (previously the
  key-value handler was silently shadowed and unreachable).

### Removed (breaking)
- `FastSQS` constructor params `title` / `description` / `version` (FastAPI
  cargo-cult; never read).
- `FastSQS.set_queue_type()` (warm-app mutation footgun — set `queue_type` at
  construction) and the `FastSQS.use()` alias (use `add_middleware`).
- `SQSRouter.wildcard()` (use `default()`) and the unused `SQSRouter.name` param.
- `SQSRouter`'s `payload_scope` param (it was inert — all modes delivered the
  same payload; subrouters do not narrow it).
- Middleware presets: `use_preset`, `MiddlewarePreset`, the `presets` module.
- `SQSEvent.from_sqs_record` (unused, bypassed validation).
- The base `Middleware._app` / `_log` coupling (middleware no longer reaches back
  into the app; `TimingMiddleware` uses stdlib `logging`).
- Removed from the public API: `RouteEntry`, `RouteValue`, `Handler`,
  `run_middleware_stack` (now `_run_middleware_stack`), and the `ProcessingContext`
  TypedDict.
- The string-route annotation-sniffing validation branch (use explicit `model=`).
- (already gone in 0.5) in-process retry/`RetryConfig`, the dead-letter /
  circuit-breaker / parallelization / visibility / queue-metrics / masking
  middleware, and the legacy `run_middlewares` runner.

### Packaging
- Real MIT `LICENSE` (was a placeholder stub).
- `Development Status :: 5 - Production/Stable`.
- `fast-depends` capped to `>=3,<4`.
- `MANIFEST.in` ships `LICENSE`, `CHANGELOG.md`, and `fastsqs/py.typed`.

### Kept (core)
Routing (pydantic + key-value), pydantic validation via `SQSEvent`, the middleware
`before`/`after` hook system with balanced unwind, partial batch failure (standard
+ FIFO), `TimingMiddleware`, `LoggingMiddleware`, subrouters, and the
`SQSTestClient`.
