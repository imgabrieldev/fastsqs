# Changelog

## 1.0.0

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
