# API Reference

The full public surface of fastsqs, generated from the source. Everything below
is exported from the top-level `fastsqs` package; the testing helpers live in
`fastsqs.testing`.

- [FastSQS](app.md) — the application object: register routes and middleware, handle Lambda events.
- [SQSRouter](router.md) — group routes across modules and nest them with subrouters.
- [SQSEvent](events.md) — the pydantic base model for typed message payloads.
- [Context, State, FifoInfo](context.md) — the typed per-invocation context and its scratch namespace.
- [QueueType](queue-type.md) — the standard / FIFO / auto selector.
- [Depends](dependencies.md) — the dependency marker, re-exported from fast-depends.
- [Middleware](middleware.md) — the middleware base class and the built-in middleware.
- [is_sqs_event](utils.md) — shape detection for multiplexed handlers.
- [Exceptions](exceptions.md) — the `FastSQSError` hierarchy.
- [Testing utilities](testing.md) — `SQSTestClient` and the synthetic-event helpers.
