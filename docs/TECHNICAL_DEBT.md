# Active V2 technical debt

The two previously classified P1 scheduling issues are **resolved** with Redis-backed regression tests and passing CI. See [resolved engineering issues](RESOLVED.md). This list contains only remaining follow-up work.

## TD-003 — Expand metrics and resilience test depth (P2)

**Current coverage:** V1 job/retry/crash tests; `TestV2SchedulingAndMetrics`; `TestDelayedIdempotentReplayAfterRunAt` (accepted request replay before/after deadline, offset equivalence, changed configuration, new past-dated request, and expired task); `TestConcurrentSchedulerTickDeduplication` (12 concurrent enqueue attempts, restart, next tick); independent UTC cron calculation tests; two scheduler replicas and 23 API smoke checks in CI. No claim of exhaustive coverage.

**Remaining work:**
- Multi-worker metric aggregation under realistic load; histogram bucket/count/sum correctness; dead-letter counters under repeated and retried failures.
- Redis metric-write failures must remain nonfatal; worker-heartbeat expiry; workers replaced during processing.
- Scheduled tasks should not execute early even with clock skew; integration coverage for boundary cases and worker starvation.
- Stress test contention among HTTP idempotency keys, cron occurrences and worker retries.

**Acceptance criteria:** Deterministic real-Redis tests in disposable DBs, with race detection, no public Droplet dependencies, and explicit assertions for counters and failure modes.

## TD-005 — Optional cron missed-tick backfill after complete outage (P2)

Scheduler replicas agree on UTC ticks and Asynq atomically deduplicates them using deterministic per-occurrence task IDs. A replica retrying a failed Redis enqueue retries **the same tick**, so a transient broker outage while a scheduler remains alive does not silently drop that occurrence. However, when **all** scheduler instances are offline across a scheduled time, the implementation **does not backfill** missed ticks after restart. This is explicit at-most-once scheduling intent during total scheduler downtime, not a multi-replica duplicate issue.

If guaranteed processing of all scheduled occurrences is required: add durable schedule metadata, a bounded catch-up window and reconciliation, with clear retention and backlog behavior. Avoid unsafe unlimited backfill after extended outages.

## Compatibility note — `scheduled` status

V2 deliberately adds `scheduled` as the sixth public job state, documented in the README. It is an intentional extension, not an open defect. Clients that exhaustively match the five original states must recognize `scheduled`.

---

No runtime changes are implied by this active-debt document.
