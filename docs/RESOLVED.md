# Resolved V2 scheduling issues

Verified in [GitHub Actions (all checks passed)](https://github.com/gamaheshwari246/go-async-jobs-v2/actions/runs/39118538047), October 18, 2026. P1 entries are no longer active technical debt.

## TD-001 — Delayed job idempotent replay after `run_at` (resolved)

The original bug: a valid delayed request accepted under `Idempotency-Key` would return HTTP 400 when replayed after `run_at`, because time-dependent validation ran before idempotency lookup.

**Implemented:** `spec.ValidateForReplay` parses and normalizes the immutable request without authorizing a new enqueue. The HTTP API uses a **read-only atomic Redis inspection** only for requests whose previous `run_at` has passed. A matching retained Asynq task yields the original ID (HTTP 200); differing key/configuration yields HTTP 409; a committed missing task yields HTTP 410; unknown past-dated requests remain HTTP 400. A stale reservation cannot create a new past-dated job.

**Tests:** `internal/integration/delayed_replay_test.go` verifies replay before/after deadline, equivalent timezone offsets, differing payload and run time conflicts, new past-dated submissions, and committed-task retention expiry. Unit tests cover static validation behavior.

## TD-002 — Cron duplicate ticks with multiple schedulers (resolved for retained occurrences)

The original limitation: two scheduler instances generated independent task IDs and enqueued duplicate logical cron occurrences.

**Implemented:** Replaced instance-local Asynq Scheduler registration with a UTC-aligned cron clock and deterministic `cron-<schedule hash>-<tick Unix nanos>` task ID. The broker atomically accepts the first enqueue per occurrence and returns `asynq.ErrTaskIDConflict` for other replicas; both log their behavior. Retries of a temporarily failed enqueue keep the same tick. The task is retained for 48 hours, bounding Asynq task-ID deduplication while its record exists; without Redis durability, exactly-once execution is not guaranteed.

**Tests:** `cmd/scheduler/scheduler_integration_test.go` exercises a 12-way concurrent enqueue, scheduler client restart and independent subsequent ticks. Unit tests verify timezone-independent tick and task-ID calculation. Docker Compose CI launches **two scheduler replicas**, observes `cron_enqueued` and `cron_tick_deduplicated`, and completes the existing API regression suite.

**Scope clarification:** Retained-occurrence deduplication does not guarantee exactly-once business effects. Missed ticks during a complete outage of all scheduler replicas are not backfilled; that optional stronger availability feature remains TD-005 (P2).

## DOC-004 — `scheduled` is an intentional sixth status (documented)

The original API listed `queued`, `running`, `failed`, `succeeded`, and `dead_lettered`. V2 deliberately adds `scheduled` for delayed jobs. README describes the extension and compatibility implications.

