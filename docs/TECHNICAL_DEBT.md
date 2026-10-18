# V2 technical debt and compatibility notes

This is the **tracked engineering backlog**, not a claim that these problems are already fixed. V2's existing scheduling and metrics features passed CI; the gaps below require follow-up work and dedicated regression tests. Priority is relative to hardening this demo into a multi-instance service.

## TD-001 — Replaying a delayed job after `run_at` (P1: correctness)

**Confirmed in:** `internal/spec/job.go` (`Validate`) and `internal/httpapi/server.go` (`submit`).

**Behavior:** `Validate` rejects a `run_at` timestamp unless it is strictly in the future. `submit` calls `Validate` **before** consulting the Redis `Idempotency-Key` reservation or looking up an existing task. Consequently, replaying an otherwise identical, previously accepted delayed request **after its `run_at` time** returns HTTP **400** rather than HTTP **200** with the original job ID. This breaks the replay contract for valid previously accepted requests, even when their idempotency record and task are retained.

**Reproduction:** Send `POST /jobs` with a future `run_at` and `Idempotency-Key: example-123`. Wait until `run_at` has passed. Send the *identical* request again with the same header: expect HTTP 400 today.

**Proposed fix:** Keep parsing and request-fingerprint verification independent of time-sensitive new-submission eligibility. For requests with an idempotency key, check for a matching accepted reservation/task first and return the original job ID; apply the future-time constraint only to genuinely new enqueues. Preserve the existing 409 on conflicting content, 410 on expired committed tasks, and lease-recovery behavior. Account for equivalent RFC3339 timestamps with different offsets.

**Acceptance criteria:** An identical, retained delayed submission is deduplicated after `run_at`; a new request with a past `run_at` still fails; key reuse with different `run_at`, payload, priority, or retry settings returns 409; tests cover before/during/after `run_at`, concurrency, and expired retention.

## TD-002 — Cron scheduler is single-instance only (P1: availability / duplicate jobs)

**Confirmed in:** `cmd/scheduler/main.go` and `docker-compose.yml`.

**Behavior:** Each scheduler instance independently registers the same cron expression and enqueues tasks using generated IDs. Running two replicas can enqueue the same logical tick twice. The current **one scheduler replica** constraint is deliberate and documented; there is no leader election, shared tick ledger, or per-occurrence deterministic task ID.

**Proposed fix:** Define a stable logical schedule identifier and UTC tick time, then ensure a unique **(schedule ID, tick)** admission key via a broker-supported deterministic task ID and/or Redis atomic claim. Combine this with lease/leader failover or another explicit coordination strategy to avoid duplicate *ticks* and to handle crashes between claiming a tick and enqueueing. Asynq task-ID deduplication alone is bounded by task retention and needs analysis of the claim/enqueue failure window.

**Acceptance criteria:** Two scheduler replicas simultaneously running do not produce duplicate logical occurrences; a restart/failover does not cause an unwanted duplicate or silently strand a claimed tick; tests cover synchronized replicas, retrying a tick, process termination, UTC/timestamp boundaries, and retention expiry. Until implemented: **keep the scheduler at exactly one replica**.

## TD-003 — Expand V2-specific regression depth (P2: test coverage)

**Confirmed in:** `internal/integration/v2_features_test.go`, `cmd/scheduler/main_test.go`, and `internal/metrics/metrics_test.go`.

**Current coverage:** The V2-specific integration test `TestV2SchedulingAndMetrics` verifies scheduling and authenticated metric exposure with Redis. Scheduler configuration/cron validation and basic metric helpers have unit tests. Existing V1 integration tests and the 23 API smoke checks also run in CI; **it is incorrect to say the entire project has only one integration test**.

**Missing focused scenarios:**
- An idempotent delayed-job replay both before and after the scheduled timestamp (TD-001).
- Competing cron replicas and scheduler crash/restart recovery (TD-002).
- Metrics from multiple workers, histogram bucket/count/sum correctness, dead-letter increments through a real exhausted-retry path, worker-heartbeat expiry, and Redis telemetry failure not breaking the job handler.
- Scheduled jobs never running early; tolerance of dispatcher latency under load; accepted timestamps with timezone offsets and boundary values.

**Acceptance criteria:** Add deterministic Redis integration tests for these scenarios without requiring the live DigitalOcean Droplet. Keep `go test -race -tags=integration ./...` and Compose CI passing. Fault injection should target disposable Redis databases only.

## DOC-004 — `scheduled` is an intentional sixth public status (documentation / compatibility, not a bug)

**Confirmed in:** `internal/httpapi/server.go` (`stateName`) and `internal/httpapi/server_test.go`.

The original five public statuses are `queued`, `running`, `failed`, `succeeded`, and `dead_lettered`. V2 adds **`scheduled`**, representing a job accepted but waiting for its `run_at` time. It is exposed through `POST /jobs` and `GET /jobs/{id}`. Clients matching exhaustively on job status must accept the extra value. This is a useful scheduling extension, not a malfunction.

**Documentation action:** State the extended status contract in the README and V2 guide. If strict backwards compatibility with clients expecting only the original five values is required, introduce API versioning or a compatibility mapping instead of silently changing statuses.

---

**Scope:** These items are documented, not resolved. No V1 source, deployment, or running containers are modified by this backlog update.
