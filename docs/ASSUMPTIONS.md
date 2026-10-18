# V2 design assumptions, defaults, and trade-offs

This document separates **assignment requirements** from **implementation-specific choices**. Values listed here describe the current V2 code and Docker Compose deployment; they are not universal Redis/Asynq settings or guaranteed production-service limits.

## 1. Priority and dispatch

| Contract or setting | V2 decision | Source and reasoning |
|---|---|---|
| Client-facing numeric priority | Integer **0–9**, default **5** | `internal/spec/job.go`; simple client API rather than ten distinct queues |
| `low` queue | **0–3**, worker weight **1** | `spec.QueueForPriority`, `cmd/worker/main.go` |
| `default` queue | **4–6**, weight **3** | Three service classes make queue management demonstrable |
| `critical` queue | **7–9**, weight **6** | Higher **probability** of selection, not strict precedence |
| Scheduling fairness | **Weighted**, with non-preemptive handlers | Numeric priorities in the *same* class have no guaranteed order; a low-priority running task is never preempted by a later high-priority one |

**Assumption:** The brief did not prescribe these buckets or the 1:3:6 weights. They are demonstration policies, not a guarantee that critical tasks execute first, receive an exact 60% of throughput, or meet a latency SLA. Adjusting the policy requires updating both the API queue mapping and worker queue weights.

## 2. Job validation, retries, and timeouts

| Setting | Default / bounds | Implications |
|---|---|---|
| `max_retries` | Default **3**, allowed **0–10** | `N` ordinary retries means **up to N+1 attempts**, not exactly-once processing |
| Per-attempt `timeout_seconds` | Default **30 seconds**, allowed **1–300 seconds** | Context-based cooperative cancellation; an external non-idempotent side effect could survive a timeout |
| Exponential retry delay | Minimum **1s, 2s, 4s, 8s…**, capped at **60s**, plus up to **20% positive jitter**, capped at 60s | `internal/backoff/backoff.go`; delays are **scheduled** delays, not precision wall-clock promises |
| Retry/delayed-task polling | **1 second** in production worker | Due time is a not-before scheduling intent; queue and worker contention introduce additional latency |
| Client job payload | JSON object, max **16 KiB** | `internal/spec/job.go`; entire HTTP request capped at **64 KiB**; unknown submission fields rejected |
| `demo.sleep` payload | `duration_ms` **0–300000**, `fail_first` **0–10** | Example handler configuration for tests; not a production job policy |
| Allowed job types | `demo.sleep`, `demo.effect` | Explicit registry in `internal/registry/registry.go`; extend registry for new task types |

**Test 11 (retry timing):** The code's unit tests assert `Delay(1,0)=1s` and `Delay(2,0)=2s`. The end-to-end HTTP test polls asynchronously; timing includes broker scheduling precision, dispatcher checks, request latency, and test-clock precision. It tests *observable non-immediate retries* rather than using wall-clock duration as definitive evidence of exact retry backoff values. The legacy V1 test's `date +%s` integer rounding can show `2s` for an actual interval between 2 and 3 seconds and fail a `>=3s` check. Verify implementation delay logic with unit tests and `job_retry_scheduled` worker logs, not just that integer duration. V2 smoke uses a **2-second** minimum wall-clock observation to reduce timing-dependent false failures, while also checking the job succeeded on its **third attempt**.

## 3. State, durability, and idempotency

- Job states exposed by V2: **`queued`, `running`, `failed`, `succeeded`, `dead_lettered`, `scheduled`**. The sixth state is an intentional V2 extension, not part of the original five-state contract.
- Accepted task completion data is retained in Asynq for **24 hours** where configured; this is **not** a permanent audit trail. Archived/dead-letter records depend on Redis operational retention.
- Optional HTTP `Idempotency-Key` uses a deterministic task ID plus a Redis reservation retained **48 hours**, with a **5-second** lease for an unconfirmed admission. This bounds retry reconciliation; it is not a globally transactional exactly-once guarantee.
- Replaying an accepted delayed request after `run_at` returns the original ID while matching records remain. A *new* past-dated request still receives HTTP 400. Committed jobs no longer retained return HTTP 410.
- The broker is Redis + Asynq with **at-least-once** intended execution; a worker may repeat an external side effect after crashing between performing it and acknowledging the task. The included `demo.effect` uses only Redis-local deduplication.
- Demo Redis uses append-only persistence, `appendfsync everysec`, **256 MiB** `maxmemory` and `noeviction`. An OS crash can lose recent writes; full Redis memory may block *acknowledgement/transition writes*, not just submissions. No hard backlog quota is implemented.

## 4. Delayed and recurring schedules

| Setting | Decision | Limit |
|---|---|---|
| Delayed `run_at` | RFC3339 timestamp with timezone; strictly future; no more than **30 days** ahead on new admission | Does not guarantee exact-second execution |
| Cron default | `*/1 * * * *` (**every minute**, UTC) | Configured through environment, not persistent per-user REST schedules |
| Cron job defaults | Type `demo.sleep`, demo payload 100 ms, priority **5** | These are demo settings, not user-defined task catalog |
| Cron replica count | One by default; **multiple replicas supported** | Deterministic UTC occurrence IDs prevent duplicate *enqueue* while records are retained |
| Cron occurrence deduplication | Redis/Asynq task-ID conflict; completed retention **48 hours** | Not exactly-once execution, and ticks missed while *all* scheduler replicas are down are not backfilled |

See [resolved scheduling issues](RESOLVED.md) and [open P2 debt](TECHNICAL_DEBT.md).

## 5. Scale, telemetry, and network deployment

- Default Docker Compose runs one API and two workers; workers are independently scalable. **Each worker has 5 handler slots**. Four replicas yield **20 slots**, not necessarily 20 CPU cores or sustained parallel handler throughput; enqueue backlog is unbounded by worker concurrency.
- Redis aggregates Prometheus-compatible **queue depth, per-attempt durations, attempt outcomes, dead-letter counters, active worker heartbeats, and occupied handler slots**. Metrics are best effort. Worker utilization is *active/advertised slots*, **not CPU utilization**.
- Histogram p50/p95 refer to **processing-attempt latency**, not request-to-completion time, and require a histogram query engine (e.g. Prometheus). There is **no running Grafana/Prometheus dashboard** included by default.
- Cloud demo API is published on **host TCP 8080**, including external interfaces. **Submission is not authenticated.** Restrict inbound TCP 8080 to trusted source IPs using a DigitalOcean Cloud Firewall. Set a private `ADMIN_TOKEN`: the Compose default `local-dev-token` is for local demos only. Do not transmit that token over plaintext public HTTP.
- Tests **1–23** in `scripts/verify_deployment_25.sh` can be run against a reachable API. **24–25** require explicit `RUN_PRIVILEGED=1` and `SSH_HOST`, with access to the Droplet; skipped tests must not be represented as passed. Tests create demo jobs, including intentional failures.

## 6. Verification and source of truth

For the standalone V2 project, run:

```bash
cd ~/projects/go-async-jobs-v2
git pull origin main
git remote -v
git branch --show-current
grep -n 'expected.*backoffs\|Two backoffs' scripts/verify_deployment_25.sh
BASE_URL=http://146.190.95.143:8080 bash scripts/verify_deployment_25.sh
```

If your output still says `expected approximately >=3s`, it is from the **V1** script, not the current V2 version. GitHub Actions exercises V2 on a disposable Redis/Compose stack; the public DigitalOcean deployment must be checked separately.

Primary implementation sources: [job specification](../internal/spec/job.go), [worker policy](../cmd/worker/main.go), [backoff and tests](../internal/backoff/backoff.go), [Compose settings](../docker-compose.yml), and [full architecture](ARCHITECTURE.md).
