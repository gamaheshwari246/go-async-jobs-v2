# Implementation plan

1. **Submission and validation.** Explicit priority 0–9, max_retries 0–10, timeout 1–300s, payload limit 16 KiB. HTTP 202 after successful Redis enqueue.
2. **Execution.** Independently scalable Go worker processes. Register handlers via ServeMux. Bound execution with WORKER_CONCURRENCY.
3. **Lifecycle and failure.** Exponential backoff (1, 2, 4 ... 60 seconds with bounded jitter), per-attempt timeout, archived tasks after exhaustion, persisted result.
4. **Operations.** GET /queue/depth; authenticated admin drain and resume; active work finishes normally.
5. **Tests and CI.** Unit retry and validation tests; real-Redis success, transient failures, timeout, panic, exhausted retries, drain and priority tests. CI on push with Redis service.
6. **Hardening after required scope.** Process-kill recovery test, submission dedupe, atomic drain admission, more durable Redis configuration, strict backlog quotas, limits on archived jobs, metrics and alerts.

## Acceptance criteria
Build both binaries; API returns 202 without executing synchronously; worker handles jobs with bounded concurrency; status shows complete state; exhausted jobs are archived; tests run in GitHub Actions. Explain remaining failure windows truthfully.
