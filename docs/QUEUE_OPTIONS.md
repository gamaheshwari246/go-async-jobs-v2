# Queue technology decision

| Option | Strengths | Costs |
|---|---|---|
| PostgreSQL-only | Transactional state/result, indexable priority, SKIP LOCKED and status queries | Need custom claim loop, retry scheduler, leases, dead-letter and polling/backpressure |
| RabbitMQ + PostgreSQL | Push-based competing consumers, acknowledgements, prefetch, robust queue semantics | Dual-write consistency problem; transactional outbox, separate result DB and retry scheduler |
| Redis Streams + custom Go workers | Consumer groups, XACK, pending reclaim, fast | Must implement retry scheduling, priority policy, lifecycle and result store |
| Kafka + external state DB | Very high throughput, replay, partition-local ordering | Per-job cancellation/priority/delayed retry complexity and external result persistence |
| Redis + Asynq | Idiomatic Go task queue; retry, backoff, priority queues, recovery and status APIs out of the box | Redis persistence and memory operations require careful configuration; finite retention |

## Decision
For a time-constrained interview assignment, use Redis + Asynq. No separate PostgreSQL queue/store or Kafka broker. Its ready-made task state machine lets the implementation focus on HTTP contract, handlers, retry tests and operational behavior. This is not a universal performance claim.

## Priority fairness
The scheduler uses weighted queues. It **does not** enforce strict per-number priority 0–9. Worker execution is non-preemptive; no queue can guarantee high-priority tasks always finish first if a lower-priority task has already started.

## Bounded buffering
WORKER_CONCURRENCY caps concurrent handler executions; it does **not** cap the Redis backlog. An enforceable backlog limit must atomically reserve capacity as part of admission; checking queue depth and enqueueing separately races. Add a limit if throughput/load goals demand it. Rate limits, payload caps, bounded retries and admission rejection are complementary.

## Future-production recommendation (conditional)

Redis + Asynq remains appropriate for the submitted implementation. If durable job history, higher operational scale, or multiple consumer services are required, evaluate **RabbitMQ + PostgreSQL** with transactional outbox publication and idempotent consumer state. For AWS-managed deployments, SQS + DLQ and a durable state store can reduce broker operations. For atomic job creation and business database writes, a Postgres-backed `FOR UPDATE SKIP LOCKED` queue can avoid a second broker at the cost of custom retry/lease handling. For event logs and analytics fan-out, evaluate Kafka; Temporal is a better fit for durable, multi-step workflows. None of these automatically provides exactly-once external effects. See [FUTURE_PRODUCTION.md](FUTURE_PRODUCTION.md).
