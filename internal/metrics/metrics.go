// Package metrics collects low-cardinality Redis-backed Prometheus metrics.
// Counters aggregate across workers; no job IDs or payloads are exposed.
package metrics

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const prefix = "go_async_jobs_v2:"

var queues = []string{"critical", "default", "low"}
var bounds = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

type Collector struct {
	Redis     redis.UniversalClient
	Inspector *asynq.Inspector
	Logger    *slog.Logger
}

func (c *Collector) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func durationKey(queue string) string { return prefix + "duration:" + queue }
func outcomeKey(queue string) string  { return prefix + "outcome:" + queue }
func deadKey(queue string) string     { return prefix + "dead:" + queue }

// RecordAttempt is best-effort telemetry. It cannot fail a business task.
func (c *Collector) RecordAttempt(queue string, elapsed time.Duration, success bool) {
	if c == nil || c.Redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	outcome := "failure"
	if success {
		outcome = "success"
	}
	p := c.Redis.Pipeline()
	p.HIncrBy(ctx, outcomeKey(queue), outcome, 1)
	p.HIncrBy(ctx, durationKey(queue), "count", 1)
	p.HIncrBy(ctx, durationKey(queue), "sum_us", elapsed.Microseconds())
	for i, bound := range bounds {
		if elapsed.Seconds() <= bound {
			p.HIncrBy(ctx, durationKey(queue), fmt.Sprintf("bucket_%d", i), 1)
		}
	}
	if _, err := p.Exec(ctx); err != nil {
		c.logger().Warn("metric_write_failed", "error", err)
	}
}

func (c *Collector) Middleware() asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			queue, _ := asynq.GetQueueName(ctx)
			started := time.Now()
			err := next.ProcessTask(ctx, task)
			c.RecordAttempt(queue, time.Since(started), err == nil)
			return err
		})
	}
}

// ErrorHandler counts terminal failures when the handler exhausts its retry
// budget. Asynq performs archival after the handler returns.
func (c *Collector) ErrorHandler() asynq.ErrorHandler {
	return asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
		retried, ok1 := asynq.GetRetryCount(ctx)
		maxRetry, ok2 := asynq.GetMaxRetry(ctx)
		if !ok1 || !ok2 || retried < maxRetry || c == nil || c.Redis == nil {
			return
		}
		queue, _ := asynq.GetQueueName(ctx)
		writeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if incrErr := c.Redis.Incr(writeCtx, deadKey(queue)).Err(); incrErr != nil {
			c.logger().Warn("dead_letter_metric_failed", "error", incrErr)
		}
	})
}

// Heartbeat announces this worker's concurrency capacity. Keys disappear 20s
// after a crash, so utilization is an estimate, not an exact CPU metric.
func (c *Collector) Heartbeat(ctx context.Context, concurrency int) {
	if c == nil || c.Redis == nil {
		return
	}
	host, _ := os.Hostname()
	key := fmt.Sprintf("%sworker:%s:%d", prefix, host, os.Getpid())
	send := func() {
		pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Redis.Set(pingCtx, key, concurrency, 20*time.Second).Err(); err != nil {
			c.logger().Warn("worker_heartbeat_failed", "error", err)
		}
	}
	send()
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = c.Redis.Del(closeCtx, key).Err()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				send()
			}
		}
	}()
}

func parseInt(m map[string]string, field string) int64 {
	val, _ := strconv.ParseInt(m[field], 10, 64)
	return val
}

// ServeHTTP serves Prometheus text exposition; authentication belongs to API.
func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c == nil || c.Redis == nil || c.Inspector == nil {
		http.Error(w, "metrics collector unavailable", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	var out bytes.Buffer
	fmt.Fprintln(&out, "# HELP go_jobs_queue_waiting Pending, scheduled, and retrying tasks.")
	fmt.Fprintln(&out, "# TYPE go_jobs_queue_waiting gauge")
	fmt.Fprintln(&out, "# HELP go_jobs_queue_active Running tasks.")
	fmt.Fprintln(&out, "# TYPE go_jobs_queue_active gauge")
	fmt.Fprintln(&out, "# HELP go_jobs_attempts_total Completed task attempts by outcome.")
	fmt.Fprintln(&out, "# TYPE go_jobs_attempts_total counter")
	fmt.Fprintln(&out, "# HELP go_jobs_dead_lettered_total Retry-exhausted tasks (best effort).")
	fmt.Fprintln(&out, "# TYPE go_jobs_dead_lettered_total counter")
	fmt.Fprintln(&out, "# HELP go_jobs_attempt_duration_seconds Duration of individual processing attempts. Use histogram_quantile(0.95, sum by (le) (rate(go_jobs_attempt_duration_seconds_bucket[5m]))) for p95.")
	fmt.Fprintln(&out, "# TYPE go_jobs_attempt_duration_seconds histogram")

	existing, err := c.Inspector.Queues()
	if err != nil {
		http.Error(w, "queue inspection failed", http.StatusServiceUnavailable)
		return
	}
	present := make(map[string]bool, len(existing))
	for _, name := range existing {
		present[name] = true
	}
	activeTotal := 0
	for _, queue := range queues {
		var waiting, active int
		if present[queue] {
			info, inspectErr := c.Inspector.GetQueueInfo(queue)
			if inspectErr != nil && inspectErr != asynq.ErrQueueNotFound {
				http.Error(w, "queue inspection failed", 503)
				return
			}
			if inspectErr == nil {
				waiting = info.Pending + info.Scheduled + info.Retry
				active = info.Active
			}
		}
		activeTotal += active
		fmt.Fprintf(&out, "go_jobs_queue_waiting{queue=%q} %d\n", queue, waiting)
		fmt.Fprintf(&out, "go_jobs_queue_active{queue=%q} %d\n", queue, active)
		counts, err := c.Redis.HGetAll(ctx, durationKey(queue)).Result()
		if err != nil {
			http.Error(w, "metric read failed", 503)
			return
		}
		outcomes, err := c.Redis.HGetAll(ctx, outcomeKey(queue)).Result()
		if err != nil {
			http.Error(w, "metric read failed", 503)
			return
		}
		dead, err := c.Redis.Get(ctx, deadKey(queue)).Int64()
		if err == redis.Nil {
			dead = 0
		} else if err != nil {
			http.Error(w, "metric read failed", 503)
			return
		}
		for _, outcome := range []string{"success", "failure"} {
			fmt.Fprintf(&out, "go_jobs_attempts_total{queue=%q,outcome=%q} %d\n", queue, outcome, parseInt(outcomes, outcome))
		}
		fmt.Fprintf(&out, "go_jobs_dead_lettered_total{queue=%q} %d\n", queue, dead)
		for i, bound := range bounds {
			fmt.Fprintf(&out, "go_jobs_attempt_duration_seconds_bucket{queue=%q,le=%q} %d\n", queue, strconv.FormatFloat(bound, 'f', -1, 64), parseInt(counts, fmt.Sprintf("bucket_%d", i)))
		}
		count := parseInt(counts, "count")
		fmt.Fprintf(&out, "go_jobs_attempt_duration_seconds_bucket{queue=%q,le=\"+Inf\"} %d\n", queue, count)
		fmt.Fprintf(&out, "go_jobs_attempt_duration_seconds_count{queue=%q} %d\n", queue, count)
		fmt.Fprintf(&out, "go_jobs_attempt_duration_seconds_sum{queue=%q} %.6f\n", queue, float64(parseInt(counts, "sum_us"))/1e6)
	}
	var cursor uint64
	workerCount, capacity := 0, 0
	for {
		keys, next, err := c.Redis.Scan(ctx, cursor, prefix+"worker:*", 100).Result()
		if err != nil {
			http.Error(w, "worker heartbeat read failed", 503)
			return
		}
		if len(keys) > 0 {
			values, err := c.Redis.MGet(ctx, keys...).Result()
			if err != nil {
				http.Error(w, "worker heartbeat read failed", 503)
				return
			}
			for _, value := range values {
				if value == nil {
					continue
				}
				workerCount++
				count, _ := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
				if count > 0 {
					capacity += count
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	fmt.Fprintln(&out, "# HELP go_jobs_workers_online Workers with live Redis heartbeats.")
	fmt.Fprintln(&out, "# TYPE go_jobs_workers_online gauge")
	fmt.Fprintf(&out, "go_jobs_workers_online %d\n", workerCount)
	fmt.Fprintln(&out, "# HELP go_jobs_worker_capacity Total configured concurrency slots of live workers.")
	fmt.Fprintln(&out, "# TYPE go_jobs_worker_capacity gauge")
	fmt.Fprintf(&out, "go_jobs_worker_capacity %d\n", capacity)
	fmt.Fprintln(&out, "# HELP go_jobs_worker_utilization Ratio of currently active tasks to configured concurrency slots.")
	fmt.Fprintln(&out, "# TYPE go_jobs_worker_utilization gauge")
	utilization := 0.0
	if capacity > 0 {
		utilization = float64(activeTotal) / float64(capacity)
	}
	fmt.Fprintf(&out, "go_jobs_worker_utilization %.6f\n", utilization)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out.Bytes())
}
