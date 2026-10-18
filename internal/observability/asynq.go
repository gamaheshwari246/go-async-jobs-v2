// Package observability provides consistent, payload-safe structured logging.
package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
)

// TaskMiddleware logs lifecycle events without logging job payloads or secrets.
// It does not change retry/ack behavior: the handler's error is returned intact.
func TaskMiddleware(logger *slog.Logger) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			jobID, _ := asynq.GetTaskID(ctx)
			queue, _ := asynq.GetQueueName(ctx)
			retried, _ := asynq.GetRetryCount(ctx)
			attempt := retried + 1
			started := time.Now()

			logger.InfoContext(ctx, "job_started",
				"job_id", jobID, "queue", queue, "task_type", task.Type(), "attempt", attempt)
			err := next.ProcessTask(ctx, task)
			if err != nil {
				logger.ErrorContext(ctx, "job_failed",
					"job_id", jobID, "queue", queue, "task_type", task.Type(),
					"attempt", attempt, "elapsed_ms", time.Since(started).Milliseconds(),
					"error", err)
				return err
			}
			logger.InfoContext(ctx, "job_succeeded",
				"job_id", jobID, "queue", queue, "task_type", task.Type(),
				"attempt", attempt, "elapsed_ms", time.Since(started).Milliseconds())
			return nil
		})
	}
}
