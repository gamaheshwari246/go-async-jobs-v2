package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"example.com/go-async-jobs/internal/spec"

	"github.com/hibiken/asynq"
)

// DemoSleep is a cooperative, cancellable example of a pluggable job handler.
func DemoSleep(ctx context.Context, task *asynq.Task) error {
	var job spec.Job
	if err := json.Unmarshal(task.Payload(), &job); err != nil {
		return fmt.Errorf("decode job: %w", err)
	}
	var payload struct {
		DurationMS int `json:"duration_ms"`
		FailFirst  int `json:"fail_first"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}

	retried, _ := asynq.GetRetryCount(ctx)
	attempt := retried + 1
	jobID, _ := asynq.GetTaskID(ctx)

	timer := time.NewTimer(time.Duration(payload.DurationMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		slog.WarnContext(ctx, "job_cancelled", "job_id", jobID,
			"attempt", attempt, "error", ctx.Err())
		return ctx.Err()
	case <-timer.C:
	}

	if attempt <= payload.FailFirst {
		return fmt.Errorf("simulated transient failure at attempt %d", attempt)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := json.Marshal(map[string]any{
		"ok": true, "attempt": attempt, "slept_ms": payload.DurationMS, "job_id": jobID,
	})
	if err != nil {
		return err
	}
	if _, err := task.ResultWriter().Write(result); err != nil {
		return fmt.Errorf("persist result: %w", err)
	}
	return nil
}
