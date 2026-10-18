package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"example.com/go-async-jobs/internal/spec"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// EffectHandler demonstrates one idempotent Redis-local business effect.
// It does NOT make arbitrary external effects exactly once.
func EffectHandler(client redis.UniversalClient) asynq.HandlerFunc {
	return func(ctx context.Context, task *asynq.Task) error {
		var job spec.Job
		if err := json.Unmarshal(task.Payload(), &job); err != nil {
			return fmt.Errorf("decode job: %w", err)
		}
		jobID, ok := asynq.GetTaskID(ctx)
		if !ok || jobID == "" {
			return errors.New("missing job ID")
		}

		key := "go-async-jobs:demo-effects:" + jobID
		first, err := client.SetNX(ctx, key, string(job.Payload), 7*24*time.Hour).Result()
		if err != nil {
			return fmt.Errorf("record Redis-local effect: %w", err)
		}
		retried, _ := asynq.GetRetryCount(ctx)
		if first {
			slog.InfoContext(ctx, "effect_committed", "job_id", jobID)
		} else {
			slog.InfoContext(ctx, "duplicate_effect_suppressed", "job_id", jobID,
				"retry_number", retried)
		}

		// Deliberately exercise the effect/ACK failure window on the first attempt.
		if first && retried == 0 {
			return errors.New("simulated failure after committing Redis-local effect")
		}
		result, err := json.Marshal(map[string]any{
			"ok": true, "job_id": jobID, "effect_performed_on_this_attempt": first,
		})
		if err != nil {
			return err
		}
		_, err = task.ResultWriter().Write(result)
		return err
	}
}
