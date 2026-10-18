package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"example.com/go-async-jobs/internal/registry"
	"example.com/go-async-jobs/internal/spec"

	"github.com/hibiken/asynq"
)

// scheduleConfig is immutable while the scheduler runs. Exactly one scheduler
// replica is supported, otherwise each replica enqueues duplicate ticks.
type scheduleConfig struct {
	Cron string
	Job  spec.Validated
}

func loadSchedule(getenv func(string) string) (scheduleConfig, error) {
	rawCron := getenv("CRON_SCHEDULE")
	if rawCron == "" {
		rawCron = "*/1 * * * *"
	}
	taskType := getenv("CRON_TASK_TYPE")
	if taskType == "" {
		taskType = "demo.sleep"
	}
	rawPayload := getenv("CRON_PAYLOAD")
	if rawPayload == "" {
		rawPayload = "{}"
	}
	priority := 5
	if raw := getenv("CRON_PRIORITY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return scheduleConfig{}, fmt.Errorf("CRON_PRIORITY: %w", err)
		}
		priority = n
	}
	if !json.Valid([]byte(rawPayload)) {
		return scheduleConfig{}, fmt.Errorf("invalid CRON_PAYLOAD JSON")
	}
	req := spec.SubmitRequest{Type: taskType, Payload: json.RawMessage(rawPayload), Priority: &priority}
	job, err := spec.Validate(req)
	if err != nil {
		return scheduleConfig{}, err
	}
	if err := registry.Validate(registry.Definitions(nil), taskType, req.Payload); err != nil {
		return scheduleConfig{}, err
	}
	return scheduleConfig{Cron: rawCron, Job: job}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	config, err := loadSchedule(os.Getenv)
	if err != nil {
		logger.Error("invalid_schedule_config", "error", err)
		os.Exit(2)
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	scheduler := asynq.NewScheduler(asynq.RedisClientOpt{Addr: addr}, &asynq.SchedulerOpts{
		Location: time.UTC,
		PostEnqueueFunc: func(info *asynq.TaskInfo, err error) {
			if err != nil {
				logger.Error("cron_enqueue_failed", "error", err)
				return
			}
			logger.Info("cron_enqueued", "job_id", info.ID, "queue", info.Queue)
		},
	})
	defer scheduler.Shutdown()
	entryID, err := scheduler.Register(config.Cron, asynq.NewTask(config.Job.Type, config.Job.Body),
		asynq.Queue(config.Job.Queue), asynq.MaxRetry(config.Job.Retries),
		asynq.Timeout(config.Job.Timeout), asynq.Retention(24*time.Hour))
	if err != nil {
		logger.Error("invalid_cron_expression", "cron", config.Cron, "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := scheduler.Start(); err != nil {
		logger.Error("scheduler_start_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("cron_scheduler_started", "entry_id", entryID, "cron", config.Cron, "queue", config.Job.Queue, "timezone", "UTC")
	<-ctx.Done()
	logger.Info("cron_scheduler_stopping")
}
